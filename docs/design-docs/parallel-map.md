# Parallel Map

## Status

Accepted

## Context

The accepted linear pull runtime keeps pure stages foreground and pull-driven.
`Flow.async` added a demand-driven scheduler boundary, and
`Flow.buffer(count)` added bounded scheduler-backed prefetch. Parallel mapping
is the next concurrency step: multiple independent mapping calls should overlap
without changing FiberStream into a graph runtime or weakening backpressure into
unbounded upstream run-ahead.

Governing documents:

- Product spec: `docs/product-specs/flow-parallel-map.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Async design: `docs/design-docs/async-boundary.md`
- Buffer design: `docs/design-docs/buffer-boundary.md`
- ADR: `docs/design-docs/adr/0008-flow-parallel-map.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Goals

- Run independent mapping blocks in scheduled worker fibers.
- Preserve ordered output.
- Keep downstream stages after the parallel map in the caller's current fiber.
- Bound pulled-but-unemitted upstream elements to `concurrency`.
- Run at most `concurrency` mapping blocks at a time.
- Close upstream and request worker cancellation on early downstream close.
- Keep Async as a compatibility target, not a runtime dependency.

## Non-Goals

- Unordered result emission.
- Ractor-backed workers.
- CPU parallelism guarantees.
- General graph scheduling or fan-out/fan-in.
- Runtime scheduler installation.
- Per-element timeout, retry, batching, or overflow policies.

## Proposed Design

`Flow.parallel_map(concurrency:) { ... }` attaches a `ParallelMapBoundary` pull
stage:

```ruby
downstream_stream = Pull.parallel_map(upstream_stream, concurrency, block)
```

`Source#parallel_map(concurrency:) { ... }` is a convenience method over
`Source#via(Flow.parallel_map(concurrency:) { ... })`. It does not introduce a
separate runtime path.

The boundary owns:

- `upstream`, the pull stream before the boundary
- `concurrency`, the positive worker limit
- `mapper`, the user block
- `permits`, a bounded token queue with `concurrency` tokens
- `jobs`, a `Thread::SizedQueue` with capacity `concurrency`
- `results`, a `Thread::SizedQueue` with capacity `concurrency + 1`
- `dispatcher`, the scheduled fiber that pulls upstream serially
- `workers`, up to `concurrency` scheduled worker fibers
- `next_sequence`, `next_emit_sequence`, and an ordered pending-result table
- `admission_closed`, `failure_sequence`, `started`, `closed`, `done`, and
  failure state flags

The dispatcher and workers are not started during construction or attach. The
first downstream `next` starts them by calling `Fiber.schedule`. If
`Fiber.scheduler` is `nil`, the stage raises `SchedulerRequiredError`.

The dispatcher is the only internal fiber that calls `upstream.next`. Before
each upstream pull it must take one permit. A permit represents one upstream
element that may be queued, actively mapped, or completed but not yet emitted
downstream. Downstream returns one permit only after it emits a mapped value.
This creates the core backpressure bound: at most `concurrency` upstream
elements can be pulled and not yet emitted while admission is open.

For each normal upstream value, the dispatcher assigns an increasing sequence
number and pushes a job:

```ruby
[:job, sequence, value]
```

Workers pop jobs, call the user block, and push tagged results:

```ruby
[:value, sequence, mapped_value]
[:error, sequence, exception]
```

When upstream returns `Pull::DONE`, the dispatcher closes upstream before
publishing a terminal message. If close succeeds, it publishes:

```ruby
[:done, next_sequence]
```

If close fails, it publishes:

```ruby
[:error, next_sequence, close_exception]
```

If `upstream.next` raises, the dispatcher closes upstream and publishes
`[:error, next_sequence, upstream_error]`; a close failure in that path is
suppressed so the upstream pull failure remains primary.

The downstream `next` consumes result messages and stores out-of-order messages
in the pending-result table until the message for `next_emit_sequence` is
available. Value messages emit the mapped value, advance `next_emit_sequence`,
and return one permit only while admission remains open. The terminal done
message marks the boundary done and returns `Pull::DONE` only after all
lower-sequence values have been emitted.

Failures are ordered by sequence, not by wall-clock completion time. When
downstream receives any `:error` result, the boundary records that sequence as
a candidate failure, closes admission immediately, closes upstream, closes the
permit and job queues to wake the dispatcher and idle workers, and stops
returning permits when earlier successful values are later emitted. If multiple
errors are observed before the first failure is delivered, the lowest-sequence
error becomes the primary failure. Downstream still emits successful values with
lower sequence numbers than the primary failure. When the primary failure's
sequence becomes `next_emit_sequence`, `next` marks the boundary done, wakes
internal queues, requests cancellation of remaining higher-sequence work, and
re-raises the primary exception. Values and failures with higher sequence
numbers are suppressed.

Closing admission must not cancel lower-sequence mapping work that is still
needed for ordered delivery. The implementation may not have a portable way to
target only higher-sequence worker fibers, so the first cancellation step is to
stop new upstream pulls and job delivery. Broad worker cancellation is reserved
for the point where the primary failure is delivered, downstream closes early,
or downstream failure makes ordered delivery irrelevant.

The `results` queue has capacity `concurrency + 1`: one result for each
pulled-but-unemitted upstream element plus one terminal message. Terminal done
or terminal close-error messages do not consume a permit because they do not
correspond to an upstream element, but they are still bounded by the result
queue capacity.

The first implementation is ordered only. Later-completing lower-sequence
mapping work can hold back higher-sequence results. This matches `Flow.map`
semantics and keeps the public API deterministic. A future unordered operation
can expose completion-order results with a separate name and contract.

`close` is idempotent. It marks the boundary closed, closes upstream, closes
the permit, job, and result queues to wake waiters, and requests dispatcher and
worker cancellation when those fibers are still alive. As with `Flow.async` and
`Flow.buffer`, cancellation is cooperative: FiberStream guarantees that close
requests cancellation and closes upstream before returning. It does not
guarantee immediate interruption of arbitrary user code blocked inside a mapper
or upstream operation.

Queued or in-flight upstream and mapping errors are not delivered after
downstream has intentionally stopped early or failed. In that case, close
discards queued messages and suppresses internal failures in favor of the
downstream result or downstream failure. User failures raised by
`upstream.close` during boundary close are not cancellation errors: they
propagate from `Source#run_with` unless a downstream failure is already
propagating.

Close/error precedence:

| Situation | Result |
| --- | --- |
| Normal upstream completion and producer-side close succeeds | Publish `[:done, next_sequence]` |
| Normal upstream completion and producer-side close fails | Publish close failure at `next_sequence` |
| Upstream pull fails and cleanup close succeeds | Publish upstream pull failure at `next_sequence` |
| Upstream pull fails and cleanup close also fails | Publish upstream pull failure; suppress close failure |
| Mapping block fails and boundary cleanup close succeeds | Deliver mapping failure at that input sequence |
| Mapping block fails and boundary cleanup close also fails | Deliver mapping failure; suppress close failure |
| Multiple upstream or mapping failures are observed | Lowest-sequence failure wins |
| Downstream completes early and boundary close succeeds | Downstream result |
| Downstream completes early and boundary close fails | Boundary close failure |
| Downstream fails and boundary close also fails | Downstream failure; close failure suppressed |

The internal queues use Ruby's `Thread::SizedQueue`, not Async primitives.
`SizedQueue#push`, `SizedQueue#pop`, and `SizedQueue#close` provide bounded
handoff and waiter wakeup. FiberStream requires an installed scheduler for
`Flow.parallel_map` because these waits may otherwise block the current thread.
The implementation does not manually resume scheduler-owned fibers.

## Contracts

- `Flow.parallel_map(concurrency:) { ... }` returns `Flow[In, Out]`.
- `Source#parallel_map(concurrency:) { ... }` delegates to
  `Flow.parallel_map` and returns a new `Source`.
- `parallel_map` construction is lazy.
- `parallel_map` requires a block.
- `concurrency` must be a positive `Integer`.
- Missing blocks raise `ArgumentError`.
- Non-Integer `concurrency` values raise `TypeError`.
- Zero or negative `concurrency` values raise `ArgumentError`.
- The dispatcher and workers start on first downstream pull.
- A scheduler is required when the dispatcher and workers start.
- Missing scheduler raises `FiberStream::SchedulerRequiredError`.
- Dispatcher and worker execution uses Ruby's `Fiber.schedule`.
- FiberStream does not set `Fiber.scheduler`.
- FiberStream does not require Async at runtime.
- Upstream is pulled serially by the dispatcher.
- At most `concurrency` mapping blocks execute at a time.
- At most `concurrency` upstream elements are pulled but not yet emitted
  downstream.
- `jobs` has capacity `concurrency`.
- `results` has capacity `concurrency + 1`, allowing one result per
  pulled-but-unemitted upstream element plus one terminal message.
- Output values are emitted in input order.
- The boundary propagates upstream and mapping errors in input order.
- Observing any upstream or mapping error closes upstream admission.
- After admission is closed, emitting earlier successful values does not return
  permits or allow additional upstream pulls.
- Lower-sequence work needed for ordered delivery is not intentionally canceled
  merely because a higher-sequence failure was observed.
- Normal upstream completion becomes `Pull::DONE` after all earlier mapped
  values are emitted.
- Closing the boundary closes upstream.
- Closing the boundary wakes internal queues and requests active dispatcher and
  worker cancellation.
- Queued or in-flight upstream and mapping errors are suppressed after
  intentional downstream early completion or downstream failure.
- User `upstream.close` failures during boundary close are propagated unless a
  downstream failure is already propagating.
- Producer-side `upstream.close` failures are propagated unless an upstream
  pull failure is already propagating.
- Closing the boundary is idempotent.
- Public APIs never expose internal queue messages or `Pull::DONE`.

## Alternatives Considered

### Unordered Parallel Map First

Completion-order results can improve latency when an early element is slow, but
they do not match Ruby `map` expectations and make downstream behavior less
deterministic. Ordered `parallel_map` is the narrower first public contract.

### Add `concurrency:` To `Flow.map`

Overloading `Flow.map` would make a pure foreground stage sometimes require a
scheduler and sometimes change backpressure. A separate `parallel_map` method
keeps the concurrency boundary explicit.

### Use Ractors For Workers

Ractors can provide CPU parallelism, but they require different contracts for
shareable objects, block execution, object copying or moving, IO ownership, and
cancellation. Scheduler-backed worker fibers are a smaller next step that fits
the existing async and buffer designs.

### Let Workers Pull Upstream Directly

Direct worker pulls reduce dispatcher machinery, but current pull streams are
stateful and not designed for concurrent `next` calls. A single dispatcher keeps
upstream pull ordering and cleanup centralized.

### Unbounded Result Buffering

Unbounded result buffering would simplify the worker loop, but it weakens
FiberStream's backpressure story. Permit-based admission keeps completed,
running, and queued work bounded by `concurrency`.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-01. Feedback resulted in these
changes:

- Added a permit-based bound covering queued work, active work, and completed
  ordered results rather than only limiting active workers.
- Clarified that upstream is pulled only by a single dispatcher because pull
  streams are not concurrent `next` APIs.
- Defined sequence-based failure handling so later failures close admission
  without canceling lower-sequence work needed for ordered delivery.
- Added an explicit close/error precedence table.
- Specified job and result queue capacities, including the extra terminal
  result slot.
- Kept Ractor execution out of the first API and recorded it as a separate
  future contract.

## Validation

- Unit tests for laziness and validation errors.
- Unit tests for missing scheduler errors.
- Unit tests under Async proving ordered values with out-of-order completion,
  bounded upstream run-ahead, active worker concurrency, upstream error
  propagation, mapping error propagation, early sink completion cleanup,
  downstream failure cleanup, and repeated pulls after completion.
- Tests proving close wakes dispatcher and workers blocked on internal queues
  without leaking intentional cancellation errors.
- Tests proving queued or in-flight upstream and mapping errors are suppressed
  after early downstream completion or downstream failure.
- Tests proving normal upstream completion plus producer-side close failure
  delivers the close failure after earlier mapped values.
- Tests proving upstream pull failure plus producer-side close failure delivers
  the upstream pull failure.
- RBS validation.
- RuboCop.

## Open Questions

None.
