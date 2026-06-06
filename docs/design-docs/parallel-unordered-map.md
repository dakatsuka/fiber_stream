# Parallel Unordered Map

## Status

Accepted

## Context

`Flow.parallel_map` added bounded scheduler-backed parallel mapping while
preserving input order. That deterministic contract is useful, but it can
increase latency when a slow earlier element blocks later completed results.
FiberStream needs a separate unordered operation for users who prefer
completion-order output and can tolerate nondeterministic cross-element order.

Governing documents:

- Product spec: `docs/product-specs/flow-parallel-unordered-map.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Async design: `docs/design-docs/async-boundary.md`
- Buffer design: `docs/design-docs/buffer-boundary.md`
- Ordered parallel design: `docs/design-docs/parallel-map.md`
- ADR: `docs/design-docs/adr/0015-flow-parallel-unordered-map.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Goals

- Run independent mapping blocks in scheduled worker fibers.
- Emit mapped values in scheduler-observed completion order.
- Keep downstream stages after the unordered parallel map in the caller's
  current fiber.
- Bound pulled-but-unemitted upstream elements to `concurrency`.
- Run at most `concurrency` mapping blocks at a time.
- Fail fast when downstream observes an upstream or mapping failure.
- Close upstream and request worker cancellation on early downstream close or
  failure.
- Keep Async as a compatibility target, not a runtime dependency.

## Non-Goals

- Ordered result emission.
- Ractor-backed workers.
- CPU parallelism guarantees.
- General graph scheduling or fan-out/fan-in.
- Runtime scheduler installation.
- Per-element timeout, retry, batching, or overflow policies.

## Proposed Design

`Flow.parallel_unordered_map(concurrency:) { ... }` attaches a
`ParallelUnorderedMapBoundary` pull stage:

```ruby
downstream_stream = Pull.parallel_unordered_map(upstream_stream, concurrency, block)
```

`Source#parallel_unordered_map(concurrency:) { ... }` is a convenience method
over `Source#via(Flow.parallel_unordered_map(concurrency:) { ... })`. It does
not introduce a separate runtime path.

The boundary owns:

- `upstream`, the pull stream before the boundary
- `concurrency`, the positive worker limit
- `mapper`, the user block
- `permits`, a bounded token queue with `concurrency` tokens
- `jobs`, a `Thread::SizedQueue` with capacity `concurrency`
- `results`, a `Thread::SizedQueue` with capacity `concurrency + 1`
- `dispatcher`, the scheduled fiber that pulls upstream serially
- `workers`, up to `concurrency` scheduled worker fibers
- `next_sequence`, `outstanding_jobs`, `terminal_message`, `started`,
  `closed`, `done`, and `upstream_closed` flags

The dispatcher and workers are not started during construction or attach. The
first downstream `next` starts them by calling `Fiber.schedule`. If
`Fiber.scheduler` is `nil` or the current fiber is blocking, the stage raises
`SchedulerRequiredError`. The non-blocking fiber requirement matters because
downstream waits on internal queues.

The dispatcher is the only internal fiber that calls `upstream.next`. Before
each upstream pull it must take one permit. A permit represents one upstream
element that may be queued, actively mapped, or completed but not yet emitted
downstream. Downstream returns one permit only after it emits a mapped value.
This preserves the core backpressure bound: at most `concurrency` upstream
elements can be pulled and not yet emitted while admission is open.

For each normal upstream value, the dispatcher assigns an increasing sequence
number, increments `outstanding_jobs`, and pushes a job:

```ruby
JobMessage.new(sequence:, value:)
```

Workers pop jobs, call the user block, and push tagged results:

```ruby
ValueMessage.new(sequence:, value: mapped_value)
ErrorMessage.new(sequence:, error: exception)
```

Sequence numbers are internal diagnostics and are not used for output ordering.
Downstream emits `ValueMessage` objects in the order they are popped from the
results queue. That order is scheduler-observed completion order. Tests and
documentation must not imply a deterministic order between concurrently mapped
elements.

When upstream returns `Pull::DONE`, the dispatcher closes upstream before
publishing a terminal message. If close succeeds, it publishes:

```ruby
DoneMessage.new
```

If close fails, it publishes:

```ruby
ErrorMessage.new(sequence: next_sequence, error: close_exception)
```

Normal-completion terminal messages are not emitted directly. Downstream stores
the terminal message in `terminal_message` and continues waiting for admitted
jobs while `outstanding_jobs` is positive. It delivers the terminal only after
all admitted jobs have produced downstream-observed value results. If a mapping
failure is observed before a delayed producer-side close failure is delivered,
the mapping failure wins and the close failure is suppressed.

If `upstream.next` raises, the dispatcher closes upstream and publishes
`ErrorMessage.new(sequence: next_sequence, error: upstream_error)`. A close
failure in that path is suppressed so the upstream pull failure remains
primary.

The downstream `next` pops one result message at a time. Value messages return
the mapped value, decrement `outstanding_jobs`, and then return one permit if
admission is still open. If a delayed terminal message exists and the value
causes `outstanding_jobs` to reach zero, the next downstream pull delivers the
terminal message without waiting for another result queue pop. Terminal done
messages mark the boundary done and return `Pull::DONE`.

Failures are fail-fast by downstream observation order. When downstream pops an
`ErrorMessage`, the boundary marks itself done, closes admission, closes
upstream, closes internal queues, requests best-effort dispatcher and worker
cancellation, and raises that exception. Already emitted values stay emitted.
Queued or in-flight values and failures that have not been observed are
suppressed.

The `results` queue has capacity `concurrency + 1`: one result for each
pulled-but-unemitted upstream element plus one terminal message. Terminal done
or terminal close-error messages do not consume a permit because they do not
correspond to an upstream element, but they are still bounded by the result
queue capacity.

`close` is idempotent. It marks the boundary closed and done, closes upstream,
closes the permit, job, and result queues to wake waiters, and requests
best-effort dispatcher and worker cancellation when those fibers are still
alive. Cancellation is cooperative: FiberStream guarantees that close requests
cancellation and closes upstream before returning. It does not guarantee
immediate interruption of arbitrary user code blocked inside a mapper or
upstream operation. Active fiber cancellation uses
`Fiber.scheduler#fiber_interrupt` when the installed scheduler supports it. With
schedulers that do not support `fiber_interrupt`, queue close and upstream close
still wake cooperative waiters, but active user mapping or upstream code may
continue until it next cooperatively returns or raises.

Queued or in-flight upstream and mapping errors are not delivered after
downstream has intentionally stopped early or failed. In that case, close
discards queued messages and suppresses internal failures in favor of the
downstream result or downstream failure. User failures raised by
`upstream.close` during boundary close are not cancellation errors: they
propagate from `Source#run_with` unless a downstream failure is already
propagating.

The internal queues use Ruby's `Thread::SizedQueue`, not Async primitives.
`SizedQueue#push`, `SizedQueue#pop`, and `SizedQueue#close` provide bounded
handoff and waiter wakeup. FiberStream requires an installed scheduler for
`Flow.parallel_unordered_map` because these waits may otherwise block the
current thread. The implementation does not manually resume scheduler-owned
fibers.

## Contracts

- `Flow.parallel_unordered_map(concurrency:) { ... }` returns `Flow[In, Out]`.
- `Source#parallel_unordered_map(concurrency:) { ... }` delegates to
  `Flow.parallel_unordered_map` and returns a new `Source`.
- `parallel_unordered_map` construction is lazy.
- `parallel_unordered_map` requires a block.
- `concurrency` must be a positive `Integer`.
- Missing blocks raise `ArgumentError`.
- Non-Integer `concurrency` values raise `TypeError`.
- Zero or negative `concurrency` values raise `ArgumentError`.
- The dispatcher and workers start on first downstream pull.
- A scheduler is required when the dispatcher and workers start.
- Missing scheduler raises `FiberStream::SchedulerRequiredError`.
- A blocking current fiber raises `FiberStream::SchedulerRequiredError`.
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
- Output values are emitted in scheduler-observed completion order.
- Output values are not emitted in guaranteed input order.
- Normal-completion terminal messages are delayed until all admitted mapping
  jobs have produced downstream-observed value results.
- The boundary propagates the first downstream-observed upstream or mapping
  error.
- Observing any upstream or mapping error closes upstream admission.
- Values and failures not yet observed when the primary failure is delivered
  are suppressed.
- Normal upstream completion becomes `Pull::DONE` after all pulled mapped
  values are emitted.
- Producer-side close failures after normal upstream completion are delayed
  until all admitted mapping jobs have produced downstream-observed value
  results. A mapping failure observed before that delayed close failure wins.
- Closing the boundary closes upstream.
- Closing the boundary wakes internal queues and requests best-effort active
  dispatcher and worker cancellation.
- Queued or in-flight upstream and mapping errors are suppressed after
  intentional downstream early completion or downstream failure.
- User `upstream.close` failures during boundary close are propagated unless a
  downstream failure is already propagating.
- Producer-side `upstream.close` failures are propagated unless an upstream
  pull failure is already propagating.
- Closing the boundary is idempotent.
- Public APIs never expose internal queue messages or `Pull::DONE`.

## Alternatives Considered

### Add `ordered: false` To `Flow.parallel_map`

A keyword would reduce the number of method names, but it would make one API
switch between deterministic input order and nondeterministic completion order.
A separate method keeps ordering visible at the call site and leaves the
existing `parallel_map` contract unchanged.

### Name The Operation `unordered_parallel_map`

Both names are explicit. `parallel_unordered_map` groups near
`parallel_map` and reads as the parallel mapping variant whose delivery is
unordered, so it is the preferred public name.

### Sequence-Ordered Error Delivery

Ordered `parallel_map` waits for earlier sequence numbers before raising a
later failure. That preserves input-order semantics but reintroduces the
head-of-line blocking this operation exists to avoid. Unordered mapping should
fail fast when downstream observes a failure result.

### Drain Started Work Before Failing

Draining started work can preserve more successful results, but it delays
failure delivery and can hang behind slow or blocked mapping calls. Fail-fast
behavior is simpler, more responsive, and consistent with unordered completion
semantics.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-06. Feedback resulted in these
changes:

- Added `outstanding_jobs` tracking and delayed terminal delivery so upstream
  normal completion cannot complete downstream before all admitted mapping jobs
  have been observed.
- Delayed producer-side close failures after normal upstream completion behind
  admitted mapping jobs, with mapping failures remaining primary if observed
  before the delayed close failure is delivered.
- Added explicit validation cases for terminal delivery racing with in-flight
  admitted work.
- Clarified that best-effort active fiber cancellation uses
  `Fiber.scheduler#fiber_interrupt` when available and may be a no-op for
  active user code on schedulers that do not support it.

## Validation

- Unit tests for laziness and validation errors.
- Unit tests for missing scheduler and blocking current fiber errors.
- Unit tests under Async proving completion-order output, unordered behavior,
  bounded pulled-but-unemitted work, active worker concurrency limits, upstream
  and mapping error propagation, early sink completion cleanup, downstream
  failure cleanup, and repeated pulls after completion.
- Tests proving upstream normal completion waits for in-flight admitted work
  before downstream completion.
- Tests proving an in-flight mapping failure wins over a delayed
  normal-completion producer-side close failure.
- Tests proving close wakes workers or dispatcher without leaking intentional
  cancellation errors.
- Tests proving queued or in-flight upstream and mapping errors are suppressed
  after early downstream completion or downstream failure.
- Tests proving normal upstream completion plus producer-side close failure
  delivers the close failure instead of normal completion.
- Tests proving upstream pull failure plus producer-side close failure delivers
  the upstream pull failure.
- RBS validation.
- RuboCop.

## Open Questions

- None blocking implementation.
