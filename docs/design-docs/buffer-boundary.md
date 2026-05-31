# Buffer Boundary

## Status

Accepted

## Context

The accepted async boundary proves scheduler integration with a demand-driven
non-blocking producer fiber. It intentionally avoids prefetch so close and
backpressure remain simple. `Flow.buffer(count)` is the next step: introduce a
bounded queue that can let upstream run ahead by a controlled number of
elements while preserving explicit cleanup and dependency-free scheduler
integration.

Governing documents:

- Product spec: `docs/product-specs/flow-buffer.md`
- Async design: `docs/design-docs/async-boundary.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- ADR: `docs/design-docs/adr/0003-flow-buffer-boundary.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Goals

- Run upstream stages before the buffer in a scheduled producer fiber.
- Keep downstream stages after the buffer in the caller's current fiber.
- Preserve element order and normal completion semantics.
- Bound queued messages to `count`.
- Permit at most one additional in-flight producer message while enqueueing
  into a full buffer.
- Block producer progress when the buffer is full.
- Close upstream and request producer cancellation on early downstream close.
- Keep Async as a compatibility target, not a runtime dependency.

## Non-Goals

- Unbounded queues.
- Overflow policies.
- Parallel map, fan-out, or unordered processing.
- Runtime scheduler installation.
- IO-specific ownership or cancellation contracts.

## Proposed Design

`Flow.buffer(count)` attaches a `BufferBoundary` pull stage:

```ruby
downstream_stream = Pull.buffer(upstream_stream, count)
```

`Source#buffer(count)` is a convenience method over
`Source#via(Flow.buffer(count))`. It does not introduce a separate runtime path.

The boundary owns:

- `upstream`, the pull stream before the boundary
- `queue`, a `Thread::SizedQueue` bounded to `count` messages
- `producer`, the scheduled fiber once started
- `started`, `closed`, and `done` state flags

The producer is not started during construction or attach. The first downstream
`next` starts the producer by calling `Fiber.schedule`. If `Fiber.scheduler` is
`nil`, the stage raises `SchedulerRequiredError`.

The producer loop repeatedly pulls `upstream.next` and pushes one tagged message
into `queue`. When the queue already contains `count` messages, push waits at
the queue boundary. While blocked on that push, the producer may hold one
in-flight value, completion, or error message, but it does not pull farther
upstream.

For each normal element the producer pushes `[:value, object]`. Terminal
messages are computed atomically: when upstream returns `Pull::DONE`, the
producer closes upstream before publishing the terminal message. If close
succeeds, it pushes `[:done]`. If close fails, it pushes
`[:error, close_exception]`. If upstream raises while pulling, the producer
closes upstream and then pushes `[:error, upstream_error]`; any close failure in
that path is suppressed so the pull failure remains the primary stream failure.

The queue stores tagged messages instead of raw stream elements so all Ruby
objects remain valid stream values:

```ruby
[:value, object]
[:done]
[:error, exception]
```

Terminal messages count toward the same `count` queued-message bound as value
messages. If the queue is full when a terminal message is computed, the
producer waits to enqueue that terminal message.

The downstream `next` pops one message. Value messages return the value.
Completion messages mark the boundary done and return `Pull::DONE`. Error
messages mark the boundary done and re-raise the stored exception. Later
downstream pulls return `Pull::DONE` without restarting the producer or pulling
upstream.

`close` is idempotent. It marks the boundary closed, closes upstream, closes the
queue so producers or consumers waiting at the boundary can wake, and requests
producer cancellation when the producer is still alive. As with `Flow.async`,
the cancellation contract is cooperative: FiberStream guarantees that close
requests producer cancellation and closes upstream before returning. It does
not guarantee immediate interruption of arbitrary user code blocked inside
upstream operations.

Cancellation exceptions caused by boundary close are internal implementation
details and must not escape as stream failures after downstream has closed
intentionally.

Queued or in-flight upstream errors are not delivered after downstream has
intentionally stopped early or failed. In that case, close discards queued
messages and suppresses queued or in-flight upstream errors in favor of the
downstream result or downstream failure. User failures raised by
`upstream.close` during boundary close are not cancellation errors: they
propagate from `Source#run_with` unless a downstream failure is already
propagating.

Producer-side close failures are delivered as terminal stream failures when no
earlier upstream pull failure is already being delivered. Terminal message
publication waits for that producer-side close result. If an upstream pull
failure and producer-side close failure both happen, the upstream pull failure
wins and the close failure is suppressed.

The internal queue uses Ruby's `Thread::SizedQueue`, not Async primitives.
`SizedQueue#push` provides full-buffer waiting, `SizedQueue#pop` provides
empty-queue waiting, and `SizedQueue#close` wakes blocked waiters with Ruby's
standard closed-queue behavior. FiberStream requires an installed scheduler for
`Flow.buffer` because these waits may otherwise block the current thread. The
implementation does not manually resume scheduler-owned fibers.

## Contracts

- `Flow.buffer(count)` returns `Flow[Elem, Elem]`.
- `Source#buffer(count)` delegates to `Flow.buffer(count)` and returns a new
  `Source`.
- `Flow.buffer(count)` construction is lazy.
- `count` must be a positive `Integer`.
- Non-Integer counts raise `TypeError`.
- Zero or negative counts raise `ArgumentError`.
- The buffer producer starts on first downstream pull.
- A scheduler is required when the producer starts.
- Missing scheduler raises `FiberStream::SchedulerRequiredError`.
- Producer execution uses Ruby's `Fiber.schedule`.
- FiberStream does not set `Fiber.scheduler`.
- FiberStream does not require Async at runtime.
- The boundary preserves stream element order.
- The boundary queues at most `count` messages and may hold one additional
  in-flight producer message while blocked on enqueue.
- The boundary propagates upstream errors to downstream.
- The boundary converts upstream normal completion into `Pull::DONE`.
- Closing the boundary closes upstream.
- Closing the boundary closes the queue and requests active producer
  cancellation.
- Queued or in-flight upstream errors are suppressed after intentional
  downstream early completion or downstream failure.
- User `upstream.close` failures during boundary close are propagated unless a
  downstream failure is already propagating.
- Producer-side `upstream.close` failures are propagated unless an upstream
  pull failure is already propagating.
- Closing the boundary is idempotent.
- Public APIs never expose internal queue messages or `Pull::DONE`.

## Alternatives Considered

### Add Buffer Size To `Flow.async`

`Flow.async(buffer: count)` would avoid another method, but it overloads one API
with two materially different backpressure contracts. Keeping `Flow.async`
demand-driven and adding `Flow.buffer(count)` makes the boundary explicit.

### Unbounded Queue

An unbounded queue is easier to use, but it weakens FiberStream's core
backpressure story and can turn a slow downstream into unbounded memory growth.

### Drop Policies In The First Buffer

Drop-oldest, drop-newest, and sliding buffers are useful, but they require
separate data-loss contracts. The first buffer should be lossless and bounded.

### Rely On Async Queues

Async-specific queues would simplify some scheduler behavior, but they would
make Async a runtime dependency. The buffer should use Ruby's scheduler
interface and Ruby's standard `Thread::SizedQueue`.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-05-31. Feedback resulted in these
changes:

- Counted terminal messages toward the same `count` bound as value messages.
- Defined the final bounded contract as `count` queued messages plus at most one
  in-flight producer message blocked on enqueue.
- Defined queued or in-flight upstream errors as suppressed after intentional
  downstream early completion or downstream failure.
- Replaced a custom fiber channel with Ruby's standard `Thread::SizedQueue`.
- Defined producer-side close failure precedence and delayed normal completion
  publication until producer-side close succeeds.

## Validation

- Unit tests for laziness and validation errors.
- Unit tests for missing scheduler errors.
- Unit tests under Async proving ordered values, bounded prefetch, full-buffer
  producer blocking, upstream error propagation, early sink completion cleanup,
  downstream failure cleanup, and repeated pulls after completion.
- Tests proving close wakes a producer blocked on a full buffer without leaking
  intentional cancellation errors.
- Tests proving queued or in-flight upstream errors are suppressed after early
  downstream completion or downstream failure.
- Tests proving normal upstream completion plus producer-side close failure
  delivers the close failure instead of normal completion.
- Tests proving upstream pull failure plus producer-side close failure delivers
  the upstream pull failure.
- Tests proving downstream boundary close preserves user close errors when
  close is the failing operation.
- RBS validation.
- RuboCop.

## Open Questions

None.
