# Async Boundary

## Status

Accepted

## Context

The accepted linear pull runtime keeps construction lazy, keeps pure stages in
the caller's current fiber, and advances upstream only when downstream pulls.
That model intentionally deferred fibers and cancellation until an operation
needs them.

`Flow.async` is the first operation that needs them. It introduces a scheduler
boundary in a linear pipeline while preserving narrow public APIs and bounded
backpressure.

Governing documents:

- Product spec: `docs/product-specs/flow-async.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- ADR: `docs/design-docs/adr/0001-initial-linear-pull-runtime.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Goals

- Run upstream stages before the boundary in a non-blocking producer fiber.
- Keep downstream stages after the boundary in the caller's current fiber.
- Preserve element order and normal completion semantics.
- Preserve one upstream pull per downstream demand across the boundary.
- Make early close close upstream and request producer cancellation.
- Keep Async as a compatibility target, not a runtime dependency.

## Non-Goals

- General graph scheduling.
- Configurable buffering.
- Parallel map or fan-out.
- Runtime scheduler installation.
- IO-specific ownership or cancellation contracts.

## Proposed Design

`Flow.async` attaches an `AsyncBoundary` pull stage:

```ruby
downstream_stream = Pull.async(upstream_stream)
```

`Source#async` is a convenience method over `Source#via(Flow.async)`. It does
not introduce a separate runtime path.

The boundary owns:

- `upstream`, the pull stream before the boundary
- `producer`, the non-blocking fiber once started
- `started`, `closed`, and `done` state flags

The producer is not started during construction or attach. The first
downstream `next` starts the producer by creating `Fiber.new(blocking: false)`.
If `Fiber.scheduler` is `nil`, the stage raises `SchedulerRequiredError`
instead of allowing scheduler-dependent behavior to fail later with a Ruby
runtime error.

Each downstream `next` resumes the producer fiber once. The producer performs at
most one `upstream.next`, then yields a tagged message back to the downstream
fiber. Value messages return the value. Completion messages mark the boundary
done and return `Pull::DONE`. Error messages mark the boundary done and
re-raise the stored exception. Later downstream pulls return `Pull::DONE`
without restarting the producer or pulling upstream.

The producer closes upstream from an `ensure` block. It stores tagged messages
instead of raw stream elements so all Ruby objects remain valid stream values:

```ruby
[:value, object]
[:done]
[:error, exception]
```

Backpressure remains demand-driven. One downstream pull resumes the producer for
at most one upstream pull, so the boundary does not prefetch. Scheduler-aware
blocking operations inside upstream code still run from a non-blocking fiber and
can yield to the active scheduler while the downstream caller waits for that
pull to finish.

`close` is idempotent. It marks the boundary closed, closes upstream, and
interrupts the producer with Ruby's fiber APIs when the producer is still alive.
The public contract is cooperative: FiberStream guarantees that close requests
producer cancellation and closes upstream before returning. It does not
guarantee that every scheduler can immediately interrupt arbitrary user code
that is blocked inside an upstream operation. Resource-owning upstream stages
must make their own `close` release resources and wake scheduler-managed
operations where Ruby supports that.

Cancellation exceptions caused by boundary close are internal implementation
details and must not escape as stream failures after downstream has closed
intentionally.

`Source#run_with` already closes the materialized stream from `ensure`; this
continues to be the outer cleanup path after successful sinks, sink failures,
and early sink completion.

## Contracts

- `Flow.async` returns `Flow[Elem, Elem]`.
- `Source#async` delegates to `Flow.async` and returns a new `Source`.
- `Flow.async` construction is lazy.
- The async producer starts on first downstream pull.
- A scheduler is required when the producer starts.
- Missing scheduler raises `FiberStream::SchedulerRequiredError`.
- Async upstream execution uses `Fiber.new(blocking: false)`.
- FiberStream does not set `Fiber.scheduler`.
- FiberStream does not require Async at runtime.
- The boundary preserves stream element order.
- The boundary performs at most one upstream pull for each downstream pull.
- The boundary propagates upstream errors to downstream.
- The boundary converts upstream normal completion into `Pull::DONE`.
- Closing the boundary closes upstream.
- Closing the boundary requests active producer cancellation.
- Closing the boundary is idempotent.
- Public APIs never expose internal producer messages or `Pull::DONE`.

## Alternatives Considered

### Start Producer During Materialization

Starting during attach would be simpler, but it would allow upstream to advance
even when a sink never pulls. Starting on the first downstream `next` keeps the
first demand edge explicit.

### Scheduled Fiber With Handoff Queue

A scheduled producer fiber and handoff queue would allow prefetch and overlap
between upstream and downstream stages, but it requires a stronger
scheduler-agnostic channel and cancellation contract. The first async boundary
keeps the producer as a demand-driven non-blocking fiber to prove scheduler
integration before adding buffering.

### Unbounded Queue

An unbounded queue would maximize overlap, but it weakens backpressure and can
turn a slow downstream into unbounded memory growth.

### Configurable Queue Size On `Flow.async`

Configurable buffering is useful, but it expands the first async API before the
cancellation and close model has been proven. A later `Flow.buffer(count)` or
`Flow.async(buffer: count)` can add a scheduler-aware handoff channel.

### Async Runtime Dependency

Depending directly on Async would simplify cancellation APIs, but it would make
FiberStream choose an event loop for users. The runtime should depend only on
Ruby's scheduler interface.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-05-31. Feedback resulted in these
changes:

- Replaced the scheduled producer plus handoff queue with a demand-driven
  non-blocking producer fiber so the first implementation has a portable close
  and backpressure contract.
- Narrowed cancellation guarantees to scheduler-agnostic behavior: close
  requests producer cancellation and closes upstream before returning, but does
  not promise immediate interruption of arbitrary blocked user code.
- Added validation requirements for downstream failure cleanup and blocked
  producer wakeup.
- Clarified that the Ruby master scheduler reference is live API material, not
  a stable Ruby 4.0-only reference.

## Validation

- Unit tests for laziness and missing-scheduler errors.
- Unit tests under Async proving ordered values, upstream error propagation,
  early sink completion cleanup, downstream failure cleanup, and repeated pulls
  after completion.
- Tests where downstream close kills a suspended producer without leaking an
  intentional cancellation error.
- A backpressure test proving downstream early completion does not pull more
  than the demanded element.
- RBS validation.
- RuboCop.

## Open Questions

None.

## Follow-Up Designs

- `docs/design-docs/buffer-boundary.md` defines `Flow.buffer(count)` as a
  separate bounded prefetch operation.
