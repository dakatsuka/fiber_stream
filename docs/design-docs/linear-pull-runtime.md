# Linear Pull Runtime

## Status

Accepted

## Context

FiberStream targets Ruby 4.x and should use `Fiber` and `Fiber.scheduler` for
non-blocking stream processing. The initial product surface is a linear pipeline
with `Source.each`, `Flow.map`, `Flow.select`, `Flow.take`, `Sink.to_a`, and
`Sink.first`. `Sink.fold` adds accumulator-based materialization. Backpressure is
a core property, so the first runtime must not be a push-only implementation
that later needs to be replaced.

## Goals

- Implement linear pipelines with lazy construction and foreground execution.
- Make downstream demand the only way upstream progresses.
- Keep completion on the normal path instead of using exceptions.
- Keep cleanup explicit and reliable.
- Avoid making `async` a runtime dependency.

## Non-Goals

- Per-stage fibers for pure linear stages.
- Queues between initial stages.
- Scheduler installation by FiberStream.
- Async boundaries, buffers, parallel stages, or graph materialization.

## Proposed Design

`Source`, `Flow`, and `Sink` are public builder objects. Constructing and
composing them records the pipeline shape but does not enumerate upstream or
call user blocks. `Source#via` accepts only `FiberStream::Flow` instances, and
`Source#run_with` accepts only `FiberStream::Sink` instances. Invalid builder
objects raise `TypeError`.

`run_with` materializes a fresh internal pull chain. A sink drives execution by
calling `next` on the downstream end of the chain. Each flow calls `next` on its
upstream only when it needs an input value. This creates the initial
backpressure invariant without queues or demand counters: one downstream demand
causes only the required upstream pulls.

The internal pull protocol is:

```ruby
value = stream.next
stream.close
```

`next` returns either a stream element or the internal completion sentinel.
Normal completion uses the sentinel, not `StopIteration`. The sentinel is a
private frozen object and stages compare it only by identity with `equal?`.
Public APIs never return the sentinel. All normal Ruby objects remain valid
stream elements as long as users cannot access the private sentinel object.
Failures are raised as exceptions and propagate through `run_with`.

`close` is idempotent and propagates upstream. `run_with` calls `close` from an
`ensure` block after success, failure, or early sink completion.

`Source.each` does not own the original enumerable and does not call `close` on
it. On materialization it creates an enumerator with
`enumerable.to_enum(:each)`. This supports normal Ruby `Enumerable`
implementations that yield to a block instead of returning an external iterator.
Resource-owning sources such as IO sources must use separate source types with
explicit ownership contracts.

`Sink.to_a` consumes all elements. `Sink.first` pulls at most one element and
then returns, so it is the first public early-completion operation. `run_with`
must close the materialized pull chain after either sink returns.

## Builder Contracts

`Flow` stores an internal attach callable:

```ruby
pull_stream = flow.attach(upstream_pull_stream)
```

`attach` returns a downstream pull stream that implements `next` and `close`.
`Flow.map` attaches a map stage that pulls one upstream value when its own
`next` is called, returns `DONE` unchanged, and applies the user block to normal
values.

`Flow.select` attaches a filter stage. On each downstream `next`, it pulls
upstream until it receives `DONE` or finds a value whose predicate result is
truthy. It returns matching values unchanged and drops falsey values. This means
one downstream demand may require multiple upstream pulls, while upstream still
cannot progress without downstream demand.

`Flow.take` attaches a limiting stage. It forwards at most `count` upstream
values. The pull that observes the count-th element returns that element, closes
upstream during the same `next`, and records the stage as completed. Later
downstream pulls return `DONE` without pulling upstream again. `take(0)` returns
`DONE` and closes upstream on the first downstream demand without calling
upstream `next`. Negative counts are rejected by `Flow.take` with
`ArgumentError`; non-Integer counts are rejected with `TypeError`.

`Sink` stores an internal run callable:

```ruby
materialized_value = sink.run(pull_stream)
```

`Sink.to_a` repeatedly calls `next` until `DONE`, collects normal elements, and
returns the collected array. Exceptions raised by sink execution propagate out
of `run_with` after the materialized stream is closed.

`Sink.first` calls `next` at most once. It returns the value when the result is a
normal element and returns `nil` when the result is `DONE`.

`Sink.fold` repeatedly calls `next` until `DONE`, replacing the accumulator with
the block result for each normal element. It returns the final accumulator. If
upstream is empty, it returns the initial accumulator.

Initial execution model:

```text
Source.each(...)
  .via(Flow.map { ... })
  .via(Flow.select { ... })
  .via(Flow.take(...))
  .run_with(Sink.to_a, Sink.first, or Sink.fold)

1. Build source and flow definitions lazily.
2. run_with materializes a pull chain.
3. Sink.to_a and Sink.fold repeatedly pull values; Sink.first pulls at most one
   value.
4. Flow stages pull upstream only when asked.
5. Source.each returns one value or DONE.
6. run_with closes the materialized chain.
```

`Fiber.scheduler` remains an environmental capability. FiberStream does not set
or require a scheduler for pure stages. Future IO and async stages should use
Ruby scheduler-aware operations so they work with any compliant scheduler,
including Async's scheduler.

## Contracts

- Internal pull streams implement `next` and `close`.
- `Pull::DONE` is a private frozen identity sentinel and must not be exposed
  through the public API.
- The internal `Pull` namespace and concrete pull stages are private constants.
- Stages compare completion with `equal?`, never `==`.
- `Flow.select` treats Ruby truthiness normally: only `false` and `nil` are
  dropped.
- `Flow.take` never emits more than `count` elements.
- `Flow.take(0)` does not pull upstream and closes upstream on first downstream
  demand.
- `Flow.take(count)` emits the count-th element before completing.
- `Flow.take(count)` closes upstream during the pull that reaches the limit.
- `Flow.take(count)` raises `ArgumentError` for negative counts.
- `Flow.take(count)` raises `TypeError` for non-Integer counts.
- `close` is idempotent.
- `close` propagates upstream.
- `Source.each` creates a new enumerator by calling `enumerable.to_enum(:each)`
  during each materialization, without snapshotting the enumerable.
- `Source.each` does not close the original enumerable.
- `run_with` executes in the current fiber and returns only after the stream
  completes or fails.
- `run_with` returns the sink materialized value.
- `run_with` re-raises stream failures.
- `Source#via` raises `TypeError` for non-`Flow` inputs.
- `Source#run_with` raises `TypeError` for non-`Sink` inputs.
- The initial runtime creates no per-stage fibers.
- FiberStream does not install a scheduler.
- Public interfaces are documented with block comments in source and RBS
  signatures.

## Alternatives Considered

### Enumerator::Lazy

`Enumerator::Lazy` is Ruby-native and compact, but it does not provide enough
control over cancellation, cleanup, async boundaries, bounded buffers, and
materialization state. FiberStream can expose Ruby-like APIs while using a
smaller internal pull protocol.

### StopIteration For Completion

Using `StopIteration` would mirror Ruby enumerators, but normal stream
completion would become an exception path. A sentinel keeps stage code simple
and leaves exceptions for failures.

### Per-Stage Fibers

Starting one fiber per stage from the beginning would prepare for async
boundaries, but it adds queues, joining, cancellation, and error propagation
before the first linear API needs them. The design defers fibers to operations
that require asynchronous boundaries.

### Explicit Cancellation API

Separate `cancel` and `close` methods are useful for future async stages. The
initial runtime uses only idempotent `close` and records explicit cancellation
reasons as deferred work.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-05-31. Feedback resulted in these
changes:

- Clarified that `Source.each` calls `enumerable.each` per materialization but
  does not snapshot values or guarantee replayability for one-shot enumerables.
- Clarified that `Pull::DONE` is a private identity sentinel compared with
  `equal?`.
- Defined `Source.each` cleanup ownership: do not close the original enumerable
  and leave resource-owning sources for separate APIs.
- Initially kept early completion as an internal runtime invariant, then added
  `Sink.first` as the first public early-completion operation.
- Added internal builder contracts for flow attachment and sink materialization.
- Expanded validation coverage for invalid builders, replayability semantics,
  sentinel identity behavior, and cleanup.
- Implementation review later clarified that `Source.each` must support
  `Enumerable` implementations that yield to a block, so materialization uses
  `enumerable.to_enum(:each)` rather than assuming `each` returns an external
  iterator.

## Validation

- Unit tests with Minitest for laziness, mapping, filtering, limiting,
  composition, materialized values, `Sink.first` early completion, failure
  propagation, invalid builder inputs, replayability semantics, sentinel
  identity behavior, backpressure, and cleanup.
- RBS validation for public API signatures.
- RuboCop with only Layout and Lint departments enabled.
- GitHub Actions running tests, RBS validation, and RuboCop on Ruby 4.0.3.
- Future integration tests running FiberStream operations under `async`.

## Open Questions

- What queue and cancellation contracts are required before adding `.async` or
  `.buffer`?
