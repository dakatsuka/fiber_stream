# Linear Pull Runtime

## Status

Accepted

## Context

FiberStream targets Ruby 4.x and should use `Fiber` and `Fiber.scheduler` for
non-blocking stream processing. The initial product surface is a linear pipeline
with `Source.each`, `Flow.map`, `Flow.select`, `Flow.take`, `Sink.to_a`, and
`Sink.first`. `Sink.count` adds count materialization, `Sink.fold` adds
accumulator-based materialization, `Sink.foreach` adds terminal side-effect
materialization, and `Sink.find` adds predicate-based terminal search.
Backpressure is a core property, so the first runtime must not be a push-only
implementation that later needs to be replaced.

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

`Source#map`, `Source#select`, and `Source#take` are convenience methods over
`Source#via(Flow.map)`, `Source#via(Flow.select)`, and `Source#via(Flow.take)`.
They do not introduce new runtime stages or behavior beyond the corresponding
flows.

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

Materialized pull streams are internal runtime objects owned by the running
pipeline. Unless a specific boundary design says otherwise, they are not public
thread-safe objects for concurrent native-thread calls to `next` or `close`.
Scheduler-backed boundaries may coordinate with producer fibers, but that
coordination is part of the boundary's cooperative scheduler contract rather
than a general cross-thread ownership guarantee.

`Source.each` does not own the original enumerable and does not call `close` on
it. On materialization it creates an enumerator with
`enumerable.to_enum(:each)`. This supports normal Ruby `Enumerable`
implementations that yield to a block instead of returning an external iterator.
Resource-owning sources such as IO sources must use separate source types with
explicit ownership contracts.

`Sink.to_a` consumes all elements. `Sink.first` pulls at most one element and
then returns, so it is the first public early-completion operation. `Sink.count`
consumes all elements and returns the number observed without storing them.
`Sink.find` pulls until a predicate matches or upstream completes. `run_with`
must close the materialized pull chain after any sink returns.

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

`Sink.count` repeatedly calls `next` until `DONE`, incrementing an integer for
each normal element. It returns `0` if upstream is empty. It stores no consumed
elements.

`Sink.fold` repeatedly calls `next` until `DONE`, replacing the accumulator with
the block result for each normal element. It returns the final accumulator. If
upstream is empty, it returns the initial accumulator.

`Sink.foreach` repeatedly calls `next` until `DONE`, invokes the block once for
each normal element, and returns the number of elements whose block completed
successfully. If the block raises, the failure propagates from `run_with`, no
later elements are pulled by the sink, and the materialized chain is closed by
the existing `run_with` cleanup path.

`Sink.find` repeatedly calls `next` until `DONE` or a predicate result is
truthy. It returns the original matching element, not the predicate result. If
upstream completes without a match, it returns `nil`. Because the returned
element is the original stream element, `Sink.find` may return `nil` or `false`
when those values match. Completion detection uses the private sentinel
identity helper, and the sentinel is never returned through the public API. If
the predicate block raises, the failure propagates from `run_with`, no later
elements are pulled by the sink, and the materialized chain is closed by the
existing `run_with` cleanup path. A cleanup close failure after an otherwise
successful match or no-match completion fails `run_with`; upstream and predicate
failures remain primary over cleanup close failures.

Initial execution model:

```text
Source.each(...)
  .via(Flow.map { ... })
  .via(Flow.select { ... })
  .via(Flow.take(...))
  .run_with(Sink.to_a, Sink.first, Sink.count, Sink.fold, Sink.foreach, or Sink.find)

1. Build source and flow definitions lazily.
2. run_with materializes a pull chain.
3. Sink.to_a, Sink.count, Sink.fold, Sink.foreach, and Sink.find repeatedly
   pull values; Sink.first pulls at most one value.
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
- Pull consumers must stop calling `next` after receiving `DONE`; repeated
  completion pulls are an internal defensive behavior, not a public contract.
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
- `Source#map` delegates to `Flow.map` and returns a new `Source`.
- `Source#select` delegates to `Flow.select` and returns a new `Source`.
- `Source#take` delegates to `Flow.take` and returns a new `Source`.
- `Source#run_with` raises `TypeError` for non-`Sink` inputs.
- `Sink.count` consumes upstream in order, does not store elements, and returns
  the number of elements observed.
- `Sink.count` returns `0` for an empty upstream.
- `Sink.foreach` consumes upstream in order and returns the number of elements
  whose block completed successfully.
- `Sink.foreach` does not pull a later element after its block raises.
- `Sink.find` requires a block and raises `ArgumentError` when missing.
- `Sink.find` consumes upstream in order until it finds a truthy predicate
  result or upstream completes.
- `Sink.find` returns the original matching stream element, not the predicate
  result.
- `Sink.find` returns `nil` when upstream completes without a match, including
  empty upstream.
- `Sink.find` may return `nil` or `false` when the matching stream element is
  `nil` or `false`.
- `Sink.find` stops pulling upstream after a match.
- `Sink.find` does not pull a later element after its block raises.
- `Sink.find` does not store consumed elements.
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
  composition, materialized values, `Sink.first` early completion,
  `Sink.foreach` side effects, `Sink.find` predicate search and early
  completion, failure propagation, invalid builder inputs, replayability
  semantics, sentinel identity behavior, backpressure, and cleanup.
- RBS validation for public API signatures.
- RuboCop with only Layout and Lint departments enabled.
- GitHub Actions running tests, RBS validation, and RuboCop on Ruby 4.x.
- Future integration tests running FiberStream operations under `async`.

## Follow-Up Designs

- `docs/design-docs/async-boundary.md` defines the first non-blocking fiber and
  cancellation contracts for `Flow.async`.
- Larger buffering remains deferred until the async boundary cancellation model
  has been implemented and validated.
