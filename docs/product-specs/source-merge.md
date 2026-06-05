# Source.merge

## Status

Draft

## Problem

Users need to combine two independently progressing sources into one stream
without waiting for one source to complete before the other can emit values.
Concatenation is sequential and zipping waits for pairs; merge should emit
values from either input as they become available while preserving
FiberStream's bounded backpressure and cleanup guarantees.

## Goals

- Add a lazy `Source#merge(source)` public API.
- Materialize and run both input sources concurrently after downstream demand
  reaches the merge.
- Emit elements from either input source in ready order.
- Preserve element order within each input source.
- Complete only after both input sources complete normally.
- Bound queued merge messages.
- Close both materialized input sources on normal completion, early downstream
  completion, and failures.
- Keep FiberStream independent of the `async` gem at runtime.
- Provide RBS signatures for the public API.

## Non-Goals

- Variadic merge.
- Deterministic round-robin interleaving.
- Ordered merge by timestamp, priority, sequence number, or comparator.
- Fairness guarantees between inputs.
- Merging latest values or pairing values.
- Unbounded buffering.
- Configurable buffer size or overflow policies in the first API slice.
- Installing or selecting a scheduler.
- Automatic element type coercion.

## Requirements

- `Source#merge(source)` returns a new source definition.
- Passing a non-`FiberStream::Source` object to `Source#merge` raises
  `TypeError`.
- `Source#merge` is lazy. It does not materialize either source, enumerate
  values, run flow blocks, start scheduler-backed boundaries, read IO, or close
  resources at construction time.
- The merge is not started during source materialization.
- Closing a materialized merge before the first downstream pull does not
  require a scheduler, does not start producers, and does not materialize or
  close either input source.
- The first downstream pull that reaches the merge starts one producer fiber
  for the receiver source and one producer fiber for the other source.
- If no `Fiber.scheduler` is installed when the merge producers must start,
  the downstream pull raises `FiberStream::SchedulerRequiredError`.
- If `Fiber.scheduler` is installed but the downstream pull reaches merge from
  a blocking fiber, the downstream pull raises
  `FiberStream::SchedulerRequiredError`.
- Calling `Source#merge(source)` itself never requires a scheduler.
- FiberStream does not install a scheduler and does not depend on Async at
  runtime.
- `Source#merge` does not make blocking source operations non-blocking.
  Scheduler-unaware blocking IO or CPU-bound work inside either input source
  can block the scheduler thread and delay the other input source.
- CPU-bound or scheduler-unaware blocking producer work should be isolated by
  the application, for example with producer ractors connected through
  `Source.ractor_port`, rather than relying on `Source#merge` for parallelism.
- Both input sources are materialized by their producer fibers after the merge
  starts.
- Values from either input source may be emitted first.
- Ready order means arrival order at the internal merge boundary under the
  installed scheduler.
- The merged source preserves element order within the receiver source.
- The merged source preserves element order within the other source.
- The merged source does not guarantee deterministic ordering between values
  from different input sources.
- The merged source does not guarantee that a ready value from one side will be
  emitted within any bounded number of downstream pulls when the other side
  repeatedly wins scheduler and merge-boundary races.
- Each emitted downstream value is the original source element. Merge does not
  wrap, copy, freeze, tag, or transform element values.
- The internal merge mailbox holds at most one message at a time. Value,
  completion, and error messages all count toward this queued bound.
- Each producer may hold at most one additional in-flight message while waiting
  to enqueue into a full merge mailbox.
- While a producer is blocked on enqueue, that producer does not pull farther
  upstream.
- If the merge boundary is closed while a producer is waiting to enqueue, that
  enqueue is cancelled internally, the producer stops pulling, and the
  boundary-close signal is not exposed as a stream failure.
- When one input source completes normally, the merged source continues
  emitting values from the other input source.
- The merged source completes only after both input sources have completed
  normally and both producer-side input closes have succeeded.
- If an input source completes normally and producer-side close for that input
  fails, that close failure is delivered downstream as a stream failure.
- If an input source pull and producer-side close both fail, the input pull
  failure is delivered downstream and the close failure is suppressed.
- If an input source fails, the failure is re-raised from `Source#run_with`.
- On input failure, merge closes the other materialized input source and
  requests producer cancellation.
- If both inputs fail, the failure whose error message is observed first by the
  downstream merge stage is primary. Later input failures are suppressed.
- Queued values that were enqueued before an observed error may be emitted
  before the error. Values queued or in flight after the observed error are
  discarded.
- If downstream intentionally completes early or fails, merge closes both
  materialized input sources and requests producer cancellation.
- If downstream intentionally completes early or fails before consuming a queued
  or in-flight upstream error, that upstream error is suppressed in favor of the
  downstream completion or failure.
- During downstream cleanup after normal early completion, user failures raised
  by input `close` are propagated from `Source#run_with`. If both input closes
  fail, the receiver close failure is primary and the other close failure is
  suppressed.
- During downstream cleanup after downstream failure, the downstream failure is
  primary and input close failures are suppressed.
- Producer cancellation and closed-boundary wakeups are internal and are always
  suppressed when they are caused by merge close.
- If materializing either input source raises, the materialization failure is
  re-raised from `Source#run_with`. Already materialized input sources are
  closed by merge cleanup; unmaterialized input sources are not closed.
- If the receiver and other source are the same `Source` object, each side is
  independently materialized from that source definition. FiberStream does not
  snapshot or make one-shot sources replayable.
- Flows attached to the receiver before `merge` apply only to receiver
  elements. Flows attached to the other source before `merge` apply only to
  other-source elements. Flows attached after `merge` apply to the combined
  output from both sources.
- Public APIs never expose internal merge messages or the private `Pull::DONE`
  sentinel.

## Public Contracts

```ruby
FiberStream::Source#merge(source)
FiberStream::SchedulerRequiredError
```

RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def merge: [Other] (Source[Other] source) -> Source[Elem | Other]
  end
end
```

## Examples

```ruby
require "async"
require "fiber_stream"

result =
  Async do
    FiberStream::Source.each([1, 2])
      .merge(FiberStream::Source.each(["a", "b"]))
      .run_with(FiberStream::Sink.to_a)
  end.wait

# Possible result: [1, "a", 2, "b"]
# Other interleavings are allowed, but each input's own order is preserved.
```

The merged source completes after both sides complete:

```ruby
result =
  Async do
    FiberStream::Source.each([1])
      .merge(FiberStream::Source.each([2, 3]))
      .run_with(FiberStream::Sink.to_a)
  end.wait

result.sort # => [1, 2, 3]
```

Flows before and after merge keep their normal scope:

```ruby
result =
  Async do
    FiberStream::Source.each([1, 2])
      .map { |number| number * 10 }
      .merge(FiberStream::Source.each([3]).map { |number| number * 100 })
      .map(&:to_s)
      .run_with(FiberStream::Sink.to_a)
  end.wait

result.sort # => ["10", "20", "300"]
```

When no scheduler is installed:

```ruby
FiberStream::Source.each([1])
  .merge(FiberStream::Source.each([2]))
  .run_with(FiberStream::Sink.to_a)

# raises FiberStream::SchedulerRequiredError
```

Merging the same source definition materializes it independently on each side:

```ruby
source = FiberStream::Source.each([1, 2])

result =
  Async do
    source.merge(source).run_with(FiberStream::Sink.to_a)
  end.wait

result.sort # => [1, 1, 2, 2]
```

`merge` is intended for sources that can cooperate with `Fiber.scheduler`.
It does not move blocking source work onto OS threads or ractors.

## Open Questions

None.
