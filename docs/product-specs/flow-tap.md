# Flow.tap

## Status

Accepted

## Problem

Users need a way to observe elements in the middle of a pipeline for logging,
metrics, tracing, or other side effects without changing the stream values or
materializing the stream at that point.

## Goals

- Add `FiberStream::Flow.tap { |element| ... }`.
- Preserve input values and ordering unchanged.
- Preserve pull-driven backpressure with no buffering or scheduler
  requirement.
- Propagate failures from the tap block as stream failures.
- Provide an RBS signature for the public API.

## Non-Goals

- Terminal side-effect materialization; use `Sink.foreach`.
- Asynchronous side effects, retry, timeout, batching, or error recovery.
- Transforming elements; use `Flow.map`.
- Filtering elements; use `Flow.select` or related flows.

## Requirements

- `FiberStream::Flow.tap { |element| ... }` creates a pass-through observing
  flow.
- `Flow.tap` requires a block.
- Missing blocks raise `ArgumentError`.
- Construction is lazy and does not pull upstream or call the block.
- For each upstream value emitted downstream, the block is called exactly once
  before the value is returned downstream.
- The value returned by the block is ignored.
- The original upstream value is emitted unchanged, preserving object identity.
- Upstream completion does not call the block.
- Upstream failures before a value is produced do not call the block.
- Block failures fail the stream and suppress the current value.
- Downstream early completion and downstream failure use the existing
  materialized-chain cleanup path.
- Repeated downstream pulls after completion return completion without pulling
  upstream or calling the block again.

## Public Contracts

```ruby
FiberStream::Flow.tap { |element| ... }
```

RBS shape:

```rbs
module FiberStream
  class Flow[In, Out]
    def self.tap: [Elem] () { (Elem) -> void } -> Flow[Elem, Elem]
  end
end
```

`Source#tap` is intentionally not added because every Ruby object already has
`Object#tap`, which eagerly yields the receiver itself and returns the same
object. A stream-observing `Source#tap` would silently change that standard
Ruby behavior for source instances. Use `source.via(FiberStream::Flow.tap {
... })` instead.

## Examples

```ruby
seen = []

result =
  FiberStream::Source.each([1, 2, 3])
    .via(FiberStream::Flow.tap { |number| seen << number })
    .map { |number| number * 10 }
    .run_with(FiberStream::Sink.to_a)

result # => [10, 20, 30]
seen   # => [1, 2, 3]
```

Use `tap` before a side-effecting stage to observe inputs, or after it to
observe outputs:

```ruby
FiberStream::Source.each(events)
  .via(FiberStream::Flow.tap { |event| logger.debug("received #{event.id}") })
  .map { |event| normalize(event) }
  .via(FiberStream::Flow.tap { |event| metrics.increment("normalized") })
  .run_with(FiberStream::Sink.to_a)
```

## Open Questions

None.
