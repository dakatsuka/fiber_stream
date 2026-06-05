# Flow.grouped

## Status

Draft

## Problem

Users need a simple way to batch adjacent stream elements into fixed-size
arrays while preserving FiberStream's lazy, pull-driven backpressure model.

## Goals

- Add `FiberStream::Flow.grouped(count)`.
- Add `Source#grouped(count)` as a convenience wrapper.
- Emit non-overlapping groups of up to `count` elements.
- Preserve upstream element order inside each group and across groups.
- Emit a final partial group when upstream completes normally.
- Bound per-stage memory to at most `count` retained elements.
- Provide RBS signatures for public APIs.

## Non-Goals

- Key-based grouping.
- Time-based, size-or-time, sliding, tumbling, or overlapping windows.
- Dropping final partial groups.
- Scheduler-backed batching or asynchronous prefetch.
- Aggregation, folding, or reduction within a group.

## Requirements

- `FiberStream::Flow.grouped(count)` creates a grouping flow.
- `Source#grouped(count)` is a convenience equivalent to
  `Source#via(FiberStream::Flow.grouped(count))`.
- `count` must be an `Integer`.
- `count` is accepted only when `count.is_a?(Integer)`; no `to_int` or
  `Integer(count)` coercion is performed.
- `count` must be positive.
- `Flow.grouped(count)` raises `TypeError` when `count` is not an `Integer`.
- `Flow.grouped(count)` raises `ArgumentError` when `count` is zero or
  negative.
- Construction is lazy and does not pull upstream.
- Each downstream demand pulls upstream until one of these happens:
  - `count` elements have been collected;
  - upstream completes normally after at least one element has been collected;
  - upstream completes normally before any element is collected;
  - upstream fails.
- A full group contains exactly `count` elements.
- When a full group is emitted, `grouped` must not pull upstream again during
  that downstream demand. Completion after an exact multiple of `count`
  elements is observed only on the next downstream demand.
- When upstream completes normally after a partial group has been collected,
  that partial group is emitted once.
- When upstream completes normally before any element is collected, downstream
  completes without emitting an empty group.
- Upstream element order is preserved in emitted arrays.
- `grouped(1)` emits each upstream element wrapped in a one-element array.
- Each emitted group is a distinct `Array` object owned by the downstream
  consumer.
- Stream elements are passed through unchanged by identity; `grouped` does not
  copy, freeze, or transform element values.
- At most `count` elements are retained inside the grouping stage between
  downstream pulls.
- There is no explicit maximum `count`; memory use is intentionally
  proportional to `count`, and callers are responsible for choosing a practical
  value.
- `Flow.grouped` itself does not require `Fiber.scheduler`.
- If upstream fails while a group is being collected, the stream fails and the
  partially collected group is not emitted.
- Upstream failures are re-raised from `Source#run_with`. Materialized-chain
  cleanup follows existing `Source#run_with` error precedence, where the
  primary stream failure takes precedence over close failures.
- Closing the materialized grouping stage closes upstream.
- After upstream completion is observed and any final partial group has been
  emitted, later defensive pulls return completion without pulling upstream
  again.
- Public APIs never expose the private `Pull::DONE` sentinel.

## Public Contracts

```ruby
FiberStream::Flow.grouped(count)
FiberStream::Source#grouped(count)
```

RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def grouped: (Integer count) -> Source[Array[Elem]]
  end

  class Flow[In, Out]
    def self.grouped: [Elem] (Integer count) -> Flow[Elem, Array[Elem]]
  end
end
```

## Examples

```ruby
result =
  FiberStream::Source.each([1, 2, 3, 4, 5])
    .grouped(2)
    .run_with(FiberStream::Sink.to_a)

result # => [[1, 2], [3, 4], [5]]
```

`grouped(1)` keeps every element but wraps it in an array:

```ruby
result =
  FiberStream::Source.each(["a", "b"])
    .via(FiberStream::Flow.grouped(1))
    .run_with(FiberStream::Sink.to_a)

result # => [["a"], ["b"]]
```

An empty source emits no groups:

```ruby
result =
  FiberStream::Source.each([])
    .grouped(3)
    .run_with(FiberStream::Sink.to_a)

result # => []
```

## Open Questions

None.
