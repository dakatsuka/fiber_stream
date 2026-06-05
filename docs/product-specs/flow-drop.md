# Flow.drop

## Status

Accepted

## Problem

Users need a basic way to skip a fixed number of leading stream elements while
preserving FiberStream's lazy, pull-driven execution model.

## Goals

- Add `Flow.drop(count)` and `Source#drop(count)`.
- Drop the first `count` upstream elements, then pass later elements through
  unchanged.
- Preserve pull-driven backpressure with no buffering.
- Match `Flow.take` validation style.
- Provide RBS signatures for public APIs.

## Non-Goals

- Predicate-based dropping; this is reserved for a later `drop_while` API.
- Time-based dropping.
- Dropping from the end of a stream.
- Batch or window operations.

## Requirements

- `FiberStream::Flow.drop(count)` creates a dropping flow.
- `Source#drop(count)` is a convenience equivalent to
  `Source#via(FiberStream::Flow.drop(count))`.
- `Flow.drop(count)` drops at most the first `count` upstream elements.
- After `count` elements have been dropped, later elements pass downstream
  unchanged and in order.
- `Flow.drop(0)` drops no elements and behaves as a pass-through stage.
- If upstream completes before `count` elements are dropped, downstream
  completes without emitting any elements.
- The flow is lazy. Construction does not pull upstream.
- The first downstream demand may pull up to `count + 1` upstream elements to
  drop the prefix and produce the first retained element.
- After the prefix has been dropped, each downstream demand pulls at most one
  upstream element unless another composed stage pulls more.
- After upstream completion is observed, later defensive pulls return
  completion without pulling upstream again.
- Upstream failures while dropping the prefix or forwarding retained elements
  fail the stream and are re-raised from `run_with`. Materialized-chain cleanup
  follows existing `Source#run_with` error precedence, where the primary stream
  failure takes precedence over close failures.
- `Flow.drop(count)` raises `TypeError` when `count` is not an `Integer`.
- `Flow.drop(count)` raises `ArgumentError` when `count` is negative.
- Public APIs never expose the private `Pull::DONE` sentinel.

## Public Contracts

```ruby
FiberStream::Flow.drop(count)
FiberStream::Source#drop(count)
```

RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def drop: (Integer count) -> Source[Elem]
  end

  class Flow[In, Out]
    def self.drop: [Elem] (Integer count) -> Flow[Elem, Elem]
  end
end
```

## Examples

```ruby
result =
  FiberStream::Source.each([1, 2, 3, 4])
    .drop(2)
    .run_with(FiberStream::Sink.to_a)

result # => [3, 4]
```

`drop(0)` passes values through:

```ruby
result =
  FiberStream::Source.each([1, 2])
    .drop(0)
    .run_with(FiberStream::Sink.to_a)

result # => [1, 2]
```

## Open Questions

None.
