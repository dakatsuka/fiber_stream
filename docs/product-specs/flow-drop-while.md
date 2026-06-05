# Flow.drop_while

## Status

Accepted

## Problem

Users need a predicate-based flow that skips a leading prefix while a condition
holds, then forwards the first non-matching element and the rest of the stream.
This complements `Flow.take_while` and avoids forcing users to hand-roll stateful
filters.

## Goals

- Add `Flow.drop_while { |element| ... }`.
- Add `Source#drop_while { |element| ... }` as the convenience method.
- Drop leading elements whose predicate result is truthy.
- Emit the first element whose predicate result is false or `nil`.
- After the first retained element, pass all later elements through unchanged
  without calling the predicate again.
- Preserve pull-driven backpressure and ordering.
- Provide RBS signatures for public APIs.

## Non-Goals

- Prefix retention; users should use `take_while`.
- Filtering all matching elements; users should use `select`.
- Returning the dropped prefix.
- Asynchronous or buffered predicate execution.

## Requirements

- `FiberStream::Flow.drop_while { |element| ... }` creates a predicate-based
  prefix-dropping flow.
- `Source#drop_while { |element| ... }` is a convenience equivalent to
  `Source#via(FiberStream::Flow.drop_while { ... })`.
- `Flow.drop_while` requires a block. Missing blocks raise `ArgumentError`.
- While in the dropping phase, each downstream demand pulls upstream until it
  sees completion or finds the first element whose predicate result is false or
  `nil`.
- Leading elements whose predicate result is truthy are dropped.
- The first element whose predicate result is false or `nil` is emitted
  unchanged.
- After the first retained element is emitted, later elements pass downstream
  unchanged and the predicate is not called again.
- If upstream completes while all observed elements are still being dropped,
  downstream completes without emitting any element.
- The flow is lazy. Construction does not pull upstream or call the predicate.
- The first downstream demand may pull multiple upstream elements to drop the
  prefix and produce the first retained element.
- After the first retained element has been emitted, each downstream demand
  pulls at most one upstream element unless another composed stage pulls more.
- After upstream completion is observed, the internal pull stage should avoid
  pulling upstream again during defensive repeated pulls. Public consumers must
  still stop after stream completion.
- Predicate exceptions fail the stream and are re-raised from `run_with`.
- Upstream failures fail the stream and are re-raised from `run_with`.
- Materialized-chain cleanup follows existing `Source#run_with` error
  precedence, where the primary stream failure takes precedence over close
  failures.
- Cleanup close failures after otherwise successful completion or early sink
  completion fail `run_with`.
- Public APIs never expose the private `Pull::DONE` sentinel.

## Public Contracts

```ruby
FiberStream::Flow.drop_while { |element| truthy_or_falsey }
FiberStream::Source#drop_while { |element| truthy_or_falsey }
```

RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def drop_while: () { (Elem) -> boolish } -> Source[Elem]
  end

  class Flow[In, Out]
    def self.drop_while: [Elem] () { (Elem) -> boolish } -> Flow[Elem, Elem]
  end
end
```

## Examples

```ruby
result =
  FiberStream::Source.each([1, 2, 3, 1])
    .drop_while { |number| number < 3 }
    .run_with(FiberStream::Sink.to_a)

result # => [3, 1]
```

The first falsey predicate result is emitted:

```ruby
result =
  FiberStream::Source.each([1, 2, 3])
    .drop_while(&:odd?)
    .run_with(FiberStream::Sink.to_a)

result # => [2, 3]
```

## Open Questions

None.
