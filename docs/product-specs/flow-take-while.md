# Flow.take_while

## Status

Accepted

## Problem

Users need a predicate-based early-completion flow that keeps a leading prefix
of elements while a condition holds. This complements fixed-count `Flow.take`
and supports common stream termination cases without buffering.

## Goals

- Add `Flow.take_while { |element| ... }`.
- Add `Source#take_while { |element| ... }` as the convenience method.
- Emit leading elements whose predicate result is truthy.
- Complete before emitting the first element whose predicate result is false or
  `nil`.
- Close upstream when the predicate first fails so resource-owning stages can
  release work promptly.
- Preserve pull-driven backpressure and ordering.
- Provide RBS signatures for public APIs.

## Non-Goals

- Dropping while a predicate holds; that belongs to `drop_while`.
- Keeping non-prefix matches; users should use `select`.
- Returning the terminating element.
- Asynchronous or buffered predicate execution.

## Requirements

- `FiberStream::Flow.take_while { |element| ... }` creates a predicate-based
  limiting flow.
- `Source#take_while { |element| ... }` is a convenience equivalent to
  `Source#via(FiberStream::Flow.take_while { ... })`.
- `Flow.take_while` requires a block. Missing blocks raise `ArgumentError`.
- For each downstream demand before completion, the flow makes at most one
  immediate upstream pull. Earlier upstream stages keep their own pull and
  bounded run-ahead contracts.
- If upstream completes before a predicate fails, downstream completes normally.
- If the predicate returns a truthy value, the upstream element is emitted
  unchanged.
- If the predicate returns `false` or `nil`, that element is not emitted and
  upstream is closed during that same downstream pull.
- If that same-pull upstream close succeeds, downstream receives completion.
- If that same-pull upstream close raises, the stream fails with the close
  failure instead of completing normally.
- After the predicate fails or upstream completion is observed, normal sinks
  observe completion and public APIs never expose the internal completion
  sentinel.
- Predicate exceptions fail the stream and are re-raised from `run_with`.
- Upstream failures fail the stream and are re-raised from `run_with`.
- Materialized-chain cleanup follows existing `Source#run_with` error
  precedence, where the primary stream failure takes precedence over close
  failures.
- Public APIs never expose the private `Pull::DONE` sentinel.

## Public Contracts

```ruby
FiberStream::Flow.take_while { |element| truthy_or_falsey }
FiberStream::Source#take_while { |element| truthy_or_falsey }
```

RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def take_while: () { (Elem) -> boolish } -> Source[Elem]
  end

  class Flow[In, Out]
    def self.take_while: [Elem] () { (Elem) -> boolish } -> Flow[Elem, Elem]
  end
end
```

## Examples

```ruby
result =
  FiberStream::Source.each([1, 2, 3, 1])
    .take_while { |number| number < 3 }
    .run_with(FiberStream::Sink.to_a)

result # => [1, 2]
```

The terminating element is not emitted:

```ruby
result =
  FiberStream::Source.each([1, 2, 3])
    .take_while(&:odd?)
    .run_with(FiberStream::Sink.to_a)

result # => [1]
```

## Open Questions

None.
