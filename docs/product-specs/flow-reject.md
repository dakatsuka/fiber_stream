# Flow.reject

## Status

Accepted

## Problem

Users need the complement of `Flow.select`: a predicate-based flow that drops
elements matching a condition and passes the non-matching elements through
unchanged. This matches Ruby's `Enumerable#reject` shape and avoids forcing
users to negate predicates manually inside `select`.

## Goals

- Add `Flow.reject { |element| ... }`.
- Add `Source#reject { |element| ... }` as the convenience method.
- Call the predicate once for each upstream element observed by the stage.
- Drop elements when the predicate result is truthy.
- Emit original elements unchanged when the predicate result is `false` or
  `nil`.
- Preserve pull-driven backpressure and output ordering.
- Provide RBS signatures for public APIs.

## Non-Goals

- Transforming retained elements; users should compose `map` or use
  `filter_map` when transformation is part of the operation.
- Prefix-only rejection; users should use `drop_while`.
- Early completion; users should use `take_while`.
- Asynchronous, buffered, parallel, or Ractor-backed predicate execution.

## Requirements

- `FiberStream::Flow.reject { |element| ... }` creates a filtering flow that
  drops elements whose predicate result is truthy.
- `Source#reject { |element| ... }` is a convenience equivalent to
  `Source#via(FiberStream::Flow.reject { ... })`.
- `Flow.reject` requires a block. Missing blocks raise `ArgumentError`.
- `Source#reject` requires a block. Missing blocks raise `ArgumentError`.
- The flow is lazy. Construction does not pull upstream or call the predicate.
- For each upstream element pulled by the stage, the predicate is called
  exactly once.
- When the predicate result is truthy, including non-boolean truthy values such
  as `0` or symbols, the upstream element is dropped.
- When the predicate result is `false` or `nil`, the original upstream element
  is emitted unchanged.
- Retained values preserve object identity.
- Output order matches the order of retained upstream elements.
- A single downstream demand may pull multiple upstream elements until the
  predicate returns `false` or `nil`, or upstream completes.
- Rejected elements are discarded immediately and are not buffered.
- The flow emits at most one downstream element for each upstream element.
- If upstream completes before another retained element, downstream completes.
- After upstream completion is observed, the internal pull stage should avoid
  pulling upstream again during defensive repeated pulls. Public consumers must
  still stop after stream completion.
- Predicate exceptions fail the stream and are re-raised from `run_with`.
- Upstream failures fail the stream and are re-raised from `run_with`.
- Predicate and upstream failures still trigger materialized-chain cleanup,
  including upstream close propagation.
- Materialized-chain cleanup follows existing `Source#run_with` error
  precedence, where the primary stream failure takes precedence over close
  failures.
- Cleanup close failures after otherwise successful completion or early sink
  completion fail `run_with`.
- Public APIs never expose the private `Pull::DONE` sentinel.

## Public Contracts

```ruby
FiberStream::Flow.reject { |element| truthy_or_falsey }
FiberStream::Source#reject { |element| truthy_or_falsey }
```

RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def reject: () { (Elem) -> boolish } -> Source[Elem]
  end

  class Flow[In, Out]
    def self.reject: [Elem] () { (Elem) -> boolish } -> Flow[Elem, Elem]
  end
end
```

## Examples

```ruby
result =
  FiberStream::Source.each([1, 2, 3, 4])
    .reject(&:even?)
    .run_with(FiberStream::Sink.to_a)

result # => [1, 3]
```

Falsey predicate results retain the original element:

```ruby
result =
  FiberStream::Source.each([1, 2, 3])
    .reject { |number| number == 2 ? nil : false }
    .run_with(FiberStream::Sink.to_a)

result # => [1, 2, 3]
```

Truthy predicate results drop elements:

```ruby
result =
  FiberStream::Source.each([1, 2, 3, 4])
    .reject { |number| number > 2 }
    .run_with(FiberStream::Sink.to_a)

result # => [1, 2]
```

## Open Questions

None.
