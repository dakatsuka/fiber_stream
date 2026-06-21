# Flow.filter_map

## Status

Accepted

## Problem

Users need a single flow that can transform elements and drop absent results
without building a stateful combination of `map` and `select`. This matches the
common Ruby `filter_map` shape and avoids allocating intermediate wrapper
values when a transform naturally returns either a useful value or a falsey
absence marker.

## Goals

- Add `Flow.filter_map { |element| ... }`.
- Add `Source#filter_map { |element| ... }` as the convenience method.
- Call the block once for each upstream element observed by the stage.
- Emit the block result when it is truthy.
- Drop the element when the block result is `false` or `nil`.
- Preserve pull-driven backpressure and output ordering.
- Provide RBS signatures for public APIs.

## Non-Goals

- Emitting `false` or `nil`; users should use `map` when falsey values are
  meaningful outputs.
- Keeping the original element; users should use `select`.
- Producing multiple output elements from one input element.
- Asynchronous, buffered, parallel, or Ractor-backed transform execution.

## Requirements

- `FiberStream::Flow.filter_map { |element| ... }` creates a transform-and-drop
  flow.
- `Source#filter_map { |element| ... }` is a convenience equivalent to
  `Source#via(FiberStream::Flow.filter_map { ... })`.
- `Flow.filter_map` requires a block. Missing blocks raise `ArgumentError`.
- `Source#filter_map` requires a block. Missing blocks raise `ArgumentError`.
- The flow is lazy. Construction does not pull upstream or call the block.
- For each upstream element pulled by the stage, the block is called exactly
  once.
- When the block result is truthy, that result is emitted downstream.
- When the block result is `false` or `nil`, no value is emitted for that
  upstream element.
- `false` and `nil` are never valid outputs from this flow because they are the
  drop signals.
- Output order matches the order of accepted upstream elements.
- A single downstream demand may pull multiple upstream elements until the
  block returns a truthy value or upstream completes.
- Rejected block results are discarded immediately and are not buffered.
- The flow emits at most one downstream element for each upstream element.
- If upstream completes before another truthy block result, downstream
  completes.
- After upstream completion is observed, the internal pull stage should avoid
  pulling upstream again during defensive repeated pulls. Public consumers must
  still stop after stream completion.
- Block exceptions fail the stream and are re-raised from `run_with`.
- Upstream failures fail the stream and are re-raised from `run_with`.
- Materialized-chain cleanup follows existing `Source#run_with` error
  precedence, where the primary stream failure takes precedence over close
  failures.
- Cleanup close failures after otherwise successful completion or early sink
  completion fail `run_with`.
- Public APIs never expose the private `Pull::DONE` sentinel.

## Public Contracts

```ruby
FiberStream::Flow.filter_map { |element| transformed_value_or_falsey }
FiberStream::Source#filter_map { |element| transformed_value_or_falsey }
```

RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def filter_map: [Out] () { (Elem) -> (Out | false | nil) } -> Source[Out]
  end

  class Flow[In, Out]
    def self.filter_map: [In, Out] () { (In) -> (Out | false | nil) } -> Flow[In, Out]
  end
end
```

The `Out` type parameter represents values that can be emitted. Falsey block
results are control signals and are excluded from the output stream at runtime.
RBS cannot prove that `Out` itself excludes `false` or `nil`, so the signature
approximates the accepted block shape while the runtime contract defines the
actual falsey-drop behavior.

## Examples

```ruby
result =
  FiberStream::Source.each(["1", "x", "2"])
    .filter_map { |text| Integer(text, exception: false) }
    .run_with(FiberStream::Sink.to_a)

result # => [1, 2]
```

Only truthy transformed values are emitted:

```ruby
result =
  FiberStream::Source.each([1, 2, 3, 4])
    .filter_map { |number| number.even? ? "even:#{number}" : nil }
    .run_with(FiberStream::Sink.to_a)

result # => ["even:2", "even:4"]
```

Falsey transform results are dropped, not emitted:

```ruby
result =
  FiberStream::Source.each([true, false, nil])
    .filter_map { |value| value }
    .run_with(FiberStream::Sink.to_a)

result # => [true]
```

Static typing may approximate the output type more broadly than runtime output:

```ruby
result =
  FiberStream::Source.each([true, false, nil])
    .filter_map { |value| value }
    .run_with(FiberStream::Sink.to_a)

result # => [true], even though an RBS checker may infer a broader boolean-ish type
```

## Open Questions

None.
