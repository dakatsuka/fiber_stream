# Flow.map_concat

## Status

Accepted

## Problem

Users need a pull-driven way to transform one upstream element into zero or
more downstream elements while preserving input order and without introducing
parallelism, buffering, or source-level graph composition. This covers common
flattening workflows such as tokenizing records, expanding small ranges, and
dropping inputs by returning an empty enumerable.

## Goals

- Add `FiberStream::Flow.map_concat { |element| ... }`.
- Add `Source#map_concat { |element| ... }` as the convenience method.
- Call the block once for each upstream element whose expansion is needed.
- Require the block result to provide `#each`.
- Emit all elements from one block result before pulling and expanding the next
  upstream element.
- Preserve pull-driven backpressure and output ordering.
- Avoid eager materialization of block results.
- Provide RBS signatures for public APIs.

## Non-Goals

- Concurrent, unordered, scheduler-backed, or Ractor-backed expansion.
- Accepting `FiberStream::Source` values as inner streams.
- Automatically closing or otherwise owning resources returned by the block.
- Treating `nil` or `false` as "no output" signals.
- Recursively flattening nested enumerable values.
- String-specific character, line, or byte splitting.

## Requirements

- `FiberStream::Flow.map_concat { |element| enumerable }` creates a
  transform-and-flatten flow.
- `Source#map_concat { |element| enumerable }` is a convenience equivalent to
  `Source#via(FiberStream::Flow.map_concat { ... })`.
- `Flow.map_concat` requires a block. Missing blocks raise `ArgumentError`.
- `Source#map_concat` requires a block through delegation. Missing blocks raise
  `ArgumentError`.
- Construction is lazy and does not pull upstream, call the block, or enumerate
  block results.
- For each upstream element expanded by the stage, the block is called exactly
  once.
- The block result must respond to `#each`.
- The block result does not need to include `Enumerable`; any object with a
  zero-argument `#each` that yields values to a block is accepted.
- A block result that does not respond to `#each` raises `TypeError`.
- `nil` and `false` block results raise `TypeError`; callers should return an
  empty enumerable when an input should emit no output.
- The block result's `#each` method must be callable without required
  arguments. Argument errors or other failures from `#each` fail the stream as
  inner enumeration failures.
- Each block result is enumerated with its `#each` method.
- Output elements are emitted exactly as yielded by the block result.
- Values yielded by the block result may be `nil` or `false`; yielded falsey
  values are emitted like any other output value.
- The flow is one-level flattening only. If a block result yields arrays or
  other enumerable values, those yielded values are emitted unchanged.
- All outputs from one block result are emitted before the next upstream element
  is pulled.
- Empty block results emit no downstream elements.
- A single downstream demand may pull and expand multiple upstream elements
  when earlier block results are empty.
- A downstream demand that emits a value must not pull the next upstream element
  after finding that value.
- Output order follows upstream order and each block result's yield order.
- The flow retains at most one active block result/enumerator at a time. It
  does not copy the block result into an array.
- Memory use can still be proportional to the object returned by the block,
  because the stage holds that object until its enumeration completes.
- Infinite block results are allowed but prevent later upstream elements from
  being pulled.
- `Flow.map_concat` itself does not require `Fiber.scheduler`.
- `String` block results are not special-cased. On supported Ruby versions,
  `String` does not provide `#each`, so returning a `String` raises `TypeError`.
  Callers that want string-derived expansion should return an explicit
  enumerable such as `text.each_line` or `text.each_char`.
- Hash block results are enumerated by normal Ruby `Hash#each` behavior, so
  they emit key-value pairs.
- The stage does not automatically call `close` on block results, including
  objects that respond to `close`. Callers should not return resource-owning
  enumerables unless abandoning or exhausting them without an automatic close is
  acceptable.
- If upstream completes while no active block result remains, downstream
  completes.
- After upstream completion is observed, later defensive pulls return
  completion without pulling upstream again.
- Block exceptions fail the stream and are re-raised from `run_with`.
- `StopIteration` raised while advancing the active external iterator means
  the current block result is exhausted. Because `map_concat` uses Ruby's
  external iterator protocol, callers that need to signal an inner
  enumeration failure should raise another exception type.
- Exceptions raised while enumerating a block result, other than
  `StopIteration` surfaced by the active external iterator, fail the stream
  and are re-raised from `run_with`.
- Upstream failures fail the stream and are re-raised from `run_with`.
- Materialized-chain cleanup follows existing `Source#run_with` error
  precedence, where the primary stream failure takes precedence over close
  failures.
- Cleanup close failures after otherwise successful completion or early sink
  completion fail `run_with`.
- Closing the materialized map-concat stage closes upstream.
- Closing the materialized map-concat stage while a block result is active does
  not drain the active result and does not pull later upstream elements.
- Public APIs never expose the private `Pull::DONE` sentinel.

## Public Contracts

```ruby
FiberStream::Flow.map_concat { |element| enumerable }
FiberStream::Source#map_concat { |element| enumerable }
```

RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def map_concat: [Out] () { (Elem) -> Enumerable[Out] } -> Source[Out]
  end

  class Flow[In, Out]
    def self.map_concat: [In, Out] () { (In) -> Enumerable[Out] } -> Flow[In, Out]
  end
end
```

The runtime contract is `respond_to?(:each)`. The RBS signature uses
`Enumerable[Out]` as the public typing approximation for that contract.

## Examples

Expand one input into many outputs:

```ruby
result =
  FiberStream::Source.each([1, 3])
    .map_concat { |count| 1..count }
    .run_with(FiberStream::Sink.to_a)

result # => [1, 1, 2, 3]
```

Return an empty enumerable to emit no output for an input:

```ruby
result =
  FiberStream::Source.each(["a b", "", "c"])
    .via(FiberStream::Flow.map_concat { |line| line.empty? ? [] : line.split })
    .run_with(FiberStream::Sink.to_a)

result # => ["a", "b", "c"]
```

The flow flattens one level only:

```ruby
result =
  FiberStream::Source.each([1])
    .map_concat { |_value| [[1, 2], [3, 4]] }
    .run_with(FiberStream::Sink.to_a)

result # => [[1, 2], [3, 4]]
```

Strings must be expanded explicitly:

```ruby
result =
  FiberStream::Source.each(["ab"])
    .map_concat { |text| text.each_char }
    .run_with(FiberStream::Sink.to_a)

result # => ["a", "b"]
```

Yielded falsey values are outputs:

```ruby
result =
  FiberStream::Source.each([1])
    .map_concat { |_value| [nil, false, true] }
    .run_with(FiberStream::Sink.to_a)

result # => [nil, false, true]
```

## Open Questions

None.
