# Minimum Linear Pipeline

## Status

Accepted

## Problem

FiberStream needs a first user-visible stream API that proves the core model
without committing to graph construction, asynchronous boundaries, or IO stages.
The first slice should let users build a linear stream, transform or filter
elements, and materialize the result.

## Goals

- Provide a small public API for `Source.each(...).via(...).run_with(Sink.to_a)`
  plus `Source.each(...).run_with(Sink.first)` and accumulator-based
  materialization with `Sink.fold`.
- Keep stream construction lazy: source enumeration and flow blocks do not run
  until `run_with`.
- Preserve backpressure from the first implementation by advancing upstream only
  when downstream asks for an element.
- Keep the runtime dependency-free. Compatibility with `async` and
  `async-http` streaming response bodies is tested as a development concern,
  not exposed as a runtime requirement.
- Provide RBS signatures for public APIs from the first implementation.

## Non-Goals

- Graph DSLs.
- Asynchronous stage boundaries.
- Bounded buffers.
- Parallel or concurrent mapping.
- IO sources and sinks.
- Background execution APIs such as `run_async`.

## Requirements

- `FiberStream::Source.each(enumerable)` creates a source definition from an
  `Enumerable`.
- The enumerable is not consumed until materialization with `run_with`.
- Each materialization creates an `Enumerator` from `enumerable.to_enum(:each)`;
  source values are pulled from that enumerator.
- Replayability depends on the enumerable. Collections such as `Array` normally
  provide repeatable traversal; one-shot enumerators or mutable custom
  enumerables may produce different values or no values on later runs.
- `Source.each` does not snapshot values.
- `Source.each` supports one-shot streaming enumerable bodies, including
  `Protocol::HTTP::Body::Readable` objects returned by `async-http`, without
  reading all chunks into memory before downstream demand.
- With streaming enumerable bodies, each downstream pull advances the body only
  as far as needed to produce the next FiberStream element unless an explicit
  buffering or parallel boundary is composed downstream.
- `Source.each` does not own or close streaming enumerable bodies after early
  downstream completion. Callers must use the HTTP client's block form or an
  explicit `ensure` close when the upstream resource needs cleanup.
- `Source#via(flow)` returns a new source definition and does not execute the
  stream.
- `Source#via` accepts a `FiberStream::Flow`; invalid flow objects raise
  `TypeError`.
- `Source#map { |element| ... }` is a convenience equivalent to
  `Source#via(FiberStream::Flow.map { ... })`.
- `Source#select { |element| ... }` is a convenience equivalent to
  `Source#via(FiberStream::Flow.select { ... })`.
- `Source#take(count)` is a convenience equivalent to
  `Source#via(FiberStream::Flow.take(count))`.
- `FiberStream::Flow.map { |element| ... }` creates a mapping flow.
- The `map` block is called once for each element pulled through the flow.
- `FiberStream::Flow.select { |element| ... }` creates a filtering flow.
- The `select` block is called for upstream elements until a matching element is
  found or upstream completes.
- `Flow.select` passes through values whose block result is truthy and drops
  values whose block result is false or `nil`.
- `FiberStream::Flow.take(count)` creates a limiting flow.
- `Flow.take(count)` passes through at most `count` elements and then completes
  downstream.
- `Flow.take(0)` completes without pulling upstream and closes upstream on the
  first downstream demand.
- `Flow.take(count)` forwards the count-th element, closes upstream during that
  same pull, and returns completion on later downstream pulls.
- `Flow.take(count)` raises `ArgumentError` when `count` is negative.
- `Flow.take(count)` raises `TypeError` when `count` is not an `Integer`.
- After `Flow.take(count)` reaches its limit, it closes upstream so later stages
  and sources can release runtime state.
- Exceptions raised by source enumeration, flow blocks, or sinks fail the stream
  and are re-raised from `run_with`.
- `FiberStream::Sink.to_a` consumes the complete stream and returns an `Array`.
- `FiberStream::Sink.first` pulls at most one element, returns the first element,
  and returns `nil` when upstream completes before producing a value.
- `FiberStream::Sink.fold(initial) { |accumulator, element| ... }` consumes the
  complete stream and returns the final accumulator.
- `Sink.fold` returns `initial` unchanged when upstream completes before
  producing a value.
- `Source#run_with` accepts a `FiberStream::Sink`; invalid sink objects raise
  `TypeError`.
- `Source#run_with(sink)` runs the stream in the current fiber until completion
  or failure and returns the sink materialized value.
- Cleanup runs after success, failure, and early sink completion.
- `Source.each` does not own or close the original enumerable. Resource-owning
  sources require separate APIs with explicit ownership contracts.
- `Sink.first` is the first public early-completion sink. `run_with` still
  closes the materialized stream after `Sink.first` returns.

## Public Contracts

```ruby
FiberStream::Source.each(enumerable)
FiberStream::Source#via(flow)
FiberStream::Source#map { |element| transformed_element }
FiberStream::Source#select { |element| truthy_or_falsey }
FiberStream::Source#take(count)
FiberStream::Source#run_with(sink)
FiberStream::Flow.map { |element| transformed_element }
FiberStream::Flow.select { |element| truthy_or_falsey }
FiberStream::Flow.take(count)
FiberStream::Sink.to_a
FiberStream::Sink.first
FiberStream::Sink.fold(initial) { |accumulator, element| new_accumulator }
```

Initial RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def self.each: [Elem] (Enumerable[Elem] enumerable) -> Source[Elem]
    def via: [Out] (Flow[Elem, Out] flow) -> Source[Out]
    def map: [Out] () { (Elem) -> Out } -> Source[Out]
    def select: () { (Elem) -> boolish } -> Source[Elem]
    def take: (Integer count) -> Source[Elem]
    def run_with: [Mat] (Sink[Elem, Mat] sink) -> Mat
  end

  class Flow[In, Out]
    def self.map: [In, Out] () { (In) -> Out } -> Flow[In, Out]
    def self.select: [Elem] () { (Elem) -> boolish } -> Flow[Elem, Elem]
    def self.take: [Elem] (Integer count) -> Flow[Elem, Elem]
  end

  class Sink[In, Mat]
    def self.to_a: [Elem] () -> Sink[Elem, Array[Elem]]
    def self.first: [Elem] () -> Sink[Elem, Elem?]
    def self.fold: [Elem, Acc] (Acc initial) { (Acc, Elem) -> Acc } -> Sink[Elem, Acc]
  end
end
```

## Examples

```ruby
result =
  FiberStream::Source.each([1, 2, 3])
    .map { |number| number * 2 }
    .run_with(FiberStream::Sink.to_a)

result # => [2, 4, 6]
```

Multiple mapping flows compose in order:

```ruby
result =
  FiberStream::Source.each([1, 2, 3])
    .map { |number| number * 2 }
    .map(&:to_s)
    .run_with(FiberStream::Sink.to_a)

result # => ["2", "4", "6"]
```

`Flow.select` keeps only matching values:

```ruby
result =
  FiberStream::Source.each([1, 2, 3, 4])
    .select(&:even?)
    .run_with(FiberStream::Sink.to_a)

result # => [2, 4]
```

`Flow.take` limits the number of values:

```ruby
result =
  FiberStream::Source.each([1, 2, 3, 4])
    .take(2)
    .run_with(FiberStream::Sink.to_a)

result # => [1, 2]
```

`Sink.first` pulls only the first value:

```ruby
result =
  FiberStream::Source.each([1, 2, 3])
    .run_with(FiberStream::Sink.first)

result # => 1
```

`Sink.fold` accumulates a materialized value:

```ruby
sum =
  FiberStream::Source.each([1, 2, 3])
    .run_with(FiberStream::Sink.fold(0) { |acc, number| acc + number })

sum # => 6
```

## Open Questions

- `docs/product-specs/flow-async.md` defines the first async compatibility
  requirements before adding IO stages.
