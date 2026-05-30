# Minimum Linear Pipeline

## Status

Accepted

## Problem

FiberStream needs a first user-visible stream API that proves the core model
without committing to graph construction, asynchronous boundaries, or IO stages.
The first slice should let users build a linear stream, transform each element,
and collect the result.

## Goals

- Provide a small public API for `Source.each(...).via(...).run_with(Sink.to_a)`.
- Keep stream construction lazy: source enumeration and flow blocks do not run
  until `run_with`.
- Preserve backpressure from the first implementation by advancing upstream only
  when downstream asks for an element.
- Keep the runtime dependency-free. Compatibility with `async` is tested as a
  development concern, not exposed as a runtime requirement.
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
- Each materialization calls `enumerable.each` to create execution state.
- Replayability depends on the enumerable. Collections such as `Array` normally
  provide repeatable traversal; one-shot enumerators or mutable custom
  enumerables may produce different values or no values on later runs.
- `Source.each` does not snapshot values.
- `Source#via(flow)` returns a new source definition and does not execute the
  stream.
- `Source#via` accepts a `FiberStream::Flow`; invalid flow objects raise
  `TypeError`.
- `FiberStream::Flow.map { |element| ... }` creates a mapping flow.
- The `map` block is called once for each element pulled through the flow.
- Exceptions raised by source enumeration, flow blocks, or sinks fail the stream
  and are re-raised from `run_with`.
- `FiberStream::Sink.to_a` consumes the complete stream and returns an `Array`.
- `Source#run_with` accepts a `FiberStream::Sink`; invalid sink objects raise
  `TypeError`.
- `Source#run_with(sink)` runs the stream in the current fiber until completion
  or failure and returns the sink materialized value.
- Cleanup runs after success, failure, and early sink completion.
- The first public sink, `Sink.to_a`, always consumes the stream to completion.
  Early completion remains a runtime cleanup invariant and is validated with
  internal test sinks until a public early-completion API is added.

## Public Contracts

```ruby
FiberStream::Source.each(enumerable)
FiberStream::Source#via(flow)
FiberStream::Source#run_with(sink)
FiberStream::Flow.map { |element| transformed_element }
FiberStream::Sink.to_a
```

Initial RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def self.each: [Elem] (Enumerable[Elem] enumerable) -> Source[Elem]
    def via: [Out] (Flow[Elem, Out] flow) -> Source[Out]
    def run_with: [Mat] (Sink[Elem, Mat] sink) -> Mat
  end

  class Flow[In, Out]
    def self.map: [In, Out] () { (In) -> Out } -> Flow[In, Out]
  end

  class Sink[In, Mat]
    def self.to_a: [Elem] () -> Sink[Elem, Array[Elem]]
  end
end
```

## Examples

```ruby
result =
  FiberStream::Source.each([1, 2, 3])
    .via(FiberStream::Flow.map { |number| number * 2 })
    .run_with(FiberStream::Sink.to_a)

result # => [2, 4, 6]
```

Multiple mapping flows compose in order:

```ruby
result =
  FiberStream::Source.each([1, 2, 3])
    .via(FiberStream::Flow.map { |number| number * 2 })
    .via(FiberStream::Flow.map(&:to_s))
    .run_with(FiberStream::Sink.to_a)

result # => ["2", "4", "6"]
```

## Open Questions

- What public API should introduce early completion first: `Sink.first`,
  `Flow.take`, or another operation?
- Which async compatibility tests should be required before adding IO stages?
