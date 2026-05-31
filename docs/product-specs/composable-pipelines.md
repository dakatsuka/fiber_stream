# Composable Pipelines

## Status

Accepted

## Problem

FiberStream already lets users compose flows through `Source#via(flow)` and
materialize a stream with `Source#run_with(sink)`. As pipelines grow, users
need to name and reuse flow chains, package flow preprocessing together with a
sink, and separate runnable pipeline construction from execution. The current
API forces reusable work to stay attached to a specific source or to be rebuilt
at each call site.

## Goals

- Let users build reusable linear flow pipelines.
- Let users compose a flow pipeline into a sink so the resulting sink accepts
  the flow's input type and materializes the original sink's value.
- Let users build a runnable pipeline object from a source and sink without
  executing it immediately.
- Preserve the existing lazy construction model.
- Preserve pull backpressure and existing cleanup/error semantics.
- Keep `Source#run_with(sink)` as the direct materialization API.
- Keep the first composability slice linear and dependency-free.

## Non-Goals

- Graph DSLs.
- Fan-in, fan-out, merge, broadcast, zip, or cycle support.
- Akka Streams materialized-value combining operators.
- Background execution or `run_async`.
- Naming or registering pipelines globally.
- Adding new transformation semantics such as line framing.

## Requirements

- `Flow#via(flow)` creates a new flow that applies the receiver first and the
  argument flow second.
- `Flow#via` accepts only `FiberStream::Flow` objects.
- Passing a non-flow object to `Flow#via` raises `TypeError`.
- `Flow#via` is lazy; it does not attach to upstream, pull values, run blocks,
  start scheduler-backed boundaries, or close anything at construction time.
- The composed flow preserves the same per-stage behavior, ordering,
  backpressure, scheduler requirements, cancellation, and error propagation as
  applying the same flows with repeated `Source#via` calls.
- A composed flow can be reused with multiple sources.
- Reusing a composed flow does not share per-materialization pull state between
  runs.
- `Flow#to(sink)` creates a new sink that first runs upstream through the
  receiver flow, then materializes the argument sink.
- `Flow#to` accepts only `FiberStream::Sink` objects.
- Passing a non-sink object to `Flow#to` raises `TypeError`.
- `Flow#to` is lazy; it does not pull upstream, run flow blocks, start
  scheduler-backed boundaries, write IO, flush, or close at construction time.
- The composed sink returns the same materialized value that the argument sink
  would return after consuming the flow output.
- The composed sink closes the materialized flow chain after normal completion,
  failure, and early sink completion.
- If both the composed sink's primary work and flow-chain close fail, the
  primary failure wins and close failure is suppressed.
- If the composed sink's primary work succeeds and flow-chain close fails, the
  close failure is delivered as the stream failure.
- `Source#to(sink)` creates a runnable pipeline object without executing it.
- `Source#to` accepts only `FiberStream::Sink` objects.
- Passing a non-sink object to `Source#to` raises `TypeError`.
- `Source#to` is lazy; it does not materialize the source, pull values, run
  flow blocks, start scheduler-backed boundaries, write IO, flush, or close at
  construction time.
- The runnable pipeline object exposes `#run`.
- `Pipeline#run` materializes the source and sink and returns the sink's
  materialized value.
- `Source#to(sink).run` is behaviorally equivalent to
  `Source#run_with(sink)`.
- `Source#run_with(sink)` remains supported.
- A runnable pipeline can be run multiple times, subject to the replayability
  and resource ownership semantics of its source and sink definitions.
- Public APIs never expose `Pull::DONE`.

## Public Contracts

```ruby
FiberStream::Flow#via(flow)
FiberStream::Flow#to(sink)
FiberStream::Source#to(sink)
FiberStream::Pipeline#run
```

Initial RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def to: [Mat] (Sink[Elem, Mat] sink) -> Pipeline[Mat]
  end

  class Flow[In, Out]
    def via: [Next] (Flow[Out, Next] flow) -> Flow[In, Next]
    def to: [Mat] (Sink[Out, Mat] sink) -> Sink[In, Mat]
  end

  class Sink[In, Mat]
  end

  class Pipeline[Mat]
    def run: () -> Mat
  end
end
```

## Examples

Reusable flow pipeline:

```ruby
normalize =
  FiberStream::Flow.map(&:strip)
    .via(FiberStream::Flow.select { |line| !line.empty? })
    .via(FiberStream::Flow.map(&:downcase))

result =
  FiberStream::Source.each([" A ", "", " B "])
    .via(normalize)
    .run_with(FiberStream::Sink.to_a)

result # => ["a", "b"]
```

Sink composition:

```ruby
upcase_lines =
  FiberStream::Flow.map { |line| "#{line.upcase}\n" }
    .to(FiberStream::Sink.io($stdout, flush: true))

Async do
  FiberStream::Source.each(["hello", "world"])
    .run_with(upcase_lines)
end.wait
```

Runnable pipeline:

```ruby
pipeline =
  FiberStream::Source.each([1, 2, 3])
    .map { |number| number * 2 }
    .to(FiberStream::Sink.to_a)

pipeline.run # => [2, 4, 6]
```

Existing direct materialization remains valid:

```ruby
FiberStream::Source.each([1, 2, 3])
  .run_with(FiberStream::Sink.to_a)
```

## Open Questions

None.
