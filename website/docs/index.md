# FiberStream

FiberStream is a Ruby library for linear stream processing with pull-based
backpressure.

It builds lazy `Source` definitions, transforms values with `Flow` stages, and
materializes results with `Sink` objects. Scheduler-backed and Ractor-backed
boundaries are explicit, so concurrency is visible in the pipeline.

```ruby
require "fiber_stream"

result =
  FiberStream::Source.each([1, 2, 3, 4])
    .map { |number| number * 2 }
    .select(&:even?)
    .take(2)
    .run_with(FiberStream::Sink.to_a)

result # => [2, 4]
```

## Start here

- [Getting Started](/guide/getting-started) explains installation and the first
  pipeline.
- [Core Concepts](/guide/core-concepts) defines sources, flows, sinks, and
  materialization.
- [Backpressure](/guide/backpressure) describes demand flow, async boundaries,
  and Ractor boundaries.

## Reference

- [Source](/reference/source)
- [Flow](/reference/flow)
- [Sink](/reference/sink)
- [Pipeline](/reference/pipeline)
- [Errors](/reference/errors)

## Project links

- [GitHub repository](https://github.com/dakatsuka/fiber_stream)
- [Changelog](https://github.com/dakatsuka/fiber_stream/blob/main/CHANGELOG.md)
