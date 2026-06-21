# Core Concepts

## Source

A `Source` describes where elements come from. It is lazy. Creating a source
does not read, enumerate, or start producer work.

```ruby
source = FiberStream::Source.each([1, 2, 3])
```

Sources can be transformed directly through convenience methods:

```ruby
source
  .map { |value| value * 2 }
  .take(2)
  .run_with(FiberStream::Sink.to_a)
# => [2, 4]
```

They can also be combined with other sources:

```ruby
source
  .concat(FiberStream::Source.each([4, 5]))
  .run_with(FiberStream::Sink.to_a)
# => [1, 2, 3, 4, 5]

source
  .zip(FiberStream::Source.each(["a", "b"]))
  .run_with(FiberStream::Sink.to_a)
# => [[1, "a"], [2, "b"]]
```

## Flow

A `Flow` transforms elements without materializing a result. Flows can map,
filter, observe, expand, limit, group, split, buffer, throttle, or introduce
concurrency boundaries.

```ruby
flow =
  FiberStream::Flow.map(&:strip)
    .via(FiberStream::Flow.reject(&:empty?))
    .via(FiberStream::Flow.map_concat { |line| line.split })
```

Use reusable flows when the same transformation belongs in more than one
pipeline.

## Sink

A `Sink` consumes elements and returns a materialized value.

```ruby
FiberStream::Sink.to_a
FiberStream::Sink.first
FiberStream::Sink.count
FiberStream::Sink.fold(0) { |sum, value| sum + value }
FiberStream::Sink.foreach { |value| puts value }
```

The sink controls demand. A sink such as `first` may finish before the source
is exhausted.

## Pipeline

`Source#to(sink)` creates a reusable `Pipeline`.

```ruby
pipeline =
  FiberStream::Source.each([1, 2, 3])
    .map { |value| value * 2 }
    .to(FiberStream::Sink.to_a)

pipeline.run # => [2, 4, 6]
```

`Pipeline#run_async` starts a scheduler-backed background fiber and returns a
`RunningPipeline` handle.

## Materialization

Materialization starts when a pipeline runs:

- `source.run_with(sink)`
- `pipeline.run`
- `pipeline.run_async`

Every run creates a new materialized pull chain. One-shot enumerables and IO
objects are not snapshotted by FiberStream.
