# Getting Started

## Install

Add FiberStream to your Gemfile:

```ruby
gem "fiber_stream"
```

Then install the bundle:

```sh
bundle install
```

FiberStream targets Ruby 4.x.

## Create a pipeline

A pipeline starts with a `Source`, applies zero or more transformations, and
finishes with a `Sink`.

```ruby
require "fiber_stream"

orders = [
  { id: 101, total: 4_800 },
  { id: 102, total: 12_400 },
  { id: 103, total: 9_900 }
]

high_value_ids =
  FiberStream::Source.each(orders)
    .select { |order| order.fetch(:total) >= 10_000 }
    .map { |order| order.fetch(:id) }
    .run_with(FiberStream::Sink.to_a)

high_value_ids # => [102]
```

`Source.each` is lazy. It does not enumerate `orders` until `run_with` starts
the stream.

## Use scheduler-backed stages

Some APIs require a `Fiber.scheduler` and a non-blocking current fiber:

- `Source.io`
- `Source#merge`
- `Flow.async`
- `Flow.buffer`
- `Flow.parallel_map`
- `Flow.parallel_unordered_map`
- `Sink.io`
- `Pipeline#run_async`

FiberStream does not install a scheduler. Use a scheduler library such as
`async` when your pipeline needs these stages.

```ruby
require "async"
require "fiber_stream"

result =
  Async do
    FiberStream::Source.each([1, 2, 3])
      .parallel_map(concurrency: 2) { |value| value * 10 }
      .run_with(FiberStream::Sink.to_a)
  end.wait

result # => [10, 20, 30]
```

## Use Ractor-backed stages

`Flow.ractor_map` runs CPU-bound mapping in worker Ractors. The mapper must be
shareable.

```ruby
mapper = Ractor.shareable_proc { |value| value * value }

FiberStream::Source.each([2, 3, 4])
  .ractor_map(workers: 2, &mapper)
  .run_with(FiberStream::Sink.to_a)
# => [4, 9, 16]
```

`Source.ractor_producer` starts FiberStream-owned producer Ractors lazily and
keeps producer output tied to downstream demand.

```ruby
producer =
  Ractor.shareable_proc do |out, values|
    values.each do |value|
      break unless out.emit(value)
    end
  end

FiberStream::Source.ractor_producer([1, 2, 3], &producer)
  .run_with(FiberStream::Sink.to_a)
# => [1, 2, 3]
```
