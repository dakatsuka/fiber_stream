# FiberStream

FiberStream is an early-stage Ruby library for linear stream processing with
pull-based backpressure.

The current API is intentionally small: build a lazy `Source`, transform,
filter, limit, or add an async boundary with `Flow` stages, and materialize the
stream with a `Sink`.

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

## Status

FiberStream currently supports linear pipelines only.

Implemented:

- `FiberStream::Source.each(enumerable)`
- `FiberStream::Source.io(io, chunk_size: 16 * 1024, close: false)`
- `Source#map { |element| ... }`
- `Source#select { |element| ... }`
- `Source#take(count)`
- `Source#async`
- `Source#buffer(count)`
- `Source#lines(chomp: true, max_length: nil)`
- `Source#to(sink)`
- `FiberStream::Flow.map { |element| ... }`
- `FiberStream::Flow.select { |element| ... }`
- `FiberStream::Flow.take(count)`
- `FiberStream::Flow.async`
- `FiberStream::Flow.buffer(count)`
- `FiberStream::Flow.lines(chomp: true, max_length: nil)`
- `Flow#via(flow)`
- `Flow#to(sink)`
- `FiberStream::Sink.to_a`
- `FiberStream::Sink.first`
- `FiberStream::Sink.fold(initial) { |accumulator, element| ... }`
- `FiberStream::Sink.io(io, close: false, flush: false)`
- `FiberStream::Pipeline#run`
- foreground `Source#run_with(sink)` execution
- public RBS signatures

Not yet implemented:

- graph DSLs
- parallel mapping
- background execution

## Backpressure

The initial runtime is pull-based. A sink asks for one element, each flow pulls
only what it needs from upstream, and the source advances only when downstream
demands a value.

`Sink.first` demonstrates sink-side early completion:

```ruby
first =
  FiberStream::Source.each([1, 2, 3])
    .run_with(FiberStream::Sink.first)

first # => 1
```

`Flow.take` demonstrates flow-side early completion and closes upstream after
the requested number of elements:

```ruby
limited =
  FiberStream::Source.each([1, 2, 3])
    .take(2)
    .run_with(FiberStream::Sink.to_a)

limited # => [1, 2]
```

## Examples

Runnable examples live under `examples/`.

```sh
bundle exec ruby examples/basic_pipeline.rb
bundle exec ruby examples/composable_pipeline.rb
bundle exec ruby examples/line_processing.rb
bundle exec ruby examples/file_copy.rb
bundle exec ruby examples/backpressure_buffer.rb
```

`examples/backpressure_buffer.rb` prints timestamped producer and consumer
events so the difference between direct demand and bounded prefetch is visible.

## Development

This project targets Ruby 4.x. The repository currently pins Ruby 4.0.3 in
`mise.toml`.

Install dependencies:

```sh
bundle install
```

Run the test suite:

```sh
bundle exec rake test
```

Run RBS validation:

```sh
bundle exec rbs validate
```

Run RuboCop:

```sh
bundle exec rubocop
```

Run all default checks:

```sh
bundle exec rake
```

## Documentation

Design and planning documents live under `docs/`:

- `docs/product-specs/`
- `docs/design-docs/`
- `docs/exec-plans/`
- `docs/references/`
