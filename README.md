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
    .via(FiberStream::Flow.map { |number| number * 2 })
    .via(FiberStream::Flow.select(&:even?))
    .via(FiberStream::Flow.take(2))
    .run_with(FiberStream::Sink.to_a)

result # => [2, 4]
```

## Status

FiberStream currently supports linear pipelines only.

Implemented:

- `FiberStream::Source.each(enumerable)`
- `FiberStream::Flow.map { |element| ... }`
- `FiberStream::Flow.select { |element| ... }`
- `FiberStream::Flow.take(count)`
- `FiberStream::Flow.async`
- `FiberStream::Sink.to_a`
- `FiberStream::Sink.first`
- `FiberStream::Sink.fold(initial) { |accumulator, element| ... }`
- foreground `Source#run_with(sink)` execution
- public RBS signatures

Not yet implemented:

- graph DSLs
- buffered async boundaries
- bounded buffers
- parallel mapping
- IO sources and sinks
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
    .via(FiberStream::Flow.take(2))
    .run_with(FiberStream::Sink.to_a)

limited # => [1, 2]
```

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
