# FiberStream

FiberStream is an early-stage Ruby library for linear stream processing with
pull-based backpressure.

The current API is intentionally small: build a lazy `Source`, transform values
with `Flow.map`, and materialize the stream with a `Sink`.

```ruby
require "fiber_stream"

result =
  FiberStream::Source.each([1, 2, 3])
    .via(FiberStream::Flow.map { |number| number * 2 })
    .run_with(FiberStream::Sink.to_a)

result # => [2, 4, 6]
```

## Status

FiberStream currently supports linear pipelines only.

Implemented:

- `FiberStream::Source.each(enumerable)`
- `FiberStream::Flow.map { |element| ... }`
- `FiberStream::Sink.to_a`
- `FiberStream::Sink.first`
- foreground `Source#run_with(sink)` execution
- public RBS signatures

Not yet implemented:

- graph DSLs
- async boundaries
- bounded buffers
- parallel mapping
- IO sources and sinks
- background execution

## Backpressure

The initial runtime is pull-based. A sink asks for one element, each flow pulls
only what it needs from upstream, and the source advances only when downstream
demands a value.

`Sink.first` demonstrates early completion:

```ruby
first =
  FiberStream::Source.each([1, 2, 3])
    .run_with(FiberStream::Sink.first)

first # => 1
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
