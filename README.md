# FiberStream

FiberStream is a Ruby library for linear stream processing with pull-based
backpressure.

It builds lazy `Source` definitions, transforms values with `Flow` stages, and
materializes results with `Sink` objects.

[![Gem Version](https://badge.fury.io/rb/fiber_stream.svg)](https://badge.fury.io/rb/fiber_stream)

## Quick Start
Please see the project [documentation](https://dakatsuka.github.io/fiber_stream/) for more details.

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

Implemented capabilities:

- in-memory, IO, FiberStream-owned Ractor producer, backpressure-aware Ractor
  port, and Ractor port merge sources
- lazy source concatenation, zipping, and scheduler-backed merging
- mapping, filtering, transform-and-filter, nil compaction, side-effect
  observation, one-to-many expansion, limiting, predicate-based limiting and
  dropping, fixed-prefix dropping, fixed-size grouping, line splitting,
  buffering, async boundaries, throttling, ordered and unordered parallel
  mapping, and ordered and unordered Ractor-backed mapping
- array, first-element, count, fold, foreach, and IO sinks
- reusable flow composition and runnable pipelines
- foreground and scheduler-backed background pipeline execution
- public RBS signatures

FiberStream intentionally keeps the public model linear: one source, an
ordered chain of flows, and one sink.

## Core Concepts

### Sources

A `Source` is a lazy stream definition. It is not consumed until the source is
run with a sink.

```ruby
source = FiberStream::Source.each([1, 2, 3])

source.run_with(FiberStream::Sink.to_a) # => [1, 2, 3]
```

Sources can be concatenated without materializing the appended source until the
first source completes:

```ruby
result =
  FiberStream::Source.each([1, 2])
    .concat(FiberStream::Source.each([3, 4]))
    .run_with(FiberStream::Sink.to_a)

result # => [1, 2, 3, 4]
```

Sources can also be zipped element-by-element. The zipped source emits pairs
and completes when either input source completes:

```ruby
result =
  FiberStream::Source.each([1, 2, 3])
    .zip(FiberStream::Source.each(["a", "b"]))
    .run_with(FiberStream::Sink.to_a)

result # => [[1, "a"], [2, "b"]]
```

IO sources read chunks on demand and require a scheduler-backed non-blocking
fiber. The `chunk_size` option is the maximum byte count passed to
`readpartial` for one downstream pull; very large values may cause the IO
implementation to attempt large allocations, so choose a bounded value
appropriate for the workload:

```ruby
require "async"
require "fiber_stream"

chunks =
  Async do
    File.open("input.txt", "rb") do |file|
      FiberStream::Source.io(file)
        .run_with(FiberStream::Sink.to_a)
    end
  end.wait
```

Owned Ractor producer sources run producer blocks in FiberStream-managed
Ractors. The producer block receives a `RactorProducer` context and emits one
value per downstream demand:

```ruby
PRODUCE_VALUES =
  Ractor.shareable_proc do |producer, values|
    values.each do |value|
      break unless producer.emit(value)
    end
  end

FiberStream::Source.ractor_producer([1, 2, 3], &PRODUCE_VALUES)
  .run_with(FiberStream::Sink.to_a)
# => [1, 2, 3]
```

Multiple owned producer Ractors can be merged directly without a
scheduler-backed `Source#merge`. Each producer still receives at most one
outstanding ack:

```ruby
PRODUCE_TAGGED_VALUES =
  Ractor.shareable_proc do |producer, tag, values|
    values.each do |value|
      break unless producer.emit([tag, value])
    end
  end

source =
  FiberStream::Source.ractor_merge_producers do |group|
    group.producer(:a, [1, 2], &PRODUCE_TAGGED_VALUES)
    group.producer(:b, [3, 4], &PRODUCE_TAGGED_VALUES)
  end

source.run_with(FiberStream::Sink.to_a)
# Example result: [[:a, 1], [:b, 3], [:a, 2], [:b, 4]]
```

Use the lower-level `Source.ractor_port` and `Source.ractor_merge_ports` APIs
when producer Ractors are owned outside FiberStream or need custom lifecycle
handling. `RactorPort::Failure` cause metadata is producer-provided and is
surfaced on `RactorPortSourceError`; redact sensitive details before sending
failures across trust boundaries.

Streaming HTTP response bodies that implement `#each`, such as
`async-http` response bodies, can be used with `Source.each` without buffering
the full body first. Use the HTTP client's block form or an explicit `ensure`
close because `Source.each` does not own the response body:

```ruby
require "async"
require "async/http/internet/instance"
require "fiber_stream"

url = "https://raw.githubusercontent.com/elastic/examples/master/" \
  "Common%20Data%20Formats/nginx_logs/nginx_logs"

status_counts = Hash.new(0)

processed =
  Sync do
    Async::HTTP::Internet.get(url) do |response|
      raise "unexpected status #{response.status}" unless response.status == 200

      FiberStream::Source.each(response.body)
        .lines(max_length: 16 * 1024)
        .map { |line| line.split.fetch(8, nil) }
        .select { |status| status&.match?(/\A\d{3}\z/) }
        .run_with(
          FiberStream::Sink.foreach do |status|
            status_counts[status] += 1
          end
        )
    end
  end
```

### Flows

Flows transform a stream lazily. Convenience methods on `Source` delegate to
the matching `FiberStream::Flow` constructors.

```ruby
result =
  FiberStream::Source.each(["a\nb", "\nc"])
    .lines
    .select { |line| line != "b" }
    .map(&:upcase)
    .run_with(FiberStream::Sink.to_a)

result # => ["A", "C"]
```

Reusable flows can be composed with `Flow#via`:

```ruby
normalize =
  FiberStream::Flow.map(&:strip)
    .via(FiberStream::Flow.select { |line| !line.empty? })

FiberStream::Source.each([" a ", "", " b "])
  .via(normalize)
  .run_with(FiberStream::Sink.to_a)
# => ["a", "b"]
```

Use `reject` when the predicate names values to drop. Truthy predicate results
drop the original element; `false` and `nil` results pass it through unchanged:

```ruby
result =
  FiberStream::Source.each([1, 2, 3, 4])
    .reject(&:even?)
    .run_with(FiberStream::Sink.to_a)

result # => [1, 3]
```

Use `filter_map` when filtering and transformation are one decision. Truthy
block results are emitted as transformed values; `false` and `nil` are
dropped:

```ruby
ids =
  FiberStream::Source.each([{ id: 1 }, {}, { id: 3 }])
    .filter_map { |record| record[:id] }
    .run_with(FiberStream::Sink.to_a)

ids # => [1, 3]
```

Use `compact` to drop only `nil` while keeping `false`, and `map_concat` to
expand one upstream element into zero or more downstream elements:

```ruby
tokens =
  FiberStream::Source.each(["alpha beta", nil, "gamma"])
    .compact
    .map_concat { |line| line.split }
    .run_with(FiberStream::Sink.to_a)

tokens # => ["alpha", "beta", "gamma"]
```

Use `Flow.tap` for observation inside a reusable flow without changing the
element:

```ruby
seen = []

observed =
  FiberStream::Flow.tap { |value| seen << value }
    .via(FiberStream::Flow.map { |value| value * 10 })

FiberStream::Source.each([1, 2])
  .via(observed)
  .run_with(FiberStream::Sink.to_a)
# => [10, 20]

seen # => [1, 2]
```

Use `parallel_map` for ordered scheduler-backed mapping when each element
waits on non-blocking IO. It preserves input order while allowing up to
`concurrency` mapping operations to be in flight:

```ruby
require "async"
require "fiber_stream"

def fetch_profile(user_id)
  # Example: perform scheduler-aware HTTP, database, or socket IO here.
  sleep 0.05
  { id: user_id, name: "user-#{user_id}" }
end

profiles =
  Sync do
    FiberStream::Source.each([1, 2, 3, 4])
      .parallel_map(concurrency: 4) { |user_id| fetch_profile(user_id) }
      .run_with(FiberStream::Sink.to_a)
  end

profiles.map { |profile| profile.fetch(:id) } # => [1, 2, 3, 4]
```

Use `parallel_unordered_map` when every result can be handled independently
and lower head-of-line blocking matters more than input order. It still limits
in-flight mapping work to `concurrency`, but emits values as mapping jobs
finish:

```ruby
require "async"
require "fiber_stream"

responses =
  Sync do
    FiberStream::Source.each(["/a", "/slow", "/b"])
      .parallel_unordered_map(concurrency: 3) { |path| fetch_path(path) }
      .run_with(FiberStream::Sink.to_a)
  end

# Results are in completion order, not necessarily input order.
responses
```

Use `ractor_map` for ordered CPU-bound mapping in Ractor workers, and
`ractor_unordered_map` when later completed CPU-bound results should not wait
behind a slower earlier input. The mapper must be shareable, usually by
creating it with `Ractor.shareable_proc`.

```ruby
require "digest"
require "fiber_stream"

records = [
  { name: "alpha.bin", payload: +"A" * 200_000 },
  { name: "bravo.bin", payload: +"B" * 120_000 }
]

HASH_RECORD =
  Ractor.shareable_proc do |record|
    payload = record.fetch(:payload)

    {
      name: record.fetch(:name),
      bytes: payload.bytesize,
      sha256: Digest::SHA256.hexdigest(payload)
    }
  end

digests =
  FiberStream::Source.each(records)
    .ractor_map(workers: 2, input_transfer: :move, &HASH_RECORD)
    .run_with(FiberStream::Sink.to_a)
```

`ractor_map` preserves input order, limits pulled-but-unemitted work to
`workers`, and does not require `Fiber.scheduler`. Use `input_transfer: :move`
or `output_transfer: :move` only when the moved object will not be reused by
the sender.

`ractor_unordered_map` uses the same Ractor transfer and shareability rules,
but emits values in worker completion order:

```ruby
fastest_first =
  FiberStream::Source.each(records)
    .ractor_unordered_map(workers: 2, input_transfer: :move, &HASH_RECORD)
    .run_with(FiberStream::Sink.to_a)

# Results are in completion order, not necessarily input order.
fastest_first
```

### Sinks

A `Sink` consumes the stream and returns a materialized value.

```ruby
FiberStream::Source.each([1, 2, 3])
  .run_with(FiberStream::Sink.fold(0) { |sum, value| sum + value })
# => 6
```

Use `Sink.count` when only the number of elements matters:

```ruby
FiberStream::Source.each([1, 2, 3])
  .run_with(FiberStream::Sink.count)
# => 3
```

Use `Sink.find` when the stream should stop after the first matching element:

```ruby
FiberStream::Source.each([1, 2, 3, 4])
  .run_with(FiberStream::Sink.find(&:even?))
# => 2
```

Use `Sink.foreach` when the terminal operation is a side effect and the stream
values should not be accumulated:

```ruby
count =
  FiberStream::Source.each(["a", "b", "c"])
    .run_with(FiberStream::Sink.foreach { |value| puts value })

count # => 3
```

### Pipelines

`Source#to(sink)` creates a reusable runnable pipeline.

```ruby
pipeline =
  FiberStream::Source.each([1, 2, 3])
    .map { |number| number * 2 }
    .to(FiberStream::Sink.to_a)

pipeline.run # => [2, 4, 6]
```

`Pipeline#run_async` starts a pipeline in a scheduler-backed background fiber
and returns a handle:

```ruby
require "async"
require "fiber_stream"

result =
  Async do
    running =
      FiberStream::Source.each([1, 2, 3])
        .map { |number| number * 2 }
        .to(FiberStream::Sink.to_a)
        .run_async

    # Foreground scheduler-managed work can continue here.

    running.wait
  end.wait

result # => [2, 4, 6]
```

The handle supports `wait`, `cancel`, `done?`, and `cancel_requested?`.

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

`Flow.drop` skips a fixed prefix and then passes later elements through:

```ruby
tail =
  FiberStream::Source.each([1, 2, 3, 4])
    .drop(2)
    .run_with(FiberStream::Sink.to_a)

tail # => [3, 4]
```

`Flow.grouped` batches adjacent elements into arrays and emits the final
partial group:

```ruby
batches =
  FiberStream::Source.each([1, 2, 3, 4, 5])
    .grouped(2)
    .run_with(FiberStream::Sink.to_a)

batches # => [[1, 2], [3, 4], [5]]
```

`Flow.scan` emits the updated accumulator for each upstream element:

```ruby
running_totals =
  FiberStream::Source.each([1, 2, 3, 4])
    .scan(0) { |sum, number| sum + number }
    .run_with(FiberStream::Sink.to_a)

running_totals # => [1, 3, 6, 10]
```

`Flow.take_while` emits the leading prefix while a predicate is truthy, then
closes upstream at the first false or nil result:

```ruby
prefix =
  FiberStream::Source.each([1, 2, 3, 1])
    .take_while { |number| number < 3 }
    .run_with(FiberStream::Sink.to_a)

prefix # => [1, 2]
```

`Flow.drop_while` skips the leading prefix while a predicate is truthy, then
passes the first false or nil result and all later elements through:

```ruby
tail =
  FiberStream::Source.each([1, 2, 3, 1])
    .drop_while { |number| number < 3 }
    .run_with(FiberStream::Sink.to_a)

tail # => [3, 1]
```

`Source#concat` preserves pull-driven demand across source boundaries. The
appended source is not materialized while the first source can still satisfy
downstream demand:

```ruby
first =
  FiberStream::Source.each([1])
    .concat(FiberStream::Source.each([2]))
    .run_with(FiberStream::Sink.first)

first # => 1
```

`Source#zip` keeps input source materialization behind downstream demand. The
other source is not materialized until the receiver has produced an element for
a pair:

```ruby
first =
  FiberStream::Source.each([1])
    .zip(FiberStream::Source.each([2]))
    .run_with(FiberStream::Sink.first)

first # => [1, 2]
```

`Source#merge` emits values from either input source in scheduler-observed
ready order while preserving each input's own order:

```ruby
merged =
  Sync do
    FiberStream::Source.each([1, 2])
      .merge(FiberStream::Source.each(["a", "b"]))
      .run_with(FiberStream::Sink.to_a)
  end

# Example result: [1, "a", 2, "b"]
```

`merge` does not make scheduler-unaware blocking source work non-blocking and
does not provide CPU parallelism. Use producer ractors with
`Source.ractor_producer` or `Source.ractor_merge_producers` when producer work
needs true isolation.

`Flow.buffer(count)` allows bounded prefetch. `Flow.throttle(rate:, per:)`
paces elements before downstream side effects. `Flow.async`, `Flow.buffer`,
`Flow.parallel_map`, `Flow.parallel_unordered_map`, `Source.io`,
`Source#merge`, `Sink.io`, and `Pipeline#run_async` require an installed
`Fiber.scheduler` and a non-blocking current fiber when demanded or started.
`Flow.throttle` requires that scheduler context only when it needs to wait.
Pass `throttle(limiter:)` with a `FiberStream::RateLimiter` when multiple
pipelines or repeated runs should share quota state. FiberStream does not
install a scheduler and does not depend on Async at runtime.

## API Surface

Sources:

- `FiberStream::Source.each(enumerable)`
- `FiberStream::Source.io(io, chunk_size: 16 * 1024, close: false)`
- `FiberStream::Source.ractor_producer(*args, transfer: :copy, ack_transfer: :copy) { |producer, *args| ... }`
- `FiberStream::Source.ractor_merge_producers(transfer: :copy, ack_transfer: :copy) { |group| ... }`
- `FiberStream::Source.ractor_port(port, ack_port:, ack_transfer: :copy, cancel: true)`
- `FiberStream::Source.ractor_merge_ports(ports, ack_transfer: :copy, cancel: true)`

Source convenience methods:

- `Source#via(flow)`
- `Source#concat(source)`
- `Source#zip(source)`
- `Source#merge(source)`
- `Source#map { |element| ... }`
- `Source#filter_map { |element| ... }`
- `Source#compact`
- `Source#map_concat { |element| enumerable }`
- `Source#parallel_map(concurrency:) { |element| ... }`
- `Source#parallel_unordered_map(concurrency:) { |element| ... }`
- `Source#ractor_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`
- `Source#ractor_unordered_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`
- `Source#select { |element| ... }`
- `Source#reject { |element| ... }`
- `Source#take(count)`
- `Source#drop(count)`
- `Source#grouped(count)`
- `Source#scan(initial) { |accumulator, element| ... }`
- `Source#take_while { |element| ... }`
- `Source#drop_while { |element| ... }`
- `Source#async`
- `Source#buffer(count)`
- `Source#throttle(rate:, per: 1, burst: nil)`
- `Source#throttle(limiter:)`
- `Source#lines(chomp: true, max_length: nil)`
- `Source#split(separator, keep_separator: false, max_length: nil)`
- `Source#to(sink)`
- `Source#run_with(sink)`

Flows:

- `FiberStream::Flow.map { |element| ... }`
- `FiberStream::Flow.filter_map { |element| ... }`
- `FiberStream::Flow.compact`
- `FiberStream::Flow.map_concat { |element| enumerable }`
- `FiberStream::Flow.tap { |element| ... }`
- `FiberStream::Flow.parallel_map(concurrency:) { |element| ... }`
- `FiberStream::Flow.parallel_unordered_map(concurrency:) { |element| ... }`
- `FiberStream::Flow.ractor_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`
- `FiberStream::Flow.ractor_unordered_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`
- `FiberStream::Flow.select { |element| ... }`
- `FiberStream::Flow.reject { |element| ... }`
- `FiberStream::Flow.take(count)`
- `FiberStream::Flow.drop(count)`
- `FiberStream::Flow.grouped(count)`
- `FiberStream::Flow.scan(initial) { |accumulator, element| ... }`
- `FiberStream::Flow.take_while { |element| ... }`
- `FiberStream::Flow.drop_while { |element| ... }`
- `FiberStream::Flow.async`
- `FiberStream::Flow.buffer(count)`
- `FiberStream::Flow.throttle(rate:, per: 1, burst: nil)`
- `FiberStream::Flow.throttle(limiter:)`
- `FiberStream::Flow.lines(chomp: true, max_length: nil)`
- `FiberStream::Flow.split(separator, keep_separator: false, max_length: nil)`
- `Flow#via(flow)`
- `Flow#to(sink)`
- `FiberStream::RateLimiter.new(rate:, per: 1, burst: nil)`
- `FiberStream::RateLimiter#acquire(permits: 1)`

`lines` and `split` default to `max_length: nil`, which allows one
unterminated line or frame to buffer without bound. Set a positive
`max_length` for untrusted, network-facing, or otherwise unbounded inputs.

Sinks:

- `FiberStream::Sink.to_a`
- `FiberStream::Sink.first`
- `FiberStream::Sink.find { |element| truthy_or_falsey }`
- `FiberStream::Sink.count`
- `FiberStream::Sink.fold(initial) { |accumulator, element| ... }`
- `FiberStream::Sink.foreach { |element| ... }`
- `FiberStream::Sink.io(io, close: false, flush: false)`

Pipelines:

- `FiberStream::Pipeline#run`
- `FiberStream::Pipeline#run_async`
- `FiberStream::RunningPipeline#wait`
- `FiberStream::RunningPipeline#cancel`
- `FiberStream::RunningPipeline#done?`
- `FiberStream::RunningPipeline#cancel_requested?`

## Examples

Runnable examples live under `examples/`.

```sh
bundle exec ruby examples/basic_pipeline.rb
bundle exec ruby examples/composable_pipeline.rb
bundle exec ruby examples/line_processing.rb
bundle exec ruby examples/file_copy.rb
bundle exec ruby examples/backpressure_buffer.rb
bundle exec ruby examples/background_execution.rb
bundle exec ruby examples/ractor_map_hashing.rb
bundle exec ruby examples/ractor_unordered_map_hashing.rb
bundle exec ruby examples/ractor_producer_sources.rb
bundle exec ruby examples/ractor_port_source.rb
bundle exec ruby examples/ractor_merge_ports_and_map.rb
bundle exec ruby examples/async_http_requests.rb
bundle exec ruby examples/async_http_streaming_body.rb
```

`examples/backpressure_buffer.rb` prints timestamped producer and consumer
events so the difference between direct demand and bounded prefetch is visible.

`examples/ractor_map_hashing.rb` demonstrates ordered Ractor-backed hashing
with a shareable mapper proc and `input_transfer: :move`.

`examples/ractor_unordered_map_hashing.rb` demonstrates completion-order
Ractor-backed hashing for uneven CPU-bound work.

`examples/ractor_producer_sources.rb` demonstrates high-level owned producer
Ractors with `Source.ractor_producer` and `Source.ractor_merge_producers`.

`examples/ractor_port_source.rb` demonstrates a producer Ractor that waits for
`RactorPort::Ack` before sending each `RactorPort::Element`.

`examples/ractor_merge_ports_and_map.rb` demonstrates CPU-bound producer
Ractors merged with `Source.ractor_merge_ports`, followed by CPU-bound
verification in `ractor_map` workers.

`examples/async_http_requests.rb` starts a local HTTP server and shows
FiberStream overlapping independent HTTP request waits with `parallel_map`.

`examples/async_http_streaming_body.rb` streams a public nginx access log with
`async-http`, feeds the response body through `Source.each(response.body)`, and
aggregates lines without storing the full body.

Benchmark scripts live under `benchmarks/`.

```sh
bundle exec ruby benchmarks/stream_transform.rb
bundle exec ruby benchmarks/latency_overlap.rb
bundle exec ruby benchmarks/async_io_fanout.rb
bundle exec ruby benchmarks/heavy_cpu_map.rb
```

## Development

This project targets Ruby 4.x. The repository currently pins Ruby 4.0.5 in
`mise.toml`.

Install dependencies:

```sh
bundle install
```

Install website dependencies when building documentation locally:

```sh
cd website
npm ci
cd ..
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

Run the default local verification gate explicitly:

```sh
bundle exec rake verify
```

Run the full release-readiness gate, including the website build:

```sh
bundle exec rake verify:full
```

Build the gem:

```sh
bundle exec gem build fiber_stream.gemspec
```

Release uses RubyGems Trusted Publishing from the `Release` GitHub Actions
workflow. Configure a pending trusted publisher for the `fiber_stream` gem with
workflow filename `release.yml` and environment `release`, then publish by
pushing a version tag such as `v0.1.0`.

## Documentation

Design and planning documents live under `docs/`:

- `docs/product-specs/`
- `docs/design-docs/`
- `docs/exec-plans/`
- `docs/references/`
