# Source

`FiberStream::Source` is a lazy stream definition.

## Constructors

### `Source.each(enumerable)`

Creates a source from an `Enumerable`.

Each materialization calls `enumerable.to_enum(:each)`. FiberStream does not
snapshot values or guarantee replayability for one-shot enumerables.

```ruby
FiberStream::Source.each([1, 2, 3])
  .run_with(FiberStream::Sink.to_a)
# => [1, 2, 3]
```

### `Source.io(io, chunk_size: 16 * 1024, close: false)`

Creates a source that reads String chunks with `readpartial`.

This source requires a scheduler-backed non-blocking fiber when demanded.

- `chunk_size` must be a positive `Integer`.
- `close: true` closes the IO object when the stream closes.

```ruby
Async do
  File.open("input.txt", "rb") do |file|
    FiberStream::Source.io(file, chunk_size: 16 * 1024)
      .run_with(FiberStream::Sink.to_a)
  end
end.wait
```

### `Source.ractor_producer(*args, transfer: :copy, ack_transfer: :copy) { |producer, *args| ... }`

Creates a source backed by one FiberStream-owned producer Ractor. The producer
block must be shareable and receives a `RactorProducer` context. Calls to
`producer.emit(value)` preserve one-outstanding-ack backpressure.

```ruby
produce_values =
  Ractor.shareable_proc do |producer, values|
    values.each do |value|
      break unless producer.emit(value)
    end
  end

FiberStream::Source.ractor_producer([1, 2, 3], &produce_values)
  .run_with(FiberStream::Sink.to_a)
# => [1, 2, 3]
```

### `Source.ractor_merge_producers(transfer: :copy, ack_transfer: :copy) { |group| ... }`

Creates a source backed by multiple FiberStream-owned producer Ractors and
emits values in coordinator-observed ready order. Register at least two
producers with the group.

```ruby
produce_tagged_values =
  Ractor.shareable_proc do |producer, tag, values|
    values.each do |value|
      break unless producer.emit([tag, value])
    end
  end

source =
  FiberStream::Source.ractor_merge_producers do |group|
    group.producer(:a, [1, 2], &produce_tagged_values)
    group.producer(:b, [3, 4], &produce_tagged_values)
  end

source.run_with(FiberStream::Sink.to_a)
# Example result: [[:a, 1], [:b, 3], [:a, 2], [:b, 4]]
```

### `Source.ractor_port(port, ack_port:, ack_transfer: :copy, cancel: true)`

Creates a source from a producer Ractor protocol.

The producer waits for `RactorPort::Ack`, then sends one
`RactorPort::Element`, `RactorPort::Complete`, or `RactorPort::Failure` to the
data port.

Failure metadata is producer-provided. Redact sensitive data before sending a
failure across a trust boundary.

```ruby
FiberStream::Source.ractor_port(data_port, ack_port: ack_port)
  .run_with(FiberStream::Sink.to_a)
```

### `Source.ractor_merge_ports(ports, ack_transfer: :copy, cancel: true)`

Merges multiple producer Ractor port pairs.

Each pair is a Hash with `:port` and `:ack_port`. The source sends at most one
outstanding ack to each active producer and emits values in coordinator-observed
ready order.

```ruby
port_pairs = [
  { port: data_port_a, ack_port: ack_port_a },
  { port: data_port_b, ack_port: ack_port_b }
]

FiberStream::Source.ractor_merge_ports(port_pairs)
  .run_with(FiberStream::Sink.to_a)
```

## Composition

### `source.via(flow)`

Returns a new source that applies `flow`.

```ruby
flow = FiberStream::Flow.map { |value| value * 2 }

FiberStream::Source.each([1, 2])
  .via(flow)
  .run_with(FiberStream::Sink.to_a)
# => [2, 4]
```

### `source.concat(other)`

Emits this source, then `other`. The appended source is not materialized until
downstream demand observes completion from the first source.

```ruby
FiberStream::Source.each([1, 2])
  .concat(FiberStream::Source.each([3]))
  .run_with(FiberStream::Sink.to_a)
# => [1, 2, 3]
```

### `source.zip(other)`

Emits two-element arrays. The zipped source completes when either input source
completes.

```ruby
FiberStream::Source.each([1, 2, 3])
  .zip(FiberStream::Source.each(["a", "b"]))
  .run_with(FiberStream::Sink.to_a)
# => [[1, "a"], [2, "b"]]
```

### `source.merge(other)`

Emits values from either source in scheduler-observed ready order. Each input's
own order is preserved. This method requires a scheduler-backed non-blocking
fiber when demanded.

```ruby
Async do
  FiberStream::Source.each([1, 2])
    .merge(FiberStream::Source.each(["a", "b"]))
    .run_with(FiberStream::Sink.to_a)
end.wait
# Example result: [1, "a", 2, "b"]
```

## Flow convenience methods

These methods delegate to the matching `Flow` constructor:

### `source.map { |element| ... }`

```ruby
FiberStream::Source.each([1, 2])
  .map { |value| value * 10 }
  .run_with(FiberStream::Sink.to_a)
# => [10, 20]
```

### `source.filter_map { |element| ... }`

```ruby
FiberStream::Source.each([{ id: 1 }, {}, { id: 3 }])
  .filter_map { |record| record[:id] }
  .run_with(FiberStream::Sink.to_a)
# => [1, 3]
```

### `source.compact`

```ruby
FiberStream::Source.each([1, nil, false, 2])
  .compact
  .run_with(FiberStream::Sink.to_a)
# => [1, false, 2]
```

### `source.map_concat { |element| enumerable }`

```ruby
FiberStream::Source.each(["a b", "", "c"])
  .map_concat { |line| line.split }
  .run_with(FiberStream::Sink.to_a)
# => ["a", "b", "c"]
```

### `source.parallel_map(concurrency:) { |element| ... }`

```ruby
Async do
  FiberStream::Source.each([1, 2, 3])
    .parallel_map(concurrency: 2) { |value| value * 10 }
    .run_with(FiberStream::Sink.to_a)
end.wait
# => [10, 20, 30]
```

### `source.parallel_unordered_map(concurrency:) { |element| ... }`

```ruby
Async do
  FiberStream::Source.each([1, 2, 3])
    .parallel_unordered_map(concurrency: 2) { |value| value * 10 }
    .run_with(FiberStream::Sink.to_a)
end.wait
# Order may vary.
```

### `source.ractor_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`

```ruby
mapper = Ractor.shareable_proc { |value| value * value }

FiberStream::Source.each([2, 3, 4])
  .ractor_map(workers: 2, &mapper)
  .run_with(FiberStream::Sink.to_a)
# => [4, 9, 16]
```

### `source.select { |element| ... }`

```ruby
FiberStream::Source.each([1, 2, 3, 4])
  .select(&:even?)
  .run_with(FiberStream::Sink.to_a)
# => [2, 4]
```

### `source.reject { |element| ... }`

```ruby
FiberStream::Source.each([1, 2, 3, 4])
  .reject(&:even?)
  .run_with(FiberStream::Sink.to_a)
# => [1, 3]
```

### `source.take(count)`

```ruby
FiberStream::Source.each([1, 2, 3])
  .take(2)
  .run_with(FiberStream::Sink.to_a)
# => [1, 2]
```

### `source.drop(count)`

```ruby
FiberStream::Source.each([1, 2, 3])
  .drop(1)
  .run_with(FiberStream::Sink.to_a)
# => [2, 3]
```

### `source.grouped(count)`

```ruby
FiberStream::Source.each([1, 2, 3, 4, 5])
  .grouped(2)
  .run_with(FiberStream::Sink.to_a)
# => [[1, 2], [3, 4], [5]]
```

### `source.scan(initial) { |accumulator, element| ... }`

```ruby
FiberStream::Source.each([1, 2, 3, 4])
  .scan(0) { |sum, value| sum + value }
  .run_with(FiberStream::Sink.to_a)
# => [1, 3, 6, 10]
```

### `source.take_while { |element| ... }`

```ruby
FiberStream::Source.each([1, 2, 3, 1])
  .take_while { |value| value < 3 }
  .run_with(FiberStream::Sink.to_a)
# => [1, 2]
```

### `source.drop_while { |element| ... }`

```ruby
FiberStream::Source.each([1, 2, 3, 1])
  .drop_while { |value| value < 3 }
  .run_with(FiberStream::Sink.to_a)
# => [3, 1]
```

### `source.async`

```ruby
Async do
  FiberStream::Source.each([1, 2, 3])
    .async
    .run_with(FiberStream::Sink.to_a)
end.wait
# => [1, 2, 3]
```

### `source.buffer(count)`

```ruby
Async do
  FiberStream::Source.each([1, 2, 3])
    .buffer(2)
    .run_with(FiberStream::Sink.to_a)
end.wait
# => [1, 2, 3]
```

### `source.throttle(rate:, per: 1, burst: nil)`

```ruby
Async do
  FiberStream::Source.each([1, 2, 3])
    .throttle(rate: 10, per: 1)
    .run_with(FiberStream::Sink.to_a)
end.wait
# => [1, 2, 3]
```

### `source.throttle(limiter:)`

```ruby
limiter = FiberStream::RateLimiter.new(rate: 10, per: 1)

FiberStream::Source.each([1, 2, 3])
  .throttle(limiter: limiter)
  .run_with(FiberStream::Sink.to_a)
# => [1, 2, 3]
```

### `source.lines(chomp: true, max_length: nil)`

```ruby
FiberStream::Source.each(["a\nb", "\nc"])
  .lines
  .run_with(FiberStream::Sink.to_a)
# => ["a", "b", "c"]
```

### `source.split(separator, keep_separator: false, max_length: nil)`

```ruby
FiberStream::Source.each(["a,b", ",c"])
  .split(",")
  .run_with(FiberStream::Sink.to_a)
# => ["a", "b", "c"]
```

## Materialization

### `source.run_with(sink)`

Runs the source with `sink` and returns the sink's materialized value.

```ruby
FiberStream::Source.each([1, 2, 3])
  .run_with(FiberStream::Sink.first)
# => 1
```

### `source.to(sink)`

Returns a reusable `Pipeline`.

```ruby
pipeline =
  FiberStream::Source.each([1, 2, 3])
    .to(FiberStream::Sink.to_a)

pipeline.run # => [1, 2, 3]
```
