# Flow

`FiberStream::Flow` transforms elements without materializing a result.

## Element flows

### `Flow.map { |element| ... }`

Maps each element. Exceptions raised by the block fail the stream.

```ruby
FiberStream::Source.each([1, 2])
  .via(FiberStream::Flow.map { |value| value * 10 })
  .run_with(FiberStream::Sink.to_a)
# => [10, 20]
```

### `Flow.filter_map { |element| ... }`

Maps each element and emits only truthy block results. `false` and `nil`
results are dropped.

```ruby
FiberStream::Source.each([{ id: 1 }, {}, { id: 3 }])
  .via(FiberStream::Flow.filter_map { |record| record[:id] })
  .run_with(FiberStream::Sink.to_a)
# => [1, 3]
```

### `Flow.compact`

Drops `nil` elements and passes every non-`nil` element through unchanged,
including `false`.

```ruby
FiberStream::Source.each([1, nil, false, 2])
  .via(FiberStream::Flow.compact)
  .run_with(FiberStream::Sink.to_a)
# => [1, false, 2]
```

### `Flow.map_concat { |element| enumerable }`

Maps each element to an enumerable and emits the yielded values in order before
pulling the next upstream element.

```ruby
FiberStream::Source.each(["a b", "", "c"])
  .via(FiberStream::Flow.map_concat { |line| line.split })
  .run_with(FiberStream::Sink.to_a)
# => ["a", "b", "c"]
```

### `Flow.tap { |element| ... }`

Calls the block for each element, ignores the block result, and emits the
original element unchanged.

```ruby
seen = []

FiberStream::Source.each([1, 2])
  .via(FiberStream::Flow.tap { |value| seen << value })
  .run_with(FiberStream::Sink.to_a)
# => [1, 2]

seen # => [1, 2]
```

### `Flow.select { |element| ... }`

Keeps elements whose block result is truthy.

```ruby
FiberStream::Source.each([1, 2, 3, 4])
  .via(FiberStream::Flow.select(&:even?))
  .run_with(FiberStream::Sink.to_a)
# => [2, 4]
```

### `Flow.reject { |element| ... }`

Drops elements whose block result is truthy. `false` and `nil` results pass the
original element through unchanged.

```ruby
FiberStream::Source.each([1, 2, 3, 4])
  .via(FiberStream::Flow.reject(&:even?))
  .run_with(FiberStream::Sink.to_a)
# => [1, 3]
```

### `Flow.take(count)`

Emits at most `count` elements. `count` must be a non-negative `Integer`.

```ruby
FiberStream::Source.each([1, 2, 3])
  .via(FiberStream::Flow.take(2))
  .run_with(FiberStream::Sink.to_a)
# => [1, 2]
```

### `Flow.drop(count)`

Drops the first `count` elements. `count` must be a non-negative `Integer`.

```ruby
FiberStream::Source.each([1, 2, 3])
  .via(FiberStream::Flow.drop(1))
  .run_with(FiberStream::Sink.to_a)
# => [2, 3]
```

### `Flow.grouped(count)`

Emits arrays containing up to `count` adjacent elements. The final partial
group is emitted when upstream completes. `count` must be positive.

```ruby
FiberStream::Source.each([1, 2, 3, 4, 5])
  .via(FiberStream::Flow.grouped(2))
  .run_with(FiberStream::Sink.to_a)
# => [[1, 2], [3, 4], [5]]
```

### `Flow.scan(initial) { |accumulator, element| ... }`

Emits the updated accumulator for each upstream element. The initial value is
not emitted before the first element.

```ruby
FiberStream::Source.each([1, 2, 3, 4])
  .via(FiberStream::Flow.scan(0) { |sum, value| sum + value })
  .run_with(FiberStream::Sink.to_a)
# => [1, 3, 6, 10]
```

### `Flow.take_while { |element| ... }`

Emits leading elements while the block result is truthy, then completes and
closes upstream.

```ruby
FiberStream::Source.each([1, 2, 3, 1])
  .via(FiberStream::Flow.take_while { |value| value < 3 })
  .run_with(FiberStream::Sink.to_a)
# => [1, 2]
```

### `Flow.drop_while { |element| ... }`

Drops leading elements while the block result is truthy, then emits the first
falsey element and all later elements.

```ruby
FiberStream::Source.each([1, 2, 3, 1])
  .via(FiberStream::Flow.drop_while { |value| value < 3 })
  .run_with(FiberStream::Sink.to_a)
# => [3, 1]
```

## Boundaries

### `Flow.async`

Starts an asynchronous boundary on first downstream demand. Requires a
`Fiber.scheduler` and a non-blocking current fiber.

```ruby
Async do
  FiberStream::Source.each([1, 2, 3])
    .via(FiberStream::Flow.async)
    .run_with(FiberStream::Sink.to_a)
end.wait
# => [1, 2, 3]
```

### `Flow.buffer(count)`

Adds a bounded asynchronous buffer. Requires a `Fiber.scheduler` and a
non-blocking current fiber.

```ruby
Async do
  FiberStream::Source.each([1, 2, 3])
    .via(FiberStream::Flow.buffer(2))
    .run_with(FiberStream::Sink.to_a)
end.wait
# => [1, 2, 3]
```

### `Flow.throttle(rate:, per: 1, burst: nil)`

Paces emitted elements with a fresh `RateLimiter` for each materialization.
The stage pulls one upstream element before acquiring a permit for downstream
emission.

```ruby
Async do
  FiberStream::Source.each([1, 2, 3])
    .via(FiberStream::Flow.throttle(rate: 10, per: 1))
    .run_with(FiberStream::Sink.to_a)
end.wait
# => [1, 2, 3]
```

### `Flow.throttle(limiter:)`

Uses a custom limiter object that responds to `acquire(permits:)`. This form
can share quota state across materializations.

```ruby
limiter = FiberStream::RateLimiter.new(rate: 10, per: 1)

FiberStream::Source.each([1, 2, 3])
  .via(FiberStream::Flow.throttle(limiter: limiter))
  .run_with(FiberStream::Sink.to_a)
# => [1, 2, 3]
```

### `Flow.parallel_map(concurrency:) { |element| ... }`

Runs up to `concurrency` scheduler-backed mapping operations at the same time.
Results are emitted in input order. Use this for scheduler-aware IO waits, not
CPU parallelism.

This flow requires a `Fiber.scheduler` and a non-blocking current fiber on
first downstream demand.

```ruby
Async do
  FiberStream::Source.each([1, 2, 3])
    .via(FiberStream::Flow.parallel_map(concurrency: 2) { |value| value * 10 })
    .run_with(FiberStream::Sink.to_a)
end.wait
# => [10, 20, 30]
```

### `Flow.parallel_unordered_map(concurrency:) { |element| ... }`

Runs up to `concurrency` scheduler-backed mapping operations at the same time.
Results are emitted in completion order, so input order is not preserved. Use
this when each mapped result can be handled independently and ordered
`parallel_map` would add unnecessary head-of-line blocking.

This flow requires a `Fiber.scheduler` and a non-blocking current fiber on
first downstream demand.

```ruby
Async do
  FiberStream::Source.each([1, 2, 3])
    .via(
      FiberStream::Flow.parallel_unordered_map(concurrency: 2) do |value|
        value * 10
      end
    )
    .run_with(FiberStream::Sink.to_a)
end.wait
# Order may vary.
```

### `Flow.ractor_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`

Runs mapping in worker Ractors. The block must be shareable. Results are
emitted in input order.

```ruby
mapper = Ractor.shareable_proc { |value| value * value }

FiberStream::Source.each([2, 3, 4])
  .via(FiberStream::Flow.ractor_map(workers: 2, &mapper))
  .run_with(FiberStream::Sink.to_a)
# => [4, 9, 16]
```

## Framing

### `Flow.lines(chomp: true, max_length: nil)`

Splits String chunks on newline bytes. With `chomp: true`, the emitted line
excludes the trailing newline and one preceding carriage return.

Set `max_length` for untrusted or unbounded streams. With `max_length: nil`,
one unterminated line can buffer without bound.

```ruby
FiberStream::Source.each(["a\nb", "\nc"])
  .via(FiberStream::Flow.lines)
  .run_with(FiberStream::Sink.to_a)
# => ["a", "b", "c"]
```

### `Flow.split(separator, keep_separator: false, max_length: nil)`

Splits String chunks on a non-empty String separator.

With `keep_separator: true`, separator-terminated frames include the separator.
Set `max_length` for untrusted or unbounded streams.

```ruby
FiberStream::Source.each(["a,b", ",c"])
  .via(FiberStream::Flow.split(","))
  .run_with(FiberStream::Sink.to_a)
# => ["a", "b", "c"]
```

## Composition

### `flow.via(next_flow)`

Returns a reusable flow that applies `flow` and then `next_flow`.

```ruby
flow =
  FiberStream::Flow.map { |value| value * 2 }
    .via(FiberStream::Flow.select { |value| value > 4 })

FiberStream::Source.each([1, 2, 3])
  .via(flow)
  .run_with(FiberStream::Sink.to_a)
# => [6]
```

### `flow.to(sink)`

Returns a sink that runs the flow before `sink`.

```ruby
sink =
  FiberStream::Flow.map { |value| value * 2 }
    .to(FiberStream::Sink.to_a)

FiberStream::Source.each([1, 2])
  .run_with(sink)
# => [2, 4]
```
