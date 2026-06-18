# Sink

`FiberStream::Sink` consumes a stream and returns a materialized value.

## Constructors

### `Sink.to_a`

Collects all elements into an Array.

```ruby
FiberStream::Source.each([1, 2, 3])
  .run_with(FiberStream::Sink.to_a)
# => [1, 2, 3]
```

### `Sink.first`

Returns the first element, or `nil` when the stream is empty. The sink closes
upstream after receiving the first element.

```ruby
FiberStream::Source.each([1, 2, 3])
  .run_with(FiberStream::Sink.first)
# => 1
```

### `Sink.count`

Consumes the complete stream and returns the number of elements observed
without storing them.

```ruby
FiberStream::Source.each([1, 2, 3])
  .run_with(FiberStream::Sink.count)
# => 3
```

### `Sink.fold(initial) { |accumulator, element| ... }`

Accumulates elements into one value.

```ruby
FiberStream::Source.each([1, 2, 3])
  .run_with(FiberStream::Sink.fold(0) { |sum, value| sum + value })
# => 6
```

### `Sink.foreach { |element| ... }`

Runs a side effect for each element and returns the number of processed
elements.

```ruby
seen = []

FiberStream::Source.each(["a", "b"])
  .run_with(FiberStream::Sink.foreach { |value| seen << value })
# => 2

seen # => ["a", "b"]
```

### `Sink.io(io, close: false, flush: false)`

Writes String chunks to an IO-like object and returns the number of chunks
written.

This sink requires a scheduler-backed non-blocking fiber. Use `close: true`
when FiberStream should close the IO object, and `flush: true` when it should
flush after writing.

```ruby
require "stringio"

io = StringIO.new

Async do
  FiberStream::Source.each(["a", "b"])
    .run_with(FiberStream::Sink.io(io))
end.wait
# => 2

io.string # => "ab"
```
