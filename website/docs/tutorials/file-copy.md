# File Copy

`Source.io` reads String chunks from an IO-like object. `Sink.io` writes String
chunks to an IO-like object. Both require a scheduler-backed non-blocking
fiber.

```ruby
require "async"
require "fiber_stream"

chunks_written =
  Async do
    input = File.open("input.txt", "rb")
    output = File.open("output.txt", "wb")

    FiberStream::Source.io(input, chunk_size: 16 * 1024, close: true)
      .map(&:upcase)
      .run_with(FiberStream::Sink.io(output, close: true, flush: true))
  end.wait
```

Use `close: true` when FiberStream should close the IO object after the stream
finishes or fails.

`chunk_size` is the maximum byte count passed to `readpartial` for one pull.
Choose a bounded value for the workload. Very large values may cause the IO
implementation to attempt large allocations.

The runnable version is `examples/file_copy.rb`.
