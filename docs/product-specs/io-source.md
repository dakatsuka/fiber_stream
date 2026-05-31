# IO Source

## Status

Accepted

## Problem

Users need FiberStream to read from Ruby IO objects without giving up the
library's pull-based backpressure and scheduler-aware non-blocking execution
model. `Source.each` intentionally does not own or close resources, so IO needs
a separate source API with explicit ownership and cleanup behavior.

## Goals

- Add `FiberStream::Source.io(io, chunk_size: ..., close: ...)` as the first
  IO source API.
- Emit byte `String` chunks from an existing IO-like object.
- Preserve lazy construction: no reads happen until the stream is demanded.
- Preserve pull backpressure: one downstream pull performs at most one IO read.
- Require a Ruby fiber scheduler and a non-blocking fiber context when IO is
  first demanded.
- Keep FiberStream independent of Async at runtime.
- Make IO ownership explicit with `close: false` by default and `close: true`
  for source-owned IO.

## Non-Goals

- Opening files by path.
- Line-oriented APIs.
- Text decoding, delimiter parsing, or framing.
- IO sinks or write support.
- Socket connection helpers.
- Timeouts, retries, or reconnection.
- Internal prefetch beyond explicit `Flow.buffer(count)` composition.

## Requirements

- `FiberStream::Source.io(io, chunk_size: 16 * 1024, close: false)` creates a
  source definition from an IO-like object.
- The IO-like object must respond to `readpartial`.
- When `close: true`, the IO-like object must respond to `close`.
- Constructing the source does not read from or close the IO object.
- `Source.io` does not snapshot or reopen IO. Each materialization reads from
  the same IO object's current position.
- Replayability depends on the IO object. A second materialization may continue
  after the first run, observe EOF, or fail because an earlier run closed the
  IO.
- The first downstream pull validates scheduler availability and non-blocking
  fiber execution.
- If no `Fiber.scheduler` is available when IO is first demanded, the source
  raises `FiberStream::SchedulerRequiredError`.
- If the current fiber is a blocking fiber when IO is first demanded, the source
  raises `FiberStream::SchedulerRequiredError`.
- With `close: true`, scheduler validation failure closes the IO before
  `Source#run_with` returns.
- If scheduler validation and IO close both fail, `SchedulerRequiredError` is
  the primary stream failure and the close failure is suppressed.
- Each downstream pull calls `io.readpartial(chunk_size)` at most once.
- A `String` chunk returned by `readpartial` is emitted unchanged as a stream
  element.
- If `readpartial` returns a non-`String` value, the source raises `TypeError`.
- `EOFError` from `readpartial` is normal stream completion.
- Exceptions other than `EOFError` raised by `readpartial` fail the stream and
  are re-raised from `Source#run_with`.
- `chunk_size` must be an `Integer`.
- `chunk_size` must be positive.
- Non-Integer `chunk_size` values raise `TypeError`.
- Zero or negative `chunk_size` values raise `ArgumentError`.
- `close` must be exactly `true` or `false`.
- Non-boolean `close` values raise `TypeError`.
- With `close: false`, FiberStream never calls `io.close`.
- With `close: true`, normal EOF closes the IO before normal stream completion
  is delivered downstream.
- With `close: true`, early downstream completion closes the IO before
  `Source#run_with` returns.
- With `close: true`, downstream failure closes the IO before the downstream
  failure is re-raised.
- With `close: true`, an IO close failure after normal EOF is delivered as a
  stream failure instead of normal completion.
- With `close: true`, if both `readpartial` and `close` fail, the read failure
  is the primary stream failure and the close failure is suppressed.
- With `close: true`, if early downstream completion and close both happen and
  close fails, the close failure is delivered from `Source#run_with`.
- With `close: true`, if downstream failure and close both fail, the downstream
  failure remains primary and the close failure is suppressed.
- Repeated pulls after EOF return normal completion without reading again.
- Direct materialization should run inside an existing non-blocking scheduler
  context, such as an Async task. Pipelines may also place `Source.io` upstream
  of `Flow.async` or `Flow.buffer` so IO reads happen in those boundaries'
  non-blocking producer fibers.
- FiberStream validates the Ruby fiber context, but non-blocking behavior also
  depends on the IO-like object's `readpartial` implementation being compatible
  with Ruby's scheduler hooks.

## Public Contracts

```ruby
FiberStream::Source.io(io, chunk_size: 16 * 1024, close: false)
FiberStream::SchedulerRequiredError
```

Initial RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def self.io: (
      untyped io,
      ?chunk_size: Integer,
      ?close: bool
    ) -> Source[String]
  end
end
```

## Examples

Direct use inside a scheduler-backed non-blocking fiber:

```ruby
require "async"
require "fiber_stream"

chunks =
  Async do
    reader, writer = IO.pipe
    writer.write("hello")
    writer.close

    FiberStream::Source.io(reader, chunk_size: 5, close: true)
      .run_with(FiberStream::Sink.to_a)
  end.wait

chunks # => ["hello"]
```

Explicit buffering:

```ruby
result =
  Async do
    FiberStream::Source.io(socket, chunk_size: 16 * 1024, close: true)
      .buffer(8)
      .take(2)
      .run_with(FiberStream::Sink.to_a)
  end.wait
```

When no scheduler-backed non-blocking context is available:

```ruby
FiberStream::Source.io(reader).run_with(FiberStream::Sink.first)

# raises FiberStream::SchedulerRequiredError
```

## Open Questions

None.
