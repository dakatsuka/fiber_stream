# IO Sink

## Status

Draft

## Problem

FiberStream can now read byte chunks from IO with `Source.io`, but users also
need to materialize a stream by writing chunks to an IO object. The write side
needs the same explicit scheduler, ownership, flushing, and close/error
contracts as the read side.

## Goals

- Add `FiberStream::Sink.io(io, close: false, flush: false)` as the first IO
  sink API.
- Write upstream byte `String` chunks to an existing IO-like object.
- Treat Ruby core IO objects such as `IO`, `File`, pipe endpoints, sockets, and
  standard streams as the primary use case.
- Preserve lazy construction: no writes, flushes, or closes happen until
  `Source#run_with`.
- Preserve pull backpressure: the sink pulls at most one upstream element for
  each write attempt.
- Require a Ruby fiber scheduler and a non-blocking fiber context before sink
  IO operations.
- Keep FiberStream independent of Async and `io-stream` at runtime.
- Make IO ownership explicit with `close: false` by default and `close: true`
  for sink-owned IO.
- Make final flushing explicit with `flush: false` by default and
  `flush: true` when users need a normal-completion flush.

## Non-Goals

- Opening files by path.
- Text encoding or transcoding.
- Line formatting, delimiter insertion, framing, or serialization.
- Gathering writes, batching, retrying partial writes, or write timeouts.
- Socket connection helpers.
- Duplex convenience APIs that combine `Source.io` and `Sink.io`.
- Depending on `io-stream`; wrappers from that gem remain caller-provided IO
  objects.

## Requirements

- `FiberStream::Sink.io(io, close: false, flush: false)` creates a sink
  definition from an IO-like object.
- The IO-like object must respond to `write`.
- Missing `write` raises `TypeError`.
- Ruby core IO objects that provide complete-write-or-raise `write` behavior,
  such as `File`, `TCPSocket`, `IO.pipe` write endpoints, `$stdout`, and
  `$stderr`, are supported directly when used in the required
  scheduler-backed non-blocking context.
- When `close: true`, the IO-like object must respond to `close`.
- Missing `close` with `close: true` raises `TypeError`.
- When `flush: true`, the IO-like object must respond to `flush`.
- Missing `flush` with `flush: true` raises `TypeError`.
- Constructing the sink does not pull upstream, write, flush, or close.
- `Sink.io` stores the same IO object across materializations.
- Each materialization writes to the IO object's current state.
- A second materialization may append after earlier writes, flush the same IO
  again, or fail because an earlier run closed the IO.
- `close` must be exactly `true` or `false`.
- Non-boolean `close` values raise `TypeError`.
- `flush` must be exactly `true` or `false`.
- Non-boolean `flush` values raise `TypeError`.
- `Sink.io` consumes upstream until normal completion or failure.
- Each upstream element must be a `String`.
- If upstream emits a non-`String` element, the sink raises `TypeError`.
- Empty strings are valid elements and are passed to `io.write`.
- Before each `write`, the sink validates scheduler availability and
  non-blocking fiber execution.
- Before a normal-completion `flush` or `close`, the sink validates scheduler
  availability and non-blocking fiber execution.
- If no `Fiber.scheduler` is available before a write, the sink raises
  `FiberStream::SchedulerRequiredError`.
- If the current fiber is blocking before a write, the sink raises
  `FiberStream::SchedulerRequiredError`.
- If no `Fiber.scheduler` is available before a normal-completion flush or
  close, the sink raises `FiberStream::SchedulerRequiredError`.
- If the current fiber is blocking before a normal-completion flush or close,
  the sink raises `FiberStream::SchedulerRequiredError`.
- With `close: true`, scheduler validation failure closes the IO before
  `Source#run_with` returns.
- If scheduler validation and IO close both fail, `SchedulerRequiredError` is
  the primary stream failure and the close failure is suppressed.
- For each valid upstream element, `Sink.io` calls `io.write(chunk)` exactly
  once.
- The IO-like object's `write` method must provide complete-write-or-raise
  semantics for the supplied chunk.
- `Sink.io` ignores the return value from `io.write`. A write is successful
  when `io.write` returns without raising.
- IO-like objects that can return after accepting only part of a chunk are not
  supported unless callers wrap them in an adapter that provides
  complete-write-or-raise behavior.
- `Sink.io` returns the number of chunks successfully written.
- If `io.write` raises, the stream fails and the write failure is re-raised
  from `Source#run_with`.
- With `flush: false`, FiberStream never calls `io.flush`.
- With `flush: true`, normal upstream completion calls `io.flush` once before
  returning from the sink.
- `flush: true` does not flush after upstream failure, element type failure,
  scheduler validation failure, or write failure.
- With `close: false`, FiberStream never calls `io.close`.
- With `close: true`, normal upstream completion closes the IO before
  `Source#run_with` returns.
- With `close: true`, upstream failure closes the IO before the upstream
  failure is re-raised.
- With `close: true`, element type failure closes the IO before the type
  failure is re-raised.
- With `close: true`, write failure closes the IO before the write failure is
  re-raised.
- With `close: true`, normal upstream completion plus close failure delivers
  the close failure as a stream failure.
- With `flush: true`, normal upstream completion plus flush failure delivers
  the flush failure as a stream failure.
- If normal upstream completion, flush failure, and close failure all occur,
  the flush failure is primary and the close failure is suppressed.
- If scheduler validation fails before write, flush, or normal-completion
  close, owned cleanup close is attempted without another scheduler validation.
- If upstream failure and close failure both occur, the upstream failure is
  primary and the close failure is suppressed.
- If element type failure and close failure both occur, the type failure is
  primary and the close failure is suppressed.
- If write failure and close failure both occur, the write failure is primary
  and the close failure is suppressed.
- Direct materialization that writes, flushes, or closes IO should run inside an
  existing non-blocking scheduler context, such as an Async task.
- If upstream completes normally and `close: false` and `flush: false`, an empty
  upstream completes without requiring a scheduler because the sink performs no
  IO operation.
- FiberStream validates the Ruby fiber context, but non-blocking behavior also
  depends on the IO-like object's `write`, `flush`, and `close`
  implementations being compatible with Ruby's scheduler hooks.

## Public Contracts

```ruby
FiberStream::Sink.io(io, close: false, flush: false)
FiberStream::SchedulerRequiredError
```

Initial RBS shape:

```rbs
module FiberStream
  class Sink[In, Mat]
    def self.io: (
      untyped io,
      ?close: bool,
      ?flush: bool
    ) -> Sink[String, Integer]
  end
end
```

## Examples

Direct use with a Ruby core pipe inside a scheduler-backed non-blocking fiber:

```ruby
require "async"
require "fiber_stream"

written =
  Async do
    reader, writer = IO.pipe

    FiberStream::Source.each(["hello", "world"])
      .run_with(FiberStream::Sink.io(writer, close: true))

    reader.read
  ensure
    reader&.close
  end.wait

written # => "helloworld"
```

Counting written chunks:

```ruby
count =
  Async do
    FiberStream::Source.each(["a", "b", "c"])
      .run_with(FiberStream::Sink.io(writer, flush: true))
  end.wait

count # => 3
```

Direct use with a Ruby core file:

```ruby
count =
  Async do
    file = File.open("out.bin", "wb")

    FiberStream::Source.each(["a", "b", "c"])
      .run_with(FiberStream::Sink.io(file, close: true))
  end.wait

count # => 3
```

Using an `io-stream` wrapper remains optional caller-managed composition:

```ruby
require "io/stream"

stream = IO.Stream(writer)

Async do
  FiberStream::Source.each(["chunk"])
    .run_with(FiberStream::Sink.io(stream, flush: true, close: true))
end.wait
```

When no scheduler-backed non-blocking context is available:

```ruby
FiberStream::Source.each(["chunk"])
  .run_with(FiberStream::Sink.io(writer))

# raises FiberStream::SchedulerRequiredError
```

## Open Questions

None.
