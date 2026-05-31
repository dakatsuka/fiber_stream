# ADR 0005: IO Sink

## Status

Draft

## Context

`Source.io` adds scheduler-aware reads from IO-like objects. FiberStream also
needs a write-side materialization API so pipelines can terminate at IO while
preserving explicit backpressure, scheduler, flush, and close behavior.

## Decision

Add `FiberStream::Sink.io(io, close: false, flush: false)` as the first IO sink
API. The sink consumes upstream elements, requires each element to be a
`String`, and writes each chunk with exactly one `io.write(chunk)` call. Empty
strings are valid chunks. Ruby core and standard-library IO objects such as
`File`, `IO.pipe` write endpoints, sockets, `$stdout`, and `$stderr` are the
primary intended outputs. The IO-like object's `write` method must provide
complete-write-or-raise semantics. FiberStream ignores `write` return values,
does not recover from short writes, and returns the number of successful write
calls.

The sink requires a scheduler-backed non-blocking execution context before
write, normal-completion flush, and normal-completion close operations. If
scheduler validation fails and the IO is owned, FiberStream closes the IO and
keeps `SchedulerRequiredError` as the primary failure if close also fails.

`close: false` is the default and means FiberStream never calls `io.close`.
Passing `close: true` transfers close responsibility for that materialization to
FiberStream. `Sink.io` stores the same IO object across materializations and
does not promise repeatable output semantics; later runs write to the object's
current state and may fail if an earlier run closed it. Owned IO is closed on
normal completion, upstream failure, element type failure, scheduler validation
failure, and write failure.

`flush: false` is the default and means FiberStream never calls `io.flush`.
Passing `flush: true` flushes once after normal upstream completion and before
owned close. Flush is not attempted after failures. Flush failure after normal
completion is delivered as a stream failure and wins over close failure.
After scheduler validation failure for write, flush, or normal-completion close,
owned cleanup close is attempted without another scheduler validation and the
original `SchedulerRequiredError` remains primary.

FiberStream does not depend on `io-stream` and does not wrap IO objects
implicitly. `Sink.io` accepts Ruby core IO objects directly and also accepts any
other object that responds to the required methods, including caller-provided
`IO::Stream` wrappers.

## Consequences

- FiberStream gains the write-side IO boundary matching `Source.io`.
- Direct IO materialization must run in an existing non-blocking scheduler
  context when writes, flushes, or normal-completion closes are needed.
- The first IO sink avoids partial-write recovery and byte-count accounting.
- Short-write-capable IO adapters need to be wrapped before use with
  `Sink.io`.
- Users can opt into low-level buffering and IO normalization with `io-stream`
  without making it a FiberStream runtime dependency.
- Ruby core IO remains the default integration path.
- Higher-level file writing, line formatting, framing, and duplex helpers
  remain future work.

## Alternatives Rejected

- Naming the API `Sink.write`.
- Returning written byte counts in the first IO sink.
- Flushing by default.
- Closing passed IO objects by default.
- Depending on `io-stream` at runtime.
