# ADR 0004: IO Source

## Status

Accepted

## Context

FiberStream has a pull-based runtime, explicit async and buffer boundaries, and
a non-resource-owning `Source.each`. The next step toward real stream
processing is reading from Ruby IO objects while preserving backpressure,
scheduler-aware non-blocking execution, and reliable cleanup.

## Decision

Add `FiberStream::Source.io(io, chunk_size: 16 * 1024, close: false)` as the
first IO source API. The source reads byte `String` chunks from an existing
IO-like object by calling `readpartial(chunk_size)` once per downstream pull.
Ruby core and standard-library IO objects such as `File`, `IO.pipe` read
endpoints, and sockets are the primary intended inputs. `EOFError` is normal
stream completion. Other read errors fail the stream.

The source requires a scheduler-backed non-blocking execution context when IO
is first demanded. It raises `FiberStream::SchedulerRequiredError` before
reading if no scheduler is available or if the current fiber is blocking. When
the IO is owned, scheduler validation failure closes the IO and preserves
`SchedulerRequiredError` as the primary failure if close also fails. FiberStream
does not install a scheduler and does not depend on Async at runtime.

The source does not close caller-provided IO by default. Passing `close: true`
transfers close responsibility for that materialization to FiberStream.
`Source.io` stores the same IO object across materializations and does not
promise replayability; later runs read from the object's current position and
may observe EOF or a previously closed IO. Owned IO is closed on EOF, early
downstream completion, downstream failure, and scheduler validation failure.
Close failure after EOF is delivered as a stream failure; if both a read and
close fail, the read failure remains primary. Close failure after early
downstream completion is delivered, while downstream failure remains primary
over close failure.

The API is typed as `Source[String]`. `readpartial` must return a `String` or
raise; non-`String` return values fail the stream with `TypeError`. FiberStream
validates scheduler context before reading, but non-blocking behavior for
custom IO-like objects still depends on their `readpartial` implementation
cooperating with Ruby scheduler hooks.

The IO source does not start a producer fiber and does not prefetch. Users who
need upstream IO to run in a producer fiber or ahead of downstream demand
compose `Source.io` with `Flow.async` or `Flow.buffer(count)`.

FiberStream does not depend on `io-stream` or construct `io-stream` wrappers.
Callers may still pass compatible wrappers explicitly when they want additional
buffering or IO normalization.

## Consequences

- FiberStream gains the first resource-aware source contract.
- Direct IO materialization must run in an existing non-blocking scheduler
  context.
- IO reads remain demand-driven unless users add an explicit async or buffer
  boundary.
- `Source.io` is not replayable in the same sense as collection-backed
  `Source.each`.
- Existing close/error precedence from buffer work carries into resource-owning
  sources.
- Higher-level file opening, line parsing, framing, and IO sinks remain future
  work.
- `io-stream` can be used as an optional adapter without becoming part of
  FiberStream's runtime surface.

## Alternatives Rejected

- Closing passed IO objects by default.
- Adding file path opening before existing IO support.
- Starting with line-oriented IO.
- Giving the source its own producer fiber.
- Reimplementing readiness handling with `read_nonblock` loops.
- Depending on `io-stream` at runtime.
