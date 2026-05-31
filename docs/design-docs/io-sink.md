# IO Sink

## Status

Accepted

## Context

`Source.io` defines scheduler-aware, resource-owning reads from IO-like
objects. The corresponding sink should write byte chunks to IO without
introducing a separate IO abstraction layer or runtime dependency. FiberStream
should remain responsible for stream processing, pull backpressure, and
materialization. Ruby core IO objects are the primary boundary. Libraries such
as `io-stream` can remain caller-provided adapters that normalize low-level IO
behavior when users need that extra layer.

Governing documents:

- Product spec: `docs/product-specs/io-sink.md`
- IO source design: `docs/design-docs/io-source.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Async design: `docs/design-docs/async-boundary.md`
- Buffer design: `docs/design-docs/buffer-boundary.md`
- References:
  - `docs/references/ruby-fiber-and-tooling.md`
  - `docs/references/socketry-io-stream.md`

## Goals

- Add a narrow byte-chunk IO sink for existing IO-like objects.
- Make Ruby core IO objects the primary supported output boundary.
- Preserve lazy construction and downstream-driven upstream pulls.
- Require scheduler-backed non-blocking execution before sink IO operations.
- Make write, flush, and close behavior explicit and testable.
- Compose with `Source.io`, `Flow.async`, `Flow.buffer(count)`, and
  caller-provided `io-stream` wrappers.

## Non-Goals

- File opening helpers.
- Text formatting, encoding, delimiters, or framing.
- Batching, retry, timeout, or partial-write recovery policies.
- IO source/sink duplex helpers.
- Runtime dependency on Async or `io-stream`.

## Proposed Design

`Sink.io(io, close: false, flush: false)` creates a sink definition backed by a
private sink runner:

```ruby
materialized_value = Sink.io(io, close: true).run(pull_stream)
```

Construction validates public arguments but does not pull upstream, write,
flush, or close. The sink stores the IO-like object itself, not a reopenable
factory or output buffer. Each materialization writes to the same object's
current state, so repeatability depends entirely on that object. Later
materializations may append after earlier writes, flush the same IO again, or
fail because an earlier materialization closed the IO.

The sink owns the IO object's close lifetime only when `close: true` is passed.

The primary intended outputs are Ruby core and standard-library IO objects that
already expose `write`, such as `File`, `IO.pipe` write endpoints, sockets,
`$stdout`, and `$stderr`. Caller-provided adapters, including `io-stream`
wrappers, are accepted structurally when they provide the required methods, but
FiberStream does not depend on or construct those adapters.

The sink runner state is:

- `io`, the IO-like object
- `close_io`, the boolean ownership flag
- `flush_io`, the boolean final-flush flag
- `chunks_written`, the number of successful `write` calls
- `io_closed`, whether FiberStream has already called `io.close`

The sink repeatedly pulls one upstream element. If upstream returns
`Pull::DONE`, normal completion begins. Otherwise the element must be a
`String`. Non-`String` elements fail the stream with `TypeError`.

For each `String` element, the sink validates that `Fiber.scheduler` is
available and `Fiber.current.blocking?` is false, then calls
`io.write(chunk)` exactly once. The IO-like object must provide
complete-write-or-raise semantics for that chunk. The write return value is
ignored. This avoids inventing partial-write semantics for arbitrary IO-like
objects. IO-like objects that can return after accepting only part of a chunk
are unsupported unless callers wrap them in an adapter, such as an `io-stream`
wrapper, that provides complete-write behavior. A write is considered
successful when it returns without raising, and the sink increments
`chunks_written` after that return. Empty strings are valid and still call
`write`.

On normal upstream completion, `flush: true` validates scheduler context and
calls `io.flush` once before close. `flush: false` never calls `flush`. Flush is
only a normal-completion operation; it is not attempted after upstream failure,
element type failure, scheduler validation failure, or write failure.

`close` is a sink-owned cleanup operation, not a public stream close method.
When `close: true`, the sink calls `io.close` once before returning or
re-raising a primary failure. With `close: false`, FiberStream never calls
`io.close`.

Scheduler validation happens before each sink IO operation that is part of
normal progress: write, normal-completion flush, and normal-completion close.
If validation fails and `close: true`, the sink still attempts to close the IO
to honor ownership and prevent leaks. Scheduler validation failure remains the
primary failure if close also fails.

When the sink is already handling an upstream failure, element type failure, or
write failure, cleanup close does not introduce a new scheduler validation
failure. It attempts owned close and suppresses close failure so the original
failure remains primary. This matches `Source.io` and the existing
`Source#run_with` primary-error rule.

The same cleanup rule applies after scheduler validation failure for write,
flush, or normal-completion close. Once validation fails, owned cleanup close is
attempted without another scheduler validation, and the original
`SchedulerRequiredError` remains primary.

Close/error precedence is:

- Normal upstream completion plus close failure delivers the close failure.
- Normal upstream completion plus flush failure delivers the flush failure.
- Normal upstream completion plus flush failure and close failure delivers the
  flush failure and suppresses the close failure.
- Upstream failure plus close failure delivers the upstream failure and
  suppresses the close failure.
- Element type failure plus close failure delivers the `TypeError` and
  suppresses the close failure.
- Scheduler validation failure plus close failure delivers
  `SchedulerRequiredError` and suppresses the close failure.
- Write failure plus close failure delivers the write failure and suppresses
  the close failure.

`Sink.io` itself does not start a producer or consumer fiber. Direct
materialization writes in the caller's current non-blocking fiber. If users
place `Flow.buffer(count)` upstream, upstream stages may run ahead according to
that boundary's bounded queue contract, but `Sink.io` still pulls and writes one
element at a time from its downstream end.

Ruby core IO integration is structural. `Sink.io` accepts any object that
responds to the needed methods, including `File`, pipes, sockets, standard
streams, and caller-provided `IO::Stream` wrappers. FiberStream does not depend
on `io-stream`, does not construct wrappers implicitly, and does not promise
`io-stream` buffering or connection-reset semantics itself.

## Contracts

- `Sink.io(io, close: false, flush: false)` returns `Sink[String, Integer]`.
- `Sink.io` validates `io.respond_to?(:write)`.
- Missing `write` raises `TypeError`.
- Ruby core `IO` objects with complete-write-or-raise `write` behavior are
  supported directly.
- `Sink.io` validates `io.respond_to?(:close)` when `close: true`.
- Missing `close` with `close: true` raises `TypeError`.
- `Sink.io` validates `io.respond_to?(:flush)` when `flush: true`.
- Missing `flush` with `flush: true` raises `TypeError`.
- `Sink.io` validates `close` as exactly `true` or `false`.
- `Sink.io` validates `flush` as exactly `true` or `false`.
- Construction is lazy and performs no upstream pulls or IO operations.
- `Sink.io` stores the same IO object across materializations.
- Each materialization writes to the IO object's current state.
- `Sink.io` does not guarantee repeatable output semantics.
- The sink consumes upstream until `Pull::DONE` or failure.
- Upstream elements must be `String` values.
- Non-`String` upstream elements raise `TypeError`.
- Empty strings are valid and are written.
- Before each write, normal-completion flush, or normal-completion close, the
  sink requires `Fiber.scheduler`.
- Before each write, normal-completion flush, or normal-completion close, the
  sink requires `Fiber.current.blocking?` to be false.
- Missing scheduler or blocking current fiber raises
  `SchedulerRequiredError`.
- Scheduler validation failure closes owned IO.
- Scheduler validation failure wins over owned IO close failure.
- Each upstream `String` causes exactly one `io.write(chunk)` call.
- The IO-like object must provide complete-write-or-raise `write` semantics.
- `io.write` return values are ignored.
- Short-write-capable objects are unsupported unless wrapped by an adapter that
  provides complete-write behavior.
- The materialized value is the count of successful write calls.
- `io.write` failures fail the stream.
- With `flush: false`, FiberStream does not call `io.flush`.
- With `flush: true`, normal completion calls `io.flush` once.
- Flush is not attempted after upstream, type, scheduler, or write failure.
- With `close: false`, FiberStream does not call `io.close`.
- With `close: true`, normal completion, upstream failure, type failure,
  scheduler failure, and write failure close IO before returning or re-raising.
- Empty upstream with `flush: false` and `close: false` does not require a
  scheduler.
- Normal completion close failure is delivered as stream failure.
- Normal completion flush failure is delivered as stream failure.
- Flush failure wins over close failure.
- Upstream, type, scheduler, and write failures win over close failure.
- Public APIs never expose `Pull::DONE`.

## Alternatives Considered

### Name The API `Sink.write`

`Sink.write(io)` describes the operation, but it is less symmetric with
`Source.io(io)`. `Sink.io` makes the IO boundary easy to find and keeps future
`Sink.file` or protocol-specific APIs available as separate names.

### Return Written Byte Count

A byte count is useful, but it requires trusting or interpreting arbitrary
`io.write` return values and opens questions about partial writes. Counting
successful chunks keeps the first sink contract simple. A future sink can add
byte accounting if needed.

### Flush By Default

Flushing by default can add unnecessary syscalls and may fight buffering
policies in wrapped IO objects such as `IO::Stream`. `flush: true` makes the
normal-completion flush explicit.

### Close Passed IO By Default

Closing by default would surprise users who pass shared handles such as
`STDOUT`, sockets owned by another component, or caller-managed `io-stream`
wrappers. `close: true` makes ownership explicit.

### Depend On `io-stream`

`io-stream` is a useful low-level adapter, but Ruby core IO objects are the
primary output boundary for FiberStream. Making `io-stream` a runtime
dependency would make FiberStream responsible for choosing an IO normalization
layer. Accepting any `write`-compatible object keeps the boundary narrow and
allows callers to use Ruby core IO directly or opt into `io-stream` when they
need it.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-05-31. Feedback resulted in these
changes:

- Required IO-like objects to provide complete-write-or-raise semantics and
  clarified that short-write-capable objects need an adapter.
- Clarified repeated materialization semantics with the same IO object.
- Clarified cleanup close after scheduler validation failure for write, flush,
  and normal-completion close.
- Specified `TypeError` for missing `write`, `close`, and `flush` methods.
- Updated the `io-stream` example to require the gem and use `IO.Stream(...)`.
- Clarified that Ruby core IO objects are the primary source and sink boundary,
  while `io-stream` remains an optional caller-provided adapter.

## Validation

- Unit tests for argument validation and lazy construction.
- Unit tests proving repeated materialization writes to the same IO object
  state.
- Unit tests that missing scheduler or blocking current fiber raises
  `SchedulerRequiredError` before writing and closes owned IO.
- Async-backed tests proving ordered writes, empty-string writes, chunk-count
  materialization, and one pull per write attempt.
- Tests proving non-`String` upstream values raise `TypeError`.
- Tests proving `close: false` never closes IO on normal completion, upstream
  failure, type failure, scheduler failure, or write failure.
- Tests proving `close: true` closes on normal completion, upstream failure,
  type failure, scheduler failure, and write failure.
- Tests proving `flush: false` never flushes.
- Tests proving `flush: true` flushes once on normal completion.
- Tests proving flush is skipped after failure.
- Tests proving close/error precedence for normal completion, upstream failure,
  type failure, scheduler failure, write failure, and flush failure.
- RBS validation.
- RuboCop.

## Open Questions

None.
