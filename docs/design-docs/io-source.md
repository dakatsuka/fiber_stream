# IO Source

## Status

Accepted

## Context

FiberStream's accepted runtime is a linear pull chain. `Flow.async` and
`Flow.buffer(count)` add explicit scheduler boundaries and cancellation
contracts, but the only source today is `Source.each`, which intentionally does
not own resources. IO sources need a public contract for scheduler use,
resource ownership, EOF, read failures, close failures, and early downstream
completion.

Governing documents:

- Product spec: `docs/product-specs/io-source.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Async design: `docs/design-docs/async-boundary.md`
- Buffer design: `docs/design-docs/buffer-boundary.md`
- References:
  - `docs/references/ruby-fiber-and-tooling.md`
  - `docs/references/socketry-io-stream.md`

## Goals

- Add a narrow, byte-chunk IO source for existing IO-like objects.
- Make Ruby core IO objects the primary supported input boundary.
- Preserve lazy construction and downstream-driven reads.
- Require scheduler-backed non-blocking execution for IO reads.
- Make IO close ownership explicit and testable.
- Compose cleanly with existing `Flow.async` and `Flow.buffer(count)` stages.

## Non-Goals

- File opening helpers.
- Line, delimiter, or frame parsing.
- Text transcoding policy.
- IO writes and sinks.
- Socket connection management.
- Timeout or retry APIs.
- Scheduler installation or Async runtime dependency.
- Runtime dependency on `io-stream`.

## Proposed Design

`Source.io(io, chunk_size: 16 * 1024, close: false)` creates a source
definition backed by a new private pull stream:

```ruby
pull_stream = Pull.io(io, chunk_size, close)
```

Construction validates the public arguments but does not read from or close the
IO object. `Source.io` stores the object itself, not a reopenable factory or
snapshot. Each materialization reads from the same object's current position,
so replayability depends entirely on that object. Later materializations may
continue after earlier reads, observe EOF, or fail because an earlier
materialization closed the IO.

The pull stream owns only its materialized read state. It owns the IO object's
lifetime only when `close: true` is passed.

The primary intended inputs are Ruby core and standard-library IO objects that
already expose `readpartial`, such as `File`, `IO.pipe` read endpoints, and
sockets. Caller-provided adapters, including `io-stream` wrappers, are accepted
structurally when they provide the same methods, but FiberStream does not
depend on or construct those adapters.

The pull stream state is:

- `io`, the IO-like object
- `chunk_size`, the maximum read size
- `close_io`, the boolean ownership flag
- `closed`, whether this pull stream has been closed
- `done`, whether EOF or terminal failure has completed the stream
- `io_closed`, whether FiberStream has already called `io.close`

On the first `next`, and defensively on later reads, the source validates that
`Fiber.scheduler` is available and `Fiber.current.blocking?` is false. If either
condition is missing, it closes owned IO and raises `SchedulerRequiredError`
before calling `readpartial`. Ruby's scheduler hooks run only in non-blocking
execution contexts, so checking the current fiber prevents a direct foreground
materialization from accidentally blocking the thread. If owned IO close also
fails during scheduler validation failure, `SchedulerRequiredError` remains the
primary failure and the close failure is suppressed.

Each `next` calls `io.readpartial(chunk_size)` at most once. `chunk_size` is the
maximum byte count FiberStream passes to `io.readpartial` for one downstream
pull, not a stream-level memory cap. The default remains `16 * 1024`;
FiberStream validates only that caller-provided values are positive integers and
does not impose a library-level upper bound. Appropriate read sizes depend on
the IO object, workload, and deployment memory budget; callers that override
the default own that memory tradeoff. Very large values may cause the IO
implementation to attempt large allocations.

A returned `String` is emitted unchanged. A non-`String` return value is treated
as an IO contract violation: the source marks itself done, closes owned IO, and
raises `TypeError`. `EOFError` marks the stream done; when `close: true`, the
source closes the IO before returning `Pull::DONE`. If that close fails, the
close failure is raised instead of returning normal completion. Exceptions
other than `EOFError` mark the stream done, close the IO when owned, and
re-raise the read failure. If close also fails on that read-failure path, the
read failure remains primary and the close failure is suppressed.

`close` is idempotent. It marks the source closed and, only when `close: true`,
calls `io.close` once. With `close: false`, close only stops future reads from
this materialization; it never calls `io.close`.

Close/error precedence is:

- EOF plus owned IO close failure delivers the close failure as stream failure.
- Read failure plus owned IO close failure delivers the read failure and
  suppresses the close failure.
- Scheduler validation failure plus owned IO close failure delivers
  `SchedulerRequiredError` and suppresses the close failure.
- Early downstream completion plus owned IO close failure delivers the close
  failure from `Source#run_with`.
- Downstream failure plus owned IO close failure delivers the downstream
  failure and suppresses the close failure through the existing `run_with`
  primary-error rule.

`Source.io` itself does not start a producer fiber and does not prefetch. Direct
materialization reads in the caller's current non-blocking fiber. If users want
IO reads to happen in an upstream producer fiber or want prefetch, they compose
the source with `Flow.async` or `Flow.buffer(count)`:

```ruby
Source.io(socket, close: true)
  .buffer(8)
  .run_with(Sink.to_a)
```

This keeps IO source semantics small and leaves concurrency shape explicit at
the flow boundary where FiberStream already has cancellation and queue
contracts. FiberStream validates the Ruby fiber context before reads, but it
cannot prove that every custom `readpartial` implementation cooperates with the
scheduler. The non-blocking guarantee depends on the IO-like object using Ruby
scheduler-aware IO behavior.

## Contracts

- `Source.io(io, chunk_size: 16 * 1024, close: false)` returns
  `Source[String]`.
- `Source.io` validates `io.respond_to?(:readpartial)`.
- `Source.io` validates `io.respond_to?(:close)` when `close: true`.
- Ruby core `IO` objects with `readpartial` are supported directly.
- `Source.io` validates `chunk_size` as a positive `Integer`.
- `Source.io` does not impose an upper bound on `chunk_size`.
- `Source.io` validates `close` as exactly `true` or `false`.
- Construction is lazy and performs no IO reads.
- `Source.io` stores the same IO object across materializations.
- Each materialization reads from the IO object's current position.
- `Source.io` does not guarantee replayability.
- First demand requires `Fiber.scheduler`.
- First demand requires `Fiber.current.blocking?` to be false.
- Missing scheduler or blocking current fiber raises
  `SchedulerRequiredError`.
- Scheduler validation failure closes owned IO.
- Scheduler validation failure wins over owned IO close failure.
- Each downstream pull performs at most one `readpartial(chunk_size)` call.
- `String` values returned by `readpartial` are emitted unchanged.
- Non-`String` values returned by `readpartial` raise `TypeError`.
- `EOFError` from `readpartial` is normal stream completion.
- Read failures other than `EOFError` fail the stream.
- With `close: false`, FiberStream does not close the IO object.
- With `close: true`, EOF closes IO before downstream receives completion.
- With `close: true`, early downstream close closes IO.
- With `close: true`, downstream failure closes IO before `run_with` returns.
- With `close: true`, close failure after EOF is delivered as stream failure.
- With `close: true`, read failure wins over close failure when both occur.
- With `close: true`, early downstream completion plus close failure delivers
  the close failure.
- With `close: true`, downstream failure wins over close failure.
- Repeated pulls after completion return `Pull::DONE` without reading again.
- `close` is idempotent.
- Public APIs never expose `Pull::DONE`.

## Alternatives Considered

### Close Passed IO By Default

Closing by default would make source ownership simple, but it is surprising for
caller-owned objects such as `STDIN`, sockets managed by another object, or test
pipes. `close: true` makes ownership explicit while still supporting
resource-owning source definitions.

### Add `Source.open(path)` First

Opening by path would give FiberStream clear ownership, but it couples the first
IO source to filesystem behavior and leaves sockets, pipes, and in-memory IO
objects for later. Starting with an existing IO-like object is smaller and
applies to more IO types.

### Add Line-Oriented Reading First

Lines are useful, but they introduce delimiter, encoding, maximum-line, and
partial-line-at-EOF contracts. Byte chunks are the lower-level primitive and can
support later line or framing flows.

### Source-Owned Producer Fiber

The source could start its own scheduled producer and hand off chunks to the
downstream fiber, but that duplicates `Flow.async` and `Flow.buffer(count)`.
Keeping the source pull-only makes concurrency explicit and reuses existing
boundary contracts.

### `read_nonblock` Loop

Using `read_nonblock` plus readiness waiting would give FiberStream more direct
control over readiness, but it would also reimplement scheduler behavior and
would need additional SSL and writable-wait handling. `readpartial` matches the
Ruby IO abstraction and lets the active scheduler mediate blocking.

### Depend On `io-stream`

`io-stream` is useful when callers want additional buffering or normalization
across IO implementations, but Ruby core IO objects are the primary boundary
for FiberStream. Adding a dependency would make FiberStream choose a low-level
IO adapter for users. Accepting structural `readpartial` objects lets callers
use Ruby core IO directly or opt into `io-stream` explicitly.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-05-31. Feedback resulted in these
changes:

- Clarified that `Source.io` is not replayable and repeated materializations
  read from the same IO object's current position.
- Defined owned IO close behavior when scheduler validation fails.
- Added a close/error precedence matrix for EOF, read failure, scheduler
  failure, early downstream completion, and downstream failure.
- Clarified that FiberStream validates the Ruby fiber context, but custom
  IO-like objects must provide scheduler-compatible `readpartial` behavior.
- Defined non-`String` `readpartial` return values as `TypeError`.

## Validation

- Unit tests for argument validation and lazy construction.
- Unit tests that missing scheduler or blocking current fiber raises
  `SchedulerRequiredError` before reading and closes owned IO.
- Async-backed tests proving chunks are emitted in order, EOF completes
  normally, and one pull performs at most one read.
- Tests proving repeated materialization reads from the same IO object state.
- Tests proving non-`String` `readpartial` return values raise `TypeError`.
- Tests proving `close: false` never closes the IO on EOF, early completion, or
  failure.
- Tests proving `close: true` closes on EOF, early completion, and downstream
  failure.
- Tests proving close failure after EOF is delivered as a stream failure.
- Tests proving read failure wins over close failure when both occur.
- Tests proving scheduler failure wins over close failure when both occur.
- Tests proving early downstream completion plus close failure delivers the
  close failure.
- Tests proving downstream failure wins over close failure.
- Tests proving repeated pulls after EOF do not read again.
- RBS validation.
- RuboCop.

## Open Questions

None.
