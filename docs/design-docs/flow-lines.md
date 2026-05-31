# Flow.lines

## Status

Accepted

## Context

FiberStream now has IO sources and sinks plus reusable linear composition.
`Source.io` emits byte chunks, but many applications want line records.
Line-splitting is common in stream libraries and in Ruby core IO iteration, but
FiberStream should implement it as a flow so users can compose it with any
`String` source, not only Ruby core IO.

Governing documents:

- Product spec: `docs/product-specs/flow-lines.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Composable pipelines: `docs/design-docs/composable-pipelines.md`
- IO source design: `docs/design-docs/io-source.md`
- References: `docs/references/stream-line-framing.md`

## Goals

- Add line-oriented framing for `String` chunk streams.
- Keep the API simple and discoverable as `Flow.lines`.
- Keep source-level convenience consistent with existing flow wrappers.
- Preserve pull-based backpressure and lazy construction.
- Provide `max_length:` protection for streams where delimiters may never
  arrive.
- Keep room for a future general `Flow.delimited` API.

## Non-Goals

- General delimiter framing.
- Encoding-aware text processing.
- Scheduler-backed line processing.
- Protocol parsing beyond newline framing.

## Proposed Design

`Flow.lines(chomp: true, max_length: nil)` creates a flow backed by a private
pull stream stage. The stage stores an upstream pull stream, a mutable line
buffer, the `chomp` flag, and an optional maximum bytesize.

The stage is demand-driven. On each downstream `#next`, it first checks whether
the current buffer already contains a newline. If so, it removes and emits one
line. If not, it pulls one upstream chunk and appends it to the buffer. This
continues until a line is available, a frame-length failure is detected, or
upstream completes.

Maximum length is checked against the next line/frame, not against the
aggregate buffer. If the buffer contains `ok\n` followed by an over-limit
unterminated line, the stage emits `ok` first and fails only when downstream
demands the later over-limit line. This keeps the semantics compatible with a
future general delimiter framing flow, where each frame is evaluated
independently.

When upstream completes, the stage emits the remaining buffer once if it is not
empty, then completes on later pulls. Empty final buffers do not emit an extra
line.

Input validation happens when elements are pulled. Non-`String` elements raise
`TypeError` and close upstream through the normal `Source#run_with` cleanup
path.

The delimiter is newline byte `"\n"`, independent of the input string's
encoding. The internal buffer and delimiter constants use binary strings so
ASCII-incompatible input encodings do not change framing behavior.
`chomp: true` removes the trailing newline byte and also removes a carriage
return byte immediately before that newline. `chomp: false` preserves the
delimiter bytes exactly as they appeared, including `"\r\n"`.

`max_length` is an optional bytesize guard. It is measured with
`String#bytesize` against the currently buffered line, including a delimiter if
one has arrived. This is intentionally byte-oriented because `Source.io`
produces byte chunks and the guard exists to bound memory, not text display
width.

When a frame exceeds `max_length`, the stage raises
`FiberStream::FrameTooLongError`. The stage closes upstream before raising so
resource-owning sources and async/buffer boundaries can release state. If
upstream close also fails, the frame-length error remains primary and the close
failure is suppressed.

`Flow.lines` itself does not create fibers and does not require a scheduler.
If users need upstream prefetch, they can compose it with `Flow.buffer(count)`.

`Source#lines(chomp: true, max_length: nil)` is a convenience wrapper around
`Source#via(FiberStream::Flow.lines(chomp:, max_length:))`. It follows the
same pattern as `Source#map`, `Source#select`, `Source#take`,
`Source#async`, and `Source#buffer`.

## Contracts

- `Flow.lines(chomp: true, max_length: nil)` returns `Flow[String, String]`.
- `Source#lines(chomp: true, max_length: nil)` returns `Source[String]`.
- `chomp` must be exactly `true` or `false`.
- `max_length` must be `nil` or a positive `Integer`.
- Construction is lazy.
- Input elements must be `String`.
- Newline byte `"\n"` delimits lines independent of input string encoding.
- Output lines are byte-oriented `String` values.
- Lines may span chunks.
- One chunk may produce many lines across many downstream pulls.
- Empty lines are emitted.
- EOF emits one final unterminated line when buffered content remains.
- `chomp: true` removes trailing newline and one preceding carriage return.
- `chomp: false` preserves trailing newline and carriage return.
- `max_length` uses `String#bytesize`.
- `max_length` applies per line/frame, not to the aggregate internal buffer.
- Exceeding `max_length` raises `FrameTooLongError`.
- Frame-length failure closes upstream and remains primary over close failure.
- Public APIs never expose `Pull::DONE`.

## Alternatives Considered

### Add `Flow.delimited` First

A general delimiter flow would be more flexible, but line splitting is the
immediate IO use case and the discoverable API users expect. `Flow.lines` can
remain as an alias or wrapper if `Flow.delimited` is added later.

### Omit `max_length`

Ruby `IO#each_line`-style unbounded buffering is simple, but long-running
network streams can consume unbounded memory when delimiters never arrive.
Keeping the default unbounded while providing `max_length:` gives users a
practical safety valve.

### Character-Based Length Limits

Character limits depend on encoding and are not aligned with the memory
protection goal. Bytesize is explicit and matches `Source.io` byte chunks.

### Omit `Source#lines`

The user request names `Flow.lines`, but existing flow operators have source
convenience wrappers. Omitting `Source#lines` would make IO use less ergonomic
and less consistent with the rest of the API.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-05-31. Feedback resulted in these
changes:

- Clarified that `max_length` applies per line/frame rather than to the
  aggregate internal buffer.
- Added `Source#lines` for consistency with existing source convenience
  wrappers.

## Validation

- Unit tests for argument validation and lazy construction.
- Unit tests for lines across chunk boundaries.
- Unit tests for multiple lines in one chunk.
- Unit tests for empty lines.
- Unit tests for final unterminated lines.
- Unit tests for `chomp: true` and `chomp: false`.
- Unit tests for CRLF handling.
- Unit tests for non-`String` input failure.
- Unit tests for `max_length` success and failure, including one chunk that
  contains a valid line before a later over-limit line.
- Unit tests proving `FrameTooLongError` closes upstream and wins over close
  failure.
- RBS validation.
- RuboCop.
- README and examples updates.

## Open Questions

None.
