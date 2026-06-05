# Flow.split

## Status

Accepted

## Context

FiberStream has `Flow.lines` for newline-oriented records from chunked
`String` sources. Users also need the same pull-driven framing behavior for
simple delimiter-separated streams where the delimiter is not a newline.

Governing documents:

- Product spec: `docs/product-specs/flow-split.md`
- Existing line design: `docs/design-docs/flow-lines.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Composable pipelines: `docs/design-docs/composable-pipelines.md`
- ADR: `docs/design-docs/adr/0014-flow-split.md`

## Goals

- Add general byte-oriented delimiter splitting for `String` chunk streams.
- Keep the API discoverable as `Flow.split`, matching Ruby terminology.
- Keep source-level convenience consistent with existing flow wrappers.
- Preserve pull-based backpressure and lazy construction.
- Provide `max_length:` protection for streams where separators may never
  arrive.
- Keep `Flow.lines` source-compatible.

## Non-Goals

- Refactoring `Flow.lines` onto `Flow.split`.
- Regular expression or multi-separator matching.
- Encoding-aware text processing.
- Scheduler-backed split processing.
- Protocol parsing beyond delimiter framing.

## Proposed Design

`Flow.split(separator, keep_separator: false, max_length: nil)` creates a flow
backed by a private pull stream stage. The stage stores an upstream pull
stream, a binary separator string, the `keep_separator` flag, an optional
maximum frame body bytesize, and a mutable binary buffer.

The stage is demand-driven. On each downstream `#next`, it first checks whether
the current buffer already contains the separator. If so, it removes and emits
one frame. If not, it pulls one upstream chunk and appends it to the buffer.
This continues until a frame is available, a frame-length failure is detected,
or upstream completes.

Separator matching is byte-oriented. The separator and internal buffer use
binary strings so ASCII-incompatible input encodings do not change framing
behavior.

Separator matching uses leftmost non-overlapping matches. For example,
splitting `ababa` on `aba` emits `["", "ba"]` rather than treating the second
`aba` as another match starting inside the first match. This follows repeated
`String#index` delimiter splitting and keeps the streaming implementation
deterministic.

When `keep_separator` is `false`, a terminated frame emits only the bytes
before the separator. When `keep_separator` is `true`, a terminated frame emits
the bytes before the separator plus the separator bytes. Final unterminated
frames are emitted without adding a separator.

Consecutive separators emit empty frames. A trailing separator empties the
buffer and does not produce an extra final empty frame after upstream
completion. For example, splitting `a||` on `|` emits `["a", ""]`, splitting
`||` emits `["", ""]`, splitting `|` emits `[""]`, and splitting an empty
input stream emits `[]`. With `keep_separator: true`, empty terminated frames
include the separator, so splitting `|` emits `["|"]`.

Maximum length is checked against the next frame body, excluding separator
bytes. If the buffer contains `ok,` followed by an over-limit unterminated
frame, the stage emits `ok` first and fails only when downstream demands the
later over-limit frame. This keeps validation per frame rather than applying
the limit to the aggregate internal buffer.

Multi-byte separators require one extra guard for `max_length`. If the buffer
does not yet contain the full separator, the stage subtracts the longest
buffer suffix that is also a prefix of the separator from the pending frame
length check. This prevents a frame such as `a-` with separator `--` and
`max_length: 1` from failing before the next chunk can complete the separator.

When upstream completes, the stage emits the remaining buffer once if it is not
empty, then completes on later pulls. Empty final buffers do not emit an extra
frame.

Input validation happens when elements are pulled. Non-`String` elements raise
`TypeError` and close upstream through the normal `Source#run_with` cleanup
path.

When a frame body exceeds `max_length`, the stage raises
`FiberStream::FrameTooLongError`. The stage closes upstream before raising so
resource-owning sources and async/buffer boundaries can release state. If
upstream close also fails, the frame-length error remains primary and the close
failure is suppressed.

`Flow.split` itself does not create fibers and does not require a scheduler.
If users need upstream prefetch, they can compose it with `Flow.buffer(count)`.

`Source#split(separator, keep_separator: false, max_length: nil)` is a
convenience wrapper around
`Source#via(FiberStream::Flow.split(separator, keep_separator:, max_length:))`.
It follows the same pattern as other source convenience wrappers.

`Flow.split("\n")` is intentionally not equivalent to `Flow.lines`. `Flow.lines`
has line-specific CRLF chomping and preserves its existing `max_length`
contract, which counts the newline byte for terminated lines. `Flow.split`
does not chomp carriage returns and measures `max_length` against the frame body
excluding separator bytes.

## Contracts

- `Flow.split(separator, keep_separator: false, max_length: nil)` returns
  `Flow[String, String]`.
- `Source#split(separator, keep_separator: false, max_length: nil)` returns
  `Source[String]`.
- `separator` must be a non-empty `String`.
- `keep_separator` must be exactly `true` or `false`.
- `max_length` must be `nil` or a positive `Integer`.
- Construction is lazy.
- Input elements must be `String`.
- Separator matching is byte-oriented and independent of input string
  encoding.
- Output frames are byte-oriented `String` values.
- Frames and separators may span chunks.
- Separator matches are leftmost and non-overlapping.
- One chunk may produce many frames across many downstream pulls.
- Consecutive separators emit empty frames.
- EOF emits one final unterminated frame when buffered content remains.
- A trailing separator does not emit an extra final empty frame.
- `keep_separator: true` preserves separator bytes on terminated frames.
- `max_length` uses `String#bytesize`.
- `max_length` applies per frame body, excluding the separator.
- Exceeding `max_length` raises `FrameTooLongError`.
- Frame-length failure closes upstream and remains primary over close failure.
- Public APIs never expose `Pull::DONE`.

## Alternatives Considered

### Add `Flow.frames`

`frames` is a common term in byte-stream protocol libraries, but Ruby users are
already familiar with `String#split` and the user-facing operation is simple
delimiter splitting. `split` is clearer for the first general API.

### Support Regular Expression Separators

Regex splitting would align with `String#split`, but streaming regex semantics
across chunk boundaries are harder to make predictable and can hide expensive
matching behavior. A single non-empty string separator is enough for the first
version.

### Include Separator Bytes In `max_length`

`Flow.lines` counts the newline delimiter when enforcing `max_length`, but
`Flow.split` makes separator retention configurable. Measuring only the frame
body keeps the guard stable regardless of `keep_separator` and separator
length.

### Refactor `Flow.lines` Onto `Flow.split`

`Flow.lines` has CRLF chomping and existing `max_length` behavior that includes
the newline byte for terminated lines. Keeping it unchanged avoids a silent
compatibility change.

## Third-Party Review

Context-free design review requested on 2026-06-05. Feedback will be recorded
before implementation.

Review findings were incorporated before implementation:

- Explicitly documented that `max_length` uses frame-body semantics and differs
  from `Flow.lines`.
- Added leftmost non-overlapping separator matching.
- Clarified that `Flow.split("\n")` does not perform CRLF chomping and is not a
  semantic replacement for `Flow.lines`.
- Added concrete examples for consecutive separators, trailing separators, and
  empty terminated frames with `keep_separator: true`.

## Validation

- Unit tests for argument validation and lazy construction.
- Unit tests for frames across chunk boundaries.
- Unit tests for separators across chunk boundaries.
- Unit tests for multiple frames in one chunk.
- Unit tests for consecutive separators and trailing separators.
- Unit tests for final unterminated frames.
- Unit tests for `keep_separator: true` and `keep_separator: false`.
- Unit tests for non-`String` input failure.
- Unit tests for `max_length` success and failure, including one chunk that
  contains a valid frame before a later over-limit frame.
- Unit tests proving partial multi-byte separator suffixes are not counted
  prematurely by `max_length`.
- Unit tests proving `FrameTooLongError` closes upstream and wins over close
  failure.
- RBS validation.
- RuboCop.

## Open Questions

None.
