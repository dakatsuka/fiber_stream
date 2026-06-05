# ADR 0014: Flow.split

## Status

Accepted

## Context

`Flow.lines` turns chunked `String` streams into newline records, but users also
need simple delimiter framing where the separator is not a newline. The API
should be broad enough for common byte-oriented formats while remaining small
and predictable.

## Decision

Add `FiberStream::Flow.split(separator, keep_separator: false, max_length: nil)`
and the corresponding
`Source#split(separator, keep_separator: false, max_length: nil)` convenience
wrapper.

The flow accepts `String` chunks and emits byte-oriented frame `String` values
split on a non-empty `String` separator, independent of input string encoding.
Frames and separators may span chunks, and one chunk may produce multiple
frames across downstream pulls. Overlapping separators use leftmost
non-overlapping matches. At upstream completion, buffered content is emitted as
one final unterminated frame.

`keep_separator: false` is the default and emits terminated frames without the
separator. `keep_separator: true` preserves the separator on terminated frames.
Final unterminated frames never add a separator.

Consecutive separators emit empty frames. A trailing separator does not emit an
extra final empty frame after upstream completion.

`max_length: nil` is the default and means unbounded frame buffering.
`max_length:` can also be a positive integer bytesize limit. The limit applies
to the frame body excluding separator bytes and is independent of
`keep_separator`. Exceeding the limit fails the stream with
`FiberStream::FrameTooLongError`, closes upstream, and keeps the frame-length
failure primary if close also fails.

`Flow.lines` remains a separate operator and keeps its existing CRLF chomping
and `max_length` behavior. `Flow.split("\n")` is not a semantic replacement for
`Flow.lines`.

## Consequences

- FiberStream can split chunked streams on simple delimiters without requiring
  custom map/fold code in user pipelines.
- Users can call either `source.via(Flow.split(separator))` or the convenience
  `source.split(separator)`.
- Users get optional protection against unbounded frame buffering.
- The API stays byte-oriented and avoids encoding policy.
- `Flow.lines` compatibility is preserved.

## Alternatives Rejected

- Naming the operator `Flow.frames`.
- Supporting regular expression separators in the first version.
- Counting separator bytes in `max_length`.
- Refactoring `Flow.lines` onto `Flow.split` as part of this change.
