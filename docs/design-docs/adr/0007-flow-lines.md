# ADR 0007: Flow.lines

## Status

Accepted

## Context

`Source.io` emits arbitrary `String` chunks, while applications often need
line records. Other stream libraries commonly provide line or delimiter
framing stages, and Ruby core IO exposes line iteration directly. FiberStream
should provide this as a composable flow so it works with IO sources,
collection-backed sources, and future sources.

## Decision

Add `FiberStream::Flow.lines(chomp: true, max_length: nil)` and the
corresponding `Source#lines(chomp: true, max_length: nil)` convenience wrapper.

The flow accepts `String` chunks and emits byte-oriented line `String` values
split on newline byte `"\n"`, independent of input string encoding. Lines may
span chunks, and one chunk may produce multiple lines across downstream pulls.
At upstream completion, buffered content is emitted as one final unterminated
line.

`chomp: true` is the default and removes the trailing newline plus one
immediately preceding carriage return. `chomp: false` preserves delimiters.

`max_length: nil` is the default and means unbounded line buffering.
`max_length:` can also be a positive integer bytesize limit. Exceeding the
limit fails the stream with `FiberStream::FrameTooLongError`, closes upstream,
and keeps the frame-length failure primary if close also fails.
The limit applies per line/frame rather than to the aggregate internal buffer;
when one chunk contains a valid line followed by a later over-limit line, the
valid line is emitted before the later line fails.

`Flow.lines` remains narrower than a future general delimiter API. If
`Flow.delimited` is added later, `Flow.lines` should remain as the ergonomic
newline wrapper or alias.

## Consequences

- FiberStream can turn IO chunks into line records without source-specific line
  APIs.
- Users can call either `source.via(Flow.lines)` or the convenience
  `source.lines`.
- Users get optional protection against unbounded line buffering.
- The first line-framing API stays byte-oriented and avoids encoding policy.
- General delimiter framing remains future work.

## Alternatives Rejected

- Adding `Flow.delimited` before `Flow.lines`.
- Omitting length protection.
- Using character length instead of bytesize for `max_length`.
- Omitting `Source#lines`.
