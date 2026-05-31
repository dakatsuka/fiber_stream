# Flow.lines

## Status

Accepted

## Problem

`Source.io` emits arbitrary `String` chunks. Application code often needs
record-like line values instead of IO chunk boundaries. Users need a reusable
flow that splits string chunks into lines while preserving FiberStream's lazy,
pull-based execution model.

## Goals

- Add `FiberStream::Flow.lines(chomp: true, max_length: nil)`.
- Add `Source#lines(chomp: true, max_length: nil)` as the source convenience
  wrapper.
- Split `String` chunks on newline boundaries.
- Preserve lines that span chunk boundaries.
- Emit a final unterminated line at upstream completion.
- Preserve pull backpressure.
- Provide optional maximum line length protection.
- Keep the API compatible with a future general delimiter framing flow.

## Non-Goals

- General delimiter framing.
- Text transcoding or encoding validation.
- Unicode grapheme, character, or display-width length limits.
- CSV, JSON Lines, or protocol-specific parsing.
- Scheduler-backed execution.

## Requirements

- `Flow.lines(chomp: true, max_length: nil)` creates a flow from `String`
  chunks to line `String` values.
- `Source#lines(chomp: true, max_length: nil)` is equivalent to
  `Source#via(FiberStream::Flow.lines(chomp:, max_length:))`.
- `chomp` must be exactly `true` or `false`.
- Non-boolean `chomp` values raise `TypeError`.
- `max_length` may be `nil` or a positive `Integer`.
- `max_length: nil` means no line length limit.
- Non-`nil`, non-`Integer` `max_length` values raise `TypeError`.
- Zero or negative `max_length` values raise `ArgumentError`.
- Construction is lazy and does not pull upstream.
- Input elements must be `String` values.
- Non-`String` input elements raise `TypeError`.
- Newline byte `"\n"` is the delimiter, independent of the input string's
  encoding.
- Output lines are byte-oriented `String` values derived from the input bytes.
- Lines may span multiple input chunks.
- A single input chunk may produce multiple output lines over multiple
  downstream demands.
- Empty lines are emitted.
- With `chomp: true`, emitted lines do not include the trailing `"\n"`.
- With `chomp: true`, a `"\r"` immediately before the trailing `"\n"` is also
  removed.
- With `chomp: false`, emitted lines include the trailing `"\n"` when the line
  was delimiter-terminated.
- With `chomp: false`, `"\r\n"` remains unchanged.
- When upstream completes and buffered content remains, the remaining content
  is emitted as the final line.
- When upstream completes and no buffered content remains, the flow completes.
- A final unterminated line is emitted without adding a delimiter.
- `max_length` is measured with `String#bytesize`.
- `max_length` applies per line, not to the aggregate internal buffer.
- `max_length` applies to the next line to be emitted, including the delimiter
  when that line is delimiter-terminated.
- If one chunk contains both a valid line and a later over-limit line, the
  valid line is emitted first and the later line fails on the downstream demand
  that reaches it.
- If a line exceeds `max_length`, the stream fails with
  `FiberStream::FrameTooLongError`.
- When `FrameTooLongError` occurs, upstream is closed before
  `Source#run_with` returns.
- If frame-length failure and upstream close failure both occur,
  `FrameTooLongError` is primary and close failure is suppressed.
- `Flow.lines` itself does not require `Fiber.scheduler`.
- Public APIs never expose `Pull::DONE`.

## Public Contracts

```ruby
FiberStream::Flow.lines(chomp: true, max_length: nil)
FiberStream::Source#lines(chomp: true, max_length: nil)
FiberStream::FrameTooLongError
```

Initial RBS shape:

```rbs
module FiberStream
  class FrameTooLongError < RuntimeError
  end

  class Source[Elem]
    def lines: (?chomp: bool, ?max_length: Integer?) -> Source[String]
  end

  class Flow[In, Out]
    def self.lines: (?chomp: bool, ?max_length: Integer?) -> Flow[String, String]
  end
end
```

## Examples

Chunk boundaries do not define line boundaries:

```ruby
result =
  FiberStream::Source.each(["hel", "lo\nwor", "ld\n"])
    .lines
    .run_with(FiberStream::Sink.to_a)

result # => ["hello", "world"]
```

Keeping delimiters:

```ruby
result =
  FiberStream::Source.each(["a\nb"])
    .via(FiberStream::Flow.lines(chomp: false))
    .run_with(FiberStream::Sink.to_a)

result # => ["a\n", "b"]
```

Protecting a long-running stream:

```ruby
FiberStream::Source.each(["too long"])
  .via(FiberStream::Flow.lines(max_length: 3))
  .run_with(FiberStream::Sink.to_a)

# raises FiberStream::FrameTooLongError
```

## Open Questions

None.
