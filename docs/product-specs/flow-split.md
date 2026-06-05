# Flow.split

## Status

Accepted

## Problem

`Source.io` and other chunked `String` sources can contain application records
that are separated by delimiters other than newlines. `Flow.lines` covers the
common newline case, but users also need a reusable byte-oriented split flow for
simple delimited protocols and file formats while preserving FiberStream's
lazy, pull-based execution model.

## Goals

- Add `FiberStream::Flow.split(separator, keep_separator: false, max_length: nil)`.
- Add `Source#split(separator, keep_separator: false, max_length: nil)` as the
  source convenience wrapper.
- Split `String` chunks on a caller-provided non-empty `String` separator.
- Preserve frames that span chunk boundaries.
- Emit a final unterminated frame at upstream completion.
- Preserve pull backpressure.
- Provide optional maximum frame body length protection.
- Keep `Flow.lines` behavior unchanged.

## Non-Goals

- Replacing or changing `Flow.lines`.
- Regular expression separators.
- Multiple separators.
- Encoding-aware text processing.
- CSV, JSON Lines, or protocol-specific parsing.
- Scheduler-backed execution.

## Requirements

- `Flow.split(separator, keep_separator: false, max_length: nil)` creates a
  flow from `String` chunks to frame `String` values.
- `Source#split(separator, keep_separator: false, max_length: nil)` is
  equivalent to `Source#via(FiberStream::Flow.split(separator, keep_separator:, max_length:))`.
- `separator` must be a `String`.
- Empty separators raise `ArgumentError`.
- Non-`String` separators raise `TypeError`.
- `keep_separator` must be exactly `true` or `false`.
- Non-boolean `keep_separator` values raise `TypeError`.
- `max_length` may be `nil` or a positive `Integer`.
- `max_length: nil` means no frame length limit.
- With `max_length: nil`, the internal buffer for one unterminated frame can
  grow without bound until a separator arrives or upstream completes.
- Users should set a positive `max_length` for untrusted, network-facing, or
  otherwise unbounded inputs.
- Non-`nil`, non-`Integer` `max_length` values raise `TypeError`.
- Zero or negative `max_length` values raise `ArgumentError`.
- Construction is lazy and does not pull upstream.
- Input elements must be `String` values.
- Non-`String` input elements raise `TypeError`.
- Separator matching is byte-oriented, independent of the input string's
  encoding.
- Output frames are byte-oriented `String` values derived from the input bytes.
- Frames may span multiple input chunks.
- A separator may span multiple input chunks.
- A single input chunk may produce multiple output frames over multiple
  downstream demands.
- When separator matches overlap, the flow uses leftmost non-overlapping
  matches, equivalent to repeated `String#index` delimiter splitting.
- Consecutive separators emit empty frames.
- With `keep_separator: false`, emitted separator-terminated frames do not
  include the separator.
- With `keep_separator: true`, emitted separator-terminated frames include the
  separator bytes.
- When upstream completes and buffered content remains, the remaining content
  is emitted as the final frame.
- A final unterminated frame is emitted without adding a separator.
- When upstream completes and no buffered content remains, the flow completes.
- A trailing separator does not emit an extra final empty frame.
- `max_length` is measured with `String#bytesize`.
- `max_length` applies to the frame body excluding the separator.
- `max_length` applies per frame, not to the aggregate internal buffer.
- If one chunk contains both a valid frame and a later over-limit frame, the
  valid frame is emitted first and the later frame fails on the downstream
  demand that reaches it.
- If a frame body exceeds `max_length`, the stream fails with
  `FiberStream::FrameTooLongError`.
- When `FrameTooLongError` occurs, upstream is closed before
  `Source#run_with` returns.
- If frame-length failure and upstream close failure both occur,
  `FrameTooLongError` is primary and close failure is suppressed.
- `Flow.split` itself does not require `Fiber.scheduler`.
- `Flow.split("\n")` is not equivalent to `Flow.lines`. `Flow.split` does not
  perform CRLF chomping, and its `max_length` applies to the frame body
  excluding the separator.
- Public APIs never expose `Pull::DONE`.

## Public Contracts

```ruby
FiberStream::Flow.split(separator, keep_separator: false, max_length: nil)
FiberStream::Source#split(separator, keep_separator: false, max_length: nil)
FiberStream::FrameTooLongError
```

Initial RBS shape:

```rbs
module FiberStream
  class FrameTooLongError < RuntimeError
  end

  class Source[Elem]
    def split: (String separator, ?keep_separator: bool, ?max_length: Integer?) -> Source[String]
  end

  class Flow[In, Out]
    def self.split: (String separator, ?keep_separator: bool, ?max_length: Integer?) -> Flow[String, String]
  end
end
```

## Examples

Chunk boundaries do not define frame boundaries:

```ruby
result =
  FiberStream::Source.each(["hel", "lo,wor", "ld,"])
    .split(",")
    .run_with(FiberStream::Sink.to_a)

result # => ["hello", "world"]
```

Interior empty frames are preserved, but a trailing separator does not create a
new frame after upstream completion:

```ruby
FiberStream::Source.each(["a||"])
  .split("|")
  .run_with(FiberStream::Sink.to_a)
# => ["a", ""]

FiberStream::Source.each(["||"])
  .split("|")
  .run_with(FiberStream::Sink.to_a)
# => ["", ""]

FiberStream::Source.each(["|"])
  .split("|")
  .run_with(FiberStream::Sink.to_a)
# => [""]
```

Keeping separators:

```ruby
result =
  FiberStream::Source.each(["a--b"])
    .via(FiberStream::Flow.split("--", keep_separator: true))
    .run_with(FiberStream::Sink.to_a)

result # => ["a--", "b"]
```

Empty terminated frames keep the separator when requested:

```ruby
FiberStream::Source.each(["|"])
  .split("|", keep_separator: true)
  .run_with(FiberStream::Sink.to_a)
# => ["|"]
```

Protecting a long-running stream:

```ruby
FiberStream::Source.each(["too long"])
  .via(FiberStream::Flow.split(",", max_length: 3))
  .run_with(FiberStream::Sink.to_a)

# raises FiberStream::FrameTooLongError
```

## Open Questions

None.
