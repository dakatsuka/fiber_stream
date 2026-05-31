# Add Flow.lines

## Status

Completed

## Objective

Add `FiberStream::Flow.lines(chomp: true, max_length: nil)` to split `String`
chunk streams into line records with optional bytesize protection.

## Context

- Product spec: `docs/product-specs/flow-lines.md`
- Design doc: `docs/design-docs/flow-lines.md`
- ADR: `docs/design-docs/adr/0007-flow-lines.md`
- Reference: `docs/references/stream-line-framing.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Composable pipelines: `docs/design-docs/composable-pipelines.md`

## Clarifications

- `Flow.lines` should remain even if a future `Flow.delimited` API is added.
- `max_length:` should exist from the first implementation.
- Default `max_length: nil` is acceptable and means unbounded buffering.

## Contract First

Add public contracts and source comments for:

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

## Steps

- [x] Explore: inspect existing flow, pull runtime, errors, docs, and tests.
- [x] Design review: request sub-agent review and incorporate feedback.
- [x] Red: write failing behavior-focused tests, with unit test files
      organized per module.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix
      findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use `Flow.lines` as the first public line-framing API.
- Use newline byte `"\n"` as the delimiter.
- Use `chomp: true` by default.
- Use `max_length: nil` by default, with positive integer bytesize limits when
  supplied.
- Apply `max_length` per line/frame rather than to the aggregate internal
  buffer.
- Raise `FrameTooLongError` for length failures.
- Add `Source#lines` for consistency with existing source convenience wrappers.
- Context-free design review on 2026-05-31 found ambiguity in `max_length`
  semantics and the missing `Source#lines` decision; both were incorporated.
- Implement line framing with a binary internal buffer and binary delimiter
  constants so newline byte detection does not depend on input string encoding.
- Return byte-oriented line strings from `Flow.lines`.
- Context-free implementation review found the initial encoding-sensitive
  delimiter search; the implementation and docs were updated, and re-review
  reported no findings.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_lines_test.rb`
- `bundle exec rake test`
- `bundle exec rbs validate`
- `bundle exec rubocop`
- `bundle exec rake`
- `bundle exec ruby examples/line_processing.rb`
- `bundle exec ruby examples/basic_pipeline.rb`
- `bundle exec ruby examples/composable_pipeline.rb`
- `bundle exec ruby examples/file_copy.rb`
- `bundle exec ruby examples/backpressure_buffer.rb`

## Completion Notes

Implemented `FiberStream::Flow.lines(chomp: true, max_length: nil)` and
`Source#lines(chomp: true, max_length: nil)` with `FrameTooLongError`.

The flow splits byte-oriented `String` chunks on newline bytes, handles CRLF
chomping, emits final unterminated lines, supports optional per-line bytesize
limits, closes upstream on frame-length failures, and keeps the frame-length
failure primary over close failures.

Added unit tests, RBS signatures, README status updates, a line-processing
example, a design reference card, and accepted docs.

## Commit

Pending commit:

```text
feat: add Flow.lines
```
