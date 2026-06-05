# Add Flow.split

## Status

Completed

## Objective

Add `FiberStream::Flow.split(separator, keep_separator: false, max_length: nil)`
to split `String` chunk streams into delimiter-separated frames with optional
bytesize protection.

## Context

- Product spec: `docs/product-specs/flow-split.md`
- Design doc: `docs/design-docs/flow-split.md`
- ADR: `docs/design-docs/adr/0014-flow-split.md`
- Existing line design: `docs/design-docs/flow-lines.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Composable pipelines: `docs/design-docs/composable-pipelines.md`

## Clarifications

- The first general API should be named `split`.
- `Flow.lines` should remain unchanged.
- `max_length` should apply to the frame body excluding separator bytes.

## Contract First

Add public contracts and source comments for:

```ruby
FiberStream::Flow.split(separator, keep_separator: false, max_length: nil)
FiberStream::Source#split(separator, keep_separator: false, max_length: nil)
FiberStream::FrameTooLongError
```

Initial RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def split: (String separator, ?keep_separator: bool, ?max_length: Integer?) -> Source[String]
  end

  class Flow[In, Out]
    def self.split: (String separator, ?keep_separator: bool, ?max_length: Integer?) -> Flow[String, String]
  end
end
```

## Steps

- [x] Explore: inspect existing flow, pull runtime, docs, and tests.
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

- Use `Flow.split` rather than `Flow.frames` for the user-facing API name.
- Use a non-empty `String` separator for the first version.
- Match separators byte-wise.
- Use leftmost non-overlapping separator matches.
- Emit interior empty frames for consecutive separators.
- Do not emit an extra final empty frame after a trailing separator.
- Keep `Flow.lines` as a separate compatibility-preserving operator.
- Context-free design review on 2026-06-05 found that `max_length`
  compatibility with `Flow.lines`, overlapping separator matching, CRLF
  differences, and empty-frame examples needed clearer documentation; the
  Product Spec, Design Doc, and ADR were updated before implementation.
- Context-free code review found no correctness defects. It requested a
  negative `max_length` validation test and updated execution-plan status; both
  were fixed before completion.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_split_test.rb`
- `bundle exec ruby -Itest test/fiber_stream/flow_lines_test.rb`
- `bundle exec rake test`
- `bundle exec rbs validate`
- `bundle exec rubocop`
- `bundle exec rake`

## Completion Notes

Implemented `FiberStream::Flow.split(separator, keep_separator: false, max_length: nil)`
and `Source#split(separator, keep_separator: false, max_length: nil)`.

The flow splits byte-oriented `String` chunks on a non-empty `String`
separator, handles frames and separators across chunk boundaries, preserves
interior empty frames, omits an extra final empty frame after a trailing
separator, supports optional separator retention, enforces per-frame body
bytesize limits, closes upstream on frame-length failures, and keeps the
frame-length failure primary over close failures.

Added unit tests, RBS signatures, Product Spec, Design Doc, and ADR coverage.

## Commit

Pending commit:

```text
feat: add Flow.split
```
