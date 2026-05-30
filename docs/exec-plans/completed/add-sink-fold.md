# Add Sink.fold

## Status

Completed

## Objective

Add `FiberStream::Sink.fold(initial) { |accumulator, element| ... }` as the
first accumulator-based sink.

## Context

- Product spec: `docs/product-specs/minimum-linear-pipeline.md`
- Design doc: `docs/design-docs/linear-pull-runtime.md`
- ADR: `docs/design-docs/adr/0001-initial-linear-pull-runtime.md`

## Clarifications

- `Sink.fold` consumes the complete stream.
- `Sink.fold` returns the final accumulator.
- Empty upstream returns the initial accumulator unchanged.
- Exceptions raised by the fold block fail the stream and are re-raised from
  `run_with` after cleanup.
- Public RBS signatures are required.

## Contract First

Public API:

- `FiberStream::Sink.fold(initial) { |accumulator, element| new_accumulator }`

Contract comments must document completion, empty-source behavior, and failure
propagation.

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: not required separately for this narrow extension; update
      existing accepted design docs.
- [x] Red: write failing behavior-focused tests.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use `fold` rather than `reduce` for the first API because it requires an
  explicit initial accumulator and avoids empty-stream ambiguity.

## Verification

Final commands:

- `bundle exec rake test`
  - 30 runs, 55 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rbs validate`
  - Passed
- `bundle exec rubocop`
  - 12 files inspected, no offenses detected

## Completion Notes

Implemented `Sink.fold`, public RBS signature, product/design documentation,
README entry, and behavior-focused tests. Review found no blocking issues.
Review feedback was reflected by updating the product spec goal and adding
direct coverage that a fold block failure closes the flow chain.

## Commit

Pending until committed.
