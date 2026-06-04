# Add Source.concat

## Status

Completed

## Objective

Add a lazy `Source#concat(source)` API that appends one source after another
while preserving pull-driven backpressure and cleanup behavior.

## Context

- Product spec: `docs/product-specs/source-concat.md`
- Design doc: `docs/design-docs/source-concat.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Existing product behavior: `docs/product-specs/minimum-linear-pipeline.md`

## Clarifications

- The requested direction is to implement `Source#concat` because it fits the
  existing pull model and makes backpressure straightforward.

## Contract First

- Add `Source#concat(source)` with a source-only argument validation error.
- Add RBS signature:
  `def concat: [Other] (Source[Other] source) -> Source[Elem | Other]`.
- Document the public method with a block comment in `lib/fiber_stream/source.rb`.

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: request sub-agent review and incorporate feedback.
- [x] Red: write failing behavior-focused tests, with unit test files organized
      per module.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use a private pull stage that materializes the right source only after
  downstream demand observes left completion.
- Treat left close failure during the left-to-right transition as the primary
  stream failure and do not materialize the right side.
- Flows attached before concat stay scoped to their source; flows attached
  after concat transform the combined output.
- Close every materialized side during concat cleanup, while preserving the
  existing primary error precedence from `Source#run_with`.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/source_test.rb`: 32 runs, 74
  assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rake test`: 278 runs, 670 assertions, 0 failures, 0 errors,
  0 skips.
- `bundle exec rbs validate`: passed.
- `bundle exec rubocop`: 53 files inspected, no offenses detected.
- Context-free implementation re-review passed with no remaining findings.

## Completion Notes

Implemented `Source#concat(source)` with a private `Pull::Concat` stage. The
right source is materialized only after downstream demand observes left
completion, left is closed before right materialization, and cleanup covers all
materialized streams on normal completion, early completion, and failure.

Added product/design documentation, RBS contract, and focused tests for
ordering, laziness, delayed right materialization, flow scoping, repeated
materialization, failure propagation, transition close failures, right close
failures, and cleanup precedence. Updated README API and usage documentation.

## Commit

Pending.
