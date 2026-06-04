# Add Flow.drop_while

## Status

Completed

## Objective

Add `Flow.drop_while { ... }` and `Source#drop_while { ... }` as lazy
predicate-based prefix-dropping operations.

## Context

- Product spec: `docs/product-specs/flow-drop-while.md`
- Design doc: `docs/design-docs/flow-drop-while.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Existing related APIs: `Flow.drop(count)`, `Flow.take_while`, `Flow.select`,
  and `Source#drop(count)`

## Clarifications

- Continue with `Flow.drop_while` after `Flow.take_while` as the next small
  1-in/1-out flow operation.

## Contract First

- Add `Flow.drop_while { |element| ... }` with block validation.
- Add `Source#drop_while { |element| ... }` as the convenience method.
- Add RBS signatures:
  `def self.drop_while: [Elem] () { (Elem) -> boolish } -> Flow[Elem, Elem]`
  and `def drop_while: () { (Elem) -> boolish } -> Source[Elem]`.
- Document public interfaces with block comments.

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

- Use a dedicated private pull stage rather than implementing through
  `Flow.select`.
- Do not close upstream at the predicate boundary because the stream continues
  with the first falsey-predicate element.
- Keep predicate exceptions and upstream failures on the normal stream failure
  path, with `Source#run_with` cleanup preserving the primary error.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_test.rb`
  - 71 runs, 127 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec ruby -Itest test/fiber_stream/source_test.rb`
  - 38 runs, 86 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rbs validate`
  - Passed.
- `bundle exec rubocop lib/fiber_stream/flow.rb lib/fiber_stream/source.rb lib/fiber_stream/pull.rb lib/fiber_stream/pull/drop_while.rb test/fiber_stream/flow_test.rb test/fiber_stream/source_test.rb`
  - 6 files inspected, no offenses detected.
- `bundle exec rake test`
  - 332 runs, 768 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rake`
  - 334 runs, 772 assertions, 0 failures, 0 errors, 0 skips.
  - `bundle exec rbs validate` passed.
  - `bundle exec rubocop` inspected 56 files with no offenses.

## Completion Notes

Implemented `Flow.drop_while`, `Source#drop_while`, the private
`Pull::DropWhile` stage, public RBS signatures, README coverage, product and
design docs, and behavior-focused tests. Design review clarified cleanup close
failure behavior and internal repeated-pull wording. Code review passed after
adding dropping-phase and pass-through upstream failure coverage and documenting
predicate exception behavior on the public flow method.

## Commit

Pending.
