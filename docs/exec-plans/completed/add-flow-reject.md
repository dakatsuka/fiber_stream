# Add Flow.reject

## Status

Completed

## Objective

Add `Flow.reject { ... }` and `Source#reject { ... }` as lazy predicate-based
filtering operations that drop truthy predicate matches and emit original
elements when the predicate result is `false` or `nil`.

## Context

- Product spec: `docs/product-specs/flow-reject.md`
- Design doc: `docs/design-docs/flow-reject.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Existing related APIs: `Flow.select`, `Flow.filter_map`, `Flow.drop_while`,
  and `Source#select`

## Clarifications

- This plan implements `reject` as the complement of `select`, matching Ruby
  truthiness and `Enumerable#reject`.
- `Source#reject` is included because it does not conflict with inherited
  `Object` behavior in the way `Source#tap` would.
- CHANGELOG and website updates remain out of scope until the version bump
  workflow.

## Contract First

- Add `Flow.reject { |element| ... }` with block validation.
- Add `Source#reject { |element| ... }` as the convenience method.
- Add RBS signatures:
  `def self.reject: [Elem] () { (Elem) -> boolish } -> Flow[Elem, Elem]`
  and `def reject: () { (Elem) -> boolish } -> Source[Elem]`.
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

- Use a dedicated private pull stage rather than requiring users to compose
  `Flow.select { |value| !predicate.call(value) }`.
- Treat every truthy predicate result, including non-boolean truthy values, as
  a rejection signal.
- Treat `false` and `nil` predicate results as retention signals and emit the
  original upstream value unchanged.
- Keep the stage synchronous, linear, pull-driven, and unbuffered.
- Leave upstream close ownership and primary error precedence on the existing
  `Source#run_with` materialized-chain cleanup path.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_reject_test.rb`
  - 22 runs, 40 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec ruby -Itest test/fiber_stream/source_test.rb -n "/reject/"`
  - 2 runs, 2 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rbs validate`
  - Passed.
- `bundle exec rubocop lib/fiber_stream/flow.rb lib/fiber_stream/source.rb lib/fiber_stream/pull.rb lib/fiber_stream/pull/reject.rb test/fiber_stream/flow_reject_test.rb test/fiber_stream/source_test.rb`
  - 6 files inspected, no offenses detected.
- `bundle exec rake`
  - 655 runs, 1496 assertions, 0 failures, 0 errors, 0 skips.
  - `bundle exec rbs validate` passed.
  - `bundle exec rubocop` inspected 94 files with no offenses.

## Completion Notes

Implemented `Flow.reject`, `Source#reject`, the private `Pull::Reject` stage,
public RBS signatures, and behavior-focused tests. Truthy predicate results
drop the original upstream element; `false` and `nil` results pass the element
through unchanged. Code review passed with no blocking findings. Added cleanup
close-failure, predicate-failure upstream-close, and order-preservation tests
recommended during review. CHANGELOG and website files were intentionally left
unchanged for the version update workflow.

## Commit

Pending user request.
