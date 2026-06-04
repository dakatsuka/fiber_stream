# Add Flow.take_while

## Status

Completed

## Objective

Add `Flow.take_while { ... }` and `Source#take_while { ... }` as lazy
predicate-based early-completion operations.

## Context

- Product spec: `docs/product-specs/flow-take-while.md`
- Design doc: `docs/design-docs/flow-take-while.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Existing related APIs: `Flow.take(count)`, `Flow.select`, and
  `Source#take(count)`

## Clarifications

- Continue with `Flow.take_while` after `Flow.drop` as the next small
  1-in/1-out flow operation.

## Contract First

- Add `Flow.take_while { |element| ... }` with block validation.
- Add `Source#take_while { |element| ... }` as the convenience method.
- Add RBS signatures:
  `def self.take_while: [Elem] () { (Elem) -> boolish } -> Flow[Elem, Elem]`
  and `def take_while: () { (Elem) -> boolish } -> Source[Elem]`.
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
- Close upstream during the pull that observes the first falsey predicate
  result.
- If that same-pull close fails, propagate the close failure instead of normal
  completion.
- Predicate and upstream failures remain primary over later cleanup close
  failures from `Source#run_with`.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_test.rb`
  - 50 runs, 89 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec ruby -Itest test/fiber_stream/source_test.rb`
  - 36 runs, 82 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rbs validate`
  - Passed.
- `bundle exec rubocop`
  - 55 files inspected, no offenses detected.
- `bundle exec rake`
  - 311 runs, 730 assertions, 0 failures, 0 errors, 0 skips.
  - `bundle exec rbs validate` passed.
  - `bundle exec rubocop` inspected 55 files with no offenses.

## Completion Notes

Implemented `Flow.take_while`, `Source#take_while`, the private
`Pull::TakeWhile` stage, public RBS signatures, README coverage, product and
design docs, and behavior-focused tests. Design review required clarifying
same-pull close failure precedence. Code review passed after adding a
regression test proving close failure after a falsey predicate marks the stage
complete without extra upstream pulls or close attempts.

## Commit

Pending.
