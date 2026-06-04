# Add Flow.drop

## Status

Completed

## Objective

Add `Flow.drop(count)` and `Source#drop(count)` as lazy fixed-prefix dropping
operations that preserve pull-driven backpressure.

## Context

- Product spec: `docs/product-specs/flow-drop.md`
- Design doc: `docs/design-docs/flow-drop.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Existing related API: `Flow.take(count)` and `Source#take(count)`

## Clarifications

- Start with `Flow.drop(n)` as the next small 1-in/1-out flow operation.

## Contract First

- Add `Flow.drop(count)` with `take`-style validation.
- Add `Source#drop(count)` as the convenience method.
- Add RBS signatures:
  `def self.drop: [Elem] (Integer count) -> Flow[Elem, Elem]` and
  `def drop: (Integer count) -> Source[Elem]`.
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
- Mark the stage done whenever upstream completion is observed, including after
  the dropped prefix, so defensive pulls do not pull upstream again.
- Keep `drop(0)` as a pass-through stage rather than special-casing it away, so
  the runtime behavior remains explicit and testable.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_test.rb`: 34 runs, 59
  assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec ruby -Itest test/fiber_stream/source_test.rb`: 34 runs, 78
  assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rake test`: 293 runs, 696 assertions, 0 failures, 0 errors,
  0 skips.
- `bundle exec rbs validate`: passed.
- `bundle exec rubocop`: 54 files inspected, no offenses detected.
- Context-free implementation re-review passed with no remaining findings.

## Completion Notes

Implemented `Flow.drop(count)` and `Source#drop(count)` with a private
`Pull::Drop` stage. The stage discards the fixed prefix on downstream demand,
passes retained values through unchanged, marks completion defensively, and
propagates close upstream through the normal pull-chain cleanup path.

Added product/design documentation, RBS contracts, README usage/API notes, and
focused tests for dropping behavior, `drop(0)`, over-large counts, laziness,
pull counts, repeated pulls after completion, close propagation, upstream
failure propagation, validation, and source convenience delegation.

## Commit

Pending.
