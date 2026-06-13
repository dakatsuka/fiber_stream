# Add Flow.map_concat

## Status

Completed

## Objective

Add `Flow.map_concat { ... }` and `Source#map_concat { ... }` as lazy
one-to-many transform operations that enumerate one block result at a time
without eager materialization.

## Context

- Product spec: `docs/product-specs/flow-map-concat.md`
- Design doc: `docs/design-docs/flow-map-concat.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Existing related APIs: `Flow.map`, `Flow.filter_map`, `Flow.grouped`, and
  `Source#map` / `Source#filter_map` / `Source#grouped`

## Clarifications

- Do not update CHANGELOG or website files in this change. They are updated
  together during version updates.
- Code review must be requested from a sub-agent with `xhigh` reasoning, per
  the implementation request.

## Contract First

- Add `Flow.map_concat { |element| enumerable }` with block validation.
- Add `Source#map_concat { |element| enumerable }` as the convenience method.
- Add RBS signatures:
  `def self.map_concat: [In, Out] () { (In) -> Enumerable[Out] } -> Flow[In, Out]`
  and
  `def map_concat: [Out] () { (Elem) -> Enumerable[Out] } -> Source[Out]`.
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

- Use a dedicated private pull stage rather than composing existing public
  flows, so one-active-expansion state and backpressure contracts are direct.
- Use `result.to_enum(:each)` for block results so objects with block-yielding
  `#each` methods work without needing to include `Enumerable`.
- Treat `StopIteration` surfaced by the active external iterator as expansion
  completion. Callers that need inner failure signaling should raise another
  exception type.
- Do not call `close` on block results. `map_concat` does not take ownership of
  resource-like enumerables.

## Verification

- Red check: `bundle exec ruby -Itest test/fiber_stream/flow_map_concat_test.rb`
  failed before implementation with `NoMethodError: undefined method
  'map_concat' for class FiberStream::Flow`.
- Red check: `bundle exec ruby -Itest test/fiber_stream/source_test.rb`
  failed before implementation for the new `Source#map_concat` convenience
  tests.
- `bundle exec ruby -Itest test/fiber_stream/flow_map_concat_test.rb`
  - 28 runs, 59 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec ruby -Itest test/fiber_stream/source_test.rb`
  - 66 runs, 151 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rbs validate`
  - Passed.
- `bundle exec rubocop lib/fiber_stream/flow.rb lib/fiber_stream/source.rb lib/fiber_stream/pull.rb lib/fiber_stream/pull/map_concat.rb test/fiber_stream/flow_map_concat_test.rb test/fiber_stream/source_test.rb`
  - 6 files inspected, no offenses detected.
- `bundle exec rake`
  - 685 runs, 1559 assertions, 0 failures, 0 errors, 0 skips.
  - `bundle exec rbs validate` passed.
  - `bundle exec rubocop` inspected 96 files with no offenses.
- Code review sub-agent requested with `xhigh` reasoning.
  - No findings.

## Completion Notes

Implemented `Flow.map_concat`, `Source#map_concat`, the private
`Pull::MapConcat` stage, public RBS signatures, and behavior-focused tests.
The stage holds one active expansion at a time, uses `to_enum(:each)`, treats
`StopIteration` surfaced by the active external iterator as expansion
completion, and preserves existing `Source#run_with` cleanup precedence.
CHANGELOG and website files were intentionally left unchanged for the version
update workflow.

## Commit

Not committed yet. Suggested commit message:

feat: add flow map_concat
