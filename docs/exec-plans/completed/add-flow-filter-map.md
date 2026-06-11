# Add Flow.filter_map

## Status

Completed

## Objective

Add `Flow.filter_map { ... }` and `Source#filter_map { ... }` as lazy
transform-and-drop operations that emit truthy transform results and drop
falsey results.

## Context

- Product spec: `docs/product-specs/flow-filter-map.md`
- Design doc: `docs/design-docs/flow-filter-map.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Existing related APIs: `Flow.map`, `Flow.select`, and `Source#map` /
  `Source#select`

## Clarifications

- Do not update CHANGELOG or website files in this change. They are updated
  together during version updates.

## Contract First

- Add `Flow.filter_map { |element| ... }` with block validation.
- Add `Source#filter_map { |element| ... }` as the convenience method.
- Add RBS signatures:
  `def self.filter_map: [In, Out] () { (In) -> (Out | false | nil) } -> Flow[In, Out]`
  and
  `def filter_map: [Out] () { (Elem) -> (Out | false | nil) } -> Source[Out]`.
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

- Use a dedicated private pull stage rather than composing public `map` and
  `select`, so the falsey-drop contract is direct and the runtime avoids an
  extra pull wrapper.
- Treat both `false` and `nil` transform results as drop signals, matching Ruby
  truthiness and `Enumerable#filter_map`.
- Do not emit falsey transform results from this flow. Users can use `map` when
  falsey values are meaningful outputs.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_filter_map_test.rb`
  - 18 runs, 32 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec ruby -Itest test/fiber_stream/source_test.rb`
  - 62 runs, 143 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rbs validate`
  - Passed.
- `bundle exec rubocop lib/fiber_stream/flow.rb lib/fiber_stream/source.rb lib/fiber_stream/pull.rb lib/fiber_stream/pull/filter_map.rb test/fiber_stream/flow_filter_map_test.rb test/fiber_stream/source_test.rb`
  - 6 files inspected, no offenses detected.
- `bundle exec rake`
  - 631 runs, 1452 assertions, 0 failures, 0 errors, 0 skips.
  - `bundle exec rbs validate` passed.
  - `bundle exec rubocop` inspected 92 files with no offenses.

## Completion Notes

Implemented `Flow.filter_map`, `Source#filter_map`, the private
`Pull::FilterMap` stage, public RBS signatures, and behavior-focused tests.
Falsey block results are treated as drop signals, while truthy results are
emitted as transformed output values. Code review passed with no findings.
CHANGELOG and website files were intentionally left unchanged for the version
update workflow.

## Commit

feat: add flow filter_map
