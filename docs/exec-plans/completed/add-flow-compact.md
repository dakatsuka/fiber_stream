# Add Flow.compact

## Status

Completed

## Objective

Add `Flow.compact` and `Source#compact` as lazy filtering operations that drop
only `nil` values while preserving every non-`nil` value unchanged.

## Context

- Product spec: `docs/product-specs/flow-compact.md`
- Design doc: `docs/design-docs/flow-compact.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Existing related APIs: `Flow.reject`, `Flow.filter_map`, and `Source#reject`
  / `Source#filter_map`

## Clarifications

- Do not update CHANGELOG or website files in this change. They are updated
  together during version updates.

## Contract First

- Add `Flow.compact` with no block or options.
- Add `Source#compact` as the convenience method.
- Add RBS signatures:
  `def self.compact: [Elem] () -> Flow[Elem, Elem]` and
  `def compact: () -> Source[Elem]`.
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
- [x] Re-review: review passed with no findings, so no fix/re-review cycle was
      required.

## Decisions

- Use a dedicated private pull stage rather than composing public `reject`, so
  the `nil`-only drop contract is direct and the runtime avoids an extra pull
  wrapper.
- Retain `false`; `compact` treats only `nil` as absence.
- Keep RBS output types unchanged because the current RBS surface cannot
  express removing `nil` from a generic union.
- Match Ruby's method shape for blockless calls: supplied blocks are accepted
  by Ruby but ignored by `Flow.compact` and `Source#compact`.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_compact_test.rb`
  - 16 runs, 27 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec ruby -Itest test/fiber_stream/source_test.rb`
  - 68 runs, 154 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rbs validate`
  - Passed.
- `bundle exec rubocop lib/fiber_stream/flow.rb lib/fiber_stream/source.rb lib/fiber_stream/pull.rb lib/fiber_stream/pull/compact.rb test/fiber_stream/flow_compact_test.rb test/fiber_stream/source_test.rb`
  - 6 files inspected, no offenses detected.
- `bundle exec rake`
  - 703 runs, 1589 assertions, 0 failures, 0 errors, 0 skips.
  - `bundle exec rbs validate` passed.
  - `bundle exec rubocop` inspected 98 files with no offenses.

## Completion Notes

Implemented `Flow.compact`, `Source#compact`, the private `Pull::Compact`
stage, public RBS signatures, product and design documentation, and
behavior-focused tests. The flow drops only `nil`, retains `false`, preserves
retained value identity, avoids pulling upstream after completion, and follows
existing cleanup/error precedence. Code review passed with no findings.
CHANGELOG and website files were intentionally left unchanged for the version
update workflow.

## Commit

Not committed yet.
