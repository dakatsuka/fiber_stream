# Add Flow.select

## Status

Completed

## Objective

Add `FiberStream::Flow.select { ... }` as the first filtering flow for linear
pipelines.

## Context

- Product spec: `docs/product-specs/minimum-linear-pipeline.md`
- Design doc: `docs/design-docs/linear-pull-runtime.md`
- ADR: `docs/design-docs/adr/0001-initial-linear-pull-runtime.md`

## Clarifications

- `Flow.select` keeps values when the predicate result is truthy.
- `Flow.select` drops values when the predicate result is `false` or `nil`.
- One downstream pull may pull multiple upstream values until a matching element
  is found or upstream completes.
- Public RBS signatures are required.

## Contract First

Public API:

- `FiberStream::Flow.select { |element| truthy_or_falsey }`

Contract comments must document that the predicate is called for upstream
elements until a matching element is available or upstream completes.

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

- Use the Ruby method name `select` rather than `filter` for the first API.
- Treat normal Ruby truthiness as the predicate contract.

## Verification

Final commands:

- `bundle exec rake test`
  - 24 runs, 44 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rbs validate`
  - Passed
- `bundle exec rubocop`
  - 12 files inspected, no offenses detected

## Completion Notes

Implemented `Flow.select`, the internal select pull stage, public RBS
signature, product/design documentation, and behavior-focused tests. Review
found no blocking issues. Added a follow-up test to cover close propagation
through `Flow.select` after early sink completion.

## Commit

Pending until committed.
