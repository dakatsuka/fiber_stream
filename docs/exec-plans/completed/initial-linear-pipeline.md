# Initial Linear Pipeline

## Status

Completed

## Objective

Implement the first usable FiberStream vertical slice:
`Source.each(...).via(Flow.map { ... }).run_with(Sink.to_a)`.

## Context

- Product spec: `docs/product-specs/minimum-linear-pipeline.md`
- Design doc: `docs/design-docs/linear-pull-runtime.md`
- ADR: `docs/design-docs/adr/0001-initial-linear-pull-runtime.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Clarifications

- Linear pipelines are the initial public runtime shape.
- Backpressure is part of the first implementation via pull-based demand.
- `run_with` executes in the current fiber and returns the sink materialized
  value.
- `async` is a development dependency and compatibility target, not a runtime
  dependency.
- Public API RBS signatures are required from the first implementation.

## Contract First

Public APIs:

- `FiberStream::Source.each(enumerable)`
- `FiberStream::Source#via(flow)`
- `FiberStream::Source#run_with(sink)`
- `FiberStream::Flow.map { |element| transformed_element }`
- `FiberStream::Sink.to_a`

Source files must document public contracts with block comments. RBS signatures
must cover the public API.

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

- Use a private identity sentinel for normal completion.
- Use `TypeError` for invalid `via` and `run_with` builder arguments.
- `Source.each` creates an enumerator with `enumerable.to_enum(:each)` on each
  materialization and does not snapshot values.
- `Source.each` does not close the original enumerable; resource-owning sources
  need separate contracts.
- The internal `Pull` namespace and concrete pull stage classes remain private
  constants.

## Verification

Final commands:

- `bundle exec rake test`
  - 15 runs, 29 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rbs validate`
  - Passed
- `bundle exec rubocop`
  - 12 files inspected, no offenses detected

## Completion Notes

Implemented the first linear pipeline slice with `Source.each`, `Flow.map`,
`Sink.to_a`, foreground `run_with`, internal pull stages, public RBS signatures,
and behavior-focused Minitest coverage. Code review found two issues:

- `Source.each` needed to support `Enumerable#each` implementations that yield
  to a block. Fixed by materializing with `enumerable.to_enum(:each)`.
- Internal pull stages were too visible. Fixed by making the `Pull` namespace
  and concrete stage constants private.

Re-review found no blocking issues. Added direct coverage that `Source.each`
does not close the original enumerable.

## Commit

Pending until committed.
