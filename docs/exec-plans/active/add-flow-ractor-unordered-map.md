# Add Flow.ractor_unordered_map

## Status

Active

## Objective

Add `FiberStream::Flow.ractor_unordered_map(workers:)` as an unordered
Ractor-backed parallel mapping stage for linear pipelines.

## Context

- Product spec: `docs/product-specs/flow-ractor-unordered-map.md`
- Design doc: `docs/design-docs/ractor-unordered-map.md`
- ADR: `docs/design-docs/adr/0018-flow-ractor-unordered-map.md`
- Ordered Ractor design: `docs/design-docs/ractor-map.md`
- Unordered scheduler-backed design:
  `docs/design-docs/parallel-unordered-map.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- References: `docs/references/ruby-ractor.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Clarifications

- The public name is `ractor_unordered_map`.
- The operation emits mapped values in coordinator-observed worker completion
  order.
- The operation makes no input-order guarantee.
- The operation is fail-fast when downstream observes an upstream or worker
  failure result.
- Upstream is pulled serially by the downstream caller.
- At most `workers` mapping jobs are admitted but not yet emitted.
- The mapper block must be shareable.
- The operation does not require `Fiber.scheduler`.

## Contract First

Public APIs:

- `FiberStream::Flow.ractor_unordered_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`
- `FiberStream::Source#ractor_unordered_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`

Initial RBS shape:

```rbs
module FiberStream
  class Flow[In, Out]
    def self.ractor_unordered_map: [In, Out] (
      workers: Integer,
      ?input_transfer: ractor_transfer_policy,
      ?output_transfer: ractor_transfer_policy
    ) { (In) -> Out } -> Flow[In, Out]
  end

  class Source[Elem]
    def ractor_unordered_map: [Out] (
      workers: Integer,
      ?input_transfer: ractor_transfer_policy,
      ?output_transfer: ractor_transfer_policy
    ) { (Elem) -> Out } -> Source[Out]
  end
end
```

Contract comments document:

- positive Integer `workers` validation
- missing block validation
- shareable mapper validation
- transfer policy validation
- lazy construction and first-demand worker start
- completion-order delivery
- no input-order guarantee
- bounded pulled-but-unemitted upstream elements
- upstream close and cooperative worker shutdown on close
- fail-fast upstream and worker error propagation
- no runtime Async dependency

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: use `.agents/reviews/design-review.md` and incorporate
      feedback.
- [x] Red: write failing behavior-focused tests, with unit test files organized
      per module.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix
      findings.
- [x] Code review: use `.agents/reviews/code-review.md` after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use a separate `ractor_unordered_map` method instead of adding an
  `ordered: false` keyword to `ractor_map`.
- Reuse `RactorMapError` for worker and transfer failures.
- Deliver failures by downstream observation order instead of input sequence.
- Track active worker sequences so unexpected worker termination can populate
  `RactorMapError#sequence`.
- Prefer emitting a ready result over admitting more upstream work on the same
  downstream pull.
- Keep terminal completion and producer-side terminal close failures as
  boundary state instead of data-result queue messages.
- Require cleanup waits to preserve scheduler responsiveness, matching ordered
  `ractor_map`.
- Code review found that upstream failures discovered while admitting more
  work were delayed behind outstanding worker values. Added a regression and
  changed admission to raise that failure on the observing downstream pull.
- Re-review found that the upstream-failure fix also made producer-side close
  failures after normal completion fail fast. Added a distinct terminal close
  error message and a regression proving admitted values are emitted first.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_ractor_unordered_map_test.rb`
  passed with 27 runs and 64 assertions.
- `bundle exec ruby -Itest test/fiber_stream/flow_ractor_map_test.rb` passed
  with 28 runs and 72 assertions.
- `bundle exec rbs validate` passed.
- `bundle exec rake docs:index` passed.
- `bundle exec rubocop` passed.
- `bundle exec rake verify:full` passed with 738 runs and 1664 assertions,
  RBS validation, RuboCop, and the VitePress website build.

## Completion Notes

Pending.

## Commit

Pending.
