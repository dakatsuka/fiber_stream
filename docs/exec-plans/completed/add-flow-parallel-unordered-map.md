# Add Flow.parallel_unordered_map

## Status

Completed

## Objective

Add `FiberStream::Flow.parallel_unordered_map(concurrency:)` as an unordered
scheduler-backed parallel mapping stage for linear pipelines.

## Context

- Product spec: `docs/product-specs/flow-parallel-unordered-map.md`
- Design doc: `docs/design-docs/parallel-unordered-map.md`
- ADR: `docs/design-docs/adr/0015-flow-parallel-unordered-map.md`
- Ordered parallel design: `docs/design-docs/parallel-map.md`
- Async design: `docs/design-docs/async-boundary.md`
- Buffer design: `docs/design-docs/buffer-boundary.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Clarifications

- The public name is `parallel_unordered_map`.
- The operation emits mapped values in scheduler-observed completion order.
- The operation is fail-fast when downstream observes an upstream or mapping
  failure result.
- Upstream is pulled serially by a dispatcher.
- At most `concurrency` mapping blocks execute at a time.
- At most `concurrency` upstream elements are pulled but not yet emitted
  downstream while admission is open.
- Missing scheduler or a blocking current fiber raises
  `FiberStream::SchedulerRequiredError`.
- Async compatibility is tested with the development dependency; runtime code
  must not require Async.

## Contract First

Public APIs:

- `FiberStream::Flow.parallel_unordered_map(concurrency:) { |element| ... }`
- `FiberStream::Source#parallel_unordered_map(concurrency:) { |element| ... }`

Initial RBS shape:

```rbs
module FiberStream
  class Flow[In, Out]
    def self.parallel_unordered_map: [In, Out] (concurrency: Integer) { (In) -> Out } -> Flow[In, Out]
  end

  class Source[Elem]
    def parallel_unordered_map: [Out] (concurrency: Integer) { (Elem) -> Out } -> Source[Out]
  end
end
```

Contract comments document:

- positive Integer `concurrency` validation
- missing block validation
- lazy construction and first-demand scheduler start
- scheduler and non-blocking fiber requirement
- completion-order delivery
- no input-order guarantee
- bounded pulled-but-unemitted upstream elements
- active worker concurrency limit
- upstream close and worker cancellation request on close
- fail-fast upstream and mapping error propagation
- no runtime Async dependency

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

- Use a separate `parallel_unordered_map` API instead of adding a keyword to
  `parallel_map`.
- Emit completion-order values and make no input-order guarantee.
- Use fail-fast error delivery by downstream observation order.
- Track admitted mapping jobs and delay normal-completion terminal delivery
  until all admitted jobs have produced downstream-observed results.
- Use scheduler-backed fibers and standard Ruby queues, not Async primitives.
- Keep Ractor-backed unordered execution out of this implementation.

Design review found that publishing normal-completion terminal messages
directly could complete downstream before slow in-flight admitted mapping jobs.
The design now tracks outstanding admitted jobs and delays normal completion or
normal-completion close failures until admitted jobs have produced observed
results. The review also requested explicit cancellation capability language;
the contract now states that active fiber interruption uses
`Fiber.scheduler#fiber_interrupt` when available.

## Verification

Final commands:

- `bundle exec ruby -Itest test/fiber_stream/flow_parallel_unordered_map_test.rb`
  - 21 runs, 41 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rake test`
  - 479 runs, 1129 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rbs validate`
  - Passed
- `bundle exec rubocop`
  - 77 files inspected, no offenses detected

## Completion Notes

Implemented `Flow.parallel_unordered_map(concurrency:)` and
`Source#parallel_unordered_map(concurrency:)` as an unordered scheduler-backed
parallel mapping boundary. Added public RBS signatures, product spec, design
doc, ADR, behavior tests, scheduler/non-blocking validation, completion-order
delivery, bounded pulled-but-unemitted work, fail-fast error delivery, delayed
normal terminal handling for admitted work, and close/error precedence coverage.

Design review found a premature terminal delivery issue; the design now tracks
outstanding admitted jobs and delays normal-completion terminal messages until
admitted jobs have produced observed results. Code review found that
producer-side `upstream.close` failures could be lost while close was already
in progress; the implementation now waits for in-progress upstream close and
preserves that close failure. Final re-review found no issues.

## Commit

Pending until committed.
