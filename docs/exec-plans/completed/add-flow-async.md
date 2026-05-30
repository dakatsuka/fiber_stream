# Add Flow.async

## Status

Completed

## Objective

Add `FiberStream::Flow.async` as the first scheduler-backed asynchronous
boundary for linear pipelines.

## Context

- Product spec: `docs/product-specs/flow-async.md`
- Design doc: `docs/design-docs/async-boundary.md`
- ADR: `docs/design-docs/adr/0002-flow-async-boundary.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Clarifications

- `Flow.async` starts its producer on first downstream demand, not at
  construction or attach time.
- `Flow.async` requires an installed `Fiber.scheduler` when the producer starts.
- Missing scheduler raises `FiberStream::SchedulerRequiredError`.
- The boundary performs at most one upstream pull for each downstream pull.
- `Flow.async` preserves element order.
- Closing the boundary closes upstream and requests producer cancellation.
- Async compatibility is tested with the development dependency; runtime code
  must not require Async.

## Contract First

Public APIs:

- `FiberStream::Flow.async`
- `FiberStream::SchedulerRequiredError`

Initial RBS shape:

```rbs
module FiberStream
  class SchedulerRequiredError < RuntimeError
  end

  class Flow[In, Out]
    def self.async: [Elem] () -> Flow[Elem, Elem]
  end
end
```

Contract comments must document:

- scheduler requirement
- lazy construction and first-demand producer start
- demand-driven non-blocking producer fiber
- upstream close and producer cancellation request on close
- upstream error propagation
- no runtime Async dependency

## Steps

- [x] Explore: inspect existing code, specs, design docs, references, and tests.
- [x] Design review: request sub-agent review and incorporate feedback.
- [x] Red: write failing behavior-focused tests.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use `Flow.async` with no arguments for the first async boundary.
- Use a demand-driven non-blocking producer fiber rather than a handoff queue
  for the first async boundary.
- Raise a FiberStream-specific scheduler error instead of exposing Ruby's
  `Fiber.schedule` missing-scheduler `RuntimeError`.
- Keep Async as a test scheduler and development dependency only.
- Guarantee that close requests producer cancellation and closes upstream before
  returning, without promising scheduler-agnostic interruption of arbitrary
  blocking user code.

## Verification

Final commands:

- `bundle exec ruby -Itest test/fiber_stream/flow_async_test.rb`
  - 9 runs, 18 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rake test`
  - 52 runs, 96 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rbs validate`
  - Passed
- `bundle exec rubocop`
  - 14 files inspected, no offenses detected

## Completion Notes

Implemented `Flow.async` as a demand-driven non-blocking producer fiber
boundary, added `SchedulerRequiredError`, public RBS signatures, Async-backed
behavior tests, README coverage, product/design docs, and ADR 0002. Design
review changed the approach from a handoff queue to a demand-driven producer
fiber for a portable first cancellation contract. Code review findings were
fixed, and final re-review found no issues.

## Commit

Pending until committed.
