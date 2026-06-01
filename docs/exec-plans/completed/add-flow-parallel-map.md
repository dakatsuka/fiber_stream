# Add Flow.parallel_map

## Status

Completed

## Objective

Add `FiberStream::Flow.parallel_map(concurrency:)` as an ordered
scheduler-backed parallel mapping stage for linear pipelines.

## Context

- Product spec: `docs/product-specs/flow-parallel-map.md`
- Design doc: `docs/design-docs/parallel-map.md`
- ADR: `docs/design-docs/adr/0008-flow-parallel-map.md`
- Async design: `docs/design-docs/async-boundary.md`
- Buffer design: `docs/design-docs/buffer-boundary.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Clarifications

- `Flow.parallel_map(concurrency:)` starts internal scheduled fibers on first
  downstream demand, not at construction or attach time.
- Missing scheduler or a blocking current fiber raises
  `FiberStream::SchedulerRequiredError`.
- The operation is ordered; unordered and Ractor-backed variants are future
  work.
- Upstream is pulled serially by a dispatcher.
- At most `concurrency` mapping blocks execute at a time.
- At most `concurrency` upstream elements are pulled but not yet emitted
  downstream while admission is open.
- Failures are delivered by input sequence, not wall-clock completion time.
- Observing any upstream or mapping failure closes admission without canceling
  lower-sequence work required for ordered delivery.
- Async compatibility is tested with the development dependency; runtime code
  must not require Async.

## Contract First

Public APIs:

- `FiberStream::Flow.parallel_map(concurrency:) { |element| ... }`
- `FiberStream::Source#parallel_map(concurrency:) { |element| ... }`

Initial RBS shape:

```rbs
module FiberStream
  class Flow[In, Out]
    def self.parallel_map: [In, Out] (concurrency: Integer) { (In) -> Out } -> Flow[In, Out]
  end

  class Source[Elem]
    def parallel_map: [Out] (concurrency: Integer) { (Elem) -> Out } -> Source[Out]
  end
end
```

Contract comments document:

- positive Integer `concurrency` validation
- missing block validation
- lazy construction and first-demand scheduler start
- scheduler and non-blocking fiber requirement
- ordered delivery
- bounded pulled-but-unemitted upstream elements
- active worker concurrency limit
- upstream close and worker cancellation request on close
- upstream and mapping error propagation
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

- Use a separate `parallel_map` API instead of adding `concurrency:` to
  `Flow.map`.
- Preserve input order for the first public API.
- Use scheduler-backed fibers and standard Ruby queues, not Async primitives.
- Keep Ractor-backed execution out of the first implementation.
- Require a non-blocking current fiber because downstream waits on internal
  queues.
- Use scheduler `fiber_interrupt` when available for best-effort active worker
  interruption; otherwise rely on queue close and upstream close.

## Verification

Final commands:

- `bundle exec ruby -Itest test/fiber_stream/flow_parallel_map_test.rb`
  - 19 runs, 41 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rake test`
  - 196 runs, 439 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rbs validate`
  - Passed
- `bundle exec rubocop`
  - 26 files inspected, no offenses detected

## Completion Notes

Implemented `Flow.parallel_map(concurrency:)` and
`Source#parallel_map(concurrency:)` as an ordered scheduler-backed parallel
mapping boundary. Added public RBS signatures, README status updates,
Async-backed behavior tests, scheduler/non-blocking validation, ordered failure
delivery, bounded pulled-but-unemitted work, close/error precedence coverage,
and best-effort worker interruption through scheduler `fiber_interrupt`.

Code review found two issues: blocking root fibers could hang with a scheduler
installed, and active worker cancellation was only documented rather than
requested. The implementation now rejects blocking current fibers and requests
best-effort scheduler interruption. Final re-review found no issues.

## Commit

Pending until committed.
