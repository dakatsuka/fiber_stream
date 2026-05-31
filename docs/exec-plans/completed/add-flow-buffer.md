# Add Flow.buffer

## Status

Completed

## Objective

Add `FiberStream::Flow.buffer(count)` as the first bounded asynchronous buffer
for linear pipelines.

## Context

- Product spec: `docs/product-specs/flow-buffer.md`
- Design doc: `docs/design-docs/buffer-boundary.md`
- ADR: `docs/design-docs/adr/0003-flow-buffer-boundary.md`
- Async design: `docs/design-docs/async-boundary.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Clarifications

- `Flow.buffer(count)` starts its producer on first downstream demand, not at
  construction or attach time.
- `Flow.buffer(count)` requires an installed `Fiber.scheduler` when the producer
  starts.
- Missing scheduler raises `FiberStream::SchedulerRequiredError`.
- `count` must be a positive `Integer`.
- The buffer queues at most `count` messages, including value, completion, and
  error messages.
- The producer may hold one additional in-flight message while waiting to
  enqueue into a full buffer.
- `Flow.buffer(count)` preserves element order.
- Closing the boundary closes upstream and requests producer cancellation.
- Queued or in-flight upstream errors are suppressed after intentional
  downstream early completion or downstream failure.
- User `upstream.close` failures during boundary close are propagated unless a
  downstream failure is already propagating.
- Producer-side `upstream.close` failures are propagated unless an upstream
  pull failure is already propagating.
- Normal completion is not delivered until producer-side `upstream.close` has
  succeeded.
- Async compatibility is tested with the development dependency; runtime code
  must not require Async.

## Contract First

Public APIs:

- `FiberStream::Flow.buffer(count)`
- `FiberStream::Source#buffer(count)`

Initial RBS shape:

```rbs
module FiberStream
  class Flow[In, Out]
    def self.buffer: [Elem] (Integer count) -> Flow[Elem, Elem]
  end

  class Source[Elem]
    def buffer: (Integer count) -> Source[Elem]
  end
end
```

Contract comments must document:

- positive Integer count validation
- lazy construction and first-demand producer start
- scheduler requirement
- bounded queued message capacity
- at most one additional in-flight producer message while enqueueing
- ordered delivery
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

- Use `Flow.buffer(count)` rather than adding an argument to `Flow.async`.
- Start with a lossless bounded buffer and defer drop policies.
- Count terminal messages toward the same `count` queued bound as value
  messages.
- Suppress queued or in-flight upstream errors after intentional downstream
  early completion or downstream failure.
- Propagate producer-side close failures unless an upstream pull failure is
  already propagating.
- Publish terminal messages only after producer-side close outcome is known.
- Raise a FiberStream-specific scheduler error instead of exposing Ruby's
  `Fiber.schedule` missing-scheduler `RuntimeError`.
- Keep Async as a test scheduler and development dependency only.
- Guarantee that close requests producer cancellation and closes upstream before
  returning, without promising scheduler-agnostic interruption of arbitrary
  blocking user code.

## Verification

Final commands:

- `bundle exec ruby -Itest test/fiber_stream/flow_buffer_test.rb`
  - 15 runs, 29 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rake test`
  - 75 runs, 141 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rbs validate`
  - Passed
- `bundle exec rubocop`
  - 15 files inspected, no offenses detected

## Completion Notes

Implemented `Flow.buffer(count)` and `Source#buffer(count)` as a bounded
asynchronous buffer backed by a scheduled producer fiber and `Thread::SizedQueue`.
Added public RBS signatures, Async-backed behavior tests, product/design docs,
ADR 0003, and validation for ordering, scheduler requirements, bounded prefetch,
queued or in-flight error suppression, close/error precedence, and cleanup.
Design and code reviews found issues in queue semantics and close/error
precedence; those findings were fixed and final re-review found no issues.

## Commit

Pending until committed.
