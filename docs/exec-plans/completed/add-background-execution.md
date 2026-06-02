# Add Background Execution

## Status

Completed

## Objective

Add `Pipeline#run_async` and `FiberStream::RunningPipeline` so an existing
linear pipeline can run in a scheduler-backed background fiber and be waited on
or cancelled through a narrow handle.

## Context

- Product spec: `docs/product-specs/background-execution.md`
- Design doc: `docs/design-docs/background-execution.md`
- ADR: `docs/design-docs/adr/0009-background-execution.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Async design: `docs/design-docs/async-boundary.md`
- Buffer design: `docs/design-docs/buffer-boundary.md`
- Parallel map design: `docs/design-docs/parallel-map.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Clarifications

- Background execution starts from `Pipeline#run_async`, not
  `Source#run_async`.
- `Pipeline#run` remains foreground and unchanged.
- Background execution uses Ruby scheduler fibers, not threads, Ractors, or
  Async runtime APIs.
- Cancellation requires scheduler `fiber_interrupt` support and raises
  `NotImplementedError` when unavailable.

## Contract First

Public APIs:

- `FiberStream::Pipeline#run_async`
- `FiberStream::RunningPipeline#wait`
- `FiberStream::RunningPipeline#cancel`
- `FiberStream::RunningPipeline#done?`
- `FiberStream::RunningPipeline#cancel_requested?`
- `FiberStream::PipelineCancelledError`

Initial RBS shape:

```rbs
module FiberStream
  class PipelineCancelledError < RuntimeError
  end

  class Pipeline[Mat]
    def run_async: () -> RunningPipeline[Mat]
  end

  class RunningPipeline[Mat]
    def wait: () -> Mat
    def cancel: () -> self
    def done?: () -> bool
    def cancel_requested?: () -> bool
  end
end
```

Contract comments document:

- scheduler and non-blocking fiber requirements
- one materialization per `run_async` call
- successful materialized value delivery
- stream failure propagation
- cancellation request semantics
- replayable wait behavior after completion
- concurrent pre-completion wait behavior
- no runtime Async dependency

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: request sub-agent review and incorporate feedback.
- [x] Red: write failing behavior-focused tests.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use `Pipeline#run_async` as the only background entry point.
- Return a handle instead of exposing the raw scheduled fiber.
- Require scheduler `fiber_interrupt` for cancellation instead of silently
  pretending all schedulers can cancel active fibers.
- Capture the scheduler at `run_async` time for cancellation.
- Use per-waiter queues so multiple pre-completion waiters can observe one
  stored completion result.
- Use `cancel_requested?` for request state instead of `cancelled?`.
- Record cancellation only after scheduler interrupt capability checks pass,
  and clear request state if interruption fails.
- Capture `Exception` from the background materialization so non-StandardError
  stream failures are delivered through `RunningPipeline#wait`.

## Verification

Final commands:

- `bundle exec ruby -Itest test/fiber_stream/pipeline_background_test.rb`
  - 15 runs, 48 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec ruby examples/background_execution.rb`
  - Printed foreground ticks and ordered background results.
- `bundle exec rake`
  - 211 runs, 487 assertions, 0 failures, 0 errors, 0 skips
  - `bundle exec rbs validate` passed
  - `bundle exec rubocop` inspected 29 files with no offenses

## Completion Notes

Implemented `Pipeline#run_async` and `FiberStream::RunningPipeline` with
scheduler-backed background materialization, replayable waits, concurrent
pre-completion wait broadcast, cancellation requests, and cancellation-state
inspection. Added `PipelineCancelledError`, public RBS signatures, README and
example coverage, and focused Async-backed tests.

Design review found ambiguity around concurrent waiters, scheduler ownership,
failed cancellation state, cancellation classification, and request-state
naming. The design now captures the scheduler at start, broadcasts completion
to per-waiter queues, classifies cancellation by exception identity, and uses
`cancel_requested?`.

Code review found two issues: failed `fiber_interrupt` could leave cancellation
recorded, and non-StandardError stream failures could escape the background
fiber instead of being delivered through `#wait`. Both were fixed and covered
with regression tests. Final re-review passed with no remaining correctness
issues.

## Commit

Pending.

Suggested message:

```text
feat: add background pipeline execution

FiberStream now has enough linear pipeline and scheduler-backed boundary
coverage for users to start a whole pipeline in the background and wait for its
materialized value without changing foreground execution semantics.

Add Pipeline#run_async, RunningPipeline, and PipelineCancelledError. The handle
supports replayable waits, concurrent waiter wakeup, cancellation requests via
the captured scheduler, and request-state inspection. Document the behavior in
the product spec, design doc, ADR, README, examples, RBS, and focused tests.

Co-Authored-By: OpenAI Codex <codex@openai.com>
```
