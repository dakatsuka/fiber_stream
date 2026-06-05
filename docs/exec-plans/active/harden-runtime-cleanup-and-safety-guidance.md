# Harden Runtime Cleanup And Safety Guidance

## Status

Active

## Objective

Address review findings that reveal cleanup contract drift, avoidable polling,
and missing safety guidance while preserving FiberStream's pull-based
backpressure model, lazy materialization, and narrow public APIs.

## Context

- Background execution design: `docs/design-docs/background-execution.md`
- Async boundary design: `docs/design-docs/async-boundary.md`
- Buffer boundary design: `docs/design-docs/buffer-boundary.md`
- Source.concat design: `docs/design-docs/source-concat.md`
- Ractor map design: `docs/design-docs/ractor-map.md`
- Ractor port source design: `docs/design-docs/ractor-port-source.md`
- Ractor merge ports design:
  `docs/design-docs/source-ractor-merge-ports.md`
- Flow.lines spec: `docs/product-specs/flow-lines.md`
- Flow.split spec: `docs/product-specs/flow-split.md`
- IO source spec: `docs/product-specs/io-source.md`
- Ractor port source spec: `docs/product-specs/ractor-port-source.md`
- Existing tests:
  - `test/fiber_stream/pipeline_background_test.rb`
  - `test/fiber_stream/flow_async_test.rb`
  - `test/fiber_stream/flow_buffer_test.rb`
  - `test/fiber_stream/source_concat_test.rb`
  - `test/fiber_stream/flow_ractor_map_test.rb`
  - `test/fiber_stream/source_ractor_port_test.rb`
  - `test/fiber_stream/flow_lines_test.rb`
  - `test/fiber_stream/flow_split_test.rb`
  - `test/fiber_stream/source_io_test.rb`

## Clarifications

- Review feedback is input, not authority. Changes must match accepted product
  specs and design docs or update those documents first.
- Public defaults for `Flow.lines`, `Flow.split`, and `Source.io` are preserved
  unless a product-spec change explicitly accepts compatibility impact.
- Ractor and scheduler boundaries remain cooperative. FiberStream does not
  promise immediate interruption of arbitrary CPU-bound user code.
- Ractor blocking waits stay isolated from scheduler-managed pipeline fibers.
- No broad mutex retrofit is planned for fiber-local boundaries unless a test
  exposes a real cross-thread ownership path.

## Contract First

Preserve existing public APIs:

- `FiberStream::Pipeline#run_async`
- `FiberStream::RunningPipeline`
- `FiberStream::Flow.async`
- `FiberStream::Flow.buffer`
- `FiberStream::Source#concat`
- `FiberStream::Flow.ractor_map`
- `FiberStream::Source.ractor_port`
- `FiberStream::Flow.lines`
- `FiberStream::Flow.split`
- `FiberStream::Source.io`

Required contract updates before implementation:

- Update `docs/product-specs/flow-lines.md` and
  `docs/product-specs/flow-split.md` to state that `max_length: nil` disables
  protection and trusted sources only should use the unbounded default.
- Update examples or docs to recommend explicit `max_length` for untrusted or
  network-facing chunk streams.
- Update `docs/product-specs/ractor-port-source.md` or
  `docs/design-docs/ractor-port-source.md` to state that
  `RactorPort::Failure` metadata is producer-provided and may contain sensitive
  application data unless sanitized by the producer.
- Update `docs/product-specs/io-source.md` to document that very large
  `chunk_size` values can cause large `readpartial` allocations. Do not add a
  hard cap in this plan.

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: request context-free sub-agent review and incorporate
      justified feedback.
- [ ] Red: add failing behavior-focused regression tests.
- [ ] Green: implement the smallest changes that satisfy the tests.
- [ ] Refactor: simplify cleanup paths while keeping tests green.
- [ ] Static checks: run targeted tests, then the repository's default checks.
- [ ] Code review: request sub-agent review after implementation.
- [ ] Re-review: fix review findings and repeat review until it passes.

## Decisions

### Adopt

- Fix `Pull::Concat` eager left materialization. The accepted design says the
  receiver side is materialized only when the concatenated source is
  materialized and then demanded. The current constructor calls the left
  materializer immediately, so implementation must move left materialization to
  first `next`.
- Implement `AsyncBoundary#cancel_producer` and
  `BufferBoundary#cancel_producer`. Both accepted designs already require
  close to request producer cancellation; the current no-op methods are
  contract drift. Treat the two boundaries separately: `AsyncBoundary` uses a
  manually resumed `Fiber.new(blocking: false)` producer, while
  `BufferBoundary` uses a `Fiber.schedule` producer and can follow the
  scheduler-interrupt shape used by scheduled boundaries.
- Narrow `RunningPipeline#run_background` non-local exception handling.
  Cancellation still needs to be classified by object identity, but process
  control exceptions such as `SystemExit` and `SignalException` should not be
  swallowed into a waitable stream error. If a fatal exception is observed,
  existing waiters must not be stranded: either publish a completion before
  re-raising where that is safe, or document and test the exact fatal path that
  cannot provide a waitable completion.
- Harden Ractor worker lifecycle sends with best-effort rescue around final
  `WorkerFailure` and `Stopped` sends so send failures do not cascade during
  worker teardown.

### Requires Careful Design

- Replace or justify coordinator cleanup sleep loops in `RactorPortSource`,
  `RactorMapBoundary`, and `RactorMergePortsSource`. A direct `Thread#join`
  may block scheduler-backed fibers and violate accepted Ractor designs. Prefer
  a scheduler-safe completion signal, or prove direct join is scheduler-safe
  with Async responsiveness tests before changing it.
- Rework `RactorMapBoundary#push_until_delivered_or_closed` without a naive
  blocking `SizedQueue#push`. The coordinator must not block forever if
  downstream closes while a bounded result queue is full. Prefer a close-aware
  enqueue path that either closes the target queue during shutdown or uses a
  condition/mutex protocol that wakes when close begins.

### Documentation First

- Keep `Flow.lines(max_length: nil)` and `Flow.split(max_length: nil)` as
  accepted defaults, but document that unbounded framing is unsafe for
  untrusted or network-facing sources.
- Keep `Source.io(chunk_size:)` validation limited to positive integers in
  this plan. Add cautionary docs rather than an arbitrary hard cap.
- Document that `RactorPort::Failure` carries producer-supplied class/message
  metadata and producers should sanitize these strings across trust boundaries.

### Defer

- Do not add a mutex to `BufferBoundary#close` in this plan. The accepted
  boundary model is scheduler-backed cooperative fiber execution, and no
  current contract gives another native thread ownership of the boundary.
- Do not replace `Source` internal `__send__(:new, ...)` calls in this plan.
  The current pattern is deliberate private-constructor encapsulation; revisit
  only if Ruby 4.x behavior or static tooling shows a real compatibility issue.
- Do not add a separate `Lines` upstream-closed flag unless tests expose a
  non-idempotent close bug. Existing pull streams are expected to make close
  idempotent.
- Treat `Merge::MergeMailbox` capacity concerns as a focused follow-up audit
  unless current tests reveal a deadlock.

## Test Plan

- `test/fiber_stream/source_concat_test.rb`
  - Add a regression proving the left materializer is not called until the
    first downstream demand reaches the concat stream.
  - Preserve right-side delayed materialization and transition close failure
    behavior.
- `test/fiber_stream/flow_async_test.rb`
  - Add a regression proving downstream close interrupts or otherwise cancels
    the manually resumed producer with the primitive supported by Ruby for
    that producer shape.
  - Prove the internal cancellation exception is suppressed after intentional
    close.
- `test/fiber_stream/flow_buffer_test.rb`
  - Add a regression proving close wakes or interrupts a producer blocked on a
    full buffer.
  - Preserve producer-side close error precedence.
- `test/fiber_stream/pipeline_background_test.rb`
  - Add coverage that `SystemExit` or `SignalException` is not converted into a
    waitable `RunningPipeline` completion if this can be tested without
    terminating the process.
  - Preserve existing non-`StandardError` stream failure delivery for
    non-fatal exceptions such as `NotImplementedError`.
- `test/fiber_stream/source_ractor_port_test.rb`
  - Preserve close waiting behavior and scheduler responsiveness for any
    cleanup wait change.
- `test/fiber_stream/flow_ractor_map_test.rb`
  - Add a regression for coordinator close with a full result queue.
  - Preserve Async responsiveness while waiting for Ractor results and cleanup.
  - Add or adjust tests for worker lifecycle send failures if practical.
- `test/fiber_stream/source_ractor_merge_ports_test.rb`
  - Include `RactorMergePortsSource` in the cleanup-wait audit because it uses
    the same sleep-then-join shape as the single-port Ractor source.

## Verification

Targeted commands:

```sh
bundle exec ruby -Itest test/fiber_stream/source_concat_test.rb
bundle exec ruby -Itest test/fiber_stream/flow_async_test.rb
bundle exec ruby -Itest test/fiber_stream/flow_buffer_test.rb
bundle exec ruby -Itest test/fiber_stream/pipeline_background_test.rb
bundle exec ruby -Itest test/fiber_stream/source_ractor_port_test.rb
bundle exec ruby -Itest test/fiber_stream/source_ractor_merge_ports_test.rb
bundle exec ruby -Itest test/fiber_stream/flow_ractor_map_test.rb
```

Final command:

```sh
bundle exec rake
```

## Third-Party Review

Reviewed by a context-free sub-agent before implementation. Accepted feedback:

- Do not replace Ractor cleanup polling with raw `Thread#join` without proving
  scheduler responsiveness. The plan now requires a scheduler-safe completion
  mechanism or explicit Async responsiveness tests.
- Split async and buffer producer cancellation design because `AsyncBoundary`
  uses a manually resumed producer fiber while `BufferBoundary` uses a
  scheduled producer fiber.
- Preserve waiter semantics when narrowing `RunningPipeline` fatal exception
  handling, and keep non-fatal non-`StandardError` delivery covered.
- Include `RactorMergePortsSource` in the cleanup-wait audit because it has the
  same sleep-then-join pattern.
- Add explicit documentation work for `RactorPort::Failure` metadata exposure
  and for the cooperative single-thread ownership assumption behind
  `BufferBoundary`.

### Issue 1: RunningPipeline Process-Control Exceptions

Design review found no disagreement with recording `SystemExit` and
`SignalException` for waiters before re-raising from the background fiber. It
also recommended preserving existing non-`StandardError` stream failure
delivery and adding docs that distinguish process-control exceptions from
ordinary stream failures.

Code review found no blocking issues after implementation. Residual risk:
pre-registered waiter wakeup during a process-control exception is not tested
directly because scheduler propagation timing makes that path hard to exercise
deterministically. The implementation uses the same `complete` broadcast path
covered by existing concurrent waiter tests and the new post-completion replay
tests.

Verification:

- `bundle exec ruby -Itest test/fiber_stream/pipeline_background_test.rb`
  - 17 runs, 58 assertions, 0 failures, 0 errors, 0 skips

## Completion Notes

Pending.

## Commit

Pending.
