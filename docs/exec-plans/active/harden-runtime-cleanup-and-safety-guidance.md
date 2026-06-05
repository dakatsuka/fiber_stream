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
  with Async responsiveness tests before changing it. Issue 3 proved direct
  `Thread#join` is scheduler-safe for the current Ruby 4.0.3 and Async 2.39.0
  compatibility target, then replaced the polling loops with direct joins.
- Rework `RactorMapBoundary#push_until_delivered_or_closed` without a naive
  blocking `SizedQueue#push`. The coordinator must not block forever if
  downstream closes while a bounded result queue is full. Prefer a close-aware
  enqueue path that either closes the target queue during shutdown or uses a
  condition/mutex protocol that wakes when close begins. Issue 2 chose the
  queue-close wakeup design: normal enqueue uses blocking `SizedQueue#push`,
  and boundary `close` closes both forward queues before waiting for worker
  shutdown.

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

### Issue 2: RactorMap Close-Aware Enqueue

Design review agreed that replacing nonblocking push plus sleep retry with
blocking `SizedQueue#push` is safe only if `close` first closes the
coordinator's forward queues. The selected implementation closes both
`@ready_workers` and `@results` after admission is closed and before worker
shutdown wait begins. Closing `@result_port` was explicitly avoided because the
coordinator still needs to observe worker lifecycle messages.

Red coverage added:

- A helper-level regression that stubs boundary `sleep` and proves enqueue
  waits on a full queue without polling.
- An adversarial worker regression that fills the result queue with extra
  worker values and proves close wakes the coordinator and returns.

Code review found no ordering or shutdown bugs. It requested a bounded timeout
around the helper-level blocked-thread wait and this was incorporated. Residual
risk: close still waits for `upstream.close` before internal queues are closed,
matching the current cooperative cleanup model.

Verification:

- `bundle exec ruby -Itest test/fiber_stream/flow_ractor_map_test.rb`
  - 27 runs, 71 assertions, 0 failures, 0 errors, 0 skips

### Issue 3: Ractor Cleanup Wait Join

Design review accepted direct `Thread#join` for coordinator cleanup waits after
a local compatibility spike showed Async ticker tasks continue while the current
task waits in `Thread#join`. The spike used Ruby 4.0.3 and Async 2.39.0 and
compared direct `join`, `join(0.001)` polling, and the existing
sleep-then-join loop; all allowed the ticker to progress. This establishes the
behavior for FiberStream's tested Async compatibility target, not for every
possible `Fiber.scheduler` implementation.

Implementation decisions:

- Replace sleep-then-join cleanup waits with direct `@coordinator.join` in
  `RactorMapBoundary`, `RactorPortSource`, and `RactorMergePortsSource`.
- Preserve wake-before-join ordering in every close path.
- Remove now-unused wait interval constants.
- Add cleanup-specific Async responsiveness tests for `Source.ractor_port` and
  `Source.ractor_merge_ports`; `Flow.ractor_map` already had cleanup wait
  responsiveness coverage.
- Test that cleanup waits no longer call boundary `sleep`, so polling cannot be
  silently reintroduced.

Code review found no production close-ordering regressions. It requested
wrapping `stream.close` in the new cleanup responsiveness tests with
`Timeout.timeout(1)` so a future wake-before-join regression fails instead of
hanging the suite; that hardening was incorporated and re-review found no
remaining issues.

Verification:

- `bundle exec ruby -Itest test/fiber_stream/flow_ractor_map_test.rb`
  - 27 runs, 71 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec ruby -Itest test/fiber_stream/source_ractor_port_test.rb`
  - 20 runs, 63 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec ruby -Itest test/fiber_stream/source_ractor_merge_ports_test.rb`
  - 22 runs, 88 assertions, 0 failures, 0 errors, 0 skips

### Issue 4: Async And Buffer Producer Cancellation

Design review confirmed that `AsyncBoundary` and `BufferBoundary` need
different cancellation primitives. `AsyncBoundary` owns a manually resumed
`Fiber.new(blocking: false)` producer, so close kills the suspended producer
with `Fiber#kill`. `BufferBoundary` owns a scheduled producer, so close uses
the scheduler captured at producer start and requests interruption with
`fiber_interrupt`.

Implementation decisions:

- Add an upstream close guard to `AsyncBoundary` so killing the producer and
  running its `ensure` does not double-close upstream.
- Use `Fiber#kill` for the manual async producer and swallow internal
  cancellation mechanics.
- Add a private `CancellationError` to `BufferBoundary`, rescue it in
  `run_producer`, and re-raise it ahead of `pull_message`'s broad
  `StandardError` rescue so intentional cancellation is not converted into an
  `ErrorMessage`.
- Capture the scheduler in `BufferBoundary#start` and use that scheduler for
  cancellation instead of the close caller's current scheduler.
- Handle the `Fiber.schedule` assignment race by checking `@closed` immediately
  after assigning `@producer` and requesting cancellation if close already
  happened while the scheduled producer was starting.

Red coverage added:

- Async regressions for closing after a yielded value and after a yielded done
  message; both prove the suspended producer is killed and upstream is closed
  exactly once.
- Buffer regression for closing while the producer is blocked in
  scheduler-aware upstream `next`; it proves the upstream `ensure` runs, the
  producer dies, and downstream observes normal close completion rather than an
  internal cancellation error.

Verification:

- `bundle exec ruby -Itest test/fiber_stream/flow_async_test.rb`
  - 12 runs, 27 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec ruby -Itest test/fiber_stream/flow_buffer_test.rb`
  - 16 runs, 34 assertions, 0 failures, 0 errors, 0 skips

### Issue 5: Source.concat Left Materialization Laziness

Design review found no objection to moving left materialization from
`Pull::Concat#initialize` to first downstream demand. It noted that the
product spec already requires demand-driven concat laziness, while one design
contract sentence was ambiguous. That sentence was clarified to state that the
receiver is materialized only when downstream demand first reaches the
concatenated source.

Implementation decisions:

- Initialize `@left` as `nil` in `Pull::Concat`.
- Materialize the left stream at the start of `next_left`.
- Store the left stream only after the left materializer returns successfully,
  so left materialization failures leave no side to close.
- Preserve existing transition ordering: close left, then switch phase and
  materialize right only after left close succeeds.
- Keep `close` limited to already materialized streams, so closing a concat
  stream before first pull materializes neither side.

Red coverage added:

- Public regression proving `left.concat(right).run_with` with a sink that
  never pulls materializes neither side.
- Internal regression proving `Pull.concat(...).close` before first `next`
  materializes neither side.

Verification:

- `bundle exec ruby -Itest test/fiber_stream/source_test.rb`
  - 60 runs, 139 assertions, 0 failures, 0 errors, 0 skips

### Issue 6: Flow.lines And Flow.split Max-Length Guidance

Design review agreed with the documentation-only scope. The accepted defaults
remain `max_length: nil` for compatibility with Ruby-like line and split
behavior, so the fix improves visibility of the existing risk rather than
adding a library-level cap.

Documentation decisions:

- State in the product specs that `max_length: nil` allows one unterminated
  line or frame to buffer without bound until a delimiter arrives or upstream
  completes.
- Clarify in the design docs that `max_length:` is the intended safety valve at
  trust boundaries.
- Add the same warning to public API comments for `Flow.lines`, `Flow.split`,
  `Source#lines`, and `Source#split`.
- Add `Source#split` and `Flow.split` to the README API surface while adding a
  concise `max_length` safety note.

No red tests were added because this issue changes guidance only and preserves
runtime behavior.

Code review found no documentation inconsistencies or overstatements. Residual
risk: the runtime still relies on callers to set `max_length` for untrusted or
unbounded inputs, which is intentional for compatibility with the accepted
defaults.

Verification:

- `bundle exec rake`
  - 457 runs, 1087 assertions, 0 failures, 0 errors, 0 skips
  - `bundle exec rbs validate`
  - `bundle exec rubocop`, 75 files inspected, no offenses detected

### Issue 7: Source.io Chunk Size Guidance

Design intent is to preserve the accepted `chunk_size` contract: callers may
provide any positive integer, and FiberStream does not impose an arbitrary
library-level upper bound. The default remains `16 * 1024`.

Documentation decisions:

- State that `chunk_size` is the maximum byte count passed to
  `io.readpartial` for one downstream pull.
- Document that very large values may cause the IO implementation to attempt
  large allocations.
- Recommend choosing a bounded value appropriate for the caller's memory budget
  and expected throughput.

No red tests were added because this issue changes guidance only and preserves
runtime behavior.

Code review found no documentation inconsistencies or overstatements. Residual
risk: this is not a runtime guard, so allocation risk from very large
`chunk_size` values remains caller-managed by design.

Verification:

- `bundle exec rake`
  - 457 runs, 1087 assertions, 0 failures, 0 errors, 0 skips
  - `bundle exec rbs validate`
  - `bundle exec rubocop`, 75 files inspected, no offenses detected

### Issue 8: RactorPort Failure Metadata Guidance

Design intent is to preserve the accepted `RactorPort::Failure` protocol:
producer failures cross Ractor boundaries as string metadata rather than
exception objects. FiberStream does not sanitize those strings because they are
producer-authored application data.

Documentation decisions:

- State that valid `Failure` `cause_class_name` and `cause_message` values are
  producer-provided metadata.
- Document that FiberStream exposes that metadata through
  `RactorPortSourceError` accessors and the exception message.
- Recommend producer-side sanitization or redaction before failures cross trust
  boundaries.

No red tests were added because this issue changes guidance only and preserves
runtime behavior.

Code review found no documentation inconsistencies or overstatements. Residual
risk: this is not runtime redaction, so actual sanitization remains a producer
responsibility by design.

Verification:

- `bundle exec rake`
  - 457 runs, 1087 assertions, 0 failures, 0 errors, 0 skips
  - `bundle exec rbs validate`
  - `bundle exec rubocop`, 75 files inspected, no offenses detected

### Issue 9: BufferBoundary Cooperative Ownership

Design intent is to preserve the accepted scheduler-backed buffer ownership
model. The materialized buffer boundary is driven by the downstream pipeline
fiber and its scheduled producer fiber under the active scheduler; it is not a
thread-safe object for arbitrary native-thread concurrent calls to `next` or
`close`.

Documentation decisions:

- Clarify in the buffer product spec and design doc that `close` idempotency
  applies within the cooperative ownership model.
- State that FiberStream does not define race-free concurrent close semantics
  for arbitrary native threads.
- Add a general linear-runtime note that internal materialized pull streams are
  owned by the running pipeline unless a specific boundary design says
  otherwise.

No red tests were added because this issue changes guidance only and preserves
runtime behavior.

Code review requested that the buffer design Contracts section also qualify
close idempotency as applying within the cooperative ownership model. That
wording was incorporated. Residual risk: arbitrary native-thread concurrent
`next` or `close` remains unsupported by design.

Verification:

- `bundle exec rake`
  - 457 runs, 1087 assertions, 0 failures, 0 errors, 0 skips
  - `bundle exec rbs validate`
  - `bundle exec rubocop`, 75 files inspected, no offenses detected

## Completion Notes

Pending.

## Commit

Pending.
