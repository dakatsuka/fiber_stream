# Background Execution

## Status

Accepted

## Context

The accepted linear pull runtime materializes a source, attaches flows, and
runs a sink in the caller's current fiber. Existing scheduler-backed stages
prove that FiberStream can cooperate with Ruby's `Fiber.scheduler`, but pipeline
materialization itself remains foreground-only.

Governing documents:

- Product spec: `docs/product-specs/background-execution.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Async boundary design: `docs/design-docs/async-boundary.md`
- Buffer boundary design: `docs/design-docs/buffer-boundary.md`
- Parallel map design: `docs/design-docs/parallel-map.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Goals

- Add a narrow background execution API to `Pipeline`.
- Keep existing source, flow, sink, and foreground pipeline contracts unchanged.
- Represent background execution with a reusable public handle type.
- Preserve materialized values and failures through explicit waiting.
- Make cancellation request background fiber interruption without requiring
  Async at runtime.
- Ensure interrupted background runs use existing `Source#run_with` cleanup.

## Non-Goals

- Non-linear pipeline scheduling or materialization.
- Runtime scheduler installation.
- Thread, process, or Ractor execution.
- Supervisors, restart policies, timeouts, retries, or task groups.
- Public access to internal pull streams.

## Proposed Design

`Pipeline#run_async` validates that it is called from a scheduler-backed
non-blocking fiber, then creates a `RunningPipeline` handle:

```ruby
running = pipeline.run_async
```

The handle schedules one background fiber with `Fiber.schedule`. The background
fiber calls the same foreground runtime path as `Pipeline#run`:

```ruby
source.run_with(sink)
```

This keeps stream materialization, close ordering, early completion behavior,
and error precedence centralized in `Source#run_with`.

The handle owns:

- the scheduled background fiber
- per-waiter completion queues used to wake all pre-completion waiters
- the stored completion result for repeated `#wait`
- the scheduler captured when the background fiber was started
- a `cancel_requested` flag
- the exact cancellation exception object sent to the background fiber

Completion messages are tagged so arbitrary Ruby objects can be valid
materialized values:

```ruby
[:value, object]
[:error, exception]
[:cancelled, exception]
```

`RunningPipeline#wait` returns successful values and re-raises failures. Before
completion, waiting can block; therefore it requires the same scheduler-backed
non-blocking current fiber as the scheduler-backed stream stages. Each
pre-completion waiter registers its own queue. Completion stores the tagged
result once and broadcasts the same result to every registered waiter. After
completion, `#wait` returns or raises the stored completion without requiring a
scheduler.

`RunningPipeline#cancel` is idempotent. Before completion, it checks the
captured scheduler for `fiber_interrupt` support, creates one
`FiberStream::PipelineCancelledError` instance for this cancellation request,
marks `cancel_requested`, and requests interruption of the scheduled background
fiber through the captured scheduler. If the captured scheduler cannot
interrupt fibers, `cancel` raises `NotImplementedError` without changing
cancellation state.

When cancellation interrupts `source.run_with`, the existing `ensure` block in
`Source#run_with` closes the materialized pull chain. Close failures are
suppressed behind the primary cancellation error, matching existing foreground
failure precedence. The background fiber classifies cancellation by object
identity: only the exact exception object sent by `cancel` becomes a
`[:cancelled, exception]` completion. If user code raises
`PipelineCancelledError` independently, it is treated like any other stream
failure. If a pipeline completes or fails before cancellation is observed, the
original success or failure remains the completion result.

The first implementation intentionally does not expose a task group,
supervisor, or timeout API. Those policies can be layered over the handle later
without changing the stream runtime contract.

## Contracts

- `Pipeline#run_async` returns `RunningPipeline[Mat]`.
- `Pipeline#run_async` creates one materialization per call.
- `Pipeline#run_async` requires `Fiber.scheduler`.
- `Pipeline#run_async` requires a non-blocking current fiber.
- `Pipeline#run` remains foreground and unchanged.
- `RunningPipeline#wait` returns the materialized value on success.
- `RunningPipeline#wait` re-raises stream failures.
- `RunningPipeline#wait` re-raises `PipelineCancelledError` after cancellation
  interrupts the background materialization.
- `RunningPipeline#wait` before completion requires a scheduler-backed
  non-blocking current fiber.
- `RunningPipeline#wait` after completion is replayable.
- Multiple pre-completion waiters all observe the same stored completion result.
- `RunningPipeline#cancel` is idempotent.
- `RunningPipeline#cancel` after completion has no effect.
- `RunningPipeline#cancel` before completion requests background fiber
  interruption with the scheduler captured by `run_async`.
- `RunningPipeline#cancel` raises `NotImplementedError` without changing state
  if the captured scheduler cannot interrupt fibers.
- `RunningPipeline#done?` reports success, failure, or cancellation completion.
- `RunningPipeline#cancel_requested?` reports whether cancellation has been
  successfully requested.
- FiberStream does not depend on Async at runtime.

## Alternatives Considered

### Add `Source#run_async(sink)`

This would mirror `Source#run_with`, but `Pipeline` already represents the
reusable source-to-sink definition. Adding background execution there keeps the
public surface smaller and avoids duplicating construction paths.

### Return The Scheduled Fiber Directly

Ruby fibers do not provide FiberStream-specific materialized value replay,
failure normalization, done state, or cancellation contracts. A handle lets
FiberStream expose only the operations it can support.

### Use Threads

Threads would make background execution available without a scheduler, but they
would diverge from FiberStream's scheduler-backed IO and cancellation model.
They also introduce cross-thread ownership questions for IO and user objects.

### Best-Effort Cancellation Without `fiber_interrupt`

Recording cancellation without a way to interrupt the background fiber would
make `cancel` appear successful while the pipeline might continue indefinitely.
The first public cancellation API should fail synchronously when the active
scheduler lacks the required capability.

## Validation

- Unit tests proving `Pipeline#run_async` requires a scheduler-backed
  non-blocking fiber.
- Unit tests proving `Source#to` remains lazy and `run_async` creates one
  background materialization per call.
- Async-backed tests proving `#wait` returns materialized values and re-raises
  stream failures.
- Tests proving repeated and concurrent `#wait` returns or raises the stored
  completion.
- Tests proving `#done?` and `#cancel_requested?` states.
- Async-backed tests proving `#cancel` interrupts a running background pipeline
  and upstream is closed before `#wait` raises `PipelineCancelledError`.
- Tests proving failed cancellation requests do not change cancellation state.
- Tests proving user-raised `PipelineCancelledError` is treated as a stream
  failure unless it is the exact cancellation object sent by the handle.
- RBS validation.
- RuboCop.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-02. Feedback resulted in these
changes:

- Replaced a single completion queue with per-waiter queues and broadcast
  completion semantics.
- Captured the scheduler at `run_async` time and specified that `cancel` uses
  that scheduler rather than the caller's active scheduler.
- Defined that failed cancellation capability checks do not mark cancellation
  requested.
- Renamed the request-state predicate to `cancel_requested?`.
- Classified cancellation by exception object identity.
- Replaced the nondeterministic cancellation example with a blocking
  scheduler-managed example.
- Added validation items for concurrent waits, failed cancellation requests,
  and user-raised cancellation errors.

## Open Questions

None.
