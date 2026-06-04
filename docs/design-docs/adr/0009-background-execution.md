# ADR 0009: Background Execution

## Status

Accepted

## Context

FiberStream supports foreground linear pipeline materialization through
`Source#run_with` and `Pipeline#run`. Scheduler-backed stages exist, but users
cannot start a whole pipeline in the background and later wait for or cancel
that materialization.

## Decision

Add `Pipeline#run_async`, returning `FiberStream::RunningPipeline`.

The handle schedules one background fiber through Ruby's scheduler, runs the
existing `source.run_with(sink)` foreground path inside that fiber, stores one
tagged completion result, and exposes:

- `#wait`
- `#cancel`
- `#done?`
- `#cancel_requested?`

Background start and blocking waits require `Fiber.scheduler` and a
non-blocking current fiber. Cancellation uses the scheduler captured by
`run_async` and `fiber_interrupt` with `FiberStream::PipelineCancelledError`;
schedulers that do not expose `fiber_interrupt` raise `NotImplementedError`
from `cancel` without recording a cancellation request.

## Consequences

- Background execution reuses existing stream materialization and cleanup
  semantics.
- FiberStream remains independent of Async at runtime.
- The public background API is narrow enough to extend later with timeout or
  supervision helpers.
- Cancellation availability is tied to scheduler interruption support instead
  of silently pretending cancellation is always possible.
- Concurrent waiters all receive the same stored completion result.
- Non-linear execution is outside this decision.
