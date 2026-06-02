# Background Execution

## Status

Accepted

## Problem

FiberStream can build and run foreground linear pipelines, and it has
scheduler-backed stages for asynchronous boundaries, buffering, IO, and ordered
parallel mapping. Users also need to start an entire pipeline in the background
so the caller can perform other scheduler-managed work, then wait for the
pipeline materialized value or request cancellation through a narrow handle.

## Goals

- Add background execution for existing linear `Pipeline` definitions.
- Keep foreground `Pipeline#run` and `Source#run_with` behavior unchanged.
- Start background work lazily from an explicit public method.
- Return a handle that exposes completion, waiting, and cancellation requests.
- Require a scheduler-backed non-blocking fiber for background start and
  blocking waits.
- Preserve normal materialized values and stream failures through `#wait`.
- Close the materialized stream when cancellation interrupts execution.
- Keep FiberStream independent of the `async` gem at runtime.

## Non-Goals

- Graph execution or graph DSLs.
- Detached process, thread, or Ractor execution.
- Installing or selecting a scheduler.
- Supervisors, restart policies, timeouts, or retries.
- Multiple materialized values from one background run.
- Progress reporting or metrics.

## Requirements

- `Pipeline#run_async` starts one background materialization of the captured
  source and sink.
- `Pipeline#run_async` returns a `FiberStream::RunningPipeline` handle.
- `Pipeline#run_async` must be called with an installed `Fiber.scheduler` from a
  non-blocking fiber.
- If no scheduler is installed, `Pipeline#run_async` raises
  `FiberStream::SchedulerRequiredError`.
- If the current fiber is blocking, `Pipeline#run_async` raises
  `FiberStream::SchedulerRequiredError`.
- Constructing a pipeline with `Source#to(sink)` remains lazy and does not start
  background work.
- Each `Pipeline#run_async` call creates a separate materialization, subject to
  the captured endpoints' replayability and resource ownership semantics.
- `RunningPipeline#wait` waits for completion and returns the sink materialized
  value on success.
- `RunningPipeline#wait` re-raises the stream failure when the background
  materialization fails.
- `RunningPipeline#wait` re-raises `FiberStream::PipelineCancelledError` when
  cancellation interrupts the background materialization.
- Calling `RunningPipeline#wait` before completion requires an installed
  scheduler and a non-blocking current fiber.
- Calling `RunningPipeline#wait` after completion returns or raises the stored
  result without requiring a scheduler.
- Multiple fibers may call `RunningPipeline#wait` before completion; all waiters
  receive the same stored completion result.
- `RunningPipeline#done?` returns whether the background run has completed with
  success, failure, or cancellation.
- `RunningPipeline#cancel` is idempotent.
- Calling `RunningPipeline#cancel` after completion has no effect.
- Calling `RunningPipeline#cancel` before completion requests cancellation of
  the background fiber through the scheduler captured when `run_async` started.
- Cancellation uses Ruby scheduler `fiber_interrupt` support. If the captured
  scheduler cannot interrupt fibers, `cancel` raises `NotImplementedError`
  without recording a cancellation request.
- Cancellation is cooperative. FiberStream does not guarantee immediate
  interruption of arbitrary user code, but when cancellation interrupts the
  background materialization, normal `Source#run_with` cleanup closes the
  materialized stream before `#wait` raises.
- Cancellation does not convert an already completed success or stream failure
  into cancellation.
- `RunningPipeline#cancel_requested?` returns true only after `cancel`
  successfully records a cancellation request.
- A background failure is classified as cancellation only when it is the exact
  cancellation exception object sent by the handle.
- FiberStream does not depend on Async at runtime. Async compatibility tests
  use the development dependency as a scheduler target.

## Public Contracts

```ruby
pipeline = FiberStream::Source.each([1, 2, 3]).to(FiberStream::Sink.to_a)
running = pipeline.run_async

running.done?
running.cancel_requested?
running.cancel
running.wait
```

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

## Examples

```ruby
require "async"
require "fiber_stream"

result =
  Async do
    running =
      FiberStream::Source.each([1, 2, 3])
        .map { |number| number * 2 }
        .to(FiberStream::Sink.to_a)
        .run_async

    running.wait
  end.wait

result # => [2, 4, 6]
```

Cancellation:

```ruby
Async do
  running =
    FiberStream::Source.each([1])
      .map { |value| sleep 60; value }
      .to(FiberStream::Sink.to_a)
      .run_async

  sleep 0
  running.cancel
  running.wait
end.wait

# raises FiberStream::PipelineCancelledError when cancellation interrupts
# before normal completion
```

When no scheduler is installed:

```ruby
FiberStream::Source.each([1])
  .to(FiberStream::Sink.to_a)
  .run_async

# raises FiberStream::SchedulerRequiredError
```

## Open Questions

None.
