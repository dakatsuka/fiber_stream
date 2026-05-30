# Flow.async

## Status

Accepted

## Problem

FiberStream's current runtime is fully foreground and pull-driven. That is a
good foundation for backpressure, but upstream work currently runs in the same
fiber as downstream materialization. Users need a first explicit asynchronous
boundary that keeps the public API dependency-free while proving FiberStream can
cooperate with Ruby's `Fiber.scheduler`.

## Goals

- Add an explicit `FiberStream::Flow.async` boundary for linear pipelines.
- Preserve lazy stream construction.
- Require an installed `Fiber.scheduler` only when the async boundary is
  materialized and demanded.
- Keep FiberStream independent of the `async` gem at runtime.
- Preserve pull backpressure: one downstream pull causes at most one upstream
  pull across the async boundary.
- Close upstream and request producer cancellation when downstream completes
  early or fails.
- Propagate upstream failures to downstream consumers.

## Non-Goals

- Installing or selecting a scheduler.
- Depending on Async at runtime.
- User-configurable buffer sizes.
- Parallel mapping or unordered execution.
- Graph execution.
- IO-specific source or sink APIs.
- Background `run_async` materialization.

## Requirements

- `FiberStream::Flow.async` creates an asynchronous flow boundary.
- Constructing `Flow.async` does not require `Fiber.scheduler` and does not pull
  upstream.
- The async producer fiber starts on the first downstream pull, not at pipeline
  construction or materialization.
- If no `Fiber.scheduler` is installed when the producer must start,
  `Flow.async` raises `FiberStream::SchedulerRequiredError`.
- `SchedulerRequiredError` is a `RuntimeError`.
- Upstream stages before the async boundary run in a non-blocking producer
  fiber.
- Downstream stages after the async boundary run in the caller's current fiber.
- Each downstream pull resumes the upstream producer fiber for at most one
  upstream pull.
- Normal completion is delivered to downstream as normal stream completion.
- Failures raised by upstream source enumeration or upstream flow blocks are
  re-raised from `Source#run_with`.
- Closing the async boundary closes upstream.
- Closing the async boundary requests producer cancellation when it is still
  running.
- Early downstream completion closes upstream before `Source#run_with` returns.
- FiberStream does not guarantee that a scheduler can immediately interrupt an
  arbitrary blocking operation inside user code. Resource-owning upstream stages
  must make `close` release their resources.
- Repeated downstream pulls after completion return completion without pulling
  upstream again.
- `Flow.async` preserves element order.
- `Flow.async` is deterministic with respect to stream values and errors, but
  it does not promise exact scheduler task ordering.
- Async compatibility tests use the development dependency as a scheduler
  target, but application code using FiberStream should not need to require
  Async unless it chooses Async as its scheduler.

## Public Contracts

```ruby
FiberStream::Flow.async
FiberStream::SchedulerRequiredError
```

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

## Examples

```ruby
require "async"
require "fiber_stream"

result =
  Async do
    FiberStream::Source.each([1, 2, 3])
      .via(FiberStream::Flow.map { |number| number * 2 })
      .via(FiberStream::Flow.async)
      .via(FiberStream::Flow.take(2))
      .run_with(FiberStream::Sink.to_a)
  end.wait

result # => [2, 4]
```

When no scheduler is installed:

```ruby
FiberStream::Source.each([1])
  .via(FiberStream::Flow.async)
  .run_with(FiberStream::Sink.to_a)

# raises FiberStream::SchedulerRequiredError
```

## Open Questions

- Should a later `Flow.buffer(count)` be separate from `Flow.async`, or should
  `Flow.async` grow an optional buffer-size argument after the demand-driven
  boundary proves the cancellation model?
