# Flow.buffer

## Status

Accepted

## Problem

`Flow.async` proves that FiberStream can move upstream work into a
scheduler-aware producer fiber, but it remains demand-driven with no prefetch.
Users also need a bounded buffer stage that can let upstream run ahead by a
controlled amount while preserving backpressure, cleanup, and dependency-free
scheduler integration.

## Goals

- Add `FiberStream::Flow.buffer(count)` as a bounded asynchronous buffer.
- Add `Source#buffer(count)` as a convenience wrapper.
- Preserve lazy construction.
- Require an installed `Fiber.scheduler` only when the buffer is demanded.
- Keep FiberStream independent of the `async` gem at runtime.
- Preserve element order.
- Bound queued messages by `count`; the producer may hold one additional
  in-flight message while waiting to enqueue into a full buffer.
- Close upstream and request producer cancellation when downstream completes
  early or fails.
- Propagate upstream failures to downstream consumers unless downstream has
  intentionally completed or failed first.

## Non-Goals

- Unbounded buffering.
- Dropping, sliding, conflating, batching, or timeout-based buffering.
- Parallel mapping or unordered execution.
- Installing or selecting a scheduler.
- Depending on Async at runtime.
- IO-specific source or sink APIs.
- Background `run_async` materialization.

## Requirements

- `FiberStream::Flow.buffer(count)` creates a bounded asynchronous buffer.
- `Source#buffer(count)` is a convenience equivalent to
  `Source#via(FiberStream::Flow.buffer(count))`.
- Constructing `Flow.buffer(count)` does not require `Fiber.scheduler` and does
  not pull upstream.
- `count` must be an `Integer`.
- `count` must be positive.
- `Flow.buffer(count)` raises `TypeError` when `count` is not an `Integer`.
- `Flow.buffer(count)` raises `ArgumentError` when `count` is zero or negative.
- The producer fiber starts on the first downstream pull, not at pipeline
  construction or materialization.
- If no `Fiber.scheduler` is installed when the producer must start,
  `Flow.buffer(count)` raises `FiberStream::SchedulerRequiredError`.
- Upstream stages before the buffer run in a scheduled producer fiber.
- Downstream stages after the buffer run in the caller's current fiber.
- The materialized buffer boundary is owned by the running pipeline's
  downstream fiber and its scheduled producer fiber. It is not a thread-safe
  object for concurrent native-thread calls to `next` or `close`.
- The buffer queue holds at most `count` messages. Value, completion, and error
  messages all count toward this queued bound.
- The producer may pull one additional upstream result while waiting to enqueue
  into a full buffer.
- The producer blocks at the buffer boundary when the buffer is full.
- While blocked on enqueue, the producer does not pull farther upstream.
- Downstream pulls receive buffered elements in original order.
- Normal upstream completion is delivered to downstream as normal stream
  completion after all buffered elements are consumed.
- Failures raised by upstream source enumeration or upstream flow blocks are
  re-raised from `Source#run_with` after all earlier buffered elements have been
  consumed.
- If upstream completes normally and producer-side `upstream.close` fails, that
  close failure is delivered downstream as a stream failure.
- If upstream pull and producer-side `upstream.close` both fail, the upstream
  pull failure is delivered downstream and the close failure is suppressed.
- Normal completion is not delivered downstream until producer-side
  `upstream.close` has succeeded.
- If downstream intentionally completes early or fails before consuming a queued
  or in-flight upstream error, that upstream error is suppressed in favor of the
  downstream completion or failure.
- Closing the buffer closes upstream.
- Closing the buffer requests producer cancellation when it is still running.
- `upstream.close` failures raised while downstream closes the buffer are
  propagated from `Source#run_with` unless a downstream failure is already being
  propagated.
- Early downstream completion closes upstream before `Source#run_with` returns.
- Close idempotency applies to repeated cleanup in the boundary's cooperative
  ownership model; it does not define race-free concurrent close semantics for
  arbitrary native threads.
- FiberStream does not guarantee that a scheduler can immediately interrupt an
  arbitrary blocking operation inside user code. Resource-owning upstream stages
  must make `close` release their resources.
- Repeated downstream pulls after completion return completion without pulling
  upstream again.
- Async compatibility tests use the development dependency as a scheduler
  target, but application code using FiberStream should not need to require
  Async unless it chooses Async as its scheduler.

## Public Contracts

```ruby
FiberStream::Flow.buffer(count)
FiberStream::Source#buffer(count)
FiberStream::SchedulerRequiredError
```

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

## Examples

```ruby
require "async"
require "fiber_stream"

result =
  Async do
    FiberStream::Source.each([1, 2, 3])
      .map { |number| number * 2 }
      .buffer(2)
      .take(2)
      .run_with(FiberStream::Sink.to_a)
  end.wait

result # => [2, 4]
```

When no scheduler is installed:

```ruby
FiberStream::Source.each([1])
  .buffer(1)
  .run_with(FiberStream::Sink.to_a)

# raises FiberStream::SchedulerRequiredError
```

## Open Questions

- Should future buffer variants expose overflow policies such as drop-oldest or
  drop-newest, or should those become separate named flows?
