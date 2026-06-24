# Flow.parallel_unordered_map

## Status

Accepted

## Problem

`Flow.parallel_map` overlaps independent per-element work, but it preserves
input order. Ordered delivery can create head-of-line blocking when an early
element is slow and later elements are already complete. Users also need a
bounded scheduler-backed mapping stage that emits results as mapping work
finishes when downstream logic does not require input order.

## Goals

- Add `FiberStream::Flow.parallel_unordered_map(concurrency:) { ... }`.
- Add `Source#parallel_unordered_map(concurrency:) { ... }` as a convenience
  wrapper.
- Preserve lazy construction.
- Require an installed `Fiber.scheduler` and a non-blocking current fiber only
  when the unordered parallel stage is demanded.
- Keep FiberStream independent of the `async` gem at runtime.
- Run at most `concurrency` user mapping blocks at a time.
- Emit mapped values in scheduler-observed completion order.
- Make no cross-element ordering guarantee.
- Bound upstream run-ahead to at most `concurrency` pulled elements that have
  not yet been emitted downstream.
- Close upstream and request worker cancellation when downstream completes
  early, fails, or observes a producer-side failure.

## Non-Goals

- Ordered result emission.
- Ractor-backed execution or process-backed execution.
- CPU parallelism guarantees.
- General fan-out, fan-in, or graph scheduling.
- Runtime scheduler installation.
- Depending on Async at runtime.
- Per-element timeout, retry, batching, or overflow policies.
- Background `run_async` materialization changes.

## Requirements

- `FiberStream::Flow.parallel_unordered_map(concurrency:) { |element| ... }`
  creates an unordered scheduler-backed parallel mapping flow.
- `Source#parallel_unordered_map(concurrency:) { |element| ... }` is a
  convenience equivalent to
  `Source#via(FiberStream::Flow.parallel_unordered_map(concurrency:) { ... })`.
- Constructing `Flow.parallel_unordered_map` does not require
  `Fiber.scheduler`, pull upstream, schedule fibers, or call the user block.
- `parallel_unordered_map` requires a block.
- `concurrency` must be an `Integer`.
- `concurrency` must be positive.
- Missing blocks raise `ArgumentError`.
- Non-Integer `concurrency` values raise `TypeError`.
- Zero or negative `concurrency` values raise `ArgumentError`.
- Internal scheduler-backed execution starts on the first downstream pull, not
  at pipeline construction or materialization.
- If no `Fiber.scheduler` is installed when the stage must start,
  `Flow.parallel_unordered_map` raises
  `FiberStream::SchedulerRequiredError`.
- If a scheduler is installed but the current fiber is blocking when the stage
  must wait on internal queues, `Flow.parallel_unordered_map` raises
  `FiberStream::SchedulerRequiredError`.
- FiberStream does not install or select a scheduler.
- Upstream stages before `parallel_unordered_map` are pulled by an internal
  scheduled dispatcher.
- The user mapping block runs in scheduled worker fibers.
- Downstream stages after `parallel_unordered_map` run in the caller's current
  fiber.
- At most `concurrency` mapping blocks are executing at the same time.
- At most `concurrency` upstream elements may be pulled and not yet emitted
  downstream. This bound includes queued work, running work, and completed
  results waiting for downstream demand.
- Downstream pulls receive mapped values in scheduler-observed completion
  order.
- `parallel_unordered_map` does not preserve input order.
- Normal upstream completion is delivered to downstream as normal stream
  completion after all pulled mapped values have been emitted.
- A normal-completion terminal message must not be delivered while any
  admitted mapping job remains queued, running, or completed but unobserved.
- Failures raised by upstream source enumeration or upstream flow blocks are
  re-raised from `Source#run_with` when downstream observes the failure.
- Failures raised by the mapping block are re-raised from `Source#run_with`
  when downstream observes the failure.
- Upstream and mapping failures are fail-fast in scheduler-observed result
  order, not input sequence order.
- When a failure is observed by downstream, FiberStream stops admitting new
  upstream elements, closes upstream, wakes internal queues, requests
  best-effort cancellation of active dispatcher and worker fibers, and raises
  that failure.
- Values already emitted before a failure remain emitted.
- Values and failures queued or in flight but not yet observed when the primary
  failure is delivered are suppressed.
- If upstream completes normally and producer-side `upstream.close` fails, that
  close failure is delivered downstream as a stream failure after all earlier
  completion-order mapped values have been emitted.
- A producer-side close failure after normal upstream completion must not be
  delivered while any admitted mapping job remains queued, running, or
  completed but unobserved.
- If an admitted mapping job fails before a delayed producer-side close failure
  is delivered, the mapping failure is delivered and the close failure is
  suppressed.
- If an upstream pull failure and producer-side `upstream.close` failure both
  happen, the upstream pull failure is delivered and the close failure is
  suppressed.
- If a mapping failure and boundary close failure both happen, the mapping
  failure is delivered and the close failure is suppressed.
- If downstream completes early and boundary close fails, the boundary close
  failure is propagated from `Source#run_with`.
- If downstream fails and boundary close also fails, the downstream failure
  remains primary and the boundary close failure is suppressed.
- If downstream intentionally completes early or fails before consuming queued
  or in-flight upstream or mapping failures, those failures are suppressed in
  favor of the downstream result or downstream failure.
- Closing the unordered parallel map boundary closes upstream.
- Closing the boundary wakes internal queues and requests best-effort
  cancellation of active dispatcher and worker fibers.
- Active fiber cancellation uses `Fiber.scheduler#fiber_interrupt` when the
  installed scheduler supports it. With schedulers that do not support
  `fiber_interrupt`, FiberStream still closes upstream and internal queues, but
  active user mapping or upstream code may continue until it next cooperatively
  returns or raises.
- Early downstream completion closes upstream before `Source#run_with`
  returns.
- FiberStream does not guarantee that a scheduler can immediately interrupt an
  arbitrary blocking operation inside user mapping or upstream code.
- Repeated downstream pulls after completion return completion without pulling
  upstream again.
- Async compatibility tests use the development dependency as a scheduler
  target, but application code using FiberStream should not need to require
  Async unless it chooses Async as its scheduler.

## Public Contracts

```ruby
FiberStream::Flow.parallel_unordered_map(concurrency:) { |element| ... }
FiberStream::Source#parallel_unordered_map(concurrency:) { |element| ... }
FiberStream::SchedulerRequiredError
```

Initial RBS shape:

```rbs
module FiberStream
  class Flow[In, Out]
    def self.parallel_unordered_map: [In, Out] (concurrency: Integer) { (In) -> Out } -> Flow[In, Out]
  end

  class Source[Elem]
    def parallel_unordered_map: [Out] (concurrency: Integer) { (Elem) -> Out } -> Source[Out]
  end
end
```

## Error Precedence

| Situation | Result |
| --- | --- |
| Normal upstream completion and producer-side close succeeds | Normal stream completion after pulled mapped values |
| Normal upstream completion and producer-side close fails | Close failure after earlier completion-order mapped values |
| Upstream pull fails and cleanup close succeeds | Upstream pull failure when observed |
| Upstream pull fails and cleanup close also fails | Upstream pull failure; close failure suppressed |
| Mapping block fails and boundary cleanup close succeeds | Mapping failure when observed |
| Mapping block fails and boundary cleanup close also fails | Mapping failure; close failure suppressed |
| Mapping block fails before delayed producer-side close failure is delivered | Mapping failure; close failure suppressed |
| Multiple upstream or mapping failures are queued | First downstream-observed failure wins |
| Downstream completes early and boundary close succeeds | Downstream result |
| Downstream completes early and boundary close fails | Boundary close failure |
| Downstream fails and boundary close also fails | Downstream failure; close failure suppressed |

## Examples

```ruby
require "async"
require "fiber_stream"

result =
  Async do
    FiberStream::Source.each([1, 2, 3])
      .parallel_unordered_map(concurrency: 2) do |number|
        sleep 0.02 if number == 1
        number * 10
      end
      .run_with(FiberStream::Sink.to_a)
  end.wait

result.sort # => [10, 20, 30]
```

When no scheduler is installed:

```ruby
FiberStream::Source.each([1])
  .parallel_unordered_map(concurrency: 2) { |number| number * 2 }
  .run_with(FiberStream::Sink.to_a)

# raises FiberStream::SchedulerRequiredError
```

## Open Questions

None. The Ractor-backed unordered operation is specified separately as
`Flow.ractor_unordered_map`.
