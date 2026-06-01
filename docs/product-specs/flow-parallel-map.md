# Flow.parallel_map

## Status

Accepted

## Problem

FiberStream can already move work across scheduler-backed boundaries with
`Flow.async` and can add bounded prefetch with `Flow.buffer(count)`. Users also
need to run independent per-element transformations concurrently while keeping
FiberStream's backpressure, ordering, cleanup, and dependency-free scheduler
contracts explicit.

## Goals

- Add `FiberStream::Flow.parallel_map(concurrency:) { ... }`.
- Add `Source#parallel_map(concurrency:) { ... }` as a convenience wrapper.
- Preserve lazy construction.
- Require an installed `Fiber.scheduler` only when the parallel stage is
  demanded.
- Keep FiberStream independent of the `async` gem at runtime.
- Run at most `concurrency` user mapping blocks at a time.
- Preserve input order in emitted output values.
- Bound upstream run-ahead to at most `concurrency` pulled elements that have
  not yet been emitted downstream.
- Close upstream and request worker cancellation when downstream completes
  early or fails.
- Propagate upstream and mapping failures deterministically.

## Non-Goals

- Unordered result emission.
- Ractor-backed execution or process-backed execution.
- CPU parallelism guarantees.
- General fan-out, fan-in, or graph scheduling.
- Runtime scheduler installation.
- Depending on Async at runtime.
- Per-element timeout, retry, batching, or overflow policies.
- Background `run_async` materialization.

## Requirements

- `FiberStream::Flow.parallel_map(concurrency:) { |element| ... }` creates an
  ordered scheduler-backed parallel mapping flow.
- `Source#parallel_map(concurrency:) { |element| ... }` is a convenience
  equivalent to
  `Source#via(FiberStream::Flow.parallel_map(concurrency:) { ... })`.
- Constructing `Flow.parallel_map` does not require `Fiber.scheduler`, pull
  upstream, schedule fibers, or call the user block.
- `parallel_map` requires a block.
- `concurrency` must be an `Integer`.
- `concurrency` must be positive.
- Missing blocks raise `ArgumentError`.
- Non-Integer `concurrency` values raise `TypeError`.
- Zero or negative `concurrency` values raise `ArgumentError`.
- Internal scheduler-backed execution starts on the first downstream pull, not
  at pipeline construction or materialization.
- If no `Fiber.scheduler` is installed when the stage must start,
  `Flow.parallel_map` raises `FiberStream::SchedulerRequiredError`.
- FiberStream does not install or select a scheduler.
- Upstream stages before `parallel_map` are pulled by an internal scheduled
  dispatcher.
- The user mapping block runs in scheduled worker fibers.
- Downstream stages after `parallel_map` run in the caller's current fiber.
- At most `concurrency` mapping blocks are executing at the same time.
- At most `concurrency` upstream elements may be pulled and not yet emitted
  downstream. This bound includes queued work, running work, and completed
  results waiting for ordered delivery.
- Downstream pulls receive mapped values in the same order as upstream input
  values, even when later mappings finish before earlier mappings.
- Normal upstream completion is delivered to downstream as normal stream
  completion after all earlier mapped values have been emitted.
- Failures raised by upstream source enumeration or upstream flow blocks are
  re-raised from `Source#run_with` after all earlier ordered mapped values have
  been emitted.
- Failures raised by the mapping block are re-raised from `Source#run_with`
  after all earlier ordered mapped values have been emitted.
- Upstream and mapping failures are ordered by input sequence, not by wall-clock
  completion time.
- When any upstream or mapping failure is observed, FiberStream stops admitting
  new upstream elements and records the failure sequence.
- If multiple failures are observed before the first failure is delivered,
  FiberStream delivers the lowest-sequence failure as the primary failure.
- Successful mapped values with lower sequence numbers than the primary failure
  are emitted before the primary failure is raised.
- Values and failures with higher sequence numbers than the primary failure are
  suppressed.
- Observing a failure must not cancel lower-sequence mapping work that is still
  needed for ordered delivery.
- Once a failure has been observed, emitting earlier successful values does not
  reopen upstream admission or permit additional upstream pulls.
- If upstream completes normally and producer-side `upstream.close` fails, that
  close failure is delivered downstream as a stream failure after all earlier
  mapped values have been emitted.
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
- Closing the parallel map boundary closes upstream.
- Closing the boundary wakes internal queues and requests cancellation of active
  dispatcher and worker fibers.
- Early downstream completion closes upstream before `Source#run_with` returns.
- FiberStream does not guarantee that a scheduler can immediately interrupt an
  arbitrary blocking operation inside user mapping or upstream code.
- Repeated downstream pulls after completion return completion without pulling
  upstream again.
- Async compatibility tests use the development dependency as a scheduler
  target, but application code using FiberStream should not need to require
  Async unless it chooses Async as its scheduler.

## Public Contracts

```ruby
FiberStream::Flow.parallel_map(concurrency:) { |element| ... }
FiberStream::Source#parallel_map(concurrency:) { |element| ... }
FiberStream::SchedulerRequiredError
```

Initial RBS shape:

```rbs
module FiberStream
  class Flow[In, Out]
    def self.parallel_map: [In, Out] (concurrency: Integer) { (In) -> Out } -> Flow[In, Out]
  end

  class Source[Elem]
    def parallel_map: [Out] (concurrency: Integer) { (Elem) -> Out } -> Source[Out]
  end
end
```

## Error Precedence

| Situation | Result |
| --- | --- |
| Normal upstream completion and producer-side close succeeds | Normal stream completion after earlier mapped values |
| Normal upstream completion and producer-side close fails | Close failure after earlier mapped values |
| Upstream pull fails and cleanup close succeeds | Upstream pull failure after earlier mapped values |
| Upstream pull fails and cleanup close also fails | Upstream pull failure; close failure suppressed |
| Mapping block fails and boundary cleanup close succeeds | Mapping failure after earlier mapped values |
| Mapping block fails and boundary cleanup close also fails | Mapping failure; close failure suppressed |
| Multiple upstream or mapping failures are observed | Lowest-sequence failure wins |
| Downstream completes early and boundary close succeeds | Downstream result |
| Downstream completes early and boundary close fails | Boundary close failure |
| Downstream fails and boundary close also fails | Downstream failure; close failure suppressed |

## Examples

```ruby
require "async"
require "fiber_stream"

result =
  Async do
    FiberStream::Source.each([1, 2, 3, 4])
      .parallel_map(concurrency: 2) { |number| number * 2 }
      .run_with(FiberStream::Sink.to_a)
  end.wait

result # => [2, 4, 6, 8]
```

When no scheduler is installed:

```ruby
FiberStream::Source.each([1])
  .parallel_map(concurrency: 2) { |number| number * 2 }
  .run_with(FiberStream::Sink.to_a)

# raises FiberStream::SchedulerRequiredError
```

## Follow-Up Specs

- A future unordered operation can expose completion-order emission without
  changing this ordered `parallel_map` contract.
- A future Ractor-backed operation should get a separate spec because Ractor
  shareability, block transport, object copying, and cancellation constraints
  differ from scheduler-backed fiber concurrency.
