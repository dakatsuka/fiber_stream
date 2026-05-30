# ADR 0002: Flow.async Boundary

## Status

Accepted

## Context

FiberStream's initial runtime is a linear pull chain with no fibers between pure
stages. That keeps backpressure and cleanup simple, but upstream work runs in
the same fiber as downstream materialization. The next step toward
asynchronous, non-blocking streams is an explicit async boundary that proves
FiberStream can cooperate with Ruby's `Fiber.scheduler` without adopting a
runtime dependency on a specific scheduler implementation.

## Decision

Add `FiberStream::Flow.async` as a flow-side asynchronous boundary. The boundary
starts a non-blocking producer fiber on the first downstream pull. Upstream
stages before the boundary run in that producer fiber. Downstream stages after
the boundary continue in the caller's current fiber.

The first boundary is demand-driven. Each downstream pull resumes the producer
for at most one upstream pull, so no prefetch or queue is introduced yet.

`Flow.async` requires a scheduler only when the producer starts. If no scheduler
is installed, FiberStream raises `FiberStream::SchedulerRequiredError`.
FiberStream does not install a scheduler and does not depend on Async at
runtime. Async remains a development dependency and compatibility target.

Closing the boundary closes upstream and requests producer cancellation.
Upstream normal completion is converted to normal downstream completion, and
upstream failures are re-raised by downstream pulls. FiberStream does not
promise scheduler-agnostic interruption of arbitrary user code blocked inside
upstream operations.

## Consequences

- The initial pull invariant is preserved across the first async boundary: one
  downstream pull causes at most one upstream pull.
- Cancellation and close behavior become part of the internal runtime contract.
- Resource-owning upstream stages must make `close` wake or release their own
  resources where Ruby and the active scheduler support it.
- Users must run async-boundary pipelines under a Ruby fiber scheduler.
- Pure pipelines still require no scheduler.
- Future `Flow.buffer` or configurable async buffering still needs a handoff
  channel and stronger cancellation model.

## Alternatives Rejected

- Implicitly installing Async or another scheduler.
- Starting async producers during materialization before downstream demand.
- Using an unbounded queue.
- Combining the first async boundary with configurable buffering.
- Adding a handoff queue to `Flow.async` before the cancellation contract is
  proven.
