# ADR 0003: Flow.buffer Boundary

## Status

Accepted

## Context

`Flow.async` introduced a scheduler-aware producer fiber without prefetch. It
preserves the initial pull invariant, but it does not let upstream run ahead of
downstream. Before adding IO sources, FiberStream needs a bounded queue
contract that can absorb short downstream stalls without losing the core
backpressure guarantee.

## Decision

Add `FiberStream::Flow.buffer(count)` as a flow-side bounded asynchronous
buffer. The boundary starts a scheduled producer fiber on the first downstream
pull. Upstream stages before the boundary run in that producer fiber.
Downstream stages after the boundary continue in the caller's current fiber.

The buffer queues at most `count` messages. Value, completion, and error
messages all count toward the same queued bound. The producer may hold one
additional in-flight message while waiting to enqueue into a full buffer, and
does not pull farther upstream until that message is queued.

`Flow.buffer(count)` requires a scheduler only when the producer starts. If no
scheduler is installed, FiberStream raises `FiberStream::SchedulerRequiredError`.
FiberStream does not install a scheduler and does not depend on Async at
runtime.

Closing the boundary closes upstream, closes the queue, and requests producer
cancellation. FiberStream does not promise scheduler-agnostic interruption of
arbitrary user code blocked inside upstream operations.

## Consequences

- FiberStream gains a bounded prefetch operation with explicit memory limits.
- The initial pull invariant is intentionally relaxed at explicit buffer
  boundaries: upstream may run ahead by at most `count` queued messages plus
  one in-flight producer message.
- Cancellation and close behavior for queued producer stages become part of the
  internal runtime contract.
- Users must run buffered pipelines under a Ruby fiber scheduler.
- Pure pipelines and demand-driven `Flow.async` pipelines keep their existing
  contracts.
- IO sources can later reuse the bounded queue and close/cancellation model.

## Alternatives Rejected

- Adding an optional buffer size to `Flow.async`.
- Using an unbounded queue.
- Adding lossy drop policies in the first buffer API.
- Depending on Async queue primitives at runtime.
