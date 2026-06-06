# ADR 0015: Flow.parallel_unordered_map

## Status

Accepted

## Context

FiberStream supports ordered scheduler-backed `Flow.parallel_map`. That API is
deterministic and Ruby-map-like, but ordered delivery can make a slow earlier
mapping call delay later completed results. Users need a separate operation for
throughput-oriented pipelines where cross-element order does not matter.

## Decision

Add `FiberStream::Flow.parallel_unordered_map(concurrency:) { ... }` and the
corresponding
`Source#parallel_unordered_map(concurrency:) { ... }` convenience wrapper.

The stage is an unordered scheduler-backed boundary. It starts internal
scheduled fibers on first downstream demand, requires an installed
`Fiber.scheduler` and a non-blocking current fiber at that point, and does not
install or select a scheduler itself.

A single dispatcher fiber pulls upstream serially and assigns internal sequence
numbers. Up to `concurrency` worker fibers run user mapping blocks. Downstream
emits results as they are observed from the result queue, which represents
scheduler-observed completion order. Input order is not preserved.

Permit-based admission bounds pulled-but-unemitted upstream elements to
`concurrency`. That bound includes queued jobs, active worker jobs, and
completed results waiting for downstream demand. Downstream returns a permit
only after it emits a mapped value.

Normal upstream completion and producer-side close failures after normal
completion are delayed until every admitted mapping job has produced a
downstream-observed value or failure. This prevents the unordered terminal
message from completing downstream ahead of still-running admitted work.

Failures from upstream pulls, mapping blocks, and producer-side upstream close
are fail-fast by downstream observation order. When downstream observes a
failure result, the boundary closes upstream admission, closes upstream, wakes
internal queues, requests best-effort dispatcher and worker cancellation, and
raises that failure. Values and failures not yet observed at that point are
suppressed.

Best-effort active fiber cancellation uses `Fiber.scheduler#fiber_interrupt`
when the installed scheduler supports it. If the scheduler does not support
that hook, FiberStream still closes upstream and internal queues, but active
user code may continue until it cooperatively returns or raises.

Internal stream failures remain primary over cleanup close failures. Early
downstream completion propagates boundary close failure when close is the
failing operation, while downstream failure suppresses close failure.

Queued or in-flight internal failures are suppressed after downstream
intentionally completes early or fails.

Ractor-backed unordered execution is not part of this API. It requires separate
contracts for shareability, block transport, object copying or moving, IO
ownership, and cancellation.

## Consequences

- FiberStream gains a bounded unordered parallel mapping operation.
- Users can avoid ordered `parallel_map` head-of-line blocking when input order
  does not matter.
- Output order is nondeterministic across concurrently mapped elements and must
  not be relied on.
- The implementation can reuse the scheduler-backed dispatcher, worker, permit,
  and queue structure from ordered `parallel_map`, but it uses admitted-work
  tracking instead of an ordered pending-result table.
- Users must run `parallel_unordered_map` pipelines under a Ruby fiber
  scheduler from a non-blocking fiber.
- Pure pipelines, `Flow.async`, `Flow.buffer`, and ordered `Flow.parallel_map`
  keep their existing contracts.

## Alternatives Rejected

- Adding `ordered: false` to `parallel_map`.
- Naming the operation `unordered_parallel_map`.
- Delivering failures by input sequence.
- Draining all started work before delivering a failure.
- Using Ractors for unordered CPU parallelism in this API.
