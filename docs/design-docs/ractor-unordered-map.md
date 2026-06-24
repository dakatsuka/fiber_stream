# Ractor Unordered Map

## Status

Accepted

## Context

`Flow.ractor_map` provides bounded CPU-parallel mapping with ordered output.
That deterministic contract can create head-of-line blocking when an earlier
input is slow. `Flow.parallel_unordered_map` already proves that unordered
completion-order output is useful for scheduler-backed work. FiberStream needs
the same user-facing shape for Ractor-backed CPU-bound work.

Governing documents:

- Product spec: `docs/product-specs/flow-ractor-unordered-map.md`
- Ordered Ractor design: `docs/design-docs/ractor-map.md`
- Unordered scheduler-backed design:
  `docs/design-docs/parallel-unordered-map.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- References: `docs/references/ruby-ractor.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Goals

- Add an unordered Ractor-backed mapping boundary for CPU-bound per-element
  work.
- Preserve FiberStream's lazy construction and linear pull API.
- Keep upstream pulls serial and bounded.
- Emit mapped values in coordinator-observed worker completion order.
- Make no input-order guarantee.
- Keep Ractor block and object transfer constraints explicit.
- Reuse the `RactorMapError` public error shape.

## Non-Goals

- Ordered result emission.
- General graph scheduling.
- Thread-pool or process-pool execution.
- Runtime scheduler installation.
- Immediate cancellation of arbitrary CPU-bound code running in a ractor.
- Per-element timeout, retry, batching, or overflow policies.

## Proposed Design

`Flow.ractor_unordered_map(workers:, input_transfer:, output_transfer:) { ... }`
attaches a `RactorUnorderedMapBoundary` pull stage:

```ruby
downstream_stream =
  Pull.ractor_unordered_map(upstream_stream, workers, input_transfer, output_transfer, mapper)
```

`Source#ractor_unordered_map(...)` is a convenience method over
`Source#via(Flow.ractor_unordered_map(...))`.

The stage uses the same private worker protocol as `RactorMapBoundary`:

```ruby
Job = Data.define(:sequence, :value)
Shutdown = Data.define
Ready = Data.define(:worker_id)
WorkerValue = Data.define(:worker_id, :sequence, :value)
WorkerFailure = Data.define(:worker_id, :sequence, :kind, :cause_class_name, :cause_message)
Stopped = Data.define(:worker_id)
```

The boundary owns:

- `upstream`, the pull stream before the boundary
- `workers`, the positive worker count
- `mapper`, the user-supplied shareable proc
- `input_transfer` and `output_transfer`
- worker ractors
- a coordinator thread for blocking Ractor waits
- ready-worker and result queues
- active worker-to-sequence tracking
- `next_sequence`, `outstanding_jobs`, and delayed terminal state
- admission, close, completion, failure, and worker shutdown state

The mapper must be shareable according to `Ractor.shareable?`. Public examples
should use `Ractor.shareable_proc`.

## Ordering And Backpressure

The boundary keeps upstream pulling in the downstream caller's execution
context. It pulls a new upstream element only when a worker is ready and the
number of admitted-but-unemitted jobs is below `workers`.

This bound includes:

- jobs being sent to workers
- jobs currently running in workers
- completed results waiting for downstream demand

Each pulled element receives an internal sequence number for diagnostics and
error metadata. Sequence numbers do not determine output order. Downstream
emits `WorkerValue` messages as they are forwarded by the coordinator and
observed from the result queue. That order is coordinator-observed worker
completion order.

Downstream result delivery is result-first. Each downstream pull checks for a
ready result before admitting more upstream work. If a worker result and
additional admission capacity are both available, the completed result is
emitted before the boundary calls `upstream.next` again. If no result is ready,
the boundary may admit work while capacity and ready workers are available.

Queue capacities are bounded:

- `ready_workers` has capacity `workers`.
- `results` has capacity `workers`.
- delayed terminal completion or terminal close failure is boundary state, not
  a result-queue message.
- worker lifecycle/control messages are tracked separately from data-result
  capacity.

After close begins, downstream may stop consuming `results`. To prevent
coordinator shutdown from blocking on a full bounded result queue, the
coordinator must treat close state as a suppression boundary: worker
`WorkerValue` and `WorkerFailure` messages observed after close are dropped
instead of enqueued. A close must also wake a coordinator blocked while
forwarding to a full result queue.

Normal upstream completion is delayed until all admitted jobs have produced
downstream-observed values. Producer-side close failures after normal upstream
completion are delayed the same way, unless an admitted worker failure is
observed first.

## Error Handling

Unordered failures are fail-fast by downstream observation order:

- upstream pull failures are re-raised directly
- input transfer failures are normalized to `RactorMapError`
- worker mapping failures are normalized to `RactorMapError`
- output transfer failures are normalized to `RactorMapError`
- worker termination failures are normalized to `RactorMapError`

The boundary tracks the active input sequence assigned to each worker in the
main ractor. If the coordinator observes a worker ractor terminate without a
corresponding result or lifecycle message, it uses the active sequence when
available and reports a normalized `:worker_termination` failure. If the
worker was idle, the failure uses the current terminal sequence.

When downstream observes any error result, the boundary marks itself done,
closes admission, closes upstream, requests worker shutdown, suppresses queued
or in-flight unobserved results, and raises that error. Already emitted values
remain emitted.

This differs intentionally from ordered `ractor_map`, which waits for lower
sequence numbers before raising later failures. Waiting by sequence would
reintroduce the head-of-line blocking that this operation avoids.

## Scheduler Interaction

Like `ractor_map`, the boundary must not call blocking Ractor wait APIs such as
`Ractor.select` from a scheduler-managed pipeline fiber. A coordinator thread
blocks on Ractor APIs and forwards worker messages to bounded Ruby queues. The
pipeline fiber waits on those queues instead of directly waiting on Ractors.

`ractor_unordered_map` does not require `Fiber.scheduler`. In ordinary
foreground execution it may block the current thread while waiting for worker
results. In scheduler-backed execution it must preserve sibling reactor
responsiveness in the same way as `ractor_map`.

Cleanup waits follow the same reactor-safety rule as normal result waits:
blocking Ractor waits happen in the coordinator thread, and scheduler-managed
pipeline fibers wait through bounded Ruby queues or thread joins in a way that
does not block sibling Async tasks. This behavior needs an explicit Async
cleanup responsiveness test.

## Cancellation And Cleanup

Closing the boundary should:

- stop admitting new upstream elements
- close upstream
- stop sending new jobs
- send exactly one shutdown message to every worker
- stop forwarding queued worker data/error messages after intentional
  downstream completion or downstream failure
- wait for the coordinator thread to exit
- wait for worker ractors to stop

Cancellation is cooperative. A worker currently running a mapper may finish
that call before observing shutdown. Early downstream completion can therefore
wait for in-flight CPU-bound mapper calls to finish.

## Contracts

- `Flow.ractor_unordered_map` returns `Flow[In, Out]`.
- `Source#ractor_unordered_map` delegates to `Flow.ractor_unordered_map`.
- Construction is lazy.
- `workers` is a required positive `Integer`.
- `input_transfer` and `output_transfer` are `:copy` or `:move`.
- The mapper block is required and must be shareable.
- Worker ractors start on first downstream demand.
- Ractor waits are isolated from scheduler-managed pipeline fibers by a
  coordinator thread or equivalent design.
- `ractor_unordered_map` does not require `Fiber.scheduler`.
- Upstream is pulled serially in the downstream caller's execution context.
- Pulled-but-unemitted work is bounded by `workers`.
- Output is coordinator-observed completion order.
- Output is not guaranteed to match input order.
- Failures are delivered by downstream observation order.
- Worker and Ractor-transfer failures are normalized to `RactorMapError`.
- Early downstream completion closes upstream and requests worker shutdown.
- Boundary close waits for coordinator and worker shutdown before returning.

## Alternatives Considered

### Add `ordered: false` To `Flow.ractor_map`

A keyword would reduce method count, but it would make one API switch between
deterministic input order and nondeterministic completion order. A separate
method keeps ordering visible at the call site and leaves the existing
`ractor_map` contract unchanged.

### Name The Operation `unordered_ractor_map`

Both names are explicit. `ractor_unordered_map` groups near `ractor_map` and
reads as the Ractor mapping variant whose delivery is unordered, so it is the
preferred public name.

### Sequence-Ordered Error Delivery

Ordered error delivery would delay a completed later failure behind a slow
earlier job. Unordered mapping should fail fast when downstream observes a
failure result.

## Third-Party Review

Pending.

## Validation

- Unit tests for laziness and validation errors.
- Unit tests proving completion-order output and lack of input-order guarantee.
- Unit tests proving bounded pulled-but-unemitted work.
- Unit tests proving upstream and worker error propagation in observation
  order.
- Unit tests proving active and idle unexpected worker termination are reported
  with a meaningful `RactorMapError#sequence`.
- Unit tests proving ready results are emitted before additional upstream
  admission when both are available.
- Unit tests proving early completion cleanup and upstream close propagation.
- Unit tests proving close wakes a coordinator blocked on a full result queue.
- Unit tests proving transfer policy behavior and `RactorMapError`
  normalization.
- Async responsiveness tests matching ordered `ractor_map`.
- RBS validation.
- RuboCop.

## Open Questions

None. A context-free design review on 2026-06-24 found missing contracts for
worker termination sequencing, scheduler-safe cleanup waits, result/admission
priority, and queue capacity or close wakeups. The design now records those
contracts and validation requirements.
