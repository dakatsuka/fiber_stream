# ADR 0018: Flow.ractor_unordered_map

## Status

Accepted

## Context

FiberStream supports ordered Ractor-backed `Flow.ractor_map`. That API is
deterministic and useful when downstream needs input order, but ordered
delivery can make a slow earlier CPU-bound mapping call delay later completed
results. FiberStream already exposes unordered completion-order delivery for
scheduler-backed work through `Flow.parallel_unordered_map`.

## Decision

Add a separate unordered Ractor-backed mapping API:

- `FiberStream::Flow.ractor_unordered_map(workers:, input_transfer: :copy, output_transfer: :copy) { ... }`
- `FiberStream::Source#ractor_unordered_map(workers:, input_transfer: :copy, output_transfer: :copy) { ... }`

The mapper block must be shareable according to `Ractor.shareable?`; examples
should use `Ractor.shareable_proc`. `workers` is required and must be a
positive Integer. Input and output transfer policies are explicit and must be
`:copy` or `:move`.

The API is unordered and bounded. At most `workers` input elements may be
queued, running, or completed but not yet emitted downstream. Upstream pulls
remain serial and occur in the downstream caller's execution context. Worker
ractors never call `upstream.next`.

Ractor waits are isolated in a coordinator thread so scheduler-managed
pipeline fibers do not call blocking Ractor APIs such as `Ractor.select`.
Downstream emits results in coordinator-observed worker completion order.
Input order is not preserved.

Worker and Ractor-transfer failures are normalized to
`FiberStream::RactorMapError`, reusing the ordered Ractor map public error
shape. Failures are fail-fast by downstream observation order instead of input
sequence order.

Closing the boundary stops admitting upstream elements, closes upstream, sends
one shutdown message to every worker, drops post-close worker data/error
messages instead of enqueueing them, and waits for the coordinator thread and
worker ractors to stop before returning. Cancellation is cooperative; an
in-flight CPU-bound mapper may run until the current mapping call completes.

## Consequences

- CPU-bound Ruby mapping work gains an unordered completion-order option.
- `ractor_map` remains ordered and does not gain a mode keyword.
- `parallel_unordered_map` remains the scheduler-backed unordered API.
- Users must tolerate nondeterministic output ordering.
- Users must provide Ractor-compatible mapper blocks and transferable inputs
  and outputs.
- Early close can wait for in-flight CPU-bound mapper calls to finish.
- Async reactor responsiveness depends on keeping blocking Ractor waits in the
  coordinator thread.

## Alternatives Rejected

- Adding `ordered: false` to `ractor_map`.
- Naming the operation `unordered_ractor_map`.
- Delivering failures by input sequence.
- Draining all started work before delivering a failure.
- Implementing CPU parallelism through Ruby threads.
