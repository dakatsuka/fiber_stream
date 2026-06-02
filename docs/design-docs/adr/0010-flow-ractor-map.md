# ADR 0010: Flow.ractor_map

## Status

Accepted

## Context

FiberStream has ordered scheduler-backed `Flow.parallel_map`, but that API does
not provide CPU parallelism for ordinary Ruby code. Ruby ractors can execute
Ruby code in parallel across CPU cores, but they require explicit contracts for
shareable mapper blocks, object transfer, worker lifecycle, and cross-ractor
failure reporting.

## Decision

Add a separate ordered Ractor-backed mapping API:

- `FiberStream::Flow.ractor_map(workers:, input_transfer: :copy, output_transfer: :copy) { ... }`
- `FiberStream::Source#ractor_map(workers:, input_transfer: :copy, output_transfer: :copy) { ... }`

The mapper block must be shareable according to `Ractor.shareable?`; examples
should use `Ractor.shareable_proc`. `workers` is required and must be a positive
Integer. Input and output transfer policies are explicit and must be `:copy` or
`:move`.

The first API is ordered and bounded. At most `workers` input elements may be
queued, running, or completed but not yet emitted downstream. Upstream pulls
remain serial and occur in the downstream caller's execution context. Worker
ractors never call `upstream.next`.

Ractor waits are isolated in a coordinator thread so scheduler-managed pipeline
fibers do not call blocking Ractor APIs such as `Ractor#value` or
`Ractor.select`. The coordinator forwards data results into bounded
`Thread::SizedQueue` instances and processes worker lifecycle messages
separately.

Worker and Ractor-transfer failures are normalized to
`FiberStream::RactorMapError`, which exposes sequence, kind, original class
name, and original message metadata. Upstream pull failures remain ordinary
upstream stream failures.

Closing the boundary stops admitting upstream elements, closes upstream, sends
one shutdown message to every worker, drops post-close worker data/error
messages instead of enqueueing them, and waits for the coordinator thread and
worker ractors to stop before returning. Cancellation is cooperative; an
in-flight CPU-bound mapper may run until the current mapping call completes.

## Consequences

- CPU-bound Ruby mapping work has an explicit Ractor-backed API.
- `parallel_map` remains the scheduler-backed concurrency API and does not gain
  Ractor options.
- Users must provide Ractor-compatible mapper blocks and transferable inputs and
  outputs.
- Early close can wait for in-flight CPU-bound mapper calls to finish.
- Async reactor responsiveness depends on keeping all blocking Ractor waits in
  the coordinator thread.
- Worker failures have a stable FiberStream error shape instead of exposing
  Ruby's Ractor transport details directly.

## Alternatives Rejected

- Adding `ractor: true` to `parallel_map`.
- Using a Ruby thread pool for CPU-bound mapping.
- Emitting unordered Ractor results from the first API.
- Requiring all inputs and outputs to already be shareable.
- Detaching workers on close instead of waiting for cooperative shutdown.
