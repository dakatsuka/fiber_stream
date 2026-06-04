# ADR 0008: Flow.parallel_map

## Status

Accepted

## Context

FiberStream supports foreground linear mapping, a demand-driven async boundary,
and bounded async buffering. Users now need independent per-element
transformations to overlap without adopting branching topology, an Async runtime
dependency, or unbounded buffering.

## Decision

Add `FiberStream::Flow.parallel_map(concurrency:) { ... }` and the
corresponding `Source#parallel_map(concurrency:) { ... }` convenience wrapper.

The stage is an ordered scheduler-backed boundary. It starts internal scheduled
fibers on first downstream demand, requires an installed `Fiber.scheduler` and
a non-blocking current fiber at that point, and does not install or select a
scheduler itself.

A single dispatcher fiber pulls upstream serially and assigns sequence numbers.
Up to `concurrency` worker fibers run user mapping blocks. Downstream emits
results in input order, buffering later completed results until earlier
sequence numbers are ready.

Permit-based admission bounds pulled-but-unemitted upstream elements to
`concurrency`. That bound includes queued jobs, active worker jobs, and
completed results waiting for ordered delivery. Downstream returns a permit only
after it emits a mapped value.

Failures from upstream pulls, mapping blocks, and producer-side upstream close
are delivered in input order after earlier mapped values. Failures are ordered
by input sequence rather than completion time. Once any failure is observed,
the boundary closes admission so no new upstream elements are requested, but it
keeps lower-sequence work alive until it can either emit those earlier values or
discover a lower-sequence failure. If multiple failures are observed before the
first failure is delivered, the lowest-sequence failure wins. Higher-sequence
values and failures are suppressed after the primary failure.

Internal stream failures remain primary over cleanup close failures. Early
downstream completion propagates boundary close failure when close is the
failing operation, while downstream failure suppresses close failure.

Queued or in-flight internal failures are suppressed after downstream
intentionally completes early or fails.

Ractor-backed execution is not part of this API. It requires separate contracts
for shareability, block transport, object copying or moving, IO ownership, and
cancellation.

## Consequences

- FiberStream gains a bounded ordered parallel mapping operation.
- The initial pull invariant is intentionally relaxed at an explicit parallel
  boundary: upstream may run ahead by at most `concurrency` elements.
- The implementation needs sequence-aware failure state and admission control
  rather than a simple fail-fast cancellation path.
- Users must run `parallel_map` pipelines under a Ruby fiber scheduler from a
  non-blocking fiber.
- Pure pipelines, `Flow.async`, and `Flow.buffer` keep their existing
  contracts.
- Ordered output may exhibit head-of-line blocking when an earlier mapping is
  slower than later mappings.
- Unordered and Ractor-backed variants remain future work with separate public
  contracts.

## Alternatives Rejected

- Making `Flow.map` accept `concurrency:`.
- Emitting unordered completion-order results from the first API.
- Letting workers call `upstream.next` concurrently.
- Using an unbounded result buffer.
- Using Ractors for the first parallel mapping operation.
