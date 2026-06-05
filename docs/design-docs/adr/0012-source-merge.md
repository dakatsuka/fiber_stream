# ADR 0012: Source.merge

## Status

Accepted

## Context

FiberStream supports sequential source composition with `Source#concat` and
pairwise composition with `Source#zip`. Users also need ready-order source
composition where either input can provide the next downstream value.

Ready-order merge cannot be implemented as a simple synchronous left/right pull
without allowing one input to delay the other. It therefore needs the same
scheduler-aware boundary model used by `Flow.async`, `Flow.buffer`, and
`Flow.parallel_map`.

## Decision

Add `Source#merge(source)` as a binary, scheduler-backed source combinator.

The merge starts only when downstream demand first reaches it. It then starts
one producer fiber per input source. Producers publish tagged value,
completion, and error messages into a private scheduler-aware bounded mailbox
with capacity one. The downstream side emits values in mailbox arrival order,
preserving order within
each input but making no deterministic cross-input ordering or fairness
promise.

The merged source completes only after both inputs complete normally and their
producer-side closes succeed. Input failures and producer-side close failures
are delivered as stream failures. Early downstream completion or failure closes
all materialized inputs and requests producer cancellation.

## Consequences

- `Source#merge` gives users a true ready-order merge instead of deterministic
  round-robin interleaving.
- `Source#merge` requires an installed `Fiber.scheduler` and a non-blocking
  current fiber when demanded, even when both inputs are simple in-memory
  sources.
- `Source#merge` does not make scheduler-unaware blocking work non-blocking
  and does not provide CPU parallelism; blocking or CPU-bound producer work
  should be isolated by the application, for example with producer ractors and
  `Source.ractor_port`.
- The first API remains narrow: binary only, no public buffer-size option, no
  fairness guarantee, and no tagging of emitted values.
- Backpressure stays bounded by one mailbox message plus at most one in-flight
  message per producer blocked on enqueue.
- The implementation needs a scheduler-aware mailbox that wakes blocked
  producers and the downstream consumer on close without surfacing cancellation
  wakeups as stream failures.
- Future APIs can add variadic merge, round-robin interleave, priority merge,
  or configurable buffering without changing this contract.

## Alternatives Considered

- Synchronous round-robin merge: rejected because it is deterministic
  interleaving rather than ready-order merge.
- Unbounded queue: rejected because it weakens backpressure.
- Variadic merge first: rejected to keep failure, cancellation, and RBS type
  contracts small.
- Public `buffer:` option first: rejected to avoid adding overflow and memory
  policy questions to the initial merge API.
