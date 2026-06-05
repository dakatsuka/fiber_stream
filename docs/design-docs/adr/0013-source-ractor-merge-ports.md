# ADR 0013: Source.ractor_merge_ports

## Status

Accepted

## Context

`Source.ractor_port` supports one backpressure-aware producer ractor.
`Source#merge` can combine sources, but it requires a scheduler and does not
provide Ractor-level producer isolation by itself. Users need to merge several
producer ractors without writing an extra merge ractor and without giving up
FiberStream's pull-based backpressure model.

## Decision

Add `Source.ractor_merge_ports(ports, ack_transfer: :copy, cancel: true)` as a
variadic Ractor port fan-in source.

The API accepts an enumerable of hashes with `:port` and `:ack_port`. It
reuses the existing `FiberStream::RactorPort` protocol envelopes and
`RactorPortSourceError`. On first downstream demand, a coordinator thread sends
one `Ack[]` to every active producer and waits on all producer data ports plus
an internal control port with `Ractor.select`.

Each active producer may have at most one outstanding ack. After downstream
emits an element from a producer, a later downstream pull replenishes only that
producer's ack. Values are emitted in coordinator-observed ready order,
per-producer order is preserved, and completion requires all producers to send
`Complete[]`.

Close marks the stream closed, closes the result mailbox, wakes the
coordinator with an internal shutdown command, waits for the coordinator
thread so observed terminal state is visible, and then sends
`Cancel[:closed]` to every non-terminal producer when `cancel: true`.
Producer terminal `Complete[]` and valid `Failure[String, String]` messages
suppress cancellation for that producer.

## Consequences

- Users get direct Ractor fan-in without building a merge producer ractor.
- The API is variadic from the start because the core use case is fan-in, not
  binary source composition.
- The source does not require `Fiber.scheduler`; blocking Ractor waits are
  isolated in the coordinator thread.
- Backpressure is bounded by one outstanding ack per active producer and one
  result-queue slot per configured producer.
- Cross-producer order is intentionally nondeterministic and has no fairness
  guarantee.
- The public `RactorPort` protocol remains unchanged.
- Error shape remains `RactorPortSourceError`, so users do not need a second
  Ractor ingress error hierarchy.
- Future APIs can add tagged output, dynamic registration, or configurable
  buffers without changing this first contract.

## Alternatives Considered

- User-written merge producer ractor: rejected because it pushes common
  backpressure, cancellation, and error-normalization logic onto users.
- `Source.ractor_port(...).merge(...)`: rejected as the primary API because it
  is binary and scheduler-backed.
- One global outstanding ack: rejected because it requires choosing producers
  by polling policy instead of ready order.
- Accept `Ractor` objects directly: rejected to keep ownership, cancellation,
  and port creation explicit.
