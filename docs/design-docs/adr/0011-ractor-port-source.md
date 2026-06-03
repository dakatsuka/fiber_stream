# ADR 0011: Ractor Port Source

## Status

Draft

## Context

FiberStream needs a way to ingest values produced by ractors. The existing
`Source.each` API is intentionally enumerable-specific and should not inherit
actor-style message contracts. Akka Streams provides a useful comparison point:
one-way actor ingress and backpressure-aware actor ingress are separate
operators, and the one-way form requires explicit buffering and overflow
behavior.

FiberStream's core model is pull-based backpressure, so the first Ractor source
should not be a one-way push boundary.

## Decision

Add a dedicated Ractor port source API in a future implementation:

```ruby
FiberStream::Source.ractor_port(
  port,
  ack_port:,
  ack_transfer: :copy,
  cancel: true
)
```

The API uses two ports: one for producer-to-source data/control messages and
one for source-to-producer acknowledgments and cancellation. The public
protocol uses typed `Data` envelopes:

- `FiberStream::RactorPort::Element[value]`
- `FiberStream::RactorPort::Complete[]`
- `FiberStream::RactorPort::Failure[cause_class_name, cause_message]`
- `FiberStream::RactorPort::Ack[]`
- `FiberStream::RactorPort::Cancel[reason]`

The source sends one `Ack[]` on first downstream demand and then sends the next
`Ack[]` only when downstream demands another element. This makes producer
progress explicit and bounded for producers that obey the protocol.

`Failure[...]` messages use cause metadata instead of exception objects so
producer failure reporting remains robust across Ractor transfer boundaries.
Producer failures, invalid protocol messages, and source-side Ractor port
failures are normalized to `FiberStream::RactorPortSourceError` with
structured kind and cause metadata.

The implementation must isolate blocking waits in a coordinator thread and
wake that coordinator through an internal shutdown port on close. Closing after
producer terminal messages does not send cancellation; early downstream
completion, downstream failure, and explicit close do.

`Source.each` remains unchanged. One-way buffered Ractor ingress is out of
scope for the first API and should be added only as a separate API with buffer
and overflow contracts.

## Consequences

- Ractor ingress has an explicit public contract instead of being hidden inside
  enumerable handling.
- The first Ractor source preserves FiberStream's backpressure model.
- Users must wrap stream values in `RactorPort::Element`.
- Producers report failures through metadata rather than sending exception
  objects directly.
- Symbol sentinels are avoided, so ordinary stream values cannot collide with
  control messages.
- The implementation must isolate blocking Ractor waits from
  scheduler-managed fibers.
- Producers are cancelled cooperatively through a protocol message; FiberStream
  does not kill producer ractors.

## Alternatives Rejected

- Treating `Ractor` or `Ractor::Port` as an enumerable passed to `Source.each`.
- Using symbols such as `:ready` and `:done` as public protocol messages.
- Starting with a one-way buffered Ractor source.
- Blocking directly in `Pull#next` on Ractor receive APIs.
