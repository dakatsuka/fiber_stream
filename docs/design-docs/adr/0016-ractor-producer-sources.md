# ADR 0016: Ractor Producer Sources

## Status

Accepted

## Context

`Source.ractor_port` and `Source.ractor_merge_ports` expose backpressure-aware
Ractor ingress through explicit port pairs. That keeps ownership and protocol
visible, but users who want FiberStream to own producer ractors must repeat
the same setup-port and acknowledgment-port ceremony for every producer.

Ruby requires acknowledgment ports to be created inside the producer ractor
because `Ractor::Port#receive` must be called by the port's creator ractor.
FiberStream cannot change that runtime rule, but it can provide high-level
owned-producer APIs that hide the ceremony.

## Decision

Add high-level Ractor producer source APIs in a future implementation:

```ruby
FiberStream::Source.ractor_producer(
  *args,
  transfer: :copy,
  ack_transfer: :copy
) { |producer, *args| ... }

FiberStream::Source.ractor_merge_producers(
  transfer: :copy,
  ack_transfer: :copy
) do |group|
  group.producer(*args, transfer: nil) { |producer, *args| ... }
end
```

The existing `Source.ractor_port` and `Source.ractor_merge_ports` APIs remain
unchanged as low-level port-oriented APIs.

The high-level APIs create producer ractors lazily on first downstream demand.
Each producer receives a `RactorProducer` context that exposes `emit`,
`complete`, `fail`, and `cancelled?`. `emit`, `complete`, and `fail` wait for
one `Ack[]` before sending one protocol message to the data port. Normal block
return sends `Complete[]`; producer exceptions send
`Failure[class_name, message]`.

`Source.ractor_merge_producers` uses a builder because merge producers are
often heterogeneous. The builder registers producer definitions at
construction, while producer ractors and ports are still created lazily.
Merged output preserves the existing `Source.ractor_merge_ports` semantics:
ready-order fan-in, per-producer order, no automatic tagging, and completion
only after all producers complete.

Producer blocks must be shareable. Producer-to-source transfer defaults to
`:copy` and can be overridden globally or per producer, with per-element
override through `producer.emit(value, transfer:)`. `ack_transfer` keeps the
same meaning as in the low-level APIs.

The high-level APIs own their producer ractors and always request cooperative
cancellation during cleanup. They do not expose `cancel: false` or producer
`Ractor` handles. Closing a started high-level source waits for started
producer ractors to terminate after cancellation, without killing them.
Close during setup continues receiving late setup acknowledgment ports
internally so it can send `Cancel[:closed]` to producers that started but were
not fully materialized.

Failures while starting producer ractors, transferring producer arguments, or
receiving setup acknowledgment ports are normalized to
`RactorPortSourceError` kind `:producer_setup`.

The implementation must monitor owned producer ractors. Unexpected producer
termination before a terminal message becomes a producer failure instead of an
indefinite port wait. Producer-context send or transfer failures after an ack
has been consumed use that same ack permission to report a copy-safe failure
message.

## Consequences

- Common Ractor source usage no longer requires hand-written setup ports,
  acknowledgment ports, or protocol envelopes.
- Low-level interop remains available for externally owned producers and
  unusual lifecycle policies.
- Users still need to understand Ractor shareability and transfer policy.
- Cancellation remains cooperative; FiberStream does not kill producer
  ractors, so close can wait for long-running producer code to reach the
  cooperative protocol.
- High-level waits need the same scheduler-safety care as existing Ractor
  receive waits.
- `RactorPortSourceError` gains a high-level setup failure kind for these new
  APIs.

## Alternatives Rejected

- Overload `Source.ractor_port`: rejected because it would mix port ownership
  and producer ownership in one API.
- Name the API `Source.ractor`: rejected because it implies accepting existing
  ractors rather than creating producer ractors.
- Start with a homogeneous merge enumerable API: rejected because the builder
  form is clearer for Ruby users with different producer arguments and blocks.
- Return producer `Ractor` handles: rejected for the first API because it
  complicates ownership and cleanup.
- Add `cancel: false`: rejected because hidden owned producers could be left
  blocked waiting for acknowledgments.
