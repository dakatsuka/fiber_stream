# Source.ractor_merge_ports

## Status

Accepted

## Problem

Users can connect one producer ractor to FiberStream with
`Source.ractor_port`, and they can merge scheduler-cooperative sources with
`Source#merge`. CPU-bound or scheduler-unaware blocking producers still need a
direct Ractor fan-in source that does not require users to build an additional
merge producer ractor by hand.

## Goals

- Add a lazy public API for merging multiple Ractor producer ports.
- Preserve the `Source.ractor_port` typed message protocol.
- Preserve backpressure with at most one outstanding ack per active producer.
- Emit values in Ractor-message ready order.
- Preserve per-producer element order.
- Complete only after every producer completes normally.
- Normalize protocol, receive, acknowledgment, and cancellation failures with
  `RactorPortSourceError`.
- Avoid blocking scheduler-managed fibers on Ractor receive APIs.
- Provide RBS signatures for the public API.

## Non-Goals

- Accepting `Ractor` objects directly.
- Dynamic producer registration.
- One-way push without acknowledgments.
- Unbounded buffering.
- Configurable buffer sizes or overflow policies.
- Deterministic round-robin ordering or fairness guarantees.
- Tagging emitted values with producer identity.
- Killing producer ractors on close.
- Replacing `Source.ractor_port` for single-producer use cases.

## Requirements

- `FiberStream::Source.ractor_merge_ports(ports, ack_transfer: :copy,
  cancel: true)` creates a source definition from multiple Ractor port pairs.
- `ports` must be an enumerable of hashes with keys `:port` and `:ack_port`.
- The `ports` enumerable is consumed at construction time to assign stable
  side indexes. It must be finite.
- Each `:port` is a producer data/control port received by FiberStream.
- Each `:ack_port` is a producer-owned port that receives
  `RactorPort::Ack` and `RactorPort::Cancel` control messages.
- At least two port pairs are required.
- Each `:port` must respond to `receive`.
- Each `:ack_port` must provide Ractor-style `send`.
- Data ports must be distinct by object identity.
- Ack ports must be distinct by object identity.
- `ack_transfer` must be `:copy` or `:move` and applies to all ack/cancel
  messages sent by this source.
- `cancel` must be `true` or `false` and applies to every non-terminal
  producer.
- Construction validates argument shape but does not receive from or send to
  any port.
- Construction does not start threads or ractors.
- The public producer-to-source messages are the existing
  `FiberStream::RactorPort::Element[value]`,
  `FiberStream::RactorPort::Complete[]`, and
  `FiberStream::RactorPort::Failure[cause_class_name, cause_message]`
  envelopes.
- The public source-to-producer messages are the existing
  `FiberStream::RactorPort::Ack[]` and
  `FiberStream::RactorPort::Cancel[:closed]` envelopes.
- The first downstream demand starts the merge coordinator and sends one
  initial `Ack[]` to every active producer.
- After emitting an element from producer `i`, the source sends the next
  `Ack[]` to producer `i` only when a later downstream pull asks for another
  element.
- The source never sends more than one outstanding `Ack[]` to the same
  producer.
- Every `Ack[]` and `Cancel[:closed]` send uses a fresh envelope instance.
  This is required so `ack_transfer: :move` never attempts to reuse a moved
  object across producers or sends.
- Each producer that obeys the protocol may have at most one unconsumed
  message in flight.
- A non-cooperative producer can still enqueue out-of-credit messages; that is
  a producer protocol violation outside FiberStream's enforceable
  backpressure boundary.
- Values from any producer may be emitted first.
- Ready order means the order in which the coordinator observes messages from
  the active Ractor data ports.
- The source preserves element order within each producer.
- The source does not guarantee deterministic ordering or fairness between
  producers.
- Each emitted downstream value is the original `Element` payload. The source
  does not wrap, tag, copy, freeze, or transform values.
- `Complete[]` marks that producer complete and suppresses cancellation for
  that producer during final cleanup.
- `Failure[String, String]` marks that producer terminal and fails the stream
  with `RactorPortSourceError` kind `:producer_failure`.
- `Failure[String, String]` contains producer-provided metadata. FiberStream
  exposes these strings through `RactorPortSourceError#cause_class_name`,
  `#cause_message`, and the error message.
- Producers should sanitize or redact values that cross trust boundaries,
  including internal paths, stack details, secrets, tenant data, or other
  sensitive content.
- Malformed `Failure` payloads and invalid protocol messages fail the stream
  with `RactorPortSourceError` kind `:invalid_message`.
- Receive failures are normalized to `RactorPortSourceError` kind `:receive`.
- Transfer failures while sending `Ack[]` are stream failures with kind
  `:ack_transfer`.
- Transfer failures while sending `Cancel[:closed]` are close failures with
  kind `:cancel_transfer`.
- When one producer completes normally, the source continues emitting values
  from other active producers.
- The source completes only after every producer has completed normally.
- On the first observed stream failure, the source raises that failure and
  final cleanup cancels every non-terminal producer when `cancel: true`.
- If downstream intentionally completes early or fails, final cleanup cancels
  every non-terminal producer when `cancel: true`.
- During downstream failure cleanup, cancellation transfer failures are
  suppressed in favor of the downstream failure.
- During normal early downstream completion, the first cancellation transfer
  failure is propagated from `Source#run_with`.
- When `cancel: false`, cleanup sends no cancellation messages.
- Closing the materialized source is idempotent.
- Closing an unstarted materialized source does not start the coordinator and
  sends no acknowledgments or cancellations.
- Blocking Ractor waits are isolated from scheduler-managed fibers.
- Waiting for coordinator results from downstream `next` must not park the
  entire scheduler thread. The internal result mailbox must wake on close and
  must be proven by scheduler responsiveness tests.
- Demanding the source does not require a `Fiber.scheduler`.
- Public APIs never expose internal merge messages or the private `Pull::DONE`
  sentinel.

## Public Contracts

```ruby
FiberStream::Source.ractor_merge_ports(
  ports,
  ack_transfer: :copy,
  cancel: true
)
```

RBS shape:

```rbs
module FiberStream
  type ractor_port_pair = { port: untyped, ack_port: untyped }

  class Source[Elem]
    def self.ractor_merge_ports: [Elem] (
      Enumerable[ractor_port_pair] ports,
      ?ack_transfer: ractor_transfer_policy,
      ?cancel: bool
    ) -> Source[Elem]
  end
end
```

## Examples

```ruby
require "fiber_stream"

data_a = Ractor::Port.new
data_b = Ractor::Port.new
setup = Ractor::Port.new

producer_a =
  Ractor.new(data_a, setup, [1, 3]) do |outbox, setup_port, values|
    inbox = Ractor::Port.new
    setup_port.send(inbox)
    enum = values.to_enum

    loop do
      case inbox.receive
      in FiberStream::RactorPort::Ack
        begin
          outbox.send(FiberStream::RactorPort::Element.new(enum.next))
        rescue StopIteration
          outbox.send(FiberStream::RactorPort::Complete.new)
          break
        end
      in FiberStream::RactorPort::Cancel
        break
      end
    end
  end

producer_b =
  Ractor.new(data_b, setup, [2, 4]) do |outbox, setup_port, values|
    inbox = Ractor::Port.new
    setup_port.send(inbox)
    enum = values.to_enum

    loop do
      case inbox.receive
      in FiberStream::RactorPort::Ack
        begin
          outbox.send(FiberStream::RactorPort::Element.new(enum.next))
        rescue StopIteration
          outbox.send(FiberStream::RactorPort::Complete.new)
          break
        end
      in FiberStream::RactorPort::Cancel
        break
      end
    end
  end

ack_a = setup.receive
ack_b = setup.receive

result =
  FiberStream::Source.ractor_merge_ports(
    [
      { port: data_a, ack_port: ack_a },
      { port: data_b, ack_port: ack_b }
    ]
  ).run_with(FiberStream::Sink.to_a)

result.sort # => [1, 2, 3, 4]

producer_a.value
producer_b.value
```

## Open Questions

None.
