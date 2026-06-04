# Ractor Port Source

## Status

Accepted

## Problem

Users need to connect producer ractors to FiberStream pipelines without losing
the library's pull-based backpressure model. `Source.each` is for Ruby
enumerables and should not absorb actor-like message ingress, because Ractor
ports have distinct completion, failure, transfer, blocking, and cancellation
contracts.

## Goals

- Add a dedicated Ractor port ingress source.
- Preserve lazy construction.
- Require a backpressure handshake from the first public API.
- Use typed FiberStream message envelopes instead of symbol sentinels.
- Avoid collisions between stream elements and control messages.
- Preserve downstream-driven progress: a producer may send the next element
  only after receiving an acknowledgment.
- Make close and early downstream completion behavior explicit.
- Keep one-way push and buffered overflow APIs out of the first scope.

## Non-Goals

- Accepting `Ractor` objects directly as sources.
- Overloading `Source.each` with Ractor behavior.
- One-way push ingress without producer acknowledgment.
- Buffer overflow policies.
- Fan-in from multiple producer ractors.
- Dynamic producer registration.
- Remote actors, distributed streams, or Akka compatibility.
- Immediate interruption of arbitrary CPU-bound producer work.

## Requirements

- `FiberStream::Source.ractor_port(port, ack_port:, ack_transfer: :copy,
  cancel: true)` creates a source definition from a Ractor port pair.
- `port` is the inbound data/control port that the producer sends to.
- `ack_port` is the outbound control port that FiberStream sends acknowledgments
  and cancellation messages to.
- `port` must respond to `receive`.
- `ack_port` must respond to `send`.
- Ruby `Ractor::Port#receive` is only allowed from the port's creator ractor.
  Therefore, in normal usage `port` is created by the ractor that runs
  FiberStream and `ack_port` is created by the producer ractor.
- Construction validates argument shape but does not receive from or send to
  either port.
- Construction does not start ractors or threads.
- The public message protocol uses FiberStream-provided `Data` envelope
  classes.
- Producer-to-source messages are:
  - `FiberStream::RactorPort::Element[value]`
  - `FiberStream::RactorPort::Complete[]`
  - `FiberStream::RactorPort::Failure[cause_class_name, cause_message]`
- Source-to-producer messages are:
  - `FiberStream::RactorPort::Ack[]`
  - `FiberStream::RactorPort::Cancel[:closed]`
- Symbol sentinels are not part of the public protocol.
- Raw values sent to `port` are invalid protocol messages.
- Invalid protocol messages fail the stream with a FiberStream-specific error.
- `Element[value]` emits `value` as a stream element.
- `Complete[]` completes the stream normally.
- `Failure[cause_class_name, cause_message]` fails the stream with
  `FiberStream::RactorPortSourceError`.
- `Failure` payloads must contain `String` cause metadata; malformed failure
  payloads are invalid protocol messages.
- Invalid protocol messages, producer failure messages, port receive failures,
  and source-to-producer transfer failures are normalized to
  `FiberStream::RactorPortSourceError`.
- `RactorPortSourceError` exposes `kind`, `cause_class_name`, and
  `cause_message`.
- `RactorPortSourceError#kind` is one of `:invalid_message`,
  `:producer_failure`, `:receive`, `:ack_transfer`, or `:cancel_transfer`.
- The source sends one initial `Ack[]` when first downstream demand starts the
  materialized source.
- After emitting an `Element`, the source sends the next `Ack[]` only when a
  later downstream pull asks for another element.
- The source never sends more than one outstanding `Ack[]` ahead of downstream
  demand.
- A producer that obeys the protocol must not send an `Element`, `Complete`, or
  `Failure` until it has received an `Ack[]`.
- Normal completion does not send another `Ack[]`.
- Stream failure does not send another `Ack[]`.
- Normal producer `Complete[]` suppresses cancellation during final cleanup.
- Producer `Failure[...]` suppresses cancellation during final cleanup.
- Early downstream completion closes the materialized source.
- When `cancel: true`, closing the materialized source sends exactly one
  `Cancel[:closed]` to `ack_port`.
- `Cancel[:closed]` is the only cancellation reason exposed by the initial
  protocol. FiberStream does not distinguish downstream completion, downstream
  failure, or explicit close in the public cancellation envelope.
- When `cancel: false`, closing the materialized source sends no cancellation
  message.
- Cancellation is cooperative. FiberStream does not terminate the producer
  ractor.
- Closing the source is idempotent.
- `ack_transfer` controls transfer of acknowledgments and cancellation messages
  sent to the producer and must be `:copy` or `:move`.
- Invalid transfer policy values raise `ArgumentError`.
- Transfer failures while sending `Ack[]` are stream failures.
- Transfer failures while sending `Cancel[...]` are close failures.
- Producer-to-source transfer policy is controlled by the producer's
  `port.send(..., move:)` call, not by `Source.ractor_port`.
- FiberStream bounds only the acknowledgments it issues. A non-cooperative
  producer can still enqueue messages without waiting for `Ack[]`; that is a
  protocol violation outside FiberStream's enforceable backpressure boundary.
- Direct Ractor port receive operations can block the current thread. The
  implementation must not call blocking Ractor receive APIs from a
  scheduler-managed pipeline fiber unless the product spec is changed to make
  that limitation explicit.

## Public Contracts

```ruby
FiberStream::Source.ractor_port(
  port,
  ack_port:,
  ack_transfer: :copy,
  cancel: true
)

FiberStream::RactorPort::Element
FiberStream::RactorPort::Complete
FiberStream::RactorPort::Failure
FiberStream::RactorPort::Ack
FiberStream::RactorPort::Cancel
FiberStream::RactorPortSourceError
```

Initial RBS shape:

```rbs
module FiberStream
  type ractor_transfer_policy = :copy | :move
  type ractor_port_cancel_reason = :closed
  type ractor_port_source_error_kind =
    :invalid_message | :producer_failure | :receive | :ack_transfer | :cancel_transfer

  module RactorPort
    class Element[Elem] < Data
      attr_reader value: Elem
    end

    class Complete < Data
    end

    class Failure < Data
      attr_reader cause_class_name: String
      attr_reader cause_message: String
    end

    class Ack < Data
    end

    class Cancel < Data
      attr_reader reason: ractor_port_cancel_reason
    end
  end

  class RactorPortSourceError < RuntimeError
    attr_reader kind: ractor_port_source_error_kind
    attr_reader cause_class_name: String
    attr_reader cause_message: String
  end

  class Source[Elem]
    def self.ractor_port: [Elem] (
      untyped port,
      ack_port: untyped,
      ?ack_transfer: ractor_transfer_policy,
      ?cancel: bool
    ) -> Source[Elem]
  end
end
```

## Examples

```ruby
require "fiber_stream"

data_port = Ractor::Port.new
setup_port = Ractor::Port.new

producer =
  Ractor.new(data_port, setup_port) do |outbox, setup|
    inbox = Ractor::Port.new
    setup.send(inbox)
    values = [1, 2, 3].to_enum

    loop do
      case inbox.receive
      in FiberStream::RactorPort::Ack
        begin
          outbox.send(FiberStream::RactorPort::Element.new(values.next))
        rescue StopIteration
          outbox.send(FiberStream::RactorPort::Complete.new)
          break
        end
      in FiberStream::RactorPort::Cancel
        break
      end
    end
  end

ack_port = setup_port.receive

result =
  FiberStream::Source.ractor_port(data_port, ack_port: ack_port)
    .map { |value| value * 2 }
    .run_with(FiberStream::Sink.to_a)

result # => [2, 4, 6]
```

## Open Questions

None.
