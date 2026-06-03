# Ractor Port Source

## Status

Draft

## Context

FiberStream already has `Flow.ractor_map` for Ractor-backed mapping, but there
is no source API for producer ractors that emit values into a stream. The
initial source model is pull-based and backpressure-driven. A Ractor ingress
source must preserve that model instead of becoming an unbounded one-way push
boundary.

Governing documents:

- Product spec: `docs/product-specs/ractor-port-source.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Ractor map design: `docs/design-docs/ractor-map.md`
- References:
  - `docs/references/ruby-ractor.md`
  - `docs/references/akka-stream-actor-source.md`

## Goals

- Define a dedicated Ractor port source API.
- Require a one-element-at-a-time acknowledgment handshake.
- Use typed message envelopes for data, completion, failure, ack, and cancel.
- Preserve lazy construction and downstream-driven execution.
- Keep scheduler-managed fibers from blocking on Ractor receive APIs.
- Keep the first API narrow enough to test thoroughly.

## Non-Goals

- Accepting `Ractor` directly instead of ports.
- One-way actor-style push ingress.
- Buffered overflow strategies.
- Multiple producers or fan-in.
- Runtime scheduler installation.
- Killing producer ractors on close.
- General actor-system integration.

## Proposed Design

`Source.ractor_port(port, ack_port:, ack_transfer: :copy, cancel: true)`
creates a source definition backed by a private pull stream:

```ruby
pull_stream =
  Pull.ractor_port(port, ack_port, ack_transfer, cancel)
```

The source uses two ports:

- `port` receives producer messages.
- `ack_port` receives source acknowledgments and cancellation messages.

The public protocol is a small set of `Data` envelope classes:

```ruby
module FiberStream
  module RactorPort
    Element = ::Data.define(:value)
    Complete = ::Data.define
    Failure = ::Data.define(:cause_class_name, :cause_message)
    Ack = ::Data.define
    Cancel = ::Data.define(:reason)
  end
end
```

Typed envelopes are preferred over symbols because symbols can collide with
valid stream elements, while envelope classes support Ruby pattern matching and
future protocol extension.

The first source API is backpressure-aware by construction. The source sends an
initial `Ack[]` on first downstream demand, waits for exactly one producer
message, and emits or completes according to that message. After emitting an
element, it does not send the next `Ack[]` until downstream calls `next` again.
That invariant gives an obeying producer at most one outstanding permission to
send a message.

This is a cooperative boundary. FiberStream bounds the acknowledgments it
issues; it cannot prevent a non-cooperative producer from sending messages to
the port before receiving an ack. Such messages are protocol violations, but a
Ractor port may still buffer them before FiberStream observes them.

Expected producer loop:

```ruby
values = [1, 2, 3].to_enum

loop do
  case ack_port.receive
  in FiberStream::RactorPort::Ack
    begin
      data_port.send(FiberStream::RactorPort::Element.new(values.next))
    rescue StopIteration
      data_port.send(FiberStream::RactorPort::Complete.new)
      break
    end
  in FiberStream::RactorPort::Cancel
    break
  end
end
```

`Complete[]` marks normal stream completion and does not cause another ack.
`Failure[cause_class_name, cause_message]` marks producer failure and raises a
`RactorPortSourceError` with `kind: :producer_failure`. The failure payload uses
known-transferable metadata rather than an exception object so producer error
reporting is robust across Ractor transfer boundaries.

Invalid protocol messages fail the stream through
`FiberStream::RactorPortSourceError`. This includes raw values, unknown
envelopes, and malformed `Failure` payloads. The implementation should
preserve the original exception as Ruby cause when a transfer, receive, or
protocol validation failure has an underlying exception.

`RactorPortSourceError` follows the structured error style of
`RactorMapError`. It exposes `kind`, `cause_class_name`, and `cause_message`.
Kinds are:

- `:invalid_message`
- `:producer_failure`
- `:receive`
- `:ack_transfer`
- `:cancel_transfer`

## Scheduler Interaction

Ruby Ractor port receive APIs can block the current thread. Following the
Ractor map design, a scheduler-managed pipeline fiber must not call blocking
Ractor receive APIs directly.

The first implementation should use a coordinator thread for Ractor waits. The
coordinator owns an internal shutdown port in addition to the producer data
port. The pull stream sends a demand token to the coordinator for each
downstream `next`. For each demand, the coordinator sends `Ack[]`, then waits
with `Ractor.select(port, shutdown_port)` for either one producer message or an
internal shutdown message. Producer messages are forwarded back to the pull
stream through a bounded thread queue. The downstream caller consumes at most
one queued result per `next`.

This design keeps Ractor waiting out of scheduler-managed fibers while
preserving the one-ack-per-demand contract.

If close happens while an ack is outstanding, close sends the internal shutdown
message so the coordinator exits its `Ractor.select` wait. Late producer
messages observed after close are dropped. The close path waits for the
coordinator thread to stop, but it must not wait for a producer response on the
data port.

## Transfer Policy

Producer-to-source transfer is controlled by the producer's
`port.send(message, move:)` call. Ruby 4.0.3 `Ractor::Port#receive` does not
accept a `move:` option, so `Source.ractor_port` cannot choose incoming transfer
policy. If a producer uses move transfer, FiberStream must not inspect moved
objects beyond the protocol validation needed to emit an element or failure.

`ack_transfer: :copy` sends `Ack[]` and `Cancel[...]` with normal Ractor send
semantics. `ack_transfer: :move` sends them with `move: true`. Control
envelopes should be shareable whenever possible, so `:copy` is expected to be
the normal policy.

Transfer failures while sending `Ack[]` are stream failures and should be
normalized to `RactorPortSourceError`. Transfer failures while sending
`Cancel[...]` are close failures.

## Close And Cancellation

`close` is idempotent. Closing the source stops issuing new acknowledgments,
sends an internal shutdown message to the coordinator, and waits for the
coordinator thread to stop. If `cancel: true`, the source sends exactly one
`Cancel[:closed]` to `ack_port` for early downstream completion, downstream
failure, or explicit close. If `cancel: false`, the source treats the producer
as non-owned and sends no cancellation message.

Cancellation is cooperative. FiberStream never kills the producer ractor and
does not promise immediate interruption of producer CPU work. A producer that
obeys the protocol should observe cancellation while waiting for the next ack.

Normal producer `Complete[]` and producer `Failure[...]` are terminal producer
messages. After either one is received, final cleanup does not send
`Cancel[:closed]`; the producer has already ended the protocol. Early
downstream completion, downstream failure, and explicit close all use the
cancelling close path. If a primary downstream failure exists, close failures
are suppressed by the existing `Source#run_with` primary-error rule.

## Contracts

- `Source.ractor_port` is separate from `Source.each`.
- Construction is lazy and does not communicate with ports.
- The source emits values only from `RactorPort::Element`.
- The source completes only from `RactorPort::Complete`.
- The source fails with `RactorPortSourceError` for `RactorPort::Failure`,
  invalid protocol messages, receive failures, ack transfer failures, and
  cancel transfer failures.
- The source sends one initial `Ack` on first demand.
- The source sends the next `Ack` only after a later downstream demand.
- The source never creates more than one outstanding producer permission.
- A non-cooperative producer can still enqueue out-of-credit messages; this is
  a producer protocol violation, not an enforceable FiberStream buffer bound.
- `close` is idempotent.
- `close` wakes the coordinator through an internal shutdown port.
- `close` waits for the coordinator thread to stop.
- `close` drops late producer messages after close.
- `close` sends one `Cancel[:closed]` when `cancel: true` and the producer has
  not already sent `Complete` or `Failure`.
- `close` sends no cancellation message when `cancel: false`.
- Blocking Ractor waits are isolated from scheduler-managed fibers.
- `ack_transfer` must be `:copy` or `:move`.
- Producer-to-source transfer policy is selected by the producer's send call.
- Public APIs never expose `Pull::DONE`.

## Alternatives Considered

### Overload `Source.each`

`Source.each` already has a stable enumerable contract. Ractor ports have
completion, failure, transfer, and cancellation behavior that does not fit
Ruby enumerable semantics. A dedicated API keeps those contracts visible.

### Symbol Sentinels

Symbols are easy to type, but they collide with legitimate stream elements and
make protocol evolution harder. Typed envelopes make protocol messages
unambiguous and pattern-matchable.

### One-Way Buffered Source First

Akka Streams exposes one-way actor ingress separately from backpressure-aware
actor ingress. FiberStream should start with the backpressure-aware shape
because it is closer to the library's core runtime invariant. A buffered
one-way source can be added later with explicit overflow policy.

### Direct Blocking Receive In `next`

Calling Ractor receive APIs directly from `next` would be much simpler, but it
could block scheduler-managed fibers. The existing Ractor map design already
uses a coordinator thread to isolate blocking Ractor waits.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-03. Feedback resulted in these
changes:

- Defined close while a demand is outstanding: the coordinator waits with
  `Ractor.select(port, shutdown_port)`, close wakes it through the internal
  shutdown port, and late producer messages after close are dropped.
- Clarified that producer terminal `Complete` and `Failure` messages suppress
  cancellation during final cleanup, while early downstream completion,
  downstream failure, and explicit close send cancellation when `cancel: true`.
- Replaced `Failure[Exception]` with known-transferable failure metadata so
  producer failures do not depend on exception-object Ractor transfer.
- Clarified that backpressure is cooperative: FiberStream bounds issued acks,
  not arbitrary messages sent by a non-cooperative producer.
- Settled `RactorPortSourceError` structured metadata before implementation.

## Validation

- Unit tests for lazy construction and first-demand ack.
- Unit tests for one ack per downstream demand.
- Unit tests for element, complete, failure, invalid protocol, transfer
  failure, and close behavior.
- Scheduler responsiveness tests mirroring the Ractor map coordinator tests.
- Close tests for close before first demand, close after one emitted element,
  close while an ack is outstanding, late producer messages after close, and
  terminal completion/failure cancel suppression.
- RBS validation for public API signatures.
- Static analysis and formatting checks used by the repository.

## Open Questions

- Should cancellation reasons be a fixed small enum beyond `:closed`?
