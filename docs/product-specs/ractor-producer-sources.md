# Ractor Producer Sources

## Status

Accepted

## Problem

`Source.ractor_port` and `Source.ractor_merge_ports` preserve FiberStream's
backpressure model for producer ractors, but they require users to hand-write
Ractor port setup, acknowledgment ports, protocol envelopes, completion, and
failure reporting. That is appropriate for low-level interop, but it is too
much ceremony for the common case where FiberStream owns the producer ractors.

## Goals

- Add high-level source APIs for FiberStream-owned producer ractors.
- Keep existing `Source.ractor_port` and `Source.ractor_merge_ports` behavior
  unchanged as low-level escape hatches.
- Hide `data_port`, `setup_port`, and `ack_port` setup from users.
- Hide the `RactorPort::Ack`, `Element`, `Complete`, `Failure`, and `Cancel`
  protocol loop behind a small producer context.
- Preserve one-outstanding-ack backpressure.
- Preserve ready-order fan-in for multiple producers.
- Preserve lazy source construction.
- Require shareable producer blocks so Ractor isolation failures are explicit.
- Expose producer-to-source transfer policy explicitly.

## Non-Goals

- Removing or changing `Source.ractor_port`.
- Removing or changing `Source.ractor_merge_ports`.
- Accepting arbitrary existing `Ractor` objects.
- Dynamic producer registration after source construction.
- Automatic tagging of merged producer outputs.
- One-way push without acknowledgments.
- Killing producer ractors.
- Hiding Ractor transfer limitations for block captures or arguments.

## Requirements

- `FiberStream::Source.ractor_producer(*args, transfer: :copy,
  ack_transfer: :copy) { |producer, *args| ... }` creates a source definition
  backed by one FiberStream-owned producer ractor.
- `FiberStream::Source.ractor_merge_producers(transfer: :copy,
  ack_transfer: :copy) { |group| ... }` creates a source definition backed by
  multiple FiberStream-owned producer ractors merged in ready order.
- Both APIs are additions. Existing low-level Ractor port APIs remain public and
  behaviorally unchanged.
- Both APIs require a block.
- Producer blocks must be shareable according to `Ractor.shareable?`. The
  intended shape is `Ractor.shareable_proc { |producer, ...| ... }`.
- `transfer` must be `:copy` or `:move`.
- `transfer` controls producer-to-source sends for `Element`, `Complete`, and
  `Failure` envelopes produced by the helper.
- `ack_transfer` must be `:copy` or `:move`.
- `ack_transfer` controls source-to-producer `Ack` and `Cancel` sends and has
  the same meaning as on the low-level APIs.
- Construction validates blocks and option shape but does not create Ractor
  ports, spawn ractors, send messages, or run producer blocks.
- Producer ractors start only after the materialized source receives downstream
  demand.
- Producer arguments are transferred to producer ractors when producer ractors
  start. Transfer failures at that point are stream failures with
  `RactorPortSourceError` kind `:producer_setup`.
- The high-level APIs always own their producer ractors and always request
  cooperative cancellation on close. They do not expose a `cancel: false`
  option. Use `Source.ractor_port` or `Source.ractor_merge_ports` for
  non-owned producer ports.
- The high-level APIs do not expose producer `Ractor` handles.
- The producer context passed to each producer block exposes:
  - `emit(value, transfer: nil)`
  - `complete`
  - `fail(error = nil, cause_class_name: nil, cause_message: nil)`
  - `cancelled?`
- `producer.emit(value)` waits for one `Ack[]` before sending one
  `RactorPort::Element[value]`.
- `producer.emit(value, transfer: :copy | :move)` overrides the source-level
  `transfer` policy for that element send.
- `producer.complete` waits for one `Ack[]` before sending
  `RactorPort::Complete[]` and marks the producer terminal.
- `producer.fail(error)` waits for one `Ack[]` before sending
  `RactorPort::Failure[error.class.name, error.message]` and marks the
  producer terminal.
- `producer.fail(cause_class_name:, cause_message:)` sends explicit failure
  metadata and marks the producer terminal.
- `producer.fail` requires either an error object or both explicit
  `String` metadata values. Invalid arguments raise `ArgumentError` before the
  producer waits for an ack and before terminal state is marked.
- If an exception has no class name or message, `producer.fail(error)` uses
  safe String fallbacks before sending the `Failure` envelope.
- `producer.cancelled?` returns whether this producer context has already
  observed `RactorPort::Cancel`. It is not a polling API and does not consume
  pending acknowledgments.
- CPU-bound loops observe cancellation only when they reach `emit`,
  `complete`, or `fail`. Producers that need prompt cancellation should call
  the context frequently and stop when `emit`, `complete`, or `fail` returns
  `false`.
- If a producer context observes `Cancel[:closed]` while waiting to emit,
  complete, or fail, it marks itself cancelled and does not send a producer
  message for that operation.
- `emit`, `complete`, and `fail` validate user arguments before waiting for an
  ack.
- If a producer-to-source send fails after an ack has already been consumed,
  the producer context must use that same ack permission to report a
  copy-safe `Failure` envelope. It must not wait for another ack to report the
  send failure.
- If reporting that same-ack failure also fails, the owned-producer monitor
  reports producer termination as a producer failure.
- If a producer block returns normally without already sending `complete` or
  `fail`, the helper sends `Complete[]` through `producer.complete`.
- If a producer block raises before sending `complete` or `fail`, the helper
  sends `Failure[exception.class.name, exception.message]` through
  `producer.fail`.
- Failure metadata is producer-provided. Producers should sanitize or redact
  values that cross trust boundaries, including internal paths, stack details,
  secrets, tenant data, or other sensitive content.
- High-level producer failures are surfaced through
  `FiberStream::RactorPortSourceError`, matching the low-level port sources.
- Failures while spawning producer ractors, transferring producer arguments, or
  receiving setup acknowledgment ports are surfaced through
  `RactorPortSourceError` kind `:producer_setup`.
- Producer blocks can still ignore cancellation or run CPU-bound work for a
  long time between producer context calls. Cancellation remains cooperative.
- Closing a started high-level source requests cancellation and waits for
  started producer ractors to terminate. FiberStream does not kill producer
  ractors; close can wait for long-running producer code to reach the
  cooperative protocol.
- The high-level APIs are intended for cooperative producers that call the
  producer context regularly. Use the low-level port APIs for non-cooperative,
  externally supervised, or intentionally detached producer ractors.
- If close happens during producer setup, FiberStream continues consuming late
  setup acknowledgment ports internally so it can send `Cancel[:closed]` to
  started producers. Late setup results are suppressed from stream
  materialization but are not ignored for cleanup.
- If setup fails after some producers have already started, the setup failure
  is the primary stream failure. FiberStream cancels every successfully setup
  non-terminal producer, continues handling late setup acknowledgment ports for
  cancellation, waits for started producers according to the close contract,
  and suppresses cleanup failures under the primary setup failure.
- FiberStream monitors every started producer ractor. Unexpected termination
  before a terminal producer message is observed becomes a producer failure
  rather than an indefinite data-port wait.
- `Source.ractor_merge_producers` evaluates its registration block at source
  construction to collect producer definitions, but it does not start producer
  ractors or run producer blocks at construction.
- The merge registration block receives a group builder.
- `group.producer(*args, transfer: nil) { |producer, *args| ... }` registers
  one producer definition.
- Each `group.producer` block must be shareable according to
  `Ractor.shareable?`.
- `group.producer(..., transfer: :copy | :move)` overrides the merge-level
  `transfer` default for that producer.
- `Source.ractor_merge_producers` requires at least two registered producers.
  Use `Source.ractor_producer` for a single producer.
- Merged outputs are not tagged by FiberStream. Users who need source identity
  should include it in emitted values.
- Merged producer values are emitted in the same ready-order semantics as
  `Source.ractor_merge_ports`.
- Per-producer element order is preserved.
- The merged source completes only after every producer has completed normally.
- On first stream failure, non-terminal producers are cooperatively cancelled
  during cleanup.

## Public Contracts

```ruby
FiberStream::Source.ractor_producer(
  *args,
  transfer: :copy,
  ack_transfer: :copy
) { |producer, *args| ... }

FiberStream::Source.ractor_merge_producers(
  transfer: :copy,
  ack_transfer: :copy
) { |group| ... }

group.producer(*args, transfer: nil) { |producer, *args| ... }
```

Initial RBS shape:

```rbs
module FiberStream
  type ractor_port_source_error_kind =
    :invalid_message | :producer_failure | :receive | :ack_transfer |
    :cancel_transfer | :producer_setup

  class RactorProducer
    def emit: [Elem] (Elem value, ?transfer: ractor_transfer_policy?) -> bool
    def complete: () -> bool
    def fail: (?untyped error, ?cause_class_name: String?, ?cause_message: String?) -> bool
    def cancelled?: () -> bool
  end

  class RactorProducerGroup
    def producer: (*untyped args, ?transfer: ractor_transfer_policy?) {
      (RactorProducer producer, *untyped args) -> void
    } -> self
  end

  class Source[Elem]
    def self.ractor_producer: [Elem] (
      *untyped args,
      ?transfer: ractor_transfer_policy,
      ?ack_transfer: ractor_transfer_policy
    ) { (RactorProducer producer, *untyped args) -> void } -> Source[Elem]

    def self.ractor_merge_producers: [Elem] (
      ?transfer: ractor_transfer_policy,
      ?ack_transfer: ractor_transfer_policy
    ) { (RactorProducerGroup group) -> void } -> Source[Elem]
  end
end
```

## Examples

Single producer:

```ruby
require "fiber_stream"

PRODUCE_VALUES =
  Ractor.shareable_proc do |producer, values|
    values.each do |value|
      break unless producer.emit(value)
    end
  end

source =
  FiberStream::Source.ractor_producer([1, 2, 3], &PRODUCE_VALUES)

source.run_with(FiberStream::Sink.to_a) # => [1, 2, 3]
```

Multiple heterogeneous producers:

```ruby
READ_A =
  Ractor.shareable_proc do |producer, path|
    File.foreach(path) do |line|
      break unless producer.emit([:a, line])
    end
  end

READ_B =
  Ractor.shareable_proc do |producer, path|
    File.foreach(path) do |line|
      break unless producer.emit([:b, line])
    end
  end

source =
  FiberStream::Source.ractor_merge_producers do |group|
    group.producer(path_a, &READ_A)
    group.producer(path_b, &READ_B)
  end
```

## Open Questions

None.
