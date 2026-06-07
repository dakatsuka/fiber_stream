# Ractor Producer Sources

## Status

Accepted

## Context

`Source.ractor_port` and `Source.ractor_merge_ports` define the low-level
Ractor ingress protocol. They are useful when users already own producer
ractors or need custom setup, but the common owned-producer workflow repeats
the same ceremony:

1. create a FiberStream-owned data port;
2. create a setup port;
3. spawn a producer ractor;
4. create the acknowledgment port inside the producer ractor;
5. send the acknowledgment port back through the setup port;
6. write the ack, element, completion, failure, and cancel loop by hand.

Ruby requires this shape because `Ractor::Port#receive` must be called by the
ractor that created the port. FiberStream cannot remove that ownership rule,
but it can hide the ceremony for producer ractors it creates.

Governing documents:

- Product spec: `docs/product-specs/ractor-producer-sources.md`
- Existing design: `docs/design-docs/ractor-port-source.md`
- Existing design: `docs/design-docs/source-ractor-merge-ports.md`
- Reference: `docs/references/ruby-ractor.md`

## Goals

- Add high-level owned-producer source APIs.
- Keep low-level port APIs unchanged.
- Reuse the existing public `RactorPort` protocol.
- Reuse existing single-port and merge-port pull implementations where
  practical.
- Preserve lazy construction and downstream-driven starts.
- Keep Ractor waits out of scheduler-managed pipeline fibers.
- Keep the first producer context small.

## Non-Goals

- Supporting externally owned producer ractors in the high-level APIs.
- Exposing producer ractor handles.
- Dynamic producer registration after construction.
- Automatic output tagging.
- Immediate interruption of arbitrary CPU-bound producer code.
- A general actor system or supervision tree.

## Proposed Design

Add two public source constructors:

```ruby
Source.ractor_producer(*args, transfer: :copy, ack_transfer: :copy) { |producer, *args| ... }

Source.ractor_merge_producers(transfer: :copy, ack_transfer: :copy) do |group|
  group.producer(*args, transfer: nil) { |producer, *args| ... }
end
```

`Source.ractor_producer` stores one `RactorProducerDefinition`.
`Source.ractor_merge_producers` evaluates its registration block immediately
with a `RactorProducerGroup` builder and stores one definition per
`group.producer` call. This builder evaluation is construction-time definition
work only; it does not create ports, spawn ractors, or run producer blocks.

Producer blocks must be shareable according to `Ractor.shareable?`, matching
`Flow.ractor_map`. This keeps Ractor block isolation explicit at construction
instead of failing later with a less local isolation error.

Each materialized high-level source owns a private producer setup adapter. On
first downstream demand, the adapter starts producer ractors and wires their
ports:

1. create one data port per producer in the FiberStream-running ractor;
2. create one setup port per producer in the FiberStream-running ractor;
3. spawn each producer ractor with its data port, setup port, producer block,
   transfer default, and arguments;
4. inside each producer ractor, create an acknowledgment port;
5. send that acknowledgment port to the setup port before running user code;
6. create a `RactorProducer` context with the data port, acknowledgment port,
   and producer-to-source transfer default;
7. run the user producer block with the context and arguments;
8. send `Complete[]` on normal return unless the block already sent a terminal
   message;
9. send `Failure[class_name, message]` on exception unless the block already
   sent a terminal message or observed cancellation.

After setup, the single-producer source delegates to the existing
`Pull.ractor_port(data_port, ack_port, ack_transfer, true)`. The merge source
delegates to `Pull.ractor_merge_ports(port_pairs, ack_transfer, true)`.

The high-level APIs do not expose `cancel: false`. They own the producer
ractors, so the cleanup path must always request cooperative cancellation.
Users who need non-owned ports or no cancellation messages should use the
existing low-level APIs.

Because the high-level APIs own producer ractors, closing a started source
waits for started producer ractors to terminate after requesting cancellation.
FiberStream does not kill producer ractors. A producer that runs CPU-bound work
for a long time between context calls can therefore delay close until it
reaches the cooperative protocol. Producer termination waits must follow the
same scheduler-safety rule as setup waits and must not call blocking Ractor
wait APIs from scheduler-managed pipeline fibers.

These APIs are therefore for cooperative producer code that calls the producer
context regularly. Non-cooperative, externally supervised, intentionally
detached, or untrusted producers should use the existing low-level port APIs
where ownership and cancellation policy stay outside FiberStream.

## Producer Context

`RactorProducer` is the only object user producer blocks need for the public
protocol:

```ruby
producer.emit(value, transfer: nil)
producer.complete
producer.fail(error = nil, cause_class_name: nil, cause_message: nil)
producer.cancelled?
```

`emit`, `complete`, and `fail` validate their arguments before waiting for the
next source `Ack[]`. After validation, they wait for one ack before sending a
producer-to-source message. If the context receives
`Cancel[:closed]` while waiting, it records cancellation and returns `false`
without sending a message. If it sends the requested message, it returns
`true`.

`emit(value, transfer: nil)` sends `RactorPort::Element[value]`. The optional
per-call transfer override must be `:copy`, `:move`, or `nil`. `nil` uses the
producer definition's transfer default.

`complete` sends `RactorPort::Complete[]` and marks the producer terminal.
`fail` sends `RactorPort::Failure[cause_class_name, cause_message]` and marks
the producer terminal. `fail` requires either an exception-like object or both
explicit String metadata values. Invalid arguments raise `ArgumentError`
before waiting for an ack and before terminal state is marked. If an exception
object is provided, the context derives metadata from `error.class.name` and
`error.message`, using safe String fallbacks if either value is unavailable.

`cancelled?` reports cancellation already observed by this context. It must not
peek at or receive from the acknowledgment port because doing so could consume
an `Ack[]` and break the one-message-per-demand protocol.

If `emit`, `complete`, or `fail` consumes an ack and then fails to send the
requested producer-to-source message because of Ractor transfer or port send
failure, it must use the same ack permission to report a copy-safe
`Failure[class_name, message]` envelope to the data port. It must not wait for
another ack to report that send failure because the source is already waiting
for the message permitted by the consumed ack. If the same-ack failure report
also cannot be sent, the producer exits and the owned-producer monitor reports
the termination as a producer failure.

## Laziness And Scheduler Interaction

Construction is side-effect-light definition work:

- validates missing blocks;
- validates shareable producer blocks;
- validates transfer policy options;
- collects merge producer definitions.

Construction does not create Ractor ports or start ractors.

Producer startup happens on first downstream demand. Setup involves waiting for
each producer to send its acknowledgment port through a setup port. Because
Ractor port receives can block the current thread, setup waits must be isolated
from scheduler-managed pipeline fibers by a coordinator thread or an
equivalent non-reactor-blocking mechanism. The downstream pull side waits for
setup completion through a close-aware mailbox, then delegates demand to the
existing low-level pull source.

If source close happens during setup, the setup adapter wakes the setup wait,
requests cancellation for any producers whose acknowledgment ports are already
known, and continues receiving late setup acknowledgment ports only for
cleanup. Late setup results are suppressed from stream materialization, but
each late acknowledgment port still receives `Cancel[:closed]` so the hidden
producer is not stranded waiting for an ack that will never arrive.

If setup fails after some producers have already started, the setup failure is
the primary stream failure. Cleanup cancels every successfully setup
non-terminal producer, continues handling late setup acknowledgment ports for
cancellation, and waits for started producer ractors according to the close
contract. Cancellation and producer-termination cleanup failures are suppressed
under the primary setup failure, matching the existing `Source#run_with`
primary-error rule.

All waits introduced by the high-level adapter must be scheduler-safe and
close-aware: setup-port waits, setup-completion mailboxes, producer termination
waits, delegated low-level result waits, and delegated close waits. None may
park a scheduler-managed pipeline fiber on a blocking Ractor wait.

## Error Handling

Producer block exceptions are converted inside the producer ractor to
`RactorPort::Failure[class_name, message]` and therefore surface as
`RactorPortSourceError` kind `:producer_failure`.

Invalid per-call transfer options inside a producer block are normal producer
block failures and are reported as producer failures.

Failures while spawning producer ractors, transferring producer arguments, or
receiving acknowledgment ports are setup failures. They are normalized to
`RactorPortSourceError` kind `:producer_setup` so callers have one Ractor
ingress error shape and can distinguish high-level setup failures from
producer protocol receive failures.

Unexpected termination of an owned producer ractor after setup but before a
terminal producer message is a producer failure. The implementation must
monitor every started producer ractor and convert unhandled producer
termination into `Failure[class_name, message]` metadata when possible, or
otherwise into a `RactorPortSourceError` kind `:producer_failure`.

The monitor must race producer termination against data-port waits so a dead
producer cannot leave the low-level port source waiting forever for the message
permitted by an ack. For merge sources, the same rule applies per producer and
the first observed producer failure follows the existing merge error
precedence.

After setup completes, protocol, receive, ack transfer, cancel transfer,
producer terminal, and merge error precedence follow the existing low-level
`Source.ractor_port` and `Source.ractor_merge_ports` contracts.

## Contracts

- `Source.ractor_producer` returns a `Source`.
- `Source.ractor_merge_producers` returns a `Source`.
- Existing low-level Ractor port APIs are unchanged.
- Producer blocks are required and must be shareable.
- Merge registration happens at construction; producer execution does not.
- Merge requires at least two registered producers.
- Producer ractors start on first downstream demand.
- Setup waits do not block scheduler-managed pipeline fibers on Ractor receive
  APIs.
- The helper creates data ports in the FiberStream-running ractor and
  acknowledgment ports inside producer ractors.
- `RactorProducer#emit` sends exactly one `Element` only after receiving one
  `Ack`.
- `RactorProducer#emit`, `#complete`, and `#fail` validate arguments before
  waiting for an ack.
- Send or transfer failures after ack consumption use the same ack permission
  to report `Failure` and never wait for another ack.
- `RactorProducer#complete` and `#fail` are terminal.
- `RactorProducer#fail` requires either an error object or both String
  metadata values.
- Normal producer block return sends `Complete` if no terminal message was
  sent.
- Producer block failure sends `Failure` if no terminal message was sent and
  cancellation has not been observed.
- High-level sources always request cooperative cancellation during cleanup.
- Closing a started high-level source waits for started producer ractors to
  terminate after cancellation.
- Close during setup continues consuming late setup acknowledgment ports for
  cancellation.
- Partial setup failure cancels and waits for already-started producers while
  preserving the setup failure as primary.
- Started producer ractors are monitored so unexpected termination cannot hang
  a data-port wait.
- High-level sources do not expose producer ractor handles.
- Merged high-level sources emit in ready order and preserve per-producer
  order.
- FiberStream does not tag merged outputs.

## Alternatives Considered

### Add Options To `Source.ractor_port`

Overloading the low-level API would mix two ownership models in one method.
Keeping `ractor_port` port-oriented and adding producer-oriented APIs keeps
the contracts easier to explain.

### Name The Single API `Source.ractor`

`ractor` is too broad and suggests accepting existing `Ractor` objects. The
new API creates producer ractors owned by FiberStream, so
`ractor_producer` is more precise.

### Positional Enumerable API For Merge

An enumerable form such as `Source.ractor_merge_producers(inputs) { ... }`
works for homogeneous producers, but it is less natural for Ruby users who
need different producer arguments and blocks. The builder form keeps
heterogeneous producers readable and leaves a homogeneous convenience overload
available for later.

### Return Producer Ractor Handles

Returning handles would expose lifecycle details that can conflict with source
cleanup and cancellation. The first high-level API should return only a
`Source`. Advanced handle access can be added later with a distinct builder or
materialized-control API if a concrete use case appears.

### Add `cancel: false`

The high-level APIs own hidden producer ractors. Suppressing cancellation would
make it easy to leave hidden producers blocked forever waiting for the next
ack. Non-owned or unusual cancellation behavior belongs in the existing
low-level port APIs.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-07. Feedback resulted in these
changes:

- Defined close during setup: late setup acknowledgment ports are still
  consumed internally for cancellation so hidden producers are not stranded.
- Defined partial setup failure cleanup: setup failure remains primary while
  already-started producers are cancelled and waited for.
- Required producer-context argument validation before ack waits and a
  same-ack failure path for transfer or send failures after an ack is
  consumed.
- Made owned-producer termination monitoring a MUST so producer death cannot
  leave low-level data-port waits hanging.
- Clarified that high-level APIs are for cooperative producers and low-level
  port APIs remain the escape hatch for externally supervised or detached
  producers.
- Expanded scheduler-safety requirements to every high-level wait, not only
  direct Ractor receive waits.
- Tightened `RactorProducer#fail` metadata validation.

## Validation

- Unit tests for construction validation and laziness.
- Unit tests proving producer ractors start only on first demand.
- Unit tests proving `emit` waits for ack before sending an element.
- Unit tests proving normal return sends `Complete`.
- Unit tests proving producer exceptions send `Failure`.
- Unit tests proving early downstream completion sends cancellation.
- Unit tests proving close during setup cancels late setup ports.
- Unit tests proving partial setup failure cancels already-started producers.
- Unit tests proving post-ack send failures report same-ack producer failures.
- Unit tests proving unexpected producer termination fails the stream instead
  of hanging.
- Unit tests proving `ractor_merge_producers` requires at least two producers.
- Unit tests proving merge output uses ready order and does not tag values.
- Scheduler responsiveness tests for setup waits, producer termination waits,
  and delegated low-level waits.
- RBS validation for new public signatures.

## Open Questions

None.
