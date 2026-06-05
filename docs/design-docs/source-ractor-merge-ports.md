# Source.ractor_merge_ports

## Status

Accepted

## Context

`Source.ractor_port` connects one producer ractor to a pull-based stream with
an explicit ack handshake. `Source#merge` combines two source definitions with
scheduler-backed producer fibers, but it does not provide CPU parallelism for
scheduler-unaware blocking work. Users who already isolate producer work in
ractors need a direct fan-in source that preserves the Ractor port protocol and
does not require a manually written merge producer ractor.

Governing documents:

- Product spec: `docs/product-specs/source-ractor-merge-ports.md`
- Existing design: `docs/design-docs/ractor-port-source.md`
- Related design: `docs/design-docs/source-merge.md`
- ADR: `docs/design-docs/adr/0013-source-ractor-merge-ports.md`
- References:
  - `docs/references/ruby-ractor.md`

## Goals

- Merge multiple producer-owned Ractor port pairs into one source.
- Keep the existing `RactorPort` public protocol unchanged.
- Preserve per-producer backpressure with one outstanding ack per active
  producer.
- Emit elements in coordinator-observed ready order.
- Avoid blocking scheduler-managed fibers on Ractor wait APIs.
- Keep close, cancellation, and error precedence explicit.

## Non-Goals

- Dynamic producer registration.
- Accepting `Ractor` instances directly.
- Public producer tags in emitted values.
- Fairness, round-robin, priority, or timestamp ordering.
- General graph materialization.
- Configurable queue sizes.
- Killing producer ractors.

## Proposed Design

`Source.ractor_merge_ports(ports, ack_transfer: :copy, cancel: true)` returns a
lazy `Source` whose factory builds a private pull stream:

```ruby
Pull.ractor_merge_ports(port_pairs, ack_transfer, cancel)
```

The public `ports` argument is an enumerable of hashes:

```ruby
[
  { port: data_port_a, ack_port: ack_port_a },
  { port: data_port_b, ack_port: ack_port_b }
]
```

Construction consumes the `ports` enumerable once, so the enumerable must be
finite. It validates that at least two pairs exist, each data port responds to
`receive`, each ack port provides Ractor-style `send`, data ports are distinct
by object identity, ack ports are distinct by object identity, `ack_transfer`
is `:copy` or `:move`, and `cancel` is boolean. Validation does not
communicate with any port.

The materialized pull stream owns:

- normalized immutable port pair records with a stable integer side index;
- a coordinator thread;
- a control `Ractor::Port` created by the FiberStream-running ractor;
- a bounded result mailbox consumed by downstream `next`;
- per-side terminal, done, cancel-sent, and pending-ack state.

The coordinator thread is the only code path that waits on producer data
ports. It waits with:

```ruby
Ractor.select(control_port, *active_data_ports)
```

The control port receives internal commands from the downstream pull stream:

- `Start`: send one initial `Ack[]` to every active producer.
- `RequestAck[side]`: send another `Ack[]` to the side whose previous element
  was emitted, if that producer is still active and has no outstanding ack.
- `Shutdown`: exit the coordinator.

Using a Ractor control port instead of a Ruby thread queue lets `close` and
downstream demand wake the coordinator while it is blocked in `Ractor.select`.
All control messages are private `Data` envelopes. Every public
`RactorPort::Ack` and `RactorPort::Cancel` send constructs a fresh envelope
instance so `ack_transfer: :move` never reuses a moved control object.

The downstream `next` starts the coordinator lazily. On first demand it sends
`Start`. After returning an element from side `i`, the pull stream records
that side as pending for replenishment. On the next downstream demand, before
waiting for a result, it sends `RequestAck[i]`. This preserves the
`Source.ractor_port` rule that a later downstream pull is what grants the next
producer permission.

Each active producer can therefore have at most one outstanding ack. Across
the whole merge, there may be up to one in-flight producer message per active
producer. The result mailbox is bounded by the number of port pairs so a
cooperative producer set cannot create unbounded FiberStream buffering.

The result mailbox is a private boundary between the coordinator thread and
the downstream pull stream. It must provide:

- `push(result)`, which waits without losing close wakeups while full;
- `pop`, which waits without parking the entire scheduler thread while empty;
- `close`, which wakes blocked pushers and poppers;
- a closed-boundary signal that downstream and the coordinator can suppress
  when caused by stream close.

If the implementation uses a standard Ruby queue, tests must prove the wait
cooperates with the target scheduler. Otherwise it must use a dedicated
scheduler-aware mailbox. This requirement is separate from isolating
`Ractor.select`; both the Ractor wait and the downstream result wait must be
safe for scheduler-managed pipelines.

When the coordinator receives a producer message, it publishes a private
result message to the downstream queue:

- `ValueResult[side, value]` for `RactorPort::Element[value]`
- `DoneResult[side]` for `RactorPort::Complete[]`
- `ErrorResult[side, error]` for producer failure, malformed failure, invalid
  protocol, receive failure, or ack transfer failure

The coordinator owns producer-terminal state. Before publishing a `DoneResult`
or valid producer-failure `ErrorResult`, it marks that side producer-terminal
under the same mutex used by `close`. This is a MUST-level contract: even if a
terminal result remains queued and downstream closes before consuming it,
cleanup must not send `Cancel[:closed]` to that terminal producer. Downstream
owns only stream-completion state used to decide when all `DoneResult`
messages have been consumed. Invalid protocol messages are not
producer-terminal; cleanup will send cancellation to that side when
`cancel: true`.

Downstream result handling mirrors `Source.ractor_port`:

- `ValueResult` returns the original element and records the side for
  replenishment on a later pull.
- `DoneResult` marks the side done and loops until all sides are done or a
  value/error is available.
- `ErrorResult` marks the merge done, raises the normalized error, and relies
  on `Source#run_with` cleanup to close the materialized source.

The source completes only when every side has observed `Complete[]`. One
completed side does not stop other sides.

## Close And Cancellation

`close` is idempotent. Closing an unstarted stream only marks it closed; it
does not create ports, start the coordinator, send acks, or send cancels.

Closing a started stream:

1. marks the stream closed and done;
2. closes the result mailbox to wake downstream waits;
3. sends `Shutdown` to the coordinator control port;
4. waits for the coordinator thread to stop so any coordinator-observed
   terminal state is visible;
5. sends `Cancel[:closed]` to every non-terminal producer when `cancel: true`.

If multiple cancellation sends fail during normal early downstream completion,
the first `RactorPortSourceError` kind `:cancel_transfer` is raised. If close
runs while another stream failure is primary, `Source#run_with` suppresses
close failures.

Producer terminal `Complete[]` and valid `Failure[String, String]` messages
suppress cancellation for that side. Ack transfer failure, receive failure,
malformed failure, and invalid protocol do not mark the side terminal, so a
closing stream attempts cancellation for that producer.

Cancellation is cooperative. FiberStream does not terminate producer ractors
and does not wait for producers to acknowledge cancellation.

## Error Handling

The API reuses `RactorPortSourceError` so callers have one error shape for
Ractor port ingress failures. Kinds keep their existing meanings:

- `:producer_failure`: producer sent `Failure[String, String]`.
- `:invalid_message`: producer sent an invalid protocol message or malformed
  `Failure`.
- `:receive`: coordinator failed while selecting or receiving from a port.
- `:ack_transfer`: source failed while sending `Ack[]`.
- `:cancel_transfer`: source failed while sending `Cancel[:closed]`.

If multiple producers fail, the first error result observed by downstream is
primary. Later queued, in-flight, or cleanup errors are suppressed by close
and `Source#run_with` primary-error rules.

Valid `RactorPort::Failure` metadata is producer-provided and FiberStream does
not sanitize it. For producer failures, `cause_class_name` and `cause_message`
are exposed on `RactorPortSourceError` and included in its exception message.
Producers should sanitize or redact values that cross trust boundaries,
including internal paths, stack details, secrets, tenant data, or other
sensitive content.

## Contracts

- `Source.ractor_merge_ports` is a class method separate from
  `Source.ractor_port`.
- The source is lazy and does not communicate with ports until first
  downstream demand.
- At least two port pairs are required.
- The `ports` enumerable is finite and consumed at construction.
- Data ports and ack ports must be distinct by object identity.
- The public Ractor port protocol remains unchanged.
- No scheduler is required.
- Ractor waits are isolated in a coordinator thread.
- Result-mailbox waits must not park the scheduler thread and must wake on
  close.
- The source sends one initial ack to every active producer on first demand.
- The source sends the next ack to a producer only after downstream later
  demands another element following an emitted value from that producer.
- No producer receives more than one outstanding ack.
- Every ack and cancel send uses a fresh protocol envelope.
- Values are emitted unwrapped.
- Per-producer order is preserved.
- Cross-producer order is ready order and not deterministic.
- Completion requires every producer to send `Complete[]`.
- Producer terminal messages suppress cancellation for that side.
- Invalid protocol and source-side failures propagate as
  `RactorPortSourceError`.
- Close sends cancellation only to non-terminal producers when `cancel: true`.
- Close sends no cancellation when `cancel: false`.
- Public APIs never expose internal result messages or `Pull::DONE`.

Public API:

```ruby
class Source[Elem]
  def self.ractor_merge_ports(ports, ack_transfer: :copy, cancel: true)
end
```

Internal API:

```ruby
Pull.ractor_merge_ports(port_pairs, ack_transfer, cancel)
```

## Alternatives Considered

### User-Written Merge Producer Ractor

Users can create one merge producer ractor, have it receive from multiple
worker ractors, and pass its output through `Source.ractor_port`. That keeps
FiberStream smaller, but every user must reproduce the same ack, cancellation,
error normalization, and ready-order fan-in logic. A library API is clearer
and less error-prone.

### Compose `Source.ractor_port(...).merge(...)`

This works for two ports when a scheduler is installed, but it inherits
`Source#merge`'s scheduler requirement and binary shape. A dedicated Ractor
fan-in can be variadic and does not need scheduler-backed producer fibers.

### Global One-Ack Backpressure

Sending only one ack total per downstream demand would be stricter, but it
would require choosing which producer is allowed to race for the next element.
That would be round-robin or priority polling, not ready-order merge. One
outstanding ack per producer is the right bounded fan-in compromise.

### Public Buffer Size

The result mailbox can be bounded by the number of producers. Exposing buffer
size now would add overflow policy questions before the basic fan-in contract
is proven.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-05. Feedback resulted in
these changes:

- Defined the result mailbox contract separately from Ractor wait isolation:
  downstream waits must not park the entire scheduler thread, must wake on
  close, and require scheduler responsiveness tests.
- Made coordinator-owned producer-terminal state a MUST-level contract so
  queued terminal results still suppress cancellation if downstream closes
  before consuming them.
- Required distinct data ports and distinct ack ports by object identity.
- Required every ack and cancel send to construct a fresh protocol envelope so
  `ack_transfer: :move` cannot reuse moved objects.
- Clarified that the `ports` enumerable is finite and consumed during
  construction to assign stable side indexes.

## Validation

- Unit tests for lazy construction and validation.
- Unit tests for initial ack to all producers.
- Unit tests for one outstanding ack per producer.
- Unit tests for ready-order values and per-producer order.
- Unit tests for normal completion after all producers complete.
- Unit tests for one producer completing before another.
- Unit tests for producer failure, invalid protocol, malformed failure,
  receive failure, ack transfer failure, and cancel transfer failure.
- Unit tests for early downstream completion and `cancel: false`.
- Scheduler responsiveness tests proving Ractor waits do not block Async
  reactor progress.
- Scheduler responsiveness tests proving downstream result-mailbox waits do
  not block Async reactor progress.
