# Source.merge

## Status

Accepted

## Context

FiberStream's current source combinators are synchronous: `Source#concat`
emits one complete source before the next, and `Source#zip` pulls one element
from each input to emit pairs. `Source#merge` has a different shape: either
input should be able to produce the next downstream value. A synchronous
left-before-right pull loop would create deterministic interleaving, but it
would not be a ready-order merge and could block one input behind another.

Governing documents:

- Product spec: `docs/product-specs/source-merge.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Related designs:
  - `docs/design-docs/source-concat.md`
  - `docs/design-docs/source-zip.md`
  - `docs/design-docs/async-boundary.md`
  - `docs/design-docs/buffer-boundary.md`
- ADR: `docs/design-docs/adr/0012-source-merge.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Goals

- Compose two source definitions into one ready-order source.
- Preserve each input source's internal element order.
- Start producer work only after downstream demand reaches the merge.
- Require a scheduler only when the merge starts.
- Keep queued merge messages bounded.
- Close all materialized input chains during normal completion, failure, and
  early downstream completion.
- Keep FiberStream independent of Async at runtime.

## Non-Goals

- General graph materialization.
- Variadic merge.
- Public merge flow.
- Deterministic round-robin interleaving.
- Fairness or priority scheduling between inputs.
- Configurable queue sizes or overflow policies.
- Ordered merge by timestamps or custom keys.

## Proposed Design

`Source#merge(source)` validates that `source` is a `FiberStream::Source` and
returns a new `Source` whose factory builds a private merge pull stream:

```ruby
Pull.merge(left_materializer, right_materializer)
```

The receiver's materializer builds the receiver source factory and attaches the
receiver flows. The other source's materializer builds the other source factory
and attaches the other source flows. Flows attached after merge apply to the
combined output because they are attached to the returned source in the normal
`Source#via` path.

`Pull::Merge` stores the two materializers without calling them during
construction or source materialization. The first downstream `next` starts the
merge. At that point:

1. If `Fiber.scheduler` is `nil` or the current fiber is blocking, raise
   `SchedulerRequiredError`.
2. Create a shared private `MergeMailbox` with capacity `1`.
3. Start two scheduled producer fibers, one for the receiver side and one for
   the other side.
4. Each producer materializes its source chain inside the producer fiber.

This boundary provides scheduler concurrency, not OS-thread or Ractor
parallelism. If an input source performs scheduler-unaware blocking IO or
CPU-bound work during `next`, that operation can block the scheduler thread and
delay the other merge input. Applications that need blocking or CPU-bound
producer work to run truly independently should isolate that work outside
`Source#merge`, for example with producer ractors connected through
`Source.ractor_port`.

The mailbox carries tagged internal messages:

```ruby
[:value, side, object]
[:done, side]
[:error, side, exception]
```

Using tagged messages lets all Ruby objects remain valid stream values. The
`side` tag is internal state used to count normal completions and to maintain
per-side materialized, completed, and closed state.

`MergeMailbox` is a private scheduler-aware bounded mailbox, not a public API.
It must provide:

- `push(message)`, which waits cooperatively while the mailbox is full;
- `pop`, which waits cooperatively while the mailbox is empty;
- `close`, which wakes blocked pushers and poppers;
- a closed-boundary signal that producers and the downstream merge stage can
  identify and suppress when it was caused by merge close.

The implementation must not rely on a blocking primitive that can park the
entire scheduler thread while a producer waits for mailbox space or the
downstream side waits for a message. If a standard Ruby queue is used, tests
must prove it cooperates with the target scheduler; otherwise the merge must
use a dedicated fiber-aware mailbox.

Each producer repeatedly pulls its materialized stream and pushes one message
into the shared mailbox. If the mailbox already contains one message, `push`
waits at the merge boundary. While waiting on that push, the producer may hold
one in-flight value, completion, or error message, but it does not pull farther
upstream.

If the mailbox is closed while a producer is waiting to push, the producer
stops pulling and suppresses the closed-boundary signal. That signal is an
internal cancellation wakeup, not a stream failure. Producer cleanup must
preserve the already-selected primary error: for example, an upstream pull
failure remains primary over any later close failure from the same side, and a
downstream failure remains primary over cancellation wakeups.

Producer-side normal completion is published only after that producer closes
its input stream. If `stream.next` returns `Pull::DONE`, the producer closes
the stream and publishes `[:done, side]` when close succeeds, or
`[:error, side, close_error]` when close fails. If `stream.next` raises, the
producer closes the stream and publishes `[:error, side, pull_error]`; any
close failure on that path is suppressed so the pull failure remains primary.

The downstream `next` starts the merge if needed, then pops messages until it
can return a value, raise an error, or complete:

- `[:value, side, object]` returns `object` immediately.
- `[:done, side]` marks that side complete and loops for the next message
  unless both sides are complete.
- When both sides are complete, the stage marks itself done and returns
  `Pull::DONE`.
- `[:error, side, exception]` marks the merge done, closes all materialized
  sides, requests producer cancellation, and raises `exception`.

Cross-source ordering is intentionally not deterministic. Mailbox arrival order
under the installed scheduler defines observable interleaving. Each producer
pulls its own input serially, so within-source order is preserved. The design
does not guarantee fairness or bounded liveness for a ready value from one side
when the other side repeatedly wins scheduler and mailbox races.

The mailbox capacity is intentionally fixed at one message for the first API
slice. This keeps backpressure strict without adding public tuning knobs. At
most one message is queued, and each producer may hold at most one additional
in-flight message while blocked on mailbox insertion. Future variants can add
configurable buffering or overflow policies once the basic cleanup and
cancellation contract is proven.

`close` is idempotent. It marks the merge closed, closes every materialized
input stream in receiver-then-other order, closes the mailbox to wake producers
or consumers blocked at the boundary, and requests producer cancellation when
producers are still running. It never materializes an unstarted input only to
close it. The stage tracks sides already closed by their producer and skips
those sides during downstream cleanup instead of relying on upstream close
idempotence for correctness. Closing an unstarted merge does not require a
scheduler.

Cancellation is cooperative and scheduler-agnostic, matching `Flow.async` and
`Flow.buffer`: FiberStream guarantees that close requests producer cancellation
and closes materialized upstream chains before returning. It does not guarantee
immediate interruption of arbitrary user code blocked inside a source or flow.
Resource-owning stages must make `close` release their resources.

If downstream closes merge intentionally before consuming a queued or in-flight
upstream error, that upstream error is suppressed in favor of the downstream
result or downstream failure. User close errors raised during downstream
cleanup are not cancellation errors; they propagate from `Source#run_with`
unless a downstream failure is already primary.

Downstream cleanup precedence is:

1. A downstream failure is primary. Input close failures, queued or in-flight
   upstream errors, producer cancellation, and closed-mailbox wakeups are
   suppressed.
2. For normal early downstream completion, queued or in-flight upstream errors,
   producer cancellation, and closed-mailbox wakeups are suppressed. Input
   close failures propagate; if both input closes fail, the receiver close
   failure is primary and the other close failure is suppressed.
3. If downstream is still consuming the merge and observes an upstream error
   message, that error is primary. Closing the other side during error cleanup
   may fail, but that close failure is suppressed.

Materialization failures happen inside producer fibers and are published as
error messages. If one side's materialization fails after the other side has
materialized, downstream error handling closes the other side. If both sides
fail, the first error message observed by downstream is primary and later
errors are suppressed. This is intentionally arrival-order based because
cross-source execution order is not deterministic.

If the same `Source` object appears on both sides, the receiver and other side
still use separate materializer calls. This matches existing `Source` reuse
semantics: FiberStream creates independent materialized pull chains when the
source definition can support that, but it does not snapshot or make one-shot
underlying objects replayable.

## Contracts

- `Source#merge` accepts only `FiberStream::Source` instances.
- `Source#merge` returns a lazy `Source`.
- Neither input is materialized during construction or source materialization.
- Producers start on the first downstream pull that reaches the merge.
- Closing an unstarted merge does not start producers and does not require a
  scheduler.
- A scheduler is required when producers start.
- Missing scheduler raises `FiberStream::SchedulerRequiredError`.
- A blocking current fiber raises `FiberStream::SchedulerRequiredError` even
  when a scheduler is installed.
- FiberStream does not install a scheduler and does not require Async at
  runtime.
- `Source#merge` does not make scheduler-unaware blocking source work
  non-blocking and does not provide CPU parallelism.
- The receiver and other source are materialized by separate producer fibers.
- Values from either input may be emitted first.
- Each input source's own element order is preserved.
- Cross-source ordering is not deterministic and is not guaranteed to be fair.
- Emitted values are unwrapped original source elements.
- Ready order means arrival order at the internal mailbox under the installed
  scheduler.
- The shared mailbox holds at most one message.
- Each producer may hold one additional in-flight message while waiting to
  enqueue.
- Closed-mailbox wakeups caused by merge close are internal cancellation
  signals and are suppressed.
- Producer-side completion waits for that producer's input close result.
- The merged source completes only after both sides complete normally.
- If one side completes, the other side may continue producing values.
- Input pull failures propagate from `Source#run_with`.
- Producer-side close failures after normal input completion propagate from
  `Source#run_with`.
- Input pull failures take precedence over close failures from the same side.
- The first observed error message is primary when both sides fail.
- On observed error, merge closes all materialized sides and requests producer
  cancellation.
- On early downstream completion or downstream failure, merge closes all
  materialized sides and requests producer cancellation.
- Queued or in-flight upstream errors are suppressed after intentional
  downstream completion or downstream failure.
- Materialization failures propagate from `Source#run_with`; unmaterialized
  sides are not closed, and materialized sides are closed by merge cleanup.
- Self-merge independently materializes the receiver and other side from the
  same source definition.
- Flows attached to either input before merge apply only to that input; flows
  attached after merge apply to combined output.
- Public APIs never expose internal merge messages or `Pull::DONE`.

Public API:

```ruby
class Source[Elem]
  def merge: [Other] (Source[Other] source) -> Source[Elem | Other]
end
```

Internal API:

```ruby
Pull.merge(left_materializer, right_materializer)
```

Both materializers return internal pull streams responding to `next` and
`close`.

## Alternatives Considered

### Synchronous Round-Robin Merge

A synchronous stage could alternate left and right pulls without a scheduler.
That would be deterministic and simpler, but it would not emit whichever input
is ready first. It could also let a slow or blocking input delay values already
available from the other input. That behavior is better named `interleave` or
`round_robin`, not `merge`.

### Reuse `Source#zip` Internals

Zip's left-before-right pull protocol deliberately waits for pairs and
discards unpaired values. Merge has independent completion and error handling
for each side, so it needs a separate boundary.

### Unbounded Queue

An unbounded queue would maximize overlap, but it weakens FiberStream's
backpressure model and can turn a slow downstream into unbounded memory growth.

### Public Buffer Size Option

`merge(source, buffer:)` may be useful, but it adds overflow and memory
contract questions to the first API. A fixed capacity of one message keeps the
initial contract narrow.

### Variadic Merge

Variadic merge is useful, but it adds input indexing, aggregate type shape, and
more complex failure precedence. A binary merge composes naturally:
`a.merge(b).merge(c)`.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-05. Feedback resulted in these
changes:

- Replaced direct `Thread::SizedQueue` design language with a private
  scheduler-aware `MergeMailbox` contract.
- Defined closed-mailbox wakeups as suppressible internal cancellation signals.
- Added downstream cleanup precedence for downstream failure, normal early
  completion, close failures, queued upstream errors, and cancellation wakeups.
- Clarified that ready order means merge-mailbox arrival order under the
  installed scheduler, with no fairness or bounded-liveness guarantee.
- Clarified that `SchedulerRequiredError` is raised when downstream demand
  reaches the merge, not at `Source#merge` construction time.
- Clarified that merge requires a non-blocking current fiber when demanded.
- Added self-merge and unstarted-close semantics.

## Validation

- Unit tests for invalid arguments and construction laziness.
- Unit tests proving no scheduler is required until first downstream demand.
- Unit tests proving missing scheduler errors on first downstream demand.
- Unit tests proving blocking current fiber errors on first downstream demand.
- Async-backed unit tests proving both sources are materialized after first
  demand.
- Async-backed unit tests proving values from both sides are emitted and each
  side's order is preserved.
- Unit tests proving the merged source completes only after both sides complete.
- Unit tests proving one side may complete while the other continues.
- Unit tests proving mailbox backpressure bounds producer run-ahead.
- Unit tests proving mailbox waits do not block the scheduler reactor under
  Async.
- Unit tests proving closing a producer blocked on enqueue suppresses the
  closed-mailbox wakeup.
- Unit tests proving flows before merge are scoped per input and flows after
  merge apply to combined output.
- Unit tests proving closing an unstarted merge does not require a scheduler
  and does not materialize either input.
- Unit tests proving early sink completion closes both materialized sides and
  suppresses queued upstream errors.
- Unit tests proving downstream failure closes both materialized sides.
- Unit tests proving downstream cleanup precedence for close failures after
  normal early completion and downstream failure.
- Unit tests proving input pull failures propagate and close the other side.
- Unit tests proving producer-side normal-completion close failures propagate.
- Unit tests proving input pull failures win over same-side close failures.
- Unit tests proving repeated pulls after completion do not restart producers.
- Unit tests proving self-merge independently materializes both sides.
- RBS validation.
- RuboCop.

## Open Questions

None.
