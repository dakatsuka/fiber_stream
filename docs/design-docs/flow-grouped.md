# Flow.grouped

## Status

Draft

## Context

FiberStream already has simple linear pull stages such as `Flow.drop`,
`Flow.take_while`, and byte-oriented framing in `Flow.lines`. Users also need a
general batching operator that groups adjacent elements into arrays without
adding scheduler requirements or weakening backpressure.

Governing documents:

- Product spec: `docs/product-specs/flow-grouped.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Composable pipelines: `docs/design-docs/composable-pipelines.md`

## Goals

- Implement `Flow.grouped(count)` as a private pull stage.
- Add `Source#grouped(count)` as the matching source convenience method.
- Preserve pull-driven demand and ordering.
- Emit a final partial group on normal upstream completion.
- Bound internal retention to at most `count` elements.
- Keep validation and RBS shape consistent with existing fixed-count flows.

## Non-Goals

- Key-based grouping.
- Sliding, overlapping, time-based, or scheduler-backed windows.
- Dropping final partial groups.
- Aggregation or transformation inside each group.
- Public exposure of internal pull stages.

## Proposed Design

`Flow.grouped(count)` validates `count` and stores an attach callable that
returns `Pull.grouped(upstream, count)`.

Validation follows existing fixed-count flows: `count` is accepted only when
`count.is_a?(Integer)`, no coercion is performed, and the value must be
positive.

`Source#grouped(count)` delegates to
`Source#via(FiberStream::Flow.grouped(count))`. It does not introduce a
separate runtime path.

`Pull::Grouped` is a synchronous linear pull stage. It owns:

- `upstream`, the pull stream before the stage
- `count`, the positive group size
- `closed`, whether `close` has been called
- `done`, whether upstream completion has been fully consumed downstream

On each `next`:

1. Return `DONE` immediately if the stage is closed or done.
2. Allocate a new empty group array.
3. Pull upstream until the group size reaches `count` or upstream returns
   `DONE`.
4. If upstream returns `DONE` and the group is empty, mark done and return
   `DONE`.
5. If upstream returns `DONE` and the group has elements, mark done and return
   the partial group.
6. If the group reaches `count`, return the full group and keep the stage open
   for later downstream demand.

The stage does not read ahead beyond the group being emitted. A downstream pull
may pull up to `count` upstream elements. Between downstream pulls, the stage
retains no elements after emitting a group. During a pull, it retains at most
`count` elements in the group array being built. When a full group is emitted,
the stage does not pull upstream again in the same downstream demand to check
whether upstream has completed. If the upstream has exactly a multiple of
`count` elements, completion is observed on the next downstream pull.

There is no explicit maximum `count`; memory use is intentionally proportional
to `count`, and callers are responsible for choosing a practical value. The
stage should allocate an empty group array and append elements as they arrive,
not eagerly allocate `count` element slots as a separate resource reservation.

When upstream raises while a group is being collected, the exception propagates
and the partially collected group is discarded. The stage does not close
upstream directly on this path; the materialized `Source#run_with` cleanup path
closes the chain and preserves existing error precedence. If a future internal
caller uses the stage outside `Source#run_with`, it remains responsible for
closing the stream chain after failure, matching other simple synchronous pull
stages.

`close` is idempotent and propagates upstream. `Flow.grouped` itself does not
create fibers, queues, or ractors and does not require `Fiber.scheduler`.

The grouping array is not reused. Returning a distinct `Array` for each group
keeps downstream mutation from affecting later groups or internal stage state.
Element values themselves are not copied, frozen, or transformed.

## Contracts

- `Flow.grouped(count)` returns `Flow[Elem, Array[Elem]]`.
- `Source#grouped(count)` returns `Source[Array[Elem]]`.
- `count` must be a positive `Integer`.
- `count` validation uses `is_a?(Integer)` and performs no coercion.
- Non-Integer counts raise `TypeError`.
- Zero or negative counts raise `ArgumentError`.
- Construction is lazy.
- A downstream pull may pull up to `count` upstream elements.
- Full groups contain exactly `count` elements.
- Full-group emission does not probe upstream for completion during the same
  downstream pull.
- Normal upstream completion emits one final partial group when one exists.
- Normal upstream completion with no buffered elements emits no empty group.
- `grouped(1)` emits each upstream element wrapped in a one-element array.
- The stage preserves upstream order.
- Each emitted group is a distinct mutable `Array`.
- The stage retains at most `count` elements.
- Upstream failures propagate and suppress any partially collected group.
- Closing the stage closes upstream.
- Closing the stage is idempotent.
- After completion, later defensive pulls return `DONE` without pulling
  upstream.
- Public APIs never expose `Pull::DONE`.

Public API:

```ruby
class Flow[In, Out]
  def self.grouped: [Elem] (Integer count) -> Flow[Elem, Array[Elem]]
end

class Source[Elem]
  def grouped: (Integer count) -> Source[Array[Elem]]
end
```

Internal API:

```ruby
Pull.grouped(upstream, count)
```

## Alternatives Considered

### Implement With `Flow.map`

`Flow.map` cannot emit one output for multiple input elements, so it cannot
express batching while preserving the current one-pull-at-a-time stream
contract.

### Implement With A Scheduler-Backed Buffer

A buffer could collect groups in a producer fiber, but fixed-size grouping does
not need prefetch or concurrency. A synchronous pull stage keeps the dependency
surface small and makes backpressure precise.

### Drop The Final Partial Group

Dropping the final partial group can be useful for some protocols, but it loses
data by default. Emitting the partial group matches Ruby's `Enumerable#each_slice`
behavior and is the safer default for a general-purpose stream operator.

### Reuse One Array For All Groups

Array reuse could reduce allocations, but it would make downstream mutation
surprising and unsafe. Distinct arrays provide a clearer public contract.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-05. Feedback resulted in these
changes:

- Clarified that exact full-group emission must not over-pull to probe for
  upstream completion.
- Clarified `count` validation as `is_a?(Integer)` with no coercion.
- Clarified materialized-stage close terminology.
- Clarified proportional memory behavior for large `count` values.
- Added validation coverage for cleanup after upstream failure and exact
  multiple completion.

## Validation

- Unit tests for validation errors.
- Unit tests for lazy construction.
- Unit tests for exact groups and final partial groups.
- Unit tests proving exact full groups do not pull upstream again until the
  next downstream demand.
- Unit tests for empty upstream completion.
- Unit tests for `grouped(1)`.
- Unit tests proving emitted groups are distinct arrays.
- Unit tests proving element identity is preserved.
- Unit tests proving a downstream pull consumes at most `count` upstream
  elements.
- Unit tests proving upstream failures discard partial groups and propagate.
- Unit tests proving `Source#run_with` closes upstream after an upstream
  failure observed by `grouped`, with primary failure precedence over close
  failure.
- Unit tests for close propagation and repeated pulls after completion.
- Unit tests for `Source#grouped`.
- RBS validation.
- RuboCop.

## Open Questions

None.
