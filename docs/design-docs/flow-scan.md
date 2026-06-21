# Flow.scan

## Status

Accepted

## Context

FiberStream already provides terminal accumulation with
`Sink.fold(initial) { |accumulator, element| ... }`, which consumes the full
stream and returns only the final accumulator. Users also need the intermediate
accumulator values that `fold` computes along the way. That pattern appears in
running totals, finite-state updates, and progress counters.

`Flow.scan` should be a simple synchronous linear pull stage. It should not add
queues, scheduler requirements, or read-ahead beyond one immediate upstream
pull per downstream demand.

Governing documents:

- Product spec: `docs/product-specs/flow-scan.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Related terminal API: `docs/product-specs/minimum-linear-pipeline.md`
- Composable pipelines: `docs/design-docs/composable-pipelines.md`

## Goals

- Implement `Flow.scan(initial, &block)` as a private pull stage.
- Add `Source#scan(initial, &block)` as the matching source convenience method.
- Mirror `Sink.fold` reducer semantics while emitting intermediate accumulators.
- Preserve pull-driven demand and ordering.
- Retain at most one accumulator reference between downstream pulls.
- Keep validation and RBS shape consistent with `Sink.fold`.

## Non-Goals

- `reduce` without an explicit initial accumulator.
- Emitting the initial accumulator before the first upstream element.
- Windowed, keyed, or grouped scan variants.
- Parallel or scheduler-backed scan stages.
- Public exposure of internal pull stages.

## Proposed Design

`Flow.scan(initial, &block)` validates the block, stores `initial` and the
reducer, and returns a flow whose attach callable builds `Pull.scan(upstream,
initial, reducer)`.

`Source#scan(initial, &block)` delegates to
`Source#via(FiberStream::Flow.scan(initial, &block))`. It does not introduce a
separate runtime path.

`Pull::Scan` is a synchronous linear pull stage. It owns:

- `upstream`, the pull stream before the stage
- `initial`, the starting accumulator assigned directly without duplication
- `reducer`, the `(accumulator, element) -> accumulator` callable
- `accumulator`, the current accumulator state
- `closed`, whether `close` has been called
- `done`, whether upstream completion has been fully consumed downstream

On initialization, `accumulator` is set to `initial`.

On each `next`:

1. Return `DONE` immediately if the stage is closed or done.
2. Pull one upstream value.
3. If upstream returns `DONE`, mark done and return `DONE`.
4. Otherwise call `reducer` with the current accumulator and the upstream
   element.
5. Assign the block result to `accumulator`.
6. Return `accumulator`.

If `upstream.next` raises, the exception propagates and no accumulator is
emitted for that pull. The stage does not mark itself `done` on failure.

The stage never emits `initial` before the first upstream element. That keeps
empty-stream behavior aligned with other element-producing flows and preserves
this invariant when upstream produces at least one element:

```text
last emitted scan value == Sink.fold(initial, reducer) over same input sequence
```

When upstream is empty, scan emits nothing while `Sink.fold` still returns
`initial`. That difference is intentional: `initial` is starting state, not a
stream element.

Each downstream pull causes at most one immediate upstream pull and at most one
reducer invocation. If that immediate upstream is another composed stage, that
stage keeps its own pull and run-ahead contract. `Flow.scan` does not buffer
upstream elements or emitted accumulators.

When the reducer raises, the exception propagates and no accumulator is emitted
for the failing element. The stage does not close upstream directly on this
path; the materialized `Source#run_with` cleanup path closes the chain and
preserves existing error precedence.

`close` is idempotent and propagates upstream. `Flow.scan` itself does not
create fibers, queues, or ractors and does not require `Fiber.scheduler`.

## Contracts

- `Flow.scan(initial, &block)` returns `Flow[Elem, Acc]`.
- `Source#scan(initial, &block)` returns `Source[Acc]`.
- `Flow.scan` requires a block.
- `Source#scan` requires a block.
- Missing scan blocks raise `ArgumentError`.
- Construction is lazy.
- Each downstream pull issues at most one immediate upstream pull. Earlier
  composed stages keep their own pull and run-ahead contracts.
- Each normal upstream element produces exactly one emitted accumulator.
- The initial accumulator is not emitted.
- Empty upstream completion emits no values.
- For non-empty upstream, the final emitted accumulator equals the `Sink.fold`
  result over the same input sequence and reducer.
- FiberStream assigns `initial` directly; it does not duplicate or freeze it.
- The reducer may return the same accumulator object after mutating it in place.
- When a downstream sink retains every emission, reused mutable accumulator
  objects are aliased by identity rather than copied into historical snapshots.
- If the reducer mutates an accumulator in place and then raises, FiberStream
  does not roll back that mutation.
- The stage retains at most one accumulator reference.
- Upstream pull failures propagate without emitting an accumulator for the
  failing pull.
- Reducer failures propagate without emitting a value for the failing element.
- On failure paths, `run_with` cleanup preserves existing primary-failure-over-
  close-failure precedence.
- `Flow.scan` composes through `Flow#via`, `Source#via`, and pipeline builders
  without a separate runtime path.
- Closing the stage closes upstream.
- Closing the stage is idempotent.
- After completion, later defensive pulls return `DONE` without pulling
  upstream.
- Public APIs never expose `Pull::DONE`.

Public API:

```ruby
class Flow[In, Out]
  def self.scan: [Elem, Acc] (Acc initial) { (Acc, Elem) -> Acc } -> Flow[Elem, Acc]
end

class Source[Elem]
  def scan: [Acc] (Acc initial) { (Acc, Elem) -> Acc } -> Source[Acc]
end
```

Internal API:

```ruby
Pull.scan(upstream, initial, reducer)
```

## Alternatives Considered

### Emit The Initial Accumulator First

Haskell `scanl` and some collection APIs emit the seed value before processing
the first element. That makes empty streams produce one value (`initial`), which
differs from `Sink.fold` element count and from common reactive-stream `scan`
behavior. Deferring emission until after the first upstream element keeps the
stream element count equal to the upstream element count and makes
`running.last` match `fold` for non-empty streams.

### Implement With `Sink.fold` Side Effects

A fold block could push intermediate values into an external array, but that
breaks composability, hides backpressure, and cannot be chained through `Flow`
stages.

### Name The Operator `running_fold`

`scan` is the conventional stream-processing name and pairs naturally with
`fold` in functional pipelines. `running_fold` is more explicit but longer and
less familiar in stream DSLs.

### Add `emit_initial:` In The First Version

An option would let callers choose seed emission, but it expands the public
surface before there is a concrete requirement. A later additive option can be
considered if users need Haskell-style `scanl` behavior.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-07. Assessment: approve with
changes. Feedback resulted in these updates:

- Clarified reducer argument order as `(accumulator, element)` to match
  `Sink.fold`.
- Added upstream failure propagation and `run_with` error-precedence
  requirements.
- Qualified per-stage pull budget wording for composed pipelines.
- Documented mutable-accumulator aliasing when sinks retain every emission.
- Documented no rollback of in-place mutation on reducer failure.
- Expanded validation coverage for upstream and reducer failure precedence and
  mutable accumulator materialization.

## Validation

- Unit tests for missing-block validation on `Flow.scan` and `Source#scan`.
- Unit tests for lazy construction.
- Unit tests for running totals and state updates.
- Unit tests proving one downstream pull causes at most one immediate upstream
  pull from `Flow.scan`.
- Unit tests proving empty upstream emits nothing.
- Unit tests proving the final emitted accumulator matches `Sink.fold`.
- Unit tests proving reducer failures propagate and close the flow chain.
- Unit tests proving upstream failures propagate.
- Unit tests proving upstream failure takes precedence over cleanup close
  failure.
- Unit tests proving reducer failure takes precedence over cleanup close
  failure.
- Unit tests proving in-place mutable accumulators emit the same object
  identity each step and that `Sink.to_a` retains aliased references.
- Unit tests for `Source#scan`.
- RBS validation.
- RuboCop.

## Open Questions

None.
