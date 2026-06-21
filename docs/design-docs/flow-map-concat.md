# Flow.map_concat

## Status

Accepted

## Context

FiberStream has one-to-one transforms (`Flow.map`), transform-and-drop
behavior (`Flow.filter_map`), and source-level concatenation (`Source.concat`).
Users also need a synchronous one-to-many flow that expands each upstream value
into a finite or infinite enumerable and emits those expanded values in strict
concat order.

Governing documents:

- Product spec: `docs/product-specs/flow-map-concat.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Composable pipelines: `docs/design-docs/composable-pipelines.md`
- Related flow design: `docs/design-docs/flow-filter-map.md`
- Related source design: `docs/design-docs/source-concat.md`

## Goals

- Implement `Flow.map_concat` as a small private pull stage.
- Add `Source#map_concat` as the matching source convenience method.
- Preserve pull-driven backpressure, laziness, output order, and cleanup
  contracts.
- Enumerate one active block result at a time without eager `to_a`
  materialization.
- Keep validation and error behavior consistent with other block-based flows.

## Non-Goals

- Scheduler-backed expansion or prefetch.
- Parallel or unordered expansion.
- Ractor-backed expansion.
- Accepting `Source` values as inner streams.
- Resource ownership for block-returned enumerables.
- Recursive flattening.
- String splitting semantics.

## Proposed Design

`Flow.map_concat` validates that a block is present and stores an attach
callable that returns `Pull.map_concat(upstream, transform)`.

`Source#map_concat` delegates to
`Source#via(FiberStream::Flow.map_concat { ... })`. It does not introduce a
separate runtime path.

`Pull::MapConcat` is a synchronous linear pull stage. It owns:

- `upstream`, the pull stream before the stage
- `transform`, the mapping block
- `current_enumerator`, the active inner enumerator or `nil`
- `closed`, whether `close` has been called
- `done`, whether upstream completion has been fully observed

On each `next`:

1. Return `DONE` immediately if closed or done.
2. If an active inner enumerator exists, pull one value from it.
3. If the inner enumerator yields a value, return that value immediately.
4. If advancing the active external iterator raises `StopIteration`, treat that
   as expansion completion, clear it, and continue.
5. If no active inner enumerator remains, pull one upstream element.
6. If upstream returns `DONE`, mark done and return `DONE`.
7. Call the transform block with the upstream element.
8. Validate that the transform result responds to `#each`.
9. Convert the result to an external iterator with `result.to_enum(:each)`.
10. Store that enumerator as the active inner enumerator and loop to pull one
    value from it.

The loop allows one downstream demand to skip any number of empty expansions
before finding an output value or upstream completion. Once the stage has found
one output value, it returns without pulling the next upstream element. This is
the key backpressure contract: a downstream pull advances only far enough to
produce one downstream value or determine completion.

The stage keeps one active expansion at a time. It does not call `to_a`, does
not prefetch all values yielded by the active expansion, and does not pull the
next upstream element until the active expansion is exhausted. The returned
expansion object can still be large or infinite; callers control that object
and its memory/resource behavior.

`nil`, `false`, and other objects without `#each` raise `TypeError`. An input
that should emit no output must return an empty enumerable. This avoids
overloading falsey values as control signals and keeps `map_concat` distinct
from `filter_map`. Falsey values yielded by a valid expansion are normal output
values and are emitted.

The stage does not special-case `String`. On supported Ruby versions `String`
does not implement `#each`, so returning a string raises the same `TypeError`
as any other non-enumerable object. Callers can return `text.each_char`,
`text.each_line`, or another explicit enumerator when they want string-derived
expansion.

The stage does not recursively flatten yielded values. If the active expansion
yields arrays, hashes, enumerators, or sources, those yielded objects are
emitted unchanged.

The stage does not call `close` on active expansion objects, even when they
respond to `close`. `map_concat` accepts Ruby enumerable values, not owned
stream resources, and the API has no close-ownership option. This keeps the
first version small and avoids surprising side effects on caller-owned objects.
Callers should avoid returning open files, sockets, or other resource-owning
enumerables unless exhausting or abandoning them without an automatic close is
acceptable. A future source-valued flattening API can provide explicit resource
ownership if needed.

Transform exceptions, inner enumeration exceptions other than `StopIteration`
surfaced by the active external iterator, and upstream exceptions propagate as
stream failures. Any `StopIteration` raised while advancing the active external
iterator is treated as the current expansion's completion because Ruby's
external iterator protocol does not distinguish natural exhaustion from an
inner `#each` that raises `StopIteration` itself. Callers that need to signal
an inner enumeration failure should raise another exception type. The stage
does not close upstream directly on these paths; the materialized
`Source#run_with` cleanup path closes the chain and preserves existing
primary-error precedence. If a future internal caller uses the stage outside
`Source#run_with`, it remains responsible for closing the stream chain after
failure, matching other simple synchronous pull stages.

`close` is idempotent. It clears the active inner enumerator reference and
closes upstream. It does not attempt to drain the active expansion.

`Flow.map_concat` itself creates no fibers, queues, ractors, or scheduler
dependencies.

## Contracts

- `Flow.map_concat` requires a block and raises `ArgumentError` when missing.
- `Source#map_concat` requires a block through delegation and raises
  `ArgumentError` when missing.
- Construction is lazy.
- The block is called once per upstream element that is expanded.
- Block results must respond to `#each`; non-enumerable results raise
  `TypeError`.
- Block results must be enumerable through zero-argument `#each`.
- Block results do not need to include `Enumerable`; objects with a
  block-yielding `#each` are accepted.
- Falsey block results are invalid, not drop signals.
- Falsey values yielded by valid block results are emitted.
- Empty block results emit no output.
- `StopIteration` surfaced by the active external iterator is expansion
  completion.
- Output values are exactly the values yielded by the block result.
- Flattening is one level only.
- All values from one block result are emitted before the next upstream element
  is pulled.
- One downstream demand may pull multiple upstream elements only when earlier
  expansions are empty.
- A downstream demand that emits a value does not pull past that value.
- Output order follows upstream order and inner yield order.
- The stage retains at most one active block result/enumerator at a time.
- Infinite block results prevent later upstream pulls.
- The stage does not call `close` on block results.
- Closing the stage closes upstream and clears the active inner enumerator.
- Closing the stage while an inner enumerator is active does not drain it and
  does not pull later upstream elements.
- Closing the stage is idempotent.
- Transform, non-`StopIteration` inner enumeration, and upstream failures
  propagate from `Source#run_with`.
- After completion, later defensive pulls return `DONE` without pulling
  upstream.
- Public APIs never expose `Pull::DONE`.

Public API:

```ruby
class Flow[In, Out]
  def self.map_concat: [In, Out] () { (In) -> Enumerable[Out] } -> Flow[In, Out]
end

class Source[Elem]
  def map_concat: [Out] () { (Elem) -> Enumerable[Out] } -> Source[Out]
end
```

Internal API:

```ruby
Pull.map_concat(upstream, transform)
```

## Alternatives Considered

### Name The API `flat_map`

`flat_map` is familiar to Ruby users, but `map_concat` is more precise for
streaming because the stage fully emits one mapped collection before advancing
to the next upstream element. It also leaves room for future APIs with
different flattening or concurrency behavior.

### Implement With `Flow.map` And A Later Flattening Stage

Composing a one-to-one map with a generic flattening stage would introduce a
second public operation before the user-facing contract is clear. A dedicated
stage keeps the first API direct and avoids buffering or wrapper allocation
between two stages.

### Treat `nil` Or `false` As Empty

`Flow.filter_map` already uses falsey values as drop signals. `map_concat`
should require an enumerable result so invalid return values fail close to the
bug. Users can return `[]` when they want no output.

### Eagerly Convert Block Results With `to_a`

`to_a` would make indexing simple, but it would force full materialization of
large or lazy enumerables and weaken backpressure. External iteration through
`to_enum(:each)` keeps one-yield-at-a-time behavior.

### Automatically Close Inner Enumerables

Some enumerable objects are also resources, such as files. Automatically
calling `close` would help those cases, but the API has no way to distinguish
newly created resources from caller-owned shared objects. The first version
does not take ownership of block results. A separate source-valued flattening
operator or an explicit ownership option can be designed later if resource
flattening becomes a product requirement.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-13. Feedback resulted in
these changes:

- Clarified that `StopIteration` surfaced by the active external iterator is
  expansion completion, while other inner enumeration failures fail the stream.
- Added validation coverage for block results that define `#each` without
  including `Enumerable`.
- Added validation coverage for Hash block results and their emitted key-value
  pair shape.
- Added validation coverage for early sink completion while an inner expansion
  is active, including the no-drain and no-next-upstream-pull contract.
- Added explicit validation coverage for `Source#map_concat` missing-block
  behavior, yielded falsey output values, and block results whose `#each`
  requires arguments.

## Validation

- Unit tests for `Flow.map_concat` and `Source#map_concat` happy paths.
- Unit tests for empty expansions.
- Unit tests proving output order across upstream elements and inner
  enumerables.
- Unit tests proving one-level flattening.
- Unit tests proving construction laziness.
- Unit tests proving a downstream demand returns after one inner output without
  pulling the next upstream element.
- Unit tests proving a downstream demand can skip empty expansions.
- Unit tests for `Flow.map_concat` and `Source#map_concat` missing block
  validation.
- Unit tests for non-enumerable, `nil`, and `false` block results.
- Unit tests for custom block results that respond to `#each` without including
  `Enumerable`.
- Unit tests for yielded `nil` and `false` output values.
- Unit tests for Hash block results emitting key-value pairs through normal
  `Hash#each` behavior.
- Unit tests for block results whose `#each` requires arguments.
- Unit tests for `String` block results raising `TypeError` and explicit
  string enumerators working.
- Unit tests proving early sink completion while an inner expansion is active
  does not drain that expansion and does not pull the next upstream element.
- Unit tests for inner enumeration failures.
- Unit tests proving `StopIteration` surfaced by the active external iterator
  completes only the current expansion.
- Unit tests for transform failures and upstream failures.
- Unit tests proving close propagation after normal completion and early sink
  completion.
- Unit tests for cleanup close failure precedence.
- Unit tests proving repeated pulls after completion do not pull upstream
  again.
- RBS validation.
- RuboCop.

## Open Questions

None.
