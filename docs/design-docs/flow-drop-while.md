# Flow.drop_while

## Status

Draft

## Context

FiberStream now has predicate-based `Flow.take_while`, which emits a leading
prefix and then completes. `Flow.drop_while` is the complementary operation: it
drops a leading prefix while a predicate is truthy, emits the first falsey
element, then forwards the rest unchanged.

Unlike `take_while`, this flow does not terminate at the predicate boundary and
therefore does not close upstream when the predicate first fails.

## Goals

- Implement `Flow.drop_while` as a small private pull stage.
- Add `Source#drop_while` as the matching convenience method.
- Preserve pull-driven backpressure, ordering, and cleanup contracts.
- Keep validation consistent with other block-based flows.

## Non-Goals

- Predicate-based early completion.
- Multi-output or buffered stages.
- Async predicate execution.
- New public runtime abstractions.

## Proposed Design

`Flow.drop_while` validates that a block is present and stores an attach
callable that returns `Pull.drop_while(upstream, predicate)`.

`Pull::DropWhile` stores `@upstream`, `@predicate`, `@closed`, `@done`, and a
phase flag such as `@dropping`. On each `next`:

1. Return `DONE` immediately if closed or already done.
2. If no longer dropping, pull one upstream element and return it, or mark done
   and return `DONE` if upstream completed.
3. While dropping, pull upstream.
4. If upstream returns `DONE`, mark done and return `DONE`.
5. Call the predicate for the element.
6. If the predicate result is truthy, discard the element and continue the
   loop.
7. If the predicate result is `false` or `nil`, switch out of dropping phase
   and return that element unchanged.

The predicate is called only during the dropping phase. Once the first retained
element has been returned, later `next` calls are pass-through.

Predicate exceptions and upstream exceptions propagate as stream failures. The
stage does not close upstream inside `next` for predicate falsey results because
normal stream processing continues after that boundary. `Source#run_with`
continues to own materialized-chain cleanup and primary error precedence.

`close` is idempotent and propagates upstream.

## Contracts

- `Flow.drop_while` requires a block and raises `ArgumentError` when missing.
- `Flow.drop_while` drops only the leading prefix whose predicate results are
  truthy.
- `false` and `nil` stop dropping; all other Ruby values continue dropping.
- The first falsey-predicate element is emitted unchanged.
- The predicate is not called after the first retained element has been emitted.
- If upstream completes before a falsey predicate result, downstream completes
  normally.
- The first downstream demand may pull multiple upstream elements.
- After the first retained element has been emitted, each later downstream
  demand pulls at most one upstream element unless another composed stage pulls
  more.
- After completion, later defensive pulls return `DONE` without pulling
  upstream.
- Predicate and upstream failures propagate from `Source#run_with`.
- Predicate and upstream failures remain primary over later cleanup close
  failures from `Source#run_with`.
- `Source#drop_while` delegates to `Flow.drop_while` and returns a new `Source`.
- Public APIs never expose `Pull::DONE`.

Public API:

```ruby
class Flow[In, Out]
  def self.drop_while: [Elem] () { (Elem) -> boolish } -> Flow[Elem, Elem]
end

class Source[Elem]
  def drop_while: () { (Elem) -> boolish } -> Source[Elem]
end
```

Internal API:

```ruby
Pull.drop_while(upstream, predicate)
```

## Alternatives Considered

### Implement With `Flow.select`

`Flow.select` evaluates every element and drops every falsey result. `drop_while`
needs stateful prefix-only behavior and must stop calling the predicate after
the first retained element, so a dedicated stage is clearer.

### Close Upstream At The Predicate Boundary

Closing upstream when the predicate first fails would lose the retained element
and all later elements. Unlike `take_while`, `drop_while` continues the stream
after the boundary.

## Third-Party Review

Context-free design review found no blocking issues. It recommended clarifying
that cleanup close failures after otherwise successful completion or early sink
completion fail `run_with`, and keeping repeated pulls after completion framed
as an internal defensive behavior rather than a public consumption contract.
The product spec now records both clarifications.

## Validation

- Unit tests for dropping a truthy prefix, emitting the first false/nil
  predicate result, not calling the predicate after the boundary, upstream
  completion before retention, laziness, missing block validation, pull counts,
  defensive pulls after completion, close propagation, predicate failure
  propagation, cleanup close failure propagation, upstream failure propagation,
  and `Source#drop_while`.
- RBS validation for public signatures.
- Existing Minitest suite and RuboCop.

## Open Questions

None.
