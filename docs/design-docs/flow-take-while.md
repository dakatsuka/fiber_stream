# Flow.take_while

## Status

Accepted

## Context

FiberStream already has two related 1-in/1-out flows:

- `Flow.select`, which keeps matching elements anywhere in the stream.
- `Flow.take(count)`, which forwards a fixed number of elements and then closes
  upstream.

`Flow.take_while` combines the predicate shape of `select` with the
early-completion behavior of `take`: emit only the leading prefix while the
predicate is truthy, then complete and close upstream.

## Goals

- Implement `Flow.take_while` as a small private pull stage.
- Add `Source#take_while` as the matching convenience method.
- Preserve pull-driven backpressure, ordering, and cleanup contracts.
- Keep validation consistent with other block-based flows.

## Non-Goals

- Predicate-based dropping.
- Multi-output or buffered stages.
- Async predicate execution.
- New public runtime abstractions.

## Proposed Design

`Flow.take_while` validates that a block is present and stores an attach
callable that returns `Pull.take_while(upstream, predicate)`.

`Pull::TakeWhile` stores `@upstream`, `@predicate`, `@closed`, and `@done`.
On each `next`:

1. Return `DONE` immediately if closed or already done.
2. Pull one upstream element.
3. If upstream returns `DONE`, mark done and return `DONE`.
4. Call the predicate for the element.
5. If the predicate result is truthy, return the element unchanged.
6. If the predicate result is `false` or `nil`, mark done, close upstream
   during the same pull, and return `DONE` without emitting the element when
   close succeeds.

The terminating element is intentionally consumed but not emitted. Closing
upstream at the predicate boundary mirrors `Flow.take` reaching its limit and
allows upstream resources, async boundaries, buffers, and producer stages to
stop promptly.

If upstream close fails on that falsey-predicate path, the close failure is the
stream failure for that pull. The stage has already consumed the terminating
element and marked itself done, so later defensive pulls return `DONE` without
pulling upstream again.

Predicate exceptions propagate as stream failures. Because the stage has not
decided to terminate normally when the predicate raises, it does not close
upstream inside `next`; `Source#run_with` closes the materialized chain through
its existing ensure path, preserving primary error precedence.

`close` is idempotent and propagates upstream.

## Contracts

- `Flow.take_while` requires a block and raises `ArgumentError` when missing.
- `Flow.take_while` emits only the leading prefix whose predicate results are
  truthy.
- `false` and `nil` terminate the stream; all other Ruby values continue it.
- The terminating element is not emitted.
- The stage closes upstream during the pull that observes the first falsey
  predicate result.
- If upstream completes before a falsey predicate result, downstream completes
  normally.
- Each downstream demand makes at most one immediate upstream pull before
  completion. Earlier upstream stages keep their own pull and bounded run-ahead
  contracts.
- After completion, later defensive pulls return `DONE` without pulling
  upstream.
- Predicate and upstream failures propagate from `Source#run_with`.
- A close failure while closing upstream after a falsey predicate result
  propagates instead of normal completion.
- Predicate and upstream failures remain primary over later cleanup close
  failures from `Source#run_with`.
- `Source#take_while` delegates to `Flow.take_while` and returns a new `Source`.
- Public APIs never expose `Pull::DONE`.

Public API:

```ruby
class Flow[In, Out]
  def self.take_while: [Elem] () { (Elem) -> boolish } -> Flow[Elem, Elem]
end

class Source[Elem]
  def take_while: () { (Elem) -> boolish } -> Source[Elem]
end
```

Internal API:

```ruby
Pull.take_while(upstream, predicate)
```

## Alternatives Considered

### Implement With `Flow.select`

`Flow.select` does not complete at the first falsey predicate result, so it
cannot express `take_while` without extra mutable state and artificial
completion behavior. A dedicated stage keeps termination and close behavior
explicit.

### Implement With Exceptions For Termination

Using an exception to leave the stream when the predicate fails would put normal
completion on the failure path. The existing pull sentinel keeps completion on
the normal path.

## Third-Party Review

Context-free review found no issue with the pull-model shape, but identified
one blocking contract gap: close failure during the falsey-predicate transition
was not specified. The design now states that successful close yields normal
completion, while close failure on that transition fails the stream. The review
also clarified that the backpressure guarantee is one immediate upstream pull
per downstream demand, and moved repeated-pull behavior out of the public
product contract and into internal validation.

## Validation

- Unit tests for prefix retention, terminating element suppression, false and
  nil termination, upstream completion before termination, laziness, missing
  block validation, pull counts, defensive pulls after completion, upstream
  close on predicate failure, predicate-transition close failure propagation,
  predicate failure precedence over cleanup close failure, upstream failure
  precedence over cleanup close failure, upstream failure propagation, and
  `Source#take_while`.
- RBS validation for public signatures.
- Existing Minitest suite and RuboCop.

## Open Questions

None.
