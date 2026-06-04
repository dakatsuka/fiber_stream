# Flow.drop

## Status

Draft

## Context

The linear pull runtime already includes `Flow.take(count)`, which completes
after forwarding a fixed number of elements. `Flow.drop(count)` is the
dual fixed-count operation: it consumes and discards a leading prefix, then
passes the remainder through unchanged.

The operation should remain a simple 1-in/1-out linear stage. It should not add
queues, scheduler requirements, or new cleanup ownership.

## Goals

- Implement `Flow.drop(count)` as a private pull stage.
- Add `Source#drop(count)` as the matching convenience method.
- Preserve pull-driven demand and ordering.
- Keep validation and RBS shape consistent with `Flow.take(count)`.

## Non-Goals

- Predicate-based dropping.
- Windowing, grouping, or batching.
- Asynchronous prefetching.
- Public exposure of internal pull stages.

## Proposed Design

`Flow.drop(count)` validates `count` and stores an attach callable that returns
`Pull.drop(upstream, count)`.

`Pull::Drop` keeps an integer `@remaining` initialized to `count`, plus
`@closed` and `@done` flags. On each `next`:

1. Return `DONE` immediately if closed or already done.
2. While `@remaining` is positive, pull upstream.
3. If upstream returns `DONE` while dropping, mark done and return `DONE`.
4. Otherwise discard the value and decrement `@remaining`.
5. Once `@remaining` reaches zero, pull one upstream value for the current
   downstream demand and return it, or mark done and return `DONE` if upstream
   completed.

For `drop(0)`, the loop is skipped and the stage pulls one upstream value for
each downstream demand. The stage does not proactively close upstream after the
prefix is dropped because it still needs the remainder of the stream.

`close` is idempotent and propagates upstream. `Source#run_with` remains
responsible for closing the materialized chain after normal completion,
failure, or early sink completion.

## Contracts

- `Flow.drop(count)` raises `TypeError` for non-Integer counts.
- `Flow.drop(count)` raises `ArgumentError` for negative counts.
- `Flow.drop(0)` behaves as pass-through.
- `Flow.drop(count)` emits no dropped element.
- `Flow.drop(count)` preserves order for retained elements.
- The first downstream demand may pull up to `count + 1` upstream elements.
- Later downstream demands pull at most one upstream element after the prefix is
  dropped.
- If upstream completes during the dropped prefix, downstream completes.
- After completion, later defensive pulls return `DONE` without pulling
  upstream.
- `Source#drop(count)` delegates to `Flow.drop(count)` and returns a new
  `Source`.
- Public APIs never expose `Pull::DONE`.

Public API:

```ruby
class Flow[In, Out]
  def self.drop: [Elem] (Integer count) -> Flow[Elem, Elem]
end

class Source[Elem]
  def drop: (Integer count) -> Source[Elem]
end
```

Internal API:

```ruby
Pull.drop(upstream, count)
```

## Alternatives Considered

### Implement With `Flow.select`

A closure around `Flow.select` could count elements and drop the prefix, but it
would make a core fixed-count operation depend on user-block flow mechanics and
would obscure the backpressure contract. A dedicated stage is clearer and
matches `Flow.take`.

### Close Upstream After Dropping The Prefix

Closing after the prefix would discard the retained remainder, so it is not a
valid implementation. Unlike `take`, `drop` is not an early-completion flow once
the prefix has been skipped.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-05. Feedback resulted in these
changes:

- Clarified that the stage marks itself done when upstream completes after the
  dropped prefix.
- Clarified upstream failure propagation and cleanup error precedence.

## Validation

- Unit tests for dropping prefixes, `drop(0)`, count greater than stream length,
  laziness, validation, repeated pulls after completion, backpressure pull
  counts, close propagation, and `Source#drop`.
- RBS validation for the new public signatures.
- Existing Minitest suite and RuboCop.

## Open Questions

None.
