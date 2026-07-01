# Sink.all?

## Status

Accepted

## Problem

Users need a terminal stream operation that answers whether every upstream
element matches a predicate without accumulating the stream or consuming
elements after the answer is known. Today this can be approximated with
`Flow.reject { ... }` followed by `Sink.first`, or with `Sink.fold`, but those
forms obscure the universal predicate intent and make early completion less
discoverable than `Sink.any?`.

## Goals

- Add `FiberStream::Sink.all? { |element| ... }`.
- Return `false` when the first upstream element with a false or nil predicate
  result is found.
- Return `true` when upstream completes without any falsey predicate result.
- Return `true` for empty upstream.
- Stop pulling upstream as soon as the result is known.
- Preserve existing lazy construction, failure propagation, and cleanup
  behavior.
- Keep the API synchronous and scheduler-independent.

## Non-Goals

- Ruby `Enumerable#all?`'s no-block truthiness form.
- Pattern matching, argument-based matching, or callable matcher arguments.
- Returning the non-matching element or predicate result.
- Concurrent or asynchronous predicate evaluation.
- Retry, timeout, batching, or rate-limit policies.

## Requirements

- `FiberStream::Sink.all? { |element| ... }` creates a sink that tests upstream
  elements with the provided predicate block.
- `Sink.all?` requires a block.
- Missing blocks raise `ArgumentError`.
- Constructing `Sink.all?` does not pull upstream or call the block.
- During materialization, `Sink.all?` pulls one upstream element at a time and
  calls the block with that element until a predicate result is falsey or
  upstream completes.
- Stream completion is detected with FiberStream's existing private completion
  sentinel identity semantics. `Sink.all?` must not expose the private
  completion sentinel through its public return value.
- The predicate block is called in input order.
- Predicate results use normal Ruby truthiness: only `false` and `nil` are
  falsey results.
- When the predicate result is false or nil, `Sink.all?` returns `false`.
- `Sink.all?` returns a boolean value, not the original stream element and not
  the predicate result.
- After a falsey predicate result, `Sink.all?` stops pulling new upstream
  elements.
- If upstream completes before any falsey predicate result, `Sink.all?` returns
  `true`.
- If upstream is empty, `Sink.all?` returns `true`.
- `Sink.all?` may return `false` for `nil` or `false` stream elements when the
  predicate result for that element is falsey.
- `Sink.all?` does not store consumed elements.
- If upstream fails, the upstream failure is re-raised from `Source#run_with`.
- If the predicate block raises, the block failure is re-raised from
  `Source#run_with`.
- If the predicate block raises for an element, `Sink.all?` stops pulling new
  upstream elements.
- `Source#run_with` closes the materialized stream after normal completion,
  upstream failure, predicate failure, or early falsey result.
- If cleanup close fails after an otherwise successful `true` or `false`
  result, `Source#run_with` raises the close failure.
- If cleanup close fails after an upstream failure or predicate failure, the
  upstream or predicate failure remains primary and the close failure is
  suppressed.
- `Sink.all?` does not require `Fiber.scheduler`.
- `Sink.all?` does not introduce internal fibers, queues, or buffering.

## Public Contracts

```ruby
FiberStream::Sink.all? { |element| truthy_or_falsey }
```

Initial RBS shape:

```rbs
module FiberStream
  class Sink[In, Mat]
    def self.all?: [Elem] () { (Elem) -> boolish } -> Sink[Elem, bool]
  end
end
```

## Examples

```ruby
all_valid =
  FiberStream::Source.each(records)
    .run_with(
      FiberStream::Sink.all? do |record|
        record.valid?
      end
    )

all_valid # => true or false
```

`Sink.all?` evaluates the predicate directly and returns a boolean answer:

```ruby
all_positive =
  FiberStream::Source.each([1, 2, 3])
    .run_with(FiberStream::Sink.all?(&:positive?))

all_positive # => true
```

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-07-01. Feedback resulted in these
changes:

- Updated `docs/design-docs/linear-pull-runtime.md` so the central runtime
  design also records `Sink.all?` sentinel handling, cleanup precedence,
  scheduler independence, and validation/test scope.

## Open Questions

None.
