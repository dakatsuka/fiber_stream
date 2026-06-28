# Sink.any?

## Status

Accepted

## Problem

Users need a terminal stream operation that answers whether any upstream element
matches a predicate without accumulating the stream or consuming elements after
the answer is known. Today this can be approximated with `Sink.find { ... }` and
checking the result, but that is ambiguous when matching stream elements can be
`nil` or `false`. A boolean sink makes existential checks explicit and avoids
encoding boolean intent through value-returning search.

## Goals

- Add `FiberStream::Sink.any? { |element| ... }`.
- Return `true` when the first upstream element with a truthy predicate result
  is found.
- Return `false` when upstream completes before any predicate result is truthy.
- Stop pulling upstream as soon as the result is known.
- Preserve existing lazy construction, failure propagation, and cleanup
  behavior.
- Keep the API synchronous and scheduler-independent.

## Non-Goals

- Ruby `Enumerable#any?`'s no-block truthiness form.
- Pattern matching, argument-based matching, or callable matcher arguments.
- Returning the matching element or predicate result.
- Concurrent or asynchronous predicate evaluation.
- Retry, timeout, batching, or rate-limit policies.

## Requirements

- `FiberStream::Sink.any? { |element| ... }` creates a sink that tests upstream
  elements with the provided predicate block.
- `Sink.any?` requires a block.
- Missing blocks raise `ArgumentError`.
- Constructing `Sink.any?` does not pull upstream or call the block.
- During materialization, `Sink.any?` pulls one upstream element at a time and
  calls the block with that element until a predicate result is truthy or
  upstream completes.
- Stream completion is detected with FiberStream's existing private completion
  sentinel identity semantics. `Sink.any?` must not expose the private
  completion sentinel through its public return value.
- The predicate block is called in input order.
- Predicate results use normal Ruby truthiness: only `false` and `nil` are
  non-matching results.
- When the predicate result is truthy, `Sink.any?` returns `true`.
- `Sink.any?` returns a boolean value, not the original stream element and not
  the predicate result.
- After a truthy predicate result, `Sink.any?` stops pulling new upstream
  elements.
- If upstream completes before any predicate result is truthy, `Sink.any?`
  returns `false`.
- If upstream is empty, `Sink.any?` returns `false`.
- `Sink.any?` may return `true` for `nil` or `false` stream elements when the
  predicate result for that element is truthy.
- `Sink.any?` does not store consumed elements.
- If upstream fails, the upstream failure is re-raised from `Source#run_with`.
- If the predicate block raises, the block failure is re-raised from
  `Source#run_with`.
- If the predicate block raises for an element, `Sink.any?` stops pulling new
  upstream elements.
- `Source#run_with` closes the materialized stream after normal completion,
  upstream failure, predicate failure, or early truthy result.
- If cleanup close fails after an otherwise successful `true` or `false`
  result, `Source#run_with` raises the close failure.
- If cleanup close fails after an upstream failure or predicate failure, the
  upstream or predicate failure remains primary and the close failure is
  suppressed.
- `Sink.any?` does not require `Fiber.scheduler`.
- `Sink.any?` does not introduce internal fibers, queues, or buffering.

## Public Contracts

```ruby
FiberStream::Sink.any? { |element| truthy_or_falsey }
```

Initial RBS shape:

```rbs
module FiberStream
  class Sink[In, Mat]
    def self.any?: [Elem] () { (Elem) -> boolish } -> Sink[Elem, bool]
  end
end
```

## Examples

```ruby
has_error =
  FiberStream::Source.each(events)
    .run_with(
      FiberStream::Sink.any? do |event|
        event.status == :error
      end
    )

has_error # => true or false
```

`Sink.any?` evaluates the predicate directly and returns a boolean answer,
which avoids the ambiguity of converting `Sink.find` results when matching
elements can be `nil` or `false`:

```ruby
contains_nil =
  FiberStream::Source.each([nil, 1, 2])
    .run_with(FiberStream::Sink.any?(&:nil?))

contains_nil # => true
```

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-28. Feedback resulted in these
changes:

- Reworded the example text to avoid implying that `Sink.any?` can be
  implemented as `!!Sink.find`, which would be incorrect for matching `nil` or
  `false` stream elements.

## Open Questions

None.
