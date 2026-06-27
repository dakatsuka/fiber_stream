# Sink.find

## Status

Accepted

## Problem

Users need a terminal stream operation that searches for the first element
matching a predicate without accumulating the stream or consuming elements after
the match. Today this can be approximated with `Flow.select { ... }` followed
by `Sink.first`, but that splits one terminal intent across a flow and a sink
and makes predicate search less discoverable than other common sink operations.

## Goals

- Add `FiberStream::Sink.find { |element| ... }`.
- Return the first upstream element whose predicate result is truthy.
- Stop pulling upstream as soon as a match is found.
- Preserve existing lazy construction, failure propagation, and cleanup
  behavior.
- Keep the API synchronous and scheduler-independent.

## Non-Goals

- Pattern matching, argument-based matching, or Ruby `Enumerable#find`'s
  optional `ifnone` callable.
- A distinct result wrapper that disambiguates "matched nil" from "not found".
- Returning an enumerator when no block is provided.
- Concurrent or asynchronous predicate evaluation.
- Retry, timeout, batching, or rate-limit policies.

## Requirements

- `FiberStream::Sink.find { |element| ... }` creates a sink that searches
  upstream elements with the provided predicate block.
- `Sink.find` requires a block.
- Missing blocks raise `ArgumentError`.
- Constructing `Sink.find` does not pull upstream or call the block.
- During materialization, `Sink.find` pulls one upstream element at a time and
  calls the block with that element until a match is found or upstream
  completes.
- Stream completion is detected with FiberStream's existing private completion
  sentinel identity semantics. `Sink.find` must not expose the private
  completion sentinel through its public return value.
- The predicate block is called in input order.
- Predicate results use normal Ruby truthiness: only `false` and `nil` are
  non-matching results.
- When the predicate result is truthy, `Sink.find` returns the original stream
  element, not the predicate result.
- After a match is found, `Sink.find` stops pulling new upstream elements.
- If upstream completes before a match is found, `Sink.find` returns `nil`.
- If upstream is empty, `Sink.find` returns `nil`.
- `Sink.find` may return `nil` when the matching stream element itself is
  `nil`; this intentionally matches Ruby's `Enumerable#find` ambiguity.
- `Sink.find` may return `false` when the matching stream element itself is
  `false` and the predicate result is truthy.
- `Sink.find` does not store consumed elements.
- If upstream fails, the upstream failure is re-raised from `Source#run_with`.
- If the predicate block raises, the block failure is re-raised from
  `Source#run_with`.
- If the predicate block raises for an element, `Sink.find` stops pulling new
  upstream elements.
- `Source#run_with` closes the materialized stream after normal completion,
  upstream failure, predicate failure, or early match.
- If cleanup close fails after an otherwise successful match or no-match
  completion, `Source#run_with` raises the close failure.
- If cleanup close fails after an upstream failure or predicate failure, the
  upstream or predicate failure remains primary and the close failure is
  suppressed.
- `Sink.find` does not require `Fiber.scheduler`.
- `Sink.find` does not introduce internal fibers, queues, or buffering.

## Public Contracts

```ruby
FiberStream::Sink.find { |element| truthy_or_falsey }
```

Initial RBS shape:

```rbs
module FiberStream
  class Sink[In, Mat]
    def self.find: [Elem] () { (Elem) -> boolish } -> Sink[Elem, Elem?]
  end
end
```

## Examples

```ruby
first_error =
  FiberStream::Source.each(events)
    .run_with(
      FiberStream::Sink.find do |event|
        event.status == :error
      end
    )

first_error # => first matching event, or nil
```

`Sink.find` is equivalent to selecting matching values and taking the first
one, but expresses the terminal search directly:

```ruby
found =
  FiberStream::Source.each(records)
    .run_with(FiberStream::Sink.find { |record| record.id == target_id })
```

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-27. Feedback resulted in these
changes:

- Added an explicit private completion sentinel non-exposure requirement.
- Clarified cleanup close failure precedence after early match, no-match
  completion, upstream failure, and predicate failure.

## Open Questions

None.
