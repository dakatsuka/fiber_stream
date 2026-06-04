# Sink.foreach

## Status

Accepted

## Context

FiberStream already has terminal sinks that collect all values (`Sink.to_a`),
return the first value (`Sink.first`), fold values into an accumulator
(`Sink.fold`), and write byte chunks to IO (`Sink.io`). Streaming workflows also
need a clear terminal operation for per-element side effects where retaining
values or threading a dummy accumulator through `Sink.fold` is not the user's
intent.

Relevant existing design:

- `docs/design-docs/linear-pull-runtime.md`
- `docs/product-specs/sink-foreach.md`

## Goals

- Add a small synchronous side-effect sink.
- Preserve the linear pull runtime and existing cleanup semantics.
- Keep implementation close to the existing `Sink.fold` loop.
- Avoid introducing scheduler requirements, fibers, queues, or buffering.

## Non-Goals

- Concurrent terminal sinks.
- Batching, retry, timeout, or delivery guarantees for side effects.
- A resource-owning sink abstraction.
- Changing `Sink.fold` semantics.

## Proposed Design

`Sink.foreach` is a public `Sink` constructor implemented in
`lib/fiber_stream/sink.rb` beside `Sink.fold`.

```ruby
def self.foreach(&block)
  raise ArgumentError, "missing block" unless block

  new do |stream|
    count = 0

    loop do
      value = stream.next
      break if Pull.done?(value)

      block.call(value)
      count += 1
    end

    count
  end
end
```

Construction is lazy because the method only captures the block and returns a
builder. Materialization happens through the existing private `Sink#run`
callable and `Source#run_with` cleanup path.

The sink increments its count only after the user block returns successfully.
If the block raises, the count is not returned and the failure propagates out of
`Source#run_with`. `run_with` closes the materialized pull chain in its existing
`ensure` block, so upstream resources and boundaries see the same cleanup
behavior as they do for `Sink.fold` failures.

`Sink.foreach` never asks upstream for another element until the previous block
call has completed. It therefore preserves the linear pull backpressure
invariant and does not create additional buffering.

## Contracts

- `Sink.foreach` requires a block and raises `ArgumentError` when missing.
- `Sink.foreach` returns `Sink[Elem, Integer]`.
- `Sink.foreach` consumes all upstream elements on success.
- `Sink.foreach` calls the block exactly once per consumed element, in order.
- `Sink.foreach` returns the number of elements whose block completed
  successfully.
- `Sink.foreach` returns `0` for empty upstream.
- User block exceptions propagate out of `Source#run_with`.
- After a user block exception, no later upstream elements are pulled by the
  sink.
- Existing `Source#run_with` cleanup closes the materialized stream after
  success or failure.
- `Sink.foreach` does not require `Fiber.scheduler`.
- `Sink.foreach` does not start fibers, allocate queues, or buffer elements.

## Alternatives Considered

### Use Sink.fold

`Sink.fold(0) { |count, element| block.call(element); count + 1 }` can implement
the behavior, but it makes side effects look like accumulator logic and invites
users to choose arbitrary accumulator values. A dedicated sink communicates the
terminal side-effect intent directly.

### Return nil

Returning `nil` would match some callback-style APIs but would discard useful
operational information. Returning the successful element count matches
`Sink.io`, which returns chunks written, and helps users report progress without
adding separate counters.

### Concurrent Terminal Side Effects

Concurrent terminal side effects need scheduler requirements, bounded
admission, failure precedence, cancellation, and ordering semantics. Those
concerns are separate from the simple synchronous sink and are intentionally
deferred.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-04. Feedback found the
`Sink.foreach` contract consistent with existing sink semantics, but noted that
the design should not name an asynchronous variant because the user explicitly
excluded it from this work. The design now refers only to generic concurrent
terminal side effects.

## Validation

- Unit tests for normal side effects, empty input, laziness, missing block,
  failure propagation, no pull after block failure, and upstream close after
  block failure.
- RBS validation for the public signature.
- RuboCop and the full default check suite.

## Open Questions

None.
