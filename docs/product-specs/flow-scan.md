# Flow.scan

## Status

Accepted

## Problem

Users need a streaming counterpart to `Sink.fold` that emits intermediate
accumulator values as elements arrive. Aggregation, state machines, and progress
tracking all follow the same pattern: maintain state, update it for each input,
and observe the running result before the stream ends.

Today callers can approximate this with `Sink.fold` and manual side effects, or
by threading mutable state through awkward `Flow.map` compositions. Those
workarounds either hide the running values inside terminal side effects or make
the accumulator contract implicit instead of exposing it as a first-class stream
operator.

## Goals

- Add `FiberStream::Flow.scan(initial) { |accumulator, element| ... }`.
- Add `Source#scan(initial) { |accumulator, element| ... }` as a convenience
  wrapper.
- Use the same reducer block contract as `Sink.fold`.
- Emit one accumulator value per upstream element, in input order.
- Preserve lazy construction and pull-driven backpressure.
- Bound per-stage memory to one accumulator reference.
- Provide RBS signatures for public APIs.

## Non-Goals

- `reduce` without an explicit initial accumulator.
- Emitting the initial accumulator before the first upstream element.
- Windowed, keyed, or grouped scan variants.
- Parallel or scheduler-backed scan stages.
- Duplicating or freezing the initial accumulator; follow `Sink.fold`
  assignment semantics.

## Requirements

- `FiberStream::Flow.scan(initial) { |accumulator, element| ... }` creates a
  scanning flow.
- `Source#scan(initial) { |accumulator, element| ... }` is a convenience
  equivalent to
  `Source#via(FiberStream::Flow.scan(initial) { |accumulator, element| ... })`.
- `Flow.scan` requires a block.
- `Source#scan` requires a block.
- Missing scan blocks raise `ArgumentError`.
- Construction is lazy and does not pull upstream.
- Each downstream demand causes `Flow.scan` to issue at most one immediate
  upstream pull. When another composed stage sits upstream of `Flow.scan`, a
  single downstream demand on scan may cause that upstream stage to pull more
  than one element under that stage's own contract.
- For each normal upstream element, `Flow.scan` calls the block as
  `block.call(accumulator, element)` with the current accumulator first and the
  upstream element second, matching `Sink.fold`.
- The block result becomes the new accumulator and is emitted downstream.
- The initial accumulator is not emitted as a stream element.
- When upstream completes normally before any element is produced, downstream
  completes without emitting a value.
- When upstream completes normally after at least one element, the final emitted
  accumulator equals the result of
  `Sink.fold(initial) { |accumulator, element| ... }` over the same input
  sequence.
- Upstream element order is preserved in emitted accumulators.
- FiberStream assigns the initial accumulator directly; it does not duplicate
  or freeze that object.
- The block may return the same accumulator object after mutating it in place.
  At each emission, downstream consumers observe the object identity and
  contents returned by the block for that pull.
- When the block reuses a mutable accumulator object across steps, sinks that
  retain every emission (for example `Sink.to_a`) store multiple references to
  the same object. Inspecting those retained values after the stream completes
  shows the final mutated state in each slot, not independent historical
  snapshots. Callers who need historical snapshots must return a new object per
  step (for example `values + [element]`) or consume emissions incrementally
  without retaining earlier references.
- At most one accumulator reference is retained inside the scan stage between
  downstream pulls.
- `Flow.scan` itself does not require `Fiber.scheduler`.
- Upstream enumeration or pull failures fail the stream and are re-raised from
  `Source#run_with` without emitting an accumulator for the failing pull.
- Exceptions raised by the scan block fail the stream and are re-raised from
  `Source#run_with` without emitting an accumulator for the failing element.
  Materialized-chain cleanup follows existing `Source#run_with` error precedence,
  where the primary stream failure takes precedence over close failures.
- If the block mutates the accumulator in place and then raises, FiberStream
  does not roll back that mutation. This matches `Sink.fold` semantics.
- Closing the materialized scan stage closes upstream.
- After upstream completion is observed, later defensive pulls return completion
  without pulling upstream again.
- Public APIs never expose the private `Pull::DONE` sentinel.

## Public Contracts

```ruby
FiberStream::Flow.scan(initial) { |accumulator, element| new_accumulator }
FiberStream::Source#scan(initial) { |accumulator, element| new_accumulator }
```

RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def scan: [Acc] (Acc initial) { (Acc, Elem) -> Acc } -> Source[Acc]
  end

  class Flow[In, Out]
    def self.scan: [Elem, Acc] (Acc initial) { (Acc, Elem) -> Acc } -> Flow[Elem, Acc]
  end
end
```

## Examples

Running totals:

```ruby
result =
  FiberStream::Source.each([1, 2, 3, 4])
    .scan(0) { |sum, value| sum + value }
    .run_with(FiberStream::Sink.to_a)

result # => [1, 3, 6, 10]
```

The final scan value matches `Sink.fold` over the same input sequence:

```ruby
source = FiberStream::Source.each([1, 2, 3])

final =
  source
    .run_with(FiberStream::Sink.fold(0) { |sum, value| sum + value })

running =
  source
    .scan(0) { |sum, value| sum + value }
    .run_with(FiberStream::Sink.to_a)

final # => 6
running.last # => 6
```

State-machine style updates:

```ruby
result =
  FiberStream::Source.each([:start, :tick, :tick, :stop])
    .scan(:idle) { |state, event| transition(state, event) }
    .run_with(FiberStream::Sink.to_a)
```

Progress counting:

```ruby
result =
  FiberStream::Source.each(items)
    .scan(0) { |count, _item| count + 1 }
    .run_with(FiberStream::Sink.to_a)
```

Immutable accumulators produce independent emitted values:

```ruby
FiberStream::Source.each([1, 2, 3])
  .scan(0) { |sum, value| sum + value }
  .run_with(FiberStream::Sink.to_a) # => [1, 3, 6]
```

Mutable accumulators reused across steps alias the same object when retained:

```ruby
FiberStream::Source.each([1, 2, 3])
  .scan([]) { |acc, value| acc << value; acc }
  .run_with(FiberStream::Sink.to_a)
# => three references to one Array; see Requirements
```

An empty source emits no accumulators:

```ruby
result =
  FiberStream::Source.each([])
    .scan(0) { |sum, value| sum + value }
    .run_with(FiberStream::Sink.to_a)

result # => []
```

## Open Questions

None for the initial API shape. A future `emit_initial:` option remains deferred
until there is a concrete requirement.
