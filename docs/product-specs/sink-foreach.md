# Sink.foreach

## Status

Accepted

## Problem

Users need to terminate a stream by running a side-effecting operation for each
element without accumulating all elements in memory or encoding side effects as
an accumulator update. `Sink.fold` can express this pattern, but it obscures the
intent of pipelines that process streaming inputs such as HTTP response bodies,
log lines, or records written to external systems.

## Goals

- Add `FiberStream::Sink.foreach { |element| ... }`.
- Consume the stream lazily under normal FiberStream pull backpressure.
- Run the user block exactly once for each consumed element.
- Return a useful materialized value on success.
- Preserve existing stream failure and cleanup behavior.
- Keep the API synchronous and scheduler-independent.

## Non-Goals

- Concurrent or asynchronous per-element execution.
- Retry, batching, timeout, or rate-limit policies.
- Error aggregation after a side-effect failure.
- Compensating or rolling back side effects.
- Resource ownership beyond the ordinary materialized stream close performed
  by `Source#run_with`.

## Requirements

- `FiberStream::Sink.foreach { |element| ... }` creates a sink that consumes all
  upstream elements.
- `Sink.foreach` requires a block.
- Missing blocks raise `ArgumentError`.
- Constructing `Sink.foreach` does not pull upstream or call the block.
- During materialization, `Sink.foreach` repeatedly pulls one upstream element
  and calls the block with that element until upstream completes.
- The block is called in input order.
- `Sink.foreach` does not store consumed elements.
- `Sink.foreach` returns the number of elements whose block completed
  successfully.
- Empty upstream returns `0`.
- If upstream fails, the upstream failure is re-raised from `Source#run_with`.
- If the block raises, the block failure is re-raised from `Source#run_with`.
- If the block raises for an element, `Sink.foreach` stops pulling new
  upstream elements.
- `Source#run_with` closes the materialized stream after normal completion,
  upstream failure, or block failure using the existing cleanup precedence.
- `Sink.foreach` does not require `Fiber.scheduler`.
- `Sink.foreach` does not introduce internal fibers, queues, or buffering.

## Public Contracts

```ruby
FiberStream::Sink.foreach { |element| ... }
```

Initial RBS shape:

```rbs
module FiberStream
  class Sink[In, Mat]
    def self.foreach: [Elem] () { (Elem) -> void } -> Sink[Elem, Integer]
  end
end
```

## Examples

```ruby
count =
  FiberStream::Source.each(events)
    .run_with(
      FiberStream::Sink.foreach do |event|
        handle_event(event)
      end
    )

count # => number of handled events
```

Streaming HTTP response bodies can use `foreach` to avoid accumulating results:

```ruby
processed =
  FiberStream::Source.each(response.body)
    .lines(max_length: 64 * 1024)
    .run_with(
      FiberStream::Sink.foreach do |line|
        process_line(line)
      end
    )
```

## Open Questions

None.
