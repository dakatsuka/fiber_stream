# Flow.compact

## Status

Draft

## Problem

Users need a flow that removes absent `nil` elements from a stream while
preserving all present values unchanged. This matches Ruby's `Array#compact`
shape and avoids forcing users to write a predicate when `nil` is the only
absence marker.

## Goals

- Add `Flow.compact`.
- Add `Source#compact` as the convenience method.
- Drop upstream elements that are `nil`.
- Emit all non-`nil` upstream elements unchanged, including `false`.
- Preserve pull-driven backpressure and output ordering.
- Provide RBS signatures for public APIs.

## Non-Goals

- Dropping `false`; users should use `filter_map`, `select`, or `reject` for
  truthiness-based filtering.
- Transforming retained elements.
- Producing multiple output elements from one input element.
- Asynchronous, buffered, parallel, or Ractor-backed execution.
- Mutating or duplicating retained values.

## Requirements

- `FiberStream::Flow.compact` creates a filtering flow that drops `nil`
  elements.
- `Source#compact` is a convenience equivalent to
  `Source#via(FiberStream::Flow.compact)`.
- The flow does not accept options and does not use a supplied block.
- The flow is lazy. Construction does not pull upstream.
- `nil` upstream elements are dropped.
- `false` upstream elements are emitted unchanged.
- All other non-`nil` upstream elements are emitted unchanged.
- Retained values preserve object identity.
- Output order matches the order of retained upstream elements.
- A single downstream demand may pull multiple upstream elements until a
  non-`nil` element is observed or upstream completes.
- Dropped `nil` elements are discarded immediately and are not buffered.
- The flow emits at most one downstream element for each upstream element.
- If upstream completes before another retained element, downstream completes.
- After upstream completion is observed, the internal pull stage should avoid
  pulling upstream again during defensive repeated pulls. Public consumers must
  still stop after stream completion.
- Upstream failures fail the stream and are re-raised from `run_with`.
- Materialized-chain cleanup follows existing `Source#run_with` error
  precedence, where the primary stream failure takes precedence over close
  failures.
- Cleanup close failures after otherwise successful completion or early sink
  completion fail `run_with`.
- Public APIs never expose the private `Pull::DONE` sentinel.

## Public Contracts

```ruby
FiberStream::Flow.compact
FiberStream::Source#compact
```

RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def compact: () -> Source[Elem]
  end

  class Flow[In, Out]
    def self.compact: [Elem] () -> Flow[Elem, Elem]
  end
end
```

The runtime contract excludes `nil` from emitted values. RBS cannot express
that this method removes `nil` from a generic union type, so the signature keeps
the element type unchanged.

## Examples

```ruby
result =
  FiberStream::Source.each([1, nil, 2])
    .compact
    .run_with(FiberStream::Sink.to_a)

result # => [1, 2]
```

`false` is a retained value, not an absence marker:

```ruby
result =
  FiberStream::Source.each([true, false, nil])
    .compact
    .run_with(FiberStream::Sink.to_a)

result # => [true, false]
```

## Open Questions

None.
