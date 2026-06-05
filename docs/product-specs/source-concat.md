# Source.concat

## Status

Accepted

## Problem

Users need to append one source after another without buffering either source
or introducing a push-based handoff. Concatenation should preserve FiberStream's
pull-driven backpressure model: the downstream sink asks for one element, and
only the currently active source advances far enough to answer that demand.

## Goals

- Add a lazy `Source#concat(source)` public API.
- Emit all elements from the receiver, then all elements from the appended
  source.
- Preserve pull-driven backpressure and avoid reading from the appended source
  before the receiver completes.
- Preserve existing source and flow cleanup behavior on normal completion,
  early sink completion, and failures.
- Provide RBS signatures for the public API.

## Non-Goals

- Interleaving, merging, zipping, or racing sources.
- Parallel materialization of concatenated sources.
- Variadic concatenation in the first API slice.
- Automatic element type coercion.

## Requirements

- `Source#concat(source)` returns a new source definition.
- Passing a non-`FiberStream::Source` object to `Source#concat` raises
  `TypeError`.
- `Source#concat` is lazy. It does not materialize either source, enumerate
  values, run flow blocks, start scheduler-backed boundaries, read IO, or close
  resources at construction time.
- The concatenated source emits receiver elements first, preserving order.
- After the receiver completes normally, the concatenated source emits appended
  source elements, preserving order.
- The appended source is not materialized until downstream demand observes
  normal completion from the receiver.
- While receiver elements remain available, downstream demand must not pull or
  materialize the appended source.
- When the receiver completes, the receiver's materialized pull chain is closed
  before the appended source is materialized.
- If closing the receiver after receiver completion fails, that close failure
  is re-raised from `run_with`, the appended source is not materialized, and
  later cleanup failures are secondary.
- On normal completion, early sink completion, or failure, every materialized
  side of the concatenation is closed.
- If the receiver fails before completing, the appended source is not
  materialized and the receiver failure is re-raised from `run_with`.
- If the appended source fails, the failure is re-raised from `run_with`.
- If materializing either side raises, the materialization failure is re-raised
  from `run_with`. Unmaterialized sides are not closed; already materialized
  sides are closed by normal concat cleanup.
- If both stream execution and cleanup fail, the stream execution failure takes
  precedence, matching existing `Source#run_with` behavior.
- Flows attached to the receiver before `concat` apply only to receiver
  elements. Flows attached to the appended source before `concat` apply only to
  appended source elements. Flows attached after `concat` apply to the combined
  output from both sources.
- Public APIs never expose the private `Pull::DONE` sentinel.

## Public Contracts

```ruby
FiberStream::Source#concat(source)
```

RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def concat: [Other] (Source[Other] source) -> Source[Elem | Other]
  end
end
```

## Examples

```ruby
result =
  FiberStream::Source.each([1, 2])
    .concat(FiberStream::Source.each([3, 4]))
    .run_with(FiberStream::Sink.to_a)

result # => [1, 2, 3, 4]
```

Concatenation composes with flows:

```ruby
result =
  FiberStream::Source.each([1, 2])
    .map { |number| number * 10 }
    .concat(FiberStream::Source.each([3]).map { |number| number * 10 })
    .run_with(FiberStream::Sink.to_a)

result # => [10, 20, 30]
```

Flows after `concat` transform the combined output:

```ruby
result =
  FiberStream::Source.each([1])
    .concat(FiberStream::Source.each([2]))
    .map { |number| number * 10 }
    .run_with(FiberStream::Sink.to_a)

result # => [10, 20]
```

## Open Questions

None.
