# Source.zip

## Status

Draft

## Problem

Users need to combine two sources element-by-element without buffering either
source or introducing scheduler-backed execution. Zipping should preserve
FiberStream's pull-driven backpressure model: one downstream demand advances
each input source only far enough to produce one output pair or discover that
the zipped stream is complete.

## Goals

- Add a lazy `Source#zip(source)` public API.
- Emit pairs containing one element from the receiver and one element from the
  other source.
- Preserve element order within each input source.
- Complete when either input source completes.
- Preserve existing source and flow cleanup behavior on normal completion,
  early sink completion, and failures.
- Provide RBS signatures for the public API.

## Non-Goals

- Variadic zipping.
- Zipping sources with custom pair objects or block-based projection.
- Padding shorter sources with default values.
- Combining latest values from asynchronous sources.
- Merging, racing, or interleaving sources.
- Parallel materialization or scheduler-backed coordination.
- Automatic element type coercion.

## Requirements

- `Source#zip(source)` returns a new source definition.
- Passing a non-`FiberStream::Source` object to `Source#zip` raises
  `TypeError`.
- `Source#zip` is lazy. It does not materialize either source, enumerate
  values, run flow blocks, start scheduler-backed boundaries, read IO, or close
  resources at construction time.
- Materializing the zipped source does not materialize either input source.
- On the first downstream pull that reaches the zip stage, the receiver source
  is materialized.
- Each downstream pull asks the receiver for one element. If the receiver
  completes, the zipped source completes without materializing or pulling the
  other source for that output pair unless the other source was already
  materialized by an earlier pair.
- If the receiver produces an element, the other source is materialized when it
  has not already been materialized, and the same downstream pull asks the other
  source for one element.
- If the other source completes, the zipped source completes and the unpaired
  receiver element is discarded.
- If both sources produce elements, the zipped source emits a two-element
  `Array` containing `[receiver_element, other_element]`.
- The zipped source preserves pair order: the first output pair contains the
  first element from each source, the second output pair contains the second
  element from each source, and so on until either source completes.
- If downstream completes before pulling the zip stage, neither input source is
  materialized or closed by the zip stage.
- On normal completion, early sink completion, or failure, every materialized
  side of the zip is closed.
- When the receiver completes normally, the zip stage attempts to close both
  materialized sides before returning completion.
- When the other source completes normally after the receiver produced an
  unpaired value, the zip stage attempts to close both materialized sides before
  returning completion.
- If one or both close operations fail during normal zip completion,
  `run_with` raises the first close failure in receiver-then-other close order.
  Later close failures are suppressed and are not exposed.
- If the receiver fails, the receiver failure is re-raised from `run_with` and
  the other source is closed if it has been materialized.
- If the other source fails, the other source failure is re-raised from
  `run_with` and the receiver source is closed.
- If materializing either side raises, the materialization failure is re-raised
  from `run_with`. Unmaterialized sides are not closed; already materialized
  sides are closed by normal zip cleanup. If that cleanup also fails, the
  materialization failure remains primary and cleanup failures are suppressed.
- If both stream execution and cleanup fail, the stream execution failure takes
  precedence, matching existing `Source#run_with` behavior.
- Flows attached to the receiver before `zip` apply only to receiver elements.
  Flows attached to the other source before `zip` apply only to other-source
  elements. Flows attached after `zip` apply to emitted pairs.
- Public APIs never expose the private `Pull::DONE` sentinel.

## Public Contracts

```ruby
FiberStream::Source#zip(source)
```

RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def zip: [Other] (Source[Other] source) -> Source[[Elem, Other]]
  end
end
```

## Examples

```ruby
result =
  FiberStream::Source.each([1, 2, 3])
    .zip(FiberStream::Source.each(["a", "b", "c"]))
    .run_with(FiberStream::Sink.to_a)

result # => [[1, "a"], [2, "b"], [3, "c"]]
```

The zipped stream completes when either side completes:

```ruby
result =
  FiberStream::Source.each([1, 2, 3])
    .zip(FiberStream::Source.each(["a"]))
    .run_with(FiberStream::Sink.to_a)

result # => [[1, "a"]]
```

Flows after `zip` transform emitted pairs:

```ruby
result =
  FiberStream::Source.each([1, 2])
    .zip(FiberStream::Source.each([10, 20]))
    .map { |left, right| left + right }
    .run_with(FiberStream::Sink.to_a)

result # => [11, 22]
```

## Open Questions

None.
