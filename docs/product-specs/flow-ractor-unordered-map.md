# Flow.ractor_unordered_map

## Status

Accepted

## Problem

`Flow.ractor_map` runs CPU-bound mapping work in Ractor workers, but it
preserves input order. A slow earlier element can hold back later completed
work. Users also need a bounded Ractor-backed mapping stage that emits mapped
values as workers finish when downstream logic does not require input order.

## Goals

- Add `FiberStream::Flow.ractor_unordered_map(workers:) { ... }`.
- Add `Source#ractor_unordered_map(workers:) { ... }` as a convenience wrapper.
- Preserve lazy construction.
- Run user mapping code in Ractor workers.
- Emit mapped values in coordinator-observed worker completion order.
- Make no input-order guarantee.
- Bound upstream run-ahead to at most `workers` pulled elements that have not
  yet been emitted downstream.
- Make Ractor block isolation explicit by requiring a shareable proc.
- Preserve explicit input and output transfer policies.
- Normalize worker and Ractor transfer failures to `RactorMapError`.
- Keep FiberStream independent of Async and external worker-pool gems at
  runtime.

## Non-Goals

- Ordered result emission.
- Replacing `Flow.ractor_map`.
- Scheduler-backed fiber execution.
- Thread-pool or process-pool execution.
- Graph scheduling, fan-out, or fan-in.
- Runtime scheduler installation.
- Per-element timeout, retry, batching, or overflow policies.
- Hiding Ractor object transfer limitations.

## Requirements

- `FiberStream::Flow.ractor_unordered_map(workers:, input_transfer: :copy,
  output_transfer: :copy) { |element| ... }` creates an unordered Ractor-backed
  mapping flow.
- `Source#ractor_unordered_map(workers:, input_transfer: :copy,
  output_transfer: :copy) { |element| ... }` is a convenience equivalent to
  `Source#via(FiberStream::Flow.ractor_unordered_map(...))`.
- Constructing `Flow.ractor_unordered_map` does not create ractors, pull
  upstream, or call the user block.
- `ractor_unordered_map` requires a block.
- The block must be shareable according to `Ractor.shareable?`.
- Missing blocks raise `ArgumentError`.
- Non-shareable blocks raise `TypeError`.
- `workers` must be an `Integer`.
- `workers` must be positive.
- Non-Integer `workers` values raise `TypeError`.
- Zero or negative `workers` values raise `ArgumentError`.
- `input_transfer` and `output_transfer` must be `:copy` or `:move`.
- Invalid transfer policy values raise `ArgumentError`.
- Internal Ractor workers start on the first downstream pull, not at pipeline
  construction or materialization.
- `ractor_unordered_map` does not require `Fiber.scheduler`.
- Blocking Ractor waits are isolated from scheduler-managed fibers by a
  coordinator thread or an equivalent non-reactor-blocking design.
- Waiting for coordinator and worker shutdown during cleanup must also avoid
  blocking sibling scheduler-managed fibers.
- Upstream stages before `ractor_unordered_map` are pulled serially by
  FiberStream, not concurrently by Ractor workers.
- Upstream pulls happen in the downstream caller's execution context.
- The user mapping block runs inside worker ractors.
- Downstream stages after `ractor_unordered_map` run in the caller's current
  execution context.
- At most `workers` mapping jobs are queued, running, or completed but not yet
  emitted downstream.
- At most `workers` Ractor workers are alive for one materialized stage.
- Downstream pulls receive mapped values in coordinator-observed completion
  order.
- `ractor_unordered_map` does not preserve input order.
- When a downstream pull observes that both a completed result is ready and
  more upstream work could be admitted, it emits the completed result before
  pulling upstream again.
- Normal upstream completion is delivered to downstream after all admitted
  mapped values have been emitted.
- Failures raised by upstream source enumeration or upstream flow blocks are
  re-raised from `Source#run_with` when downstream observes the failure.
- Worker mapping failures, Ractor transfer failures, isolation failures, and
  worker termination failures are normalized to `FiberStream::RactorMapError`.
- Upstream and mapping failures are fail-fast by downstream observation order,
  not input sequence order.
- When downstream observes a failure, FiberStream stops admitting new upstream
  elements, closes upstream, requests worker shutdown, and raises that failure.
- Values already emitted before a failure remain emitted.
- Queued or in-flight values and failures not yet observed when the primary
  failure is delivered are suppressed.
- If upstream completes normally and producer-side `upstream.close` fails, that
  close failure is delivered after all admitted mapped values have been
  emitted, unless a mapping failure is observed first.
- Closing the boundary closes upstream.
- Early downstream completion closes upstream before `Source#run_with` returns.
- Closing the boundary stops admitting new upstream elements and requests
  worker shutdown.
- Closing the boundary sends exactly one shutdown message to every worker
  ractor and waits for coordinator and worker shutdown before returning.
- Closing the boundary wakes any coordinator thread blocked while forwarding a
  result to downstream.
- In-flight worker jobs may continue until they finish the current mapping
  call. FiberStream does not promise immediate interruption of arbitrary
  CPU-bound Ruby code running inside a worker ractor.
- Queued or in-flight upstream and mapping errors are suppressed after
  intentional downstream early completion or downstream failure.
- Repeated downstream pulls after completion return completion without pulling
  upstream again.
- `input_transfer: :copy` and `output_transfer: :copy` use Ractor default copy
  semantics.
- `input_transfer: :move` and `output_transfer: :move` use `move: true` for
  user input and output values.
- `RactorMapError` exposes the input `sequence`, failure `kind`, original
  `cause_class_name`, and original `cause_message`.

## Public Contracts

```ruby
FiberStream::Flow.ractor_unordered_map(
  workers:,
  input_transfer: :copy,
  output_transfer: :copy
) { |element| ... }

FiberStream::Source#ractor_unordered_map(
  workers:,
  input_transfer: :copy,
  output_transfer: :copy
) { |element| ... }

FiberStream::RactorMapError
```

Initial RBS shape:

```rbs
module FiberStream
  class Flow[In, Out]
    def self.ractor_unordered_map: [In, Out] (
      workers: Integer,
      ?input_transfer: ractor_transfer_policy,
      ?output_transfer: ractor_transfer_policy
    ) { (In) -> Out } -> Flow[In, Out]
  end

  class Source[Elem]
    def ractor_unordered_map: [Out] (
      workers: Integer,
      ?input_transfer: ractor_transfer_policy,
      ?output_transfer: ractor_transfer_policy
    ) { (Elem) -> Out } -> Source[Out]
  end
end
```

## Error Precedence

| Situation | Result |
| --- | --- |
| Normal upstream completion and worker shutdown succeeds | Normal stream completion after admitted mapped values |
| Upstream pull fails | Upstream pull failure when observed |
| Worker mapping fails | `RactorMapError` when observed |
| Input or output transfer fails | `RactorMapError` when observed |
| Worker terminates unexpectedly | `RactorMapError` when observed |
| Multiple upstream or mapping failures are queued | First downstream-observed failure wins |
| Mapping failure is observed before delayed producer-side close failure | Mapping failure; close failure suppressed |
| Downstream completes early and boundary close succeeds | Downstream result |
| Downstream completes early and boundary close fails | Boundary close failure |
| Downstream fails and boundary close also fails | Downstream failure; close failure suppressed |

## Examples

```ruby
require "digest"
require "fiber_stream"

HASH_CHUNK =
  Ractor.shareable_proc do |chunk|
    Digest::SHA256.hexdigest(chunk)
  end

result =
  FiberStream::Source.each(chunks)
    .ractor_unordered_map(workers: 4, &HASH_CHUNK)
    .run_with(FiberStream::Sink.to_a)

result.sort
```

## Open Questions

None.
