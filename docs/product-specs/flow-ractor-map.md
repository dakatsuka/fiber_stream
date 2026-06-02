# Flow.ractor_map

## Status

Draft

## Problem

`Flow.parallel_map` overlaps independent mapping work with scheduler-backed
fibers, but it does not promise CPU parallelism for ordinary Ruby code. Users
need an explicit stream stage that can hand per-element CPU-bound work to
Ractor workers while preserving FiberStream's bounded backpressure, ordered
delivery, cleanup, and narrow public API style.

## Goals

- Add `FiberStream::Flow.ractor_map(workers:) { ... }`.
- Add `Source#ractor_map(workers:) { ... }` as a convenience wrapper.
- Preserve lazy construction.
- Run user mapping code in Ractor workers.
- Preserve input order in emitted output values.
- Bound upstream run-ahead to at most `workers` pulled elements that have not
  yet been emitted downstream.
- Make Ractor block isolation explicit by requiring a shareable proc.
- Expose object transfer policy explicitly.
- Propagate upstream and worker failures deterministically.
- Keep FiberStream independent of Async and external worker-pool gems at
  runtime.

## Non-Goals

- Replacing `Flow.parallel_map`.
- Unordered result emission.
- Thread-pool or process-pool execution.
- Graph scheduling, fan-out, or fan-in.
- Runtime scheduler installation.
- Per-element timeout, retry, batching, or overflow policies.
- Sharing mutable user objects across workers.
- Hiding Ractor object transfer limitations.

## Requirements

- `FiberStream::Flow.ractor_map(workers:, input_transfer: :copy,
  output_transfer: :copy) { |element| ... }` creates an ordered Ractor-backed
  mapping flow.
- `Source#ractor_map(workers:, input_transfer: :copy, output_transfer: :copy)
  { |element| ... }` is a convenience equivalent to
  `Source#via(FiberStream::Flow.ractor_map(...))`.
- Constructing `Flow.ractor_map` does not create ractors, pull upstream, or
  call the user block.
- `ractor_map` requires a block.
- The block must be shareable according to `Ractor.shareable?`. The intended
  user shape is `Ractor.shareable_proc { |element| ... }`.
- Missing blocks raise `ArgumentError`.
- Non-shareable blocks raise `TypeError`.
- `workers` must be an `Integer`.
- `workers` must be positive.
- Non-Integer `workers` values raise `TypeError`.
- Zero or negative `workers` values raise `ArgumentError`.
- `input_transfer` must be one of `:copy` or `:move`.
- `output_transfer` must be one of `:copy` or `:move`.
- Invalid transfer policy values raise `ArgumentError`.
- Internal Ractor workers start on the first downstream pull, not at pipeline
  construction or materialization.
- Upstream stages before `ractor_map` are pulled serially by FiberStream, not
  concurrently by Ractor workers.
- The user mapping block runs inside worker ractors.
- Downstream stages after `ractor_map` run in the caller's current execution
  context.
- At most `workers` mapping jobs are queued, running, or completed but not yet
  emitted downstream.
- At most `workers` Ractor workers are alive for one materialized stage.
- Downstream pulls receive mapped values in the same order as upstream input
  values.
- Normal upstream completion is delivered to downstream as normal stream
  completion after all earlier mapped values have been emitted.
- Failures raised by upstream source enumeration or upstream flow blocks are
  delivered after all earlier ordered mapped values have been emitted.
- Failures raised inside worker ractors are delivered after all earlier ordered
  mapped values have been emitted.
- Upstream and mapping failures are ordered by input sequence, not by wall-clock
  completion time.
- If multiple failures are observed before the first failure is delivered,
  FiberStream delivers the lowest-sequence failure as the primary failure.
- Values and failures with higher sequence numbers than the primary failure are
  suppressed.
- Closing the Ractor map boundary closes upstream.
- Early downstream completion closes upstream before `Source#run_with` returns.
- Closing the boundary stops admitting new upstream elements and requests worker
  shutdown.
- In-flight worker jobs may continue until they reach a cooperative shutdown
  point. FiberStream does not promise immediate interruption of arbitrary
  CPU-bound Ruby code running inside a worker ractor.
- Queued or in-flight upstream and mapping errors are suppressed after
  intentional downstream early completion or downstream failure.
- Repeated downstream pulls after completion return completion without pulling
  upstream again.
- `input_transfer: :copy` sends input objects using Ractor's default copy
  semantics.
- `input_transfer: :move` sends input objects with `move: true`; FiberStream
  must not access an input object after moving it to a worker.
- `output_transfer: :copy` returns worker results using Ractor's default copy
  semantics.
- `output_transfer: :move` returns worker results with `move: true`.
- Transfer failures, isolation failures, worker termination failures, and
  uncopyable-object failures are stream failures.

## Public Contracts

```ruby
FiberStream::Flow.ractor_map(
  workers:,
  input_transfer: :copy,
  output_transfer: :copy
) { |element| ... }

FiberStream::Source#ractor_map(
  workers:,
  input_transfer: :copy,
  output_transfer: :copy
) { |element| ... }
```

Initial RBS shape:

```rbs
module FiberStream
  type ractor_transfer_policy = :copy | :move

  class Flow[In, Out]
    def self.ractor_map: [In, Out] (
      workers: Integer,
      ?input_transfer: ractor_transfer_policy,
      ?output_transfer: ractor_transfer_policy
    ) { (In) -> Out } -> Flow[In, Out]
  end

  class Source[Elem]
    def ractor_map: [Out] (
      workers: Integer,
      ?input_transfer: ractor_transfer_policy,
      ?output_transfer: ractor_transfer_policy
    ) { (Elem) -> Out } -> Source[Out]
  end
end
```

## Error Precedence

The first public contract should match `Flow.parallel_map` unless Ractor
transport makes that impossible:

| Situation | Result |
| --- | --- |
| Normal upstream completion and worker shutdown succeeds | Normal stream completion after earlier mapped values |
| Upstream pull fails | Upstream pull failure after earlier mapped values |
| Worker mapping fails | Mapping failure after earlier mapped values |
| Multiple upstream or mapping failures are observed | Lowest-sequence failure wins |
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
    .ractor_map(workers: 4, &HASH_CHUNK)
    .run_with(FiberStream::Sink.to_a)
```

Move input objects when the caller will not use them again:

```ruby
result =
  FiberStream::Source.each(chunks)
    .ractor_map(workers: 4, input_transfer: :move, &HASH_CHUNK)
    .run_with(FiberStream::Sink.to_a)
```

## Open Questions

- Should `ractor_map` require an installed `Fiber.scheduler` and a non-blocking
  current fiber, or should it be allowed to block the current thread while
  waiting for Ractor results?
- Should the first implementation isolate Ractor waiting in a coordinator
  thread so Async reactor threads are not blocked?
- Should Ractor failures be re-raised directly when possible, or normalized to
  `FiberStream::RactorMapError` with original class/message/cause metadata?
- Should `output_transfer: :move` be part of the first implementation or
  deferred until input movement is validated?
- Should `workers` default to a value such as `Etc.nprocessors`, or should it
  remain required so resource use is explicit?
