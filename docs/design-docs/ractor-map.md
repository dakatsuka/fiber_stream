# Ractor Map

## Status

Draft

## Context

`Flow.parallel_map` provides ordered scheduler-backed concurrency, but ordinary
Ruby CPU-bound mapping code still runs under the ractor-wide GVL of the current
ractor. Ractors can execute Ruby code in parallel across CPU cores, but they
come with strict object isolation, transfer, block, and lifecycle constraints.

Governing documents:

- Product spec: `docs/product-specs/flow-ractor-map.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Parallel map design: `docs/design-docs/parallel-map.md`
- Background execution design: `docs/design-docs/background-execution.md`
- References: `docs/references/ruby-ractor.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Goals

- Add an ordered Ractor-backed mapping boundary for CPU-bound per-element work.
- Preserve FiberStream's lazy construction and linear pull API.
- Keep upstream pulls serial and bounded.
- Preserve input order for downstream output.
- Make Ractor block and object transfer constraints explicit.
- Keep the first API narrow enough to implement and test rigorously.

## Non-Goals

- General graph scheduling.
- Unordered output.
- Thread-pool or process-pool execution.
- Runtime scheduler installation.
- Transparent object sharing across workers.
- Immediate cancellation of arbitrary CPU-bound Ruby code running in a ractor.
- Per-element timeout, retry, batching, or overflow policies.

## Proposed Design

`Flow.ractor_map(workers:, input_transfer:, output_transfer:) { ... }` attaches
a `RactorMapBoundary` pull stage:

```ruby
downstream_stream =
  Pull.ractor_map(upstream_stream, workers, input_transfer, output_transfer, mapper)
```

`Source#ractor_map(...)` is a convenience method over
`Source#via(Flow.ractor_map(...))`.

The stage follows the same high-level ordered boundary shape as
`Flow.parallel_map`:

- a single dispatcher pulls upstream serially
- each pulled value receives an input sequence number
- at most `workers` pulled values can be queued, running, or completed but not
  emitted
- results are tagged with sequence numbers
- downstream emits only `next_emit_sequence`
- failures are delivered in input order

The boundary owns:

- `upstream`, the pull stream before the boundary
- `workers`, the positive worker count
- `mapper`, the user-supplied shareable proc
- `input_transfer` and `output_transfer`
- a permit mechanism bounding pulled-but-unemitted work to `workers`
- worker ractors
- job and result communication channels
- `next_sequence`, `next_emit_sequence`, and an ordered pending-result table
- admission, close, completion, failure, and worker shutdown state

The mapper must be shareable according to `Ractor.shareable?`. The public docs
should show `Ractor.shareable_proc do |element| ... end` as the intended shape.
This avoids presenting ordinary Ruby block capture as a supported pattern.

## Worker Protocol

The first worker protocol should use explicit tagged messages:

```ruby
[:job, sequence, value]
[:shutdown]
```

Worker results:

```ruby
[:value, sequence, mapped_value]
[:error, sequence, error_payload]
[:stopped]
```

The exact channel implementation is an open design item. Candidate protocols:

1. Use each worker ractor's default incoming port and collect results with
   `Ractor.select`.
2. Create a result port in the main ractor or coordinator and pass it to each
   worker.
3. Use a coordinator thread that blocks on Ractor APIs and forwards results to
   scheduler-friendly queues consumed by the downstream fiber.

The design should avoid requiring Ractor workers to call `upstream.next`.
Existing pull streams are not concurrent `next` APIs, and keeping upstream
pulls in one dispatcher preserves ordering and cleanup control.

## Transfer Policy

`input_transfer: :copy` uses normal Ractor send semantics. Shareable objects are
shared by reference; unshareable objects are copied when possible.

`input_transfer: :move` sends with `move: true`. After this point FiberStream
must not inspect, log, compare, or otherwise access the moved input object. Any
metadata FiberStream needs, such as the input sequence, must be stored before
the move.

`output_transfer: :copy` and `output_transfer: :move` apply the same principle
to worker results traveling back to the boundary.

Transfer failures are stream failures at the input sequence being transferred.

## Ordering And Backpressure

Permits bound pulled-but-unemitted input elements to `workers`, matching
`Flow.parallel_map`. A permit is consumed before pulling one upstream element
and returned only when that element's mapped value is emitted downstream while
admission remains open.

This bound includes:

- jobs waiting to be sent to workers
- jobs currently running in workers
- completed results waiting behind a lower sequence number

The first public API is ordered. Head-of-line blocking is accepted: if sequence
0 is slow, sequence 1 can complete but is not emitted first.

## Error Handling

The ordered failure model should match `Flow.parallel_map`:

- upstream pull failures receive the next input sequence
- transfer failures receive the sequence being transferred
- mapping failures receive the input sequence being mapped
- worker termination failures receive either the active input sequence or a
  terminal sequence if no input was active
- lower-sequence failures win over higher-sequence failures
- successful lower-sequence values are emitted before the primary failure
- higher-sequence values and failures are suppressed after the primary failure

Ractor exception transport needs a spike. A worker failure may arrive as a
direct exception object, a `Ractor::RemoteError` cause, or a payload that must
be normalized because the original exception cannot safely cross ractors.

Two candidate contracts:

1. Re-raise the original exception when Ruby delivers one safely.
2. Normalize worker failures to `FiberStream::RactorMapError` containing
   `cause_class_name`, `cause_message`, and optional original cause.

The product spec currently leaves this open.

## Scheduler Interaction

Ractor waits can block the current thread. That is a bad default if a
FiberStream pipeline is running inside an Async reactor thread.

Candidate designs:

### Require Scheduler And Use Scheduled Coordination

This matches `Flow.parallel_map`, but it is not enough by itself if the
coordination code calls blocking Ractor APIs from a scheduler fiber. The stage
would still risk blocking the reactor thread.

### Allow Blocking Current Thread

This is simpler and may be acceptable for non-Async CPU pipelines, but it would
be inconsistent with existing scheduler-backed boundaries and surprising inside
Async applications.

### Use A Coordinator Thread

A coordinator thread can block on Ractor APIs and forward tagged results to
FiberStream's existing queue-based downstream logic. This preserves Async
reactor responsiveness at the cost of one additional thread per materialized
boundary. It also separates Ractor waiting from downstream pull logic.

The recommended next step is a spike comparing these options before accepting
the design.

## Cancellation And Cleanup

Closing the boundary should:

- stop admitting new upstream elements
- close upstream
- stop sending new jobs
- send shutdown messages to idle workers
- stop forwarding queued worker failures after intentional downstream
  completion or downstream failure

The first contract should be cooperative. FiberStream should not promise
immediate interruption of a Ractor currently running CPU-bound user code.
Worker ractors may finish an in-flight job before observing shutdown.

If a Ractor worker is still running after downstream no longer needs it, the
implementation must decide whether to:

- detach it and let it terminate naturally
- drain its result and then terminate
- use a Ruby termination mechanism if available and safe

This is part of the spike.

## Contracts

- `Flow.ractor_map` returns `Flow[In, Out]`.
- `Source#ractor_map` delegates to `Flow.ractor_map`.
- Construction is lazy.
- `workers` is a required positive `Integer`.
- `input_transfer` and `output_transfer` are `:copy` or `:move`.
- The mapper block is required and must be shareable.
- Worker ractors start on first downstream demand.
- Upstream is pulled serially.
- Pulled-but-unemitted work is bounded by `workers`.
- Output is ordered by input sequence.
- Failures are delivered by input sequence.
- Early downstream completion closes upstream and requests worker shutdown.
- Public APIs do not expose internal worker messages or `Pull::DONE`.

## Alternatives Considered

### Add `ractor: true` To `parallel_map`

This would hide major semantic differences behind one method. Ractor-backed
execution has different block, transfer, error, and cancellation contracts, so
a separate method is clearer.

### Use A Thread Pool

Ruby threads in the same ractor do not run ordinary Ruby CPU-bound code in
parallel under CRuby's ractor-wide GVL. A thread-pool API may still be useful
for blocking IO or native work that releases the GVL, but it should not be the
CPU-bound mapping API.

### Unordered Ractor Map First

Unordered output could reduce latency for uneven CPU jobs, but it changes map
semantics and complicates downstream behavior. Ordered output is the narrower
first contract and aligns with existing `parallel_map`.

### Require Only Shareable Inputs And Outputs

This would avoid copy/move policy, but it would make the API too narrow for
common workloads such as mutable strings or parsed record hashes. Explicit
transfer policy is more honest and useful.

## Third-Party Review

Pending.

## Validation

- Spike proving whether Ractor coordination can avoid blocking Async reactor
  fibers.
- Unit tests for lazy construction and input validation.
- Tests proving non-shareable blocks are rejected.
- Tests proving ordered values with out-of-order worker completion.
- Tests proving bounded upstream run-ahead.
- Tests proving `:copy` and `:move` transfer behavior where Ruby permits it.
- Tests proving transfer failures become stream failures.
- Tests proving upstream, mapping, and worker failures preserve ordered error
  delivery.
- Tests proving early downstream completion closes upstream and requests worker
  shutdown.
- Tests proving queued or in-flight worker failures are suppressed after
  downstream completion or failure.
- RBS validation.
- RuboCop.

## Open Questions

- Should the accepted design use a coordinator thread?
- What exact public error type should represent worker failures that cannot be
  re-raised directly?
- Is `output_transfer: :move` worth including in the first implementation?
- Should `workers` remain required?
- What worker termination mechanism is safe across supported Ruby versions?
