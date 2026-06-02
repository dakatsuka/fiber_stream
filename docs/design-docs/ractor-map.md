# Ractor Map

## Status

Accepted

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

The dispatcher is not a native thread. It is boundary logic that runs in the
downstream caller's execution context during downstream pulls. This matters
because upstream stages may have scheduler requirements or caller-owned
resource assumptions. The coordinator thread never calls `upstream.next`; it is
only responsible for blocking Ractor waits and forwarding worker messages to
main-ractor queues.

The boundary owns:

- `upstream`, the pull stream before the boundary
- `workers`, the positive worker count
- `mapper`, the user-supplied shareable proc
- `input_transfer` and `output_transfer`
- a permit mechanism bounding pulled-but-unemitted work to `workers`
- worker ractors
- job and result communication channels
- a coordinator thread for Ractor wait isolation
- `next_sequence`, `next_emit_sequence`, and an ordered pending-result table
- admission, close, completion, failure, and worker shutdown state

The mapper must be shareable according to `Ractor.shareable?`. The public docs
should show `Ractor.shareable_proc do |element| ... end` as the intended shape.
This avoids presenting ordinary Ruby block capture as a supported pattern.

## Spike Results

Local Ruby 4.0.3 spikes on 2026-06-03 found:

- `Ractor#value` and `Ractor.select` block the Async reactor thread when called
  directly from an `Async do` fiber. A sibling Async task scheduled to tick
  every 0.05s did not run until the Ractor wait returned at about 0.15s.
- A coordinator `Thread` that blocks on `Ractor.select` and forwards results to
  `Thread::Queue` or `Thread::SizedQueue` did not block the Async reactor in
  ticker probes.
- Ruby 4.0.3 exposes `Ractor#value`, `Ractor.select`, and `Ractor::Port`; it
  does not expose `Ractor#take`.
- `Ractor.current.default_port` is a shareable `Ractor::Port` with `send`,
  `receive`, `close`, and `closed?`.
- Passing the main ractor's default port to worker ractors allows workers to
  send multiple tagged result messages back to the main ractor.
- `Ractor#close` cannot close another ractor's incoming port from the main
  ractor. Explicit shutdown messages are the practical first worker shutdown
  mechanism.
- `Ractor.shareable_proc` accepts shareable captures but rejects unshareable
  captures with `Ractor::IsolationError`.
- Exceptions can be sent through a `Ractor::Port` as values in the tested
  cases. Unhandled worker exceptions observed through `Ractor#value` or
  `Ractor.select` arrive as `Ractor::RemoteError` with the original exception
  as `cause`.
- Copy transfer leaves mutable strings usable in the sender and gives workers a
  copied object. Move transfer makes the sender's object raise
  `Ractor::MovedError` when accessed after send.
- Some objects fail transfer with different Ruby exception classes, including
  `TypeError`, `FiberError`, `NoMethodError`, and `Ractor::Error`.

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
[:ready, worker_id]
[:stopped]
```

The first implementation should use a coordinator thread and a shared result
port. The main ractor's boundary code sends jobs to worker ractors. Worker
ractors send result and lifecycle messages to the shared result port. The
coordinator thread blocks on that port and forwards data-result messages into
bounded `Thread::SizedQueue` instances consumed by the downstream caller.

The design should avoid requiring Ractor workers to call `upstream.next`.
Existing pull streams are not concurrent `next` APIs, and keeping upstream
pulls in one dispatcher preserves ordering and cleanup control.

Workers wrap mapper execution and output transfer separately:

1. receive `[:job, sequence, value]`
2. call the mapper
3. send `[:value, sequence, mapped_value]` using `output_transfer`
4. if mapper execution fails, send a known-shareable error payload as
   `[:error, sequence, payload]`
5. if sending the mapped value fails, send a known-shareable error payload as
   `[:error, sequence, payload]`
6. send `[:ready, worker_id]` after each completed or failed job unless a
   shutdown has been requested

The error payload must contain only known-shareable values such as symbols,
integers, strings, and arrays or hashes of those values. It should include
`kind`, `cause_class_name`, and `cause_message`.

## Transfer Policy

`input_transfer: :copy` uses normal Ractor send semantics. Shareable objects are
shared by reference; unshareable objects are copied when possible.

`input_transfer: :move` sends with `move: true`. After this point FiberStream
must not inspect, log, compare, or otherwise access the moved input object. Any
metadata FiberStream needs, such as the input sequence, must be stored before
the move.

`output_transfer: :copy` and `output_transfer: :move` apply the same principle
to worker results traveling back to the boundary. If output transfer fails, the
worker reports a normalized `:output_transfer` failure at the input sequence
using a known-shareable error payload.

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

Channel capacities:

- `permits` contains exactly `workers` tokens.
- `ready_workers` is a `Thread::SizedQueue` with capacity `workers`.
- `data_results` is a `Thread::SizedQueue` with capacity `workers`.
- lifecycle/control messages such as `[:ready, worker_id]` and `[:stopped]`
  are tracked separately from data results and do not consume data-result
  capacity.
- terminal stream completion is boundary state, not a worker result message.

There is no central unbounded job queue. The boundary sends a job only when it
has both a permit and a ready worker.

After close begins, downstream may stop consuming `data_results`. To prevent
coordinator shutdown from blocking on a full bounded data queue, the coordinator
must treat close state as a suppression boundary: worker `[:value, ...]` and
`[:error, ...]` messages observed after close are dropped instead of enqueued.
The coordinator continues to process lifecycle messages such as `[:ready, ...]`
and `[:stopped]` until every worker has stopped.

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

Upstream pull failures are re-raised directly, matching existing stream stages.
Failures that happen inside worker ractors or during Ractor transfer are
normalized to `FiberStream::RactorMapError`. The error contains:

- `sequence`
- `kind`, one of `:input_transfer`, `:output_transfer`, `:worker`,
  `:worker_termination`, or `:isolation`
- `cause_class_name`
- `cause_message`

When the original exception object is available in the main ractor, the
implementation should preserve it as Ruby `cause`. When only a known-shareable
payload can cross the Ractor boundary, the `RactorMapError` still carries class
and message metadata.

## Scheduler Interaction

Ractor waits can block the current thread. Local spikes confirmed that calling
`Ractor#value` or `Ractor.select` directly from an Async fiber blocks sibling
Async tasks until the Ractor wait completes. That would be a bad default for
FiberStream users running pipelines inside Async.

### Coordinator Thread

A coordinator thread can block on Ractor APIs and forward tagged results to
FiberStream's existing queue-based downstream logic. This preserves Async
reactor responsiveness at the cost of one additional thread per materialized
boundary. It also separates Ractor waiting from downstream pull logic.

The first implementation should use this design unless a simpler non-blocking
Ractor wait mechanism appears in the supported Ruby version.

Because Ractor waiting is isolated from scheduler-managed fibers, `ractor_map`
does not need to require `Fiber.scheduler`. In non-scheduler foreground use,
downstream may block the current thread while waiting for results, which is
consistent with ordinary foreground `run_with` execution. In scheduler-backed
use, result queue waits must not block the reactor thread; the Async spike with
`Thread::Queue#pop` and `Thread::SizedQueue#pop` satisfied this requirement for
the current compatibility target.

## Cancellation And Cleanup

Closing the boundary should:

- stop admitting new upstream elements
- close upstream
- stop sending new jobs
- send exactly one shutdown message to every worker, including active workers
- stop forwarding queued worker failures after intentional downstream
  completion or downstream failure
- wait for the coordinator thread to exit
- wait for worker ractors to acknowledge shutdown or finish their current job
  and then stop

The first contract should be cooperative. FiberStream should not promise
immediate interruption of a Ractor currently running CPU-bound user code.
Worker ractors may finish an in-flight job before observing shutdown.

Shutdown delivery must not depend on workers being idle at close time. The
boundary sends a shutdown message to every worker ractor during close. Idle
workers receive it immediately. Active workers finish their current mapper
call, send any final result or normalized error payload, optionally send
`[:ready, worker_id]`, then receive the already queued shutdown message and
send `[:stopped]`.

Because result messages can still arrive after close, the coordinator must not
block trying to forward those suppressed results to downstream queues. It drops
post-close data/error messages, keeps processing worker lifecycle messages, and
exits only after all workers have reported `[:stopped]`.

`close` waits for the coordinator thread and worker ractors before returning.
This avoids silently detaching workers or leaking coordinator threads after
`Source#run_with` returns. The tradeoff is that early downstream completion can
wait for in-flight CPU-bound mapper calls to finish. Users who need prompt
interruption of long-running CPU loops need a later timeout or cooperative user
protocol; that is out of scope for the first API.

Cleanup waits must follow the same reactor-safety rule as normal result waits:
blocking Ractor waits happen in the coordinator thread, and scheduler-managed
pipeline fibers wait through bounded Ruby queues or thread joins in a way that
does not block sibling Async tasks. This behavior needs an explicit Async
responsiveness test.

## Contracts

- `Flow.ractor_map` returns `Flow[In, Out]`.
- `Source#ractor_map` delegates to `Flow.ractor_map`.
- Construction is lazy.
- `workers` is a required positive `Integer`.
- `input_transfer` and `output_transfer` are `:copy` or `:move`.
- The mapper block is required and must be shareable.
- Worker ractors start on first downstream demand.
- Ractor waits are isolated from scheduler-managed pipeline fibers by a
  coordinator thread or equivalent design.
- `ractor_map` does not require `Fiber.scheduler`.
- Upstream is pulled serially in the downstream caller's execution context.
- The coordinator thread never calls `upstream.next`.
- Pulled-but-unemitted work is bounded by `workers`.
- Job admission requires both a permit and a ready worker.
- Output is ordered by input sequence.
- Failures are delivered by input sequence.
- Worker and Ractor-transfer failures are normalized to `RactorMapError`.
- Early downstream completion closes upstream and requests worker shutdown.
- Boundary close waits for coordinator and worker shutdown before returning.
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

Reviewed by context-free sub-agents on 2026-06-03. Feedback resulted in these
changes:

- Defined that upstream pulls happen in the downstream caller's execution
  context and never in the coordinator thread.
- Required the coordinator thread to handle only Ractor waits and result
  forwarding.
- Added `FiberStream::RactorMapError` as the normalized public worker and
  transfer failure contract.
- Defined output-transfer failure reporting with known-shareable error payloads.
- Specified bounded `Thread::SizedQueue` capacities for ready-worker and data
  result queues.
- Required close to send shutdown to every worker, including active workers.
- Required close to wait for coordinator and worker shutdown before returning.
- Required the coordinator to drop post-close data/error messages so a full
  data-result queue cannot block lifecycle processing during shutdown.
- Added validation for Async responsiveness during normal waits and cleanup
  waits.

## Validation

- Spike proving whether Ractor coordination can avoid blocking Async reactor
  fibers. Completed locally on 2026-06-03; coordinator thread is favored.
- Unit tests for lazy construction and input validation.
- Tests proving non-shareable blocks are rejected.
- Tests proving ordered values with out-of-order worker completion.
- Tests proving bounded upstream run-ahead.
- Tests proving `:copy` and `:move` transfer behavior where Ruby permits it.
- Tests proving transfer failures become stream failures.
- Tests proving upstream, mapping, and worker failures preserve ordered error
  delivery.
- Tests proving worker failures become `RactorMapError` with sequence, kind,
  class, and message metadata.
- Tests proving output transfer failures become ordered `RactorMapError`
  failures.
- Tests proving early downstream completion closes upstream and requests worker
  shutdown.
- Tests proving early close waits for coordinator and worker shutdown.
- Tests proving active workers receive shutdown after finishing their current
  job and do not hang waiting for another job.
- Tests proving coordinator shutdown cannot hang behind a full `data_results`
  queue after downstream closes.
- Tests proving queued or in-flight worker failures are suppressed after
  downstream completion or failure.
- Tests proving an Async ticker continues while downstream waits for Ractor
  results.
- Tests proving an Async ticker continues while close waits for in-flight
  worker shutdown.
- Tests proving `ractor_map` works after scheduler-required upstream stages
  when the caller is already in the required scheduler context.
- RBS validation.
- RuboCop.

## Open Questions

None.
