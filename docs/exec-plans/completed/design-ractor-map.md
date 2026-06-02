# Design Flow.ractor_map

## Status

Completed

## Objective

Finalize the public contract and implementation approach for
`FiberStream::Flow.ractor_map` as an ordered Ractor-backed mapping stage for
CPU-bound per-element stream work.

This plan does not implement the public API. It should produce accepted specs,
an accepted design, an ADR, and spike notes that are strong enough to support a
separate implementation plan.

## Context

- Product spec: `docs/product-specs/flow-ractor-map.md`
- Design doc: `docs/design-docs/ractor-map.md`
- Existing design: `docs/design-docs/parallel-map.md`
- Background execution design: `docs/design-docs/background-execution.md`
- References: `docs/references/ruby-ractor.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Clarifications

- `ractor_map` is a separate API from `parallel_map`.
- The first public API should preserve input order.
- The first public API should require a shareable mapper proc.
- `workers` is required and must be explicit.
- `input_transfer` defaults to `:copy`; `:move` is opt-in.
- Local Ruby 4.0.3 spikes showed that direct Ractor waits block Async reactor
  progress, so the design should isolate Ractor waits in a coordinator thread.
- With Ractor waits isolated, `ractor_map` should not require
  `Fiber.scheduler`.
- Upstream pulls stay in the downstream caller's execution context; the
  coordinator thread never calls `upstream.next`.
- Worker and Ractor-transfer failures normalize to `FiberStream::RactorMapError`.
- Boundary close waits for the coordinator thread and worker ractors before
  returning.
- Close sends one shutdown message to every worker, including active workers.
- Coordinator-forwarded data results use bounded `Thread::SizedQueue`
  instances.
- After close begins, the coordinator drops worker data/error messages instead
  of enqueueing them, and processes only lifecycle messages needed for worker
  shutdown.

## Contract First

Proposed public APIs:

- `FiberStream::Flow.ractor_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`
- `FiberStream::Source#ractor_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`
- `FiberStream::RactorMapError`

Initial RBS shape:

```rbs
module FiberStream
  type ractor_transfer_policy = :copy | :move
  type ractor_map_error_kind =
    :input_transfer | :output_transfer | :worker | :worker_termination | :isolation

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

  class RactorMapError < RuntimeError
    attr_reader sequence: Integer
    attr_reader kind: ractor_map_error_kind
    attr_reader cause_class_name: String
    attr_reader cause_message: String
  end
end
```

Contract comments must eventually document:

- CPU-bound intent and Ractor execution
- required shareable block
- worker count validation
- transfer policy validation and effects
- ordered output
- bounded upstream run-ahead
- failure ordering
- cooperative cancellation and worker shutdown
- scheduler/coordinator behavior
- `RactorMapError` metadata

## Steps

- [x] Explore: inspect existing code, specs, design docs, references, and tests.
- [x] Draft product spec and design doc.
- [x] Spike: test Ractor result waiting under Async and determine whether a
      coordinator thread is required.
- [x] Spike: test worker exception transport and decide whether to re-raise
      directly or normalize to `RactorMapError`.
- [x] Spike: test `:copy` and `:move` transfer behavior for representative
      inputs and outputs.
- [x] Spike: test worker shutdown and termination options.
- [x] Design review: request sub-agent review and incorporate feedback.
- [x] Finalize product spec and design doc as accepted.
- [x] Add an ADR recording the accepted public contract and rejected
      alternatives.
- [x] Static checks: run available documentation and code checks.

## Decisions

- Use `ractor_map` rather than adding Ractor options to `parallel_map`.
- Preserve ordered output for the first public API.
- Require `Ractor.shareable_proc` style mapper blocks via `Ractor.shareable?`
  validation.
- Keep `workers` required.
- Default `input_transfer` and `output_transfer` to `:copy`.
- Use a coordinator thread for Ractor waits so Async reactor fibers are not
  blocked by `Ractor#value` or `Ractor.select`.
- Do not require `Fiber.scheduler` for `ractor_map`.
- Use explicit shutdown messages as the first worker shutdown mechanism.
- Keep upstream pulls in the downstream caller's execution context.
- Normalize worker and Ractor-transfer failures to `FiberStream::RactorMapError`.
- Wait for coordinator and worker shutdown during boundary close.
- Send shutdown to every worker during close so active workers observe shutdown
  after their current job.
- Use bounded `Thread::SizedQueue` instances for coordinator-forwarded data
  results.
- Drop post-close worker data/error messages in the coordinator so a full
  bounded data queue cannot block shutdown.

## Verification

Spike commands run as inline Ruby probes on Ruby 4.0.3:

- Ractor wait under Async:
  - Direct `Ractor#value` and `Ractor.select` blocked sibling Async ticks until
    the Ractor result was ready.
  - A coordinator `Thread` blocking on `Ractor.select` and forwarding to
    `Thread::Queue` or `Thread::SizedQueue` allowed sibling Async ticks to
    continue.
- Worker exception transport:
  - Unhandled worker exceptions observed through `Ractor#value` or
    `Ractor.select` arrive as `Ractor::RemoteError` with the original exception
    as `cause`.
  - Exceptions rescued inside a worker can be sent through a `Ractor::Port` as
    result values in tested cases.
- Transfer behavior:
  - Mutable strings copy by value under default transfer.
  - `move: true` makes the sender's object raise `Ractor::MovedError` on later
    access.
  - Some objects fail transfer with `TypeError`, `FiberError`, `NoMethodError`,
    or `Ractor::Error`.
- Worker shutdown:
  - Explicit `:shutdown` messages work.
  - Closing another ractor's incoming port from the main ractor raises
    `Ractor::Error`.
- Design review:
  - A context-free review found missing dispatcher/coordinator ownership,
    incomplete shutdown guarantees, unresolved worker error contracts, missing
    output-transfer failure sequencing, unspecified channel capacities, and
    insufficient scheduler responsiveness validation.
  - The design now keeps upstream pulls in the downstream caller context,
    confines coordinator threads to Ractor waits, normalizes worker and
    Ractor-transfer failures to `RactorMapError`, specifies channel capacities,
    and requires close to wait for coordinator and worker shutdown.
  - Re-review found that active workers could miss shutdown, queue capacity
    wording was ambiguous, and cleanup waits needed Async responsiveness
    validation. The design now sends shutdown to every worker, names
    `Thread::SizedQueue`, and requires an Async ticker test while close waits.
  - Final re-review found that coordinator shutdown could still hang if close
    happened while `data_results` was full. The design now requires the
    coordinator to drop post-close data/error messages and continue processing
    lifecycle messages until every worker stops.
- Final commands:
  - `bundle exec rake`
    - 211 runs, 487 assertions, 0 failures, 0 errors, 0 skips
    - `bundle exec rbs validate` passed
    - `bundle exec rubocop` inspected 29 files with no offenses

## Completion Notes

Finalized `Flow.ractor_map` as a separate ordered Ractor-backed mapping API for
CPU-bound Ruby work. The accepted design keeps upstream pulls in the downstream
caller context, isolates blocking Ractor waits in a coordinator thread, bounds
pulled-but-unemitted work to `workers`, requires a shareable mapper proc, makes
copy/move transfer policies explicit, and normalizes worker/transfer failures
to `FiberStream::RactorMapError`.

Design review found risks around scheduler ownership, shutdown leaks, worker
error contracts, output transfer failure reporting, channel capacity, and
cleanup responsiveness. The final design addresses these by defining
coordinator scope, close semantics, bounded queues, post-close result
suppression, RactorMapError metadata, and Async responsiveness validation.

## Commit

```text
docs: accept ractor map design

CPU-bound stream mapping needs a separate contract from scheduler-backed
parallel_map because Ractor execution changes object transfer, block isolation,
worker shutdown, and error reporting.

Finalize the Flow.ractor_map product spec and design, add ADR 0010, and record
the Ruby 4.0.3 spike results and review decisions. The accepted design uses
ordered bounded Ractor workers, shareable mapper procs, explicit copy/move
transfer policies, coordinator-thread Ractor waits, cooperative worker
shutdown, and RactorMapError normalization.

Co-Authored-By: OpenAI Codex <codex@openai.com>
```
