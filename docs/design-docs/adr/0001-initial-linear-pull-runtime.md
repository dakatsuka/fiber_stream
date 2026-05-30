# ADR 0001: Initial Linear Pull Runtime

## Status

Accepted

## Context

FiberStream should eventually support asynchronous, non-blocking streams with
backpressure. The first implementation should be small enough to complete, but
it must not choose an execution model that conflicts with backpressure,
cleanup, or future scheduler integration.

## Decision

FiberStream will start with linear pipelines only. Public APIs build lazy stream
definitions. `Source#run_with` materializes a fresh pull chain and runs it in
the current fiber. `Source.each` creates fresh execution state with
`enumerable.to_enum(:each)` for each materialization, but it does not snapshot
values or promise replayability for one-shot enumerables. Downstream pulls drive
upstream progress, and normal completion is represented internally with a
private, identity-compared `Pull::DONE` sentinel. `Sink.first` is included as the
first public early-completion operation.

The initial runtime will not create a fiber per stage. Fiber and scheduler
integration will be introduced only for features that need asynchronous
boundaries, IO, buffering, or parallelism.

FiberStream will not depend on `async` at runtime. `async` is a development
dependency and compatibility target.

## Consequences

- The first implementation has real pull-based backpressure.
- `Source.each`, `Flow.map`, `Sink.to_a`, and `Sink.first` can be implemented
  without queues.
- Cleanup is explicit through idempotent `close`.
- `Source.each` supports normal Ruby `Enumerable` implementations that yield to
  a block.
- `Source.each` does not own or close the original enumerable; future
  resource-owning sources need separate contracts.
- `run_with` has a simple foreground execution contract.
- Future `.async`, `.buffer`, and IO operations need additional cancellation and
  queue contracts before implementation.
- Users can run pure pipelines without a scheduler.
- Async compatibility can be tested without coupling the public API to Async.

## Alternatives Rejected

- Push-first runtime with backpressure added later.
- `Enumerator::Lazy` as the internal runtime protocol.
- `StopIteration` for normal stream completion.
- Per-stage fibers for the initial pure linear stages.
