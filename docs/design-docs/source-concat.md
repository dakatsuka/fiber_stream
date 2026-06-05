# Source.concat

## Status

Accepted

## Context

FiberStream's current runtime materializes a single source factory and attaches
zero or more flows into a private pull chain. A sink drives execution by calling
`next` on the downstream end of the chain. `Source#concat` needs to compose two
complete source definitions while preserving this pull-first execution model
and the cleanup guarantees documented by the linear pull runtime.

## Goals

- Compose two source definitions without eager materialization.
- Keep downstream demand as the only reason either source advances.
- Avoid pulling or materializing the appended source until the receiver
  completes.
- Close materialized pull chains exactly through idempotent `close` calls.
- Keep the API and internal stage small enough to support later source
  combinators.

## Non-Goals

- Graph materialization.
- Parallel source execution.
- A public concat flow.
- Variadic source concatenation.

## Proposed Design

`Source` already stores a source factory plus an ordered list of flows.
`Source#concat(source)` returns a new `Source` whose factory materializes a
private concat pull stream. The concat pull stream receives two zero-arity
materialization callables:

```ruby
left_stream = left_materializer.call
right_stream = right_materializer.call
```

The receiver's materializer builds the receiver source factory and attaches the
receiver flows. The appended source's materializer builds the appended source
factory and attaches the appended source flows. This preserves existing
per-source flow ownership and does not require treating source flows as public
state.

`Pull::Concat` starts in the left phase. Each downstream `next` delegates to the
active left stream. Normal values are returned immediately. When the left stream
returns `DONE`, the concat stage closes the left stream, switches to the right
phase, materializes the right stream, and pulls from it to satisfy the same
downstream demand. This means the first right element can be returned by the
same pull that observed left completion, while the right side is still not
materialized before the downstream asks for another element.

The left close during this phase transition is part of the active pull, not
background cleanup. If that close raises, concat treats it as the primary
failure for the current `next`, does not materialize the right side, and leaves
normal final cleanup to suppress secondary cleanup failures.

In the right phase, `next` delegates to the right stream. When the right stream
returns `DONE`, the concat stage closes it, marks itself completed, and returns
`DONE`. Later defensive pulls return `DONE` without touching either side.

`close` is idempotent. It closes every materialized stream and never
materializes a side only to close it. If multiple close operations fail, the
first close failure is raised. When `next` is already failing due to a source or
flow exception, cleanup failures are suppressed so the primary execution error
takes precedence, matching `Source#run_with`.

Materialization failures propagate as stream failures. If the right materializer
raises after the left side has completed and closed, the right materialization
failure is re-raised from `Source#run_with`; no unmaterialized side is closed.

## Contracts

- `Source#concat` accepts only `FiberStream::Source` instances.
- `Source#concat` returns a lazy `Source`.
- The receiver is materialized only when the concatenated source is
  materialized.
- The appended source is materialized only after downstream demand observes
  receiver completion.
- Receiver values are emitted before appended source values.
- Each side's own flow chain is preserved.
- The receiver stream is closed after normal receiver completion and before the
  appended source is materialized.
- If receiver close fails during the transition to the appended source, the
  close failure propagates and the appended source is not materialized.
- `close` closes all materialized streams and does not materialize unstarted
  streams.
- Failures from the receiver prevent appended source materialization.
- Failures from either side propagate from `Source#run_with`.
- Materializer failures propagate from `Source#run_with`; unmaterialized sides
  are not closed.
- Flows attached to either input source before concat apply only to that input
  source; flows attached after concat apply to the concatenated output.
- Public APIs never expose `Pull::DONE`.

Public API:

```ruby
class Source[Elem]
  def concat: [Other] (Source[Other] source) -> Source[Elem | Other]
end
```

Internal API:

```ruby
Pull.concat(left_materializer, right_materializer)
```

Both materializers return internal pull streams responding to `next` and
`close`.

## Alternatives Considered

### Eagerly Materialize Both Sources

Materializing both sources at the start would make implementation simpler, but
it would start scheduler-backed boundaries, touch IO resources, and run source
setup for the appended source before downstream demand can reach it. That would
weaken the backpressure and resource timing expected from the pull model.

### Implement Concat As A Flow

A flow has a single upstream, so representing source concatenation as a flow
would require smuggling the appended source through flow state or changing the
flow attach contract. A source-level pull stage keeps ownership clearer.

### Convert Sources To Enumerables

Wrapping sources in Ruby enumerators would add another completion protocol and
make cleanup precedence harder to keep consistent with the existing pull
runtime. Reusing the internal pull protocol keeps normal completion and cleanup
behavior consistent.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-05. Feedback resulted in these
changes:

- Clarified that a receiver close failure during the left-to-right transition
  is the primary failure for that pull and prevents appended source
  materialization.
- Clarified materialization failure behavior for either side.
- Clarified that flows before concat are scoped to their source, while flows
  after concat apply to the combined output.

## Validation

- Unit tests for ordering, laziness, right-side delayed materialization,
  composition with flows, invalid arguments, repeated materialization, early
  sink completion, failure propagation, close failure during phase transition,
  and cleanup.
- RBS validation for the new public signature.
- Existing Minitest suite and static checks.

## Open Questions

None.
