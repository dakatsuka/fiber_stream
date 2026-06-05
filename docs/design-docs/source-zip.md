# Source.zip

## Status

Accepted

## Context

FiberStream's current runtime materializes a source factory and attaches zero or
more flows into a private pull chain. A sink drives execution by calling `next`
on the downstream end of the chain. `Source#concat` already composes two source
definitions sequentially; `Source#zip` needs to compose two source definitions
element-by-element while preserving the same pull-first execution model and
cleanup guarantees.

Governing documents:

- Product spec: `docs/product-specs/source-zip.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Related design: `docs/design-docs/source-concat.md`
- ADR: `docs/design-docs/adr/0001-initial-linear-pull-runtime.md`

## Goals

- Compose two source definitions without eager construction-time
  materialization.
- Keep downstream demand as the only reason either source advances.
- Pull at most one element from each input source for each downstream output
  pair.
- Complete when either input source completes.
- Close materialized pull chains exactly through idempotent `close` calls.
- Keep the API and internal stage small enough to support later source
  combinators.

## Non-Goals

- Graph materialization.
- Parallel source execution.
- Scheduler-backed coordination.
- A public zip flow.
- Variadic source zipping.
- Zip-with projection APIs.
- Padding or latest-value semantics.

## Proposed Design

`Source` stores a source factory plus an ordered list of flows. `Source#zip`
returns a new `Source` whose factory materializes a private zip pull stream.
The zip pull stream receives two zero-arity materialization callables:

```ruby
Pull.zip(left_materializer, right_materializer)
```

The receiver's materializer builds the receiver source factory and attaches the
receiver flows. The other source's materializer builds the other source factory
and attaches the other source flows. This preserves existing per-source flow
ownership and does not require treating source flows as public state.

`Pull::Zip` stores these materializers without immediately calling them. The
first downstream `next` materializes the left side before pulling it. This
means `source.zip(other).take(0)` can complete without materializing either
input source, because downstream demand never reaches the zip stage. It also
keeps IO acquisition and scheduler-boundary demand points behind downstream
demand.

Each downstream `next` first pulls the left stream. If the left stream returns
`DONE`, the zip stage marks itself completed, closes all materialized sides,
and returns `DONE` without materializing or pulling the right stream for that
output pair unless the right side was already materialized by an earlier pair.

If the left stream returns a value, `Pull::Zip` materializes the right side
when needed, then pulls the right stream. If the right stream returns `DONE`,
the zip stage marks itself completed, closes both materialized sides, discards
the unpaired left value, and returns `DONE`. If the right stream returns a
value, the stage returns `[left_value, right_value]`.

Pull order is intentionally left-before-right. This provides a deterministic
failure and side-effect order for sequential, pull-driven sources. It also
means a left value can be consumed and discarded when the right side completes
on the same downstream pull. That behavior matches common zip semantics: output
contains only complete pairs.

`close` is idempotent. It closes every materialized stream and never
materializes a side only to close it. Close order is left, then right. If both
closes fail and no execution error is already primary, the left close failure is
raised and the right close failure is suppressed and not exposed. When `next`
is already failing due to a source or flow exception, cleanup failures are
suppressed so the primary execution error takes precedence, matching
`Source#run_with`.

Materialization failures propagate as stream failures. If the left materializer
raises, the right side is not materialized. If the right materializer raises
after a left value has been pulled for a pair, the already materialized left
side is closed by zip materialization cleanup. The right materialization
failure remains the primary error; any left close failure during this cleanup
is suppressed and not exposed.

## Contracts

- `Source#zip` accepts only `FiberStream::Source` instances.
- `Source#zip` returns a lazy `Source`.
- The receiver source is materialized only when downstream demand first reaches
  the zip stage.
- The other source is materialized only after the receiver has produced an
  element for a pair.
- When both input sources are materialized, they are materialized in
  receiver-then-other order.
- If downstream completes before pulling the zip stage, neither input source is
  materialized.
- Each side's own flow chain is preserved.
- Each output pair pulls at most one value from each input source.
- Output pairs are two-element arrays `[receiver_element, other_element]`.
- Output pair order follows input element order from both sources.
- The zipped source completes when either input source completes.
- If the receiver completes before producing a value for a pair, the other
  source is not materialized or pulled for that pair unless it was already
  materialized by an earlier pair.
- If the other source completes after the receiver produced a value for a pair,
  that unpaired receiver value is discarded.
- Normal zip completion attempts to close all materialized streams before
  returning `Pull::DONE`.
- `close` closes all materialized streams and does not materialize unstarted
  streams.
- When close failures happen without a primary execution failure, the first
  close failure in receiver-then-other close order propagates from
  `Source#run_with`; later close failures are suppressed and not exposed.
- Failures from either side propagate from `Source#run_with`.
- Materializer failures propagate from `Source#run_with`; unmaterialized sides
  are not closed, and cleanup failures from already materialized sides are
  suppressed.
- Flows attached to either input source before zip apply only to that input
  source; flows attached after zip apply to emitted pairs.
- Public APIs never expose `Pull::DONE`.

Public API:

```ruby
class Source[Elem]
  def zip: [Other] (Source[Other] source) -> Source[[Elem, Other]]
end
```

Internal API:

```ruby
Pull.zip(left_materializer, right_materializer)
```

Both materializers return internal pull streams responding to `next` and
`close`.

## Alternatives Considered

### Materialize Both Inputs During Zip Materialization

Materializing both input sources when the zipped source is materialized would
make the pull stage slightly simpler, but it would acquire resources even when
downstream completes before pulling zip. For example,
`source.zip(other).take(0)` would materialize and then close both input sources
without needing a pair. First-pull materialization better preserves
FiberStream's demand-driven resource timing.

### Pull Right Before Left

Right-before-left would be equally valid for pure sources, but it would make
receiver-side failures and effects happen after the argument source. Pulling
the receiver first follows Ruby method-call intuition and keeps deterministic
precedence easy to document.

### Implement Zip As A Flow

A flow has a single upstream, so representing source zipping as a flow would
require storing a source definition inside flow state or changing the flow
attach contract. A source-level pull stage keeps ownership clearer.

### Add `zip_with`

Projecting pairs with a block is useful, but it adds block error handling and a
second public API shape. The first zip API can emit pairs and let users compose
`map` afterward.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-05. Feedback resulted in these
changes:

- Changed input-source materialization from zip materialization time to
  demand-driven side materialization so zero-pull pipelines such as
  `zip(...).take(0)` do not acquire input resources, and an empty receiver does
  not acquire other-source resources.
- Clarified normal-completion close failure precedence: both materialized sides
  are closed, the first close failure in receiver-then-other order propagates,
  and later close failures are suppressed.
- Clarified that materialization failures remain primary when cleanup of an
  already materialized side also fails.

## Validation

- Unit tests for pair ordering, shortest-source completion, invalid arguments,
  construction laziness, first-pull materialization, `zip(...).take(0)` not
  materializing either input, repeated materialization, composition with flows,
  early sink completion, failure propagation from both sides, materialization
  failure cleanup, and cleanup failure precedence.
- RBS validation for the new public signature.
- Existing Minitest suite and static checks.

## Open Questions

None.
