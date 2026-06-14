# Flow.compact

## Status

Draft

## Context

FiberStream already has synchronous linear filtering through `Flow.select`,
`Flow.reject`, and transform-and-filter behavior through `Flow.filter_map`.
`Flow.compact` is a narrower filtering operation: it drops only `nil` upstream
elements and passes every non-`nil` element through unchanged.

This flow should remain a small pull-driven linear stage. It does not need a
scheduler, queue, async boundary, or resource ownership contract beyond the
existing pull-stage close contract.

Governing documents:

- Product spec: `docs/product-specs/flow-compact.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Related public APIs: `Flow.select`, `Flow.reject`, and `Flow.filter_map`

## Goals

- Implement `Flow.compact` as a small private pull stage.
- Add `Source#compact` as the matching convenience method.
- Preserve pull-driven backpressure, ordering, laziness, identity, and cleanup
  contracts.
- Keep `nil` handling aligned with Ruby's `compact` behavior.

## Non-Goals

- Truthiness-based filtering.
- Transforming retained elements.
- Multi-output flat-mapping.
- Async, buffered, parallel, or Ractor-backed execution.
- New public runtime abstractions.

## Proposed Design

`Flow.compact` does not accept options, does not use a supplied block, and
stores an attach callable that returns `Pull.compact(upstream)`.

`Pull::Compact` stores `@upstream`, `@closed`, and `@done`. On each `next`:

1. Return `DONE` immediately if closed or already done.
2. Pull one upstream element.
3. If upstream returns `DONE`, mark done and return `DONE`.
4. If the value is `nil`, discard it and continue the loop.
5. Otherwise, return the original upstream value unchanged.

The loop means one downstream demand may pull multiple upstream elements, just
like `Flow.select` and `Flow.reject`, but upstream still advances only in
response to downstream demand. The stage never buffers dropped values and emits
at most one output per input.

Upstream exceptions propagate as stream failures. The stage does not close
upstream inside `next` for `nil` values because filtering does not terminate
the stream. `Source#run_with` continues to own materialized-chain cleanup and
primary error precedence.

`close` is idempotent and propagates upstream.

## Contracts

- `Flow.compact` does not accept options and does not use a supplied block.
- `Source#compact` does not accept options and does not use a supplied block.
- `nil` upstream elements are dropped.
- `false` upstream elements are emitted unchanged.
- Every other non-`nil` upstream element is emitted unchanged.
- Retained values preserve object identity.
- Output order matches retained input order.
- A single downstream demand may pull multiple upstream elements.
- Dropped `nil` values are discarded immediately and not buffered.
- The stage emits at most one downstream element for each upstream element.
- If upstream completes before another retained element, downstream completes
  normally.
- After completion, later defensive pulls return `DONE` without pulling
  upstream.
- Upstream failures propagate from `Source#run_with`.
- Upstream failures still trigger materialized-chain cleanup, including
  upstream close propagation.
- Upstream failures remain primary over later cleanup close failures from
  `Source#run_with`.
- Cleanup close failures after otherwise successful completion or early sink
  completion fail `Source#run_with`.
- `Source#compact` delegates to `Flow.compact` and returns a new `Source`.
- Public APIs never expose `Pull::DONE`.

Public API:

```rbs
class Flow[In, Out]
  def self.compact: [Elem] () -> Flow[Elem, Elem]
end

class Source[Elem]
  def compact: () -> Source[Elem]
end
```

Internal API:

```ruby
Pull.compact(upstream)
```

## Alternatives Considered

### Implement With `Flow.reject`

Users can write `Flow.reject(&:nil?)`, and an implementation could internally
compose that flow. A dedicated stage gives the API a direct contract matching
Ruby's `compact`, avoids requiring a predicate block for a common operation,
and avoids materializing an extra pull wrapper.

### Use `Flow.filter_map`

`filter_map { |value| value }` would drop both `false` and `nil`, which is not
the `compact` contract. `false` is a present value and must pass through.

### Narrow RBS Output Types

The runtime behavior removes `nil`, but RBS cannot express subtracting `nil`
from a generic element type in this API. Keeping `Source[Elem]` and
`Flow[Elem, Elem]` is less precise but matches the rest of the public
signatures without inventing unsupported type machinery.

## Third-Party Review

Context-free design review found no blocking issues. It recommended clarifying
the accidental-block behavior because Ruby methods without `&block` silently
ignore supplied blocks. The product spec and design now explicitly state that
`Flow.compact` and `Source#compact` do not use supplied blocks, and the
validation plan includes that behavior.

## Validation

- Unit tests for dropping `nil`, retaining `false`, ignoring supplied blocks,
  preserving order,
  preserving retained object identity, empty upstream completion, all-`nil`
  completion, laziness, pull counts for dropped prefixes, defensive pulls after
  completion, close propagation after normal completion and early sink
  completion, upstream failure propagation, upstream close propagation after
  upstream failure, cleanup close failure precedence, and `Source#compact`.
- RBS validation for public signatures.
- Existing Minitest suite and RuboCop.

## Open Questions

None.
