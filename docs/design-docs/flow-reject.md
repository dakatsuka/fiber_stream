# Flow.reject

## Status

Draft

## Context

FiberStream already has synchronous linear filtering through `Flow.select`.
`Flow.reject` is the complement: it evaluates a predicate for each upstream
element, drops elements when the predicate is truthy, and passes the original
element through when the predicate is `false` or `nil`.

This flow should remain a small pull-driven linear stage. It does not need a
scheduler, queue, async boundary, or resource ownership contract beyond the
existing pull-stage close contract.

Governing documents:

- Product spec: `docs/product-specs/flow-reject.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Related public API: `Flow.select`

## Goals

- Implement `Flow.reject` as a small private pull stage.
- Add `Source#reject` as the matching convenience method.
- Preserve pull-driven backpressure, ordering, laziness, identity, and cleanup
  contracts.
- Keep validation consistent with other block-based flows.
- Keep truthiness behavior aligned with Ruby and the existing `Flow.select`
  contract.

## Non-Goals

- Transforming retained elements.
- Prefix-only dropping.
- Early stream completion.
- Multi-output, buffered, async, parallel, or Ractor-backed filtering.
- New public runtime abstractions.

## Proposed Design

`Flow.reject` validates that a block is present and stores an attach callable
that returns `Pull.reject(upstream, predicate)`.

`Pull::Reject` stores `@upstream`, `@predicate`, `@closed`, and `@done`. On each
`next`:

1. Return `DONE` immediately if closed or already done.
2. Pull one upstream element.
3. If upstream returns `DONE`, mark done and return `DONE`.
4. Call the predicate block with the upstream element.
5. If the predicate result is truthy, discard the element and continue the
   loop.
6. If the predicate result is `false` or `nil`, return the original upstream
   element unchanged.

The loop means one downstream demand may pull multiple upstream elements, just
like `Flow.select`, but upstream still advances only in response to downstream
demand. The stage never buffers rejected elements and emits at most one output
per input.

Predicate exceptions and upstream exceptions propagate as stream failures. The
stage does not close upstream inside `next` for truthy predicate results because
filtering does not terminate the stream. `Source#run_with` continues to own
materialized-chain cleanup and primary error precedence.

`close` is idempotent and propagates upstream.

## Contracts

- `Flow.reject` requires a block and raises `ArgumentError` when missing.
- `Source#reject` requires a block through `Flow.reject` delegation and raises
  `ArgumentError` when missing.
- `Flow.reject` calls the predicate once per upstream element observed by the
  stage.
- Truthy predicate results drop the original upstream element.
- Non-boolean truthy predicate results, such as `0` or symbols, are truthy and
  drop the original upstream element.
- `false` and `nil` predicate results emit the original upstream element
  unchanged.
- Retained values preserve object identity.
- Output order matches retained input order.
- A single downstream demand may pull multiple upstream elements.
- Rejected elements are discarded immediately and not buffered.
- The stage emits at most one downstream element for each upstream element.
- If upstream completes before another retained element, downstream completes
  normally.
- After completion, later defensive pulls return `DONE` without pulling
  upstream.
- Predicate and upstream failures propagate from `Source#run_with`.
- Predicate and upstream failures still trigger materialized-chain cleanup,
  including upstream close propagation.
- Predicate and upstream failures remain primary over later cleanup close
  failures from `Source#run_with`.
- Cleanup close failures after otherwise successful completion or early sink
  completion fail `Source#run_with`.
- `Source#reject` delegates to `Flow.reject` and returns a new `Source`.
- Public APIs never expose `Pull::DONE`.

Public API:

```rbs
class Flow[In, Out]
  def self.reject: [Elem] () { (Elem) -> boolish } -> Flow[Elem, Elem]
end

class Source[Elem]
  def reject: () { (Elem) -> boolish } -> Source[Elem]
end
```

Internal API:

```ruby
Pull.reject(upstream, predicate)
```

## Alternatives Considered

### Implement With `Flow.select`

Users can write `Flow.select { |value| !predicate.call(value) }`, but this
forces every caller to encode the complement manually. A dedicated stage gives
the API a direct, discoverable contract matching `Enumerable#reject`, keeps
tests focused, and avoids materializing an extra pull wrapper.

### Implement `Source#reject` Only

Adding only the source convenience method would make `reject` unavailable for
explicit flow composition with `via`, `to`, or reusable flow definitions.
FiberStream exposes filtering behavior through `Flow.select`; `reject` should
use the same split between reusable flow and source convenience API.

### Reject Only Strict `true`

Ruby's selection APIs use truthiness rather than strict booleans, and
FiberStream's existing predicate-based flows follow the same model. Treating
all truthy values as rejection signals keeps `reject` symmetric with `select`.

## Third-Party Review

Context-free design review found no blocking issues. It recommended three
clarifications:

- Validation should explicitly prove materialized-chain cleanup after predicate
  and upstream failures.
- Truthiness validation should include non-boolean truthy predicate results.
- The `Flow.select { !predicate.call(value) }` alternative should be described
  as semantically valid but less direct, rather than subtly wrong.

The product spec and design now record failure cleanup, non-boolean truthiness,
and the revised alternative rationale.

## Validation

- Unit tests for dropping truthy predicate results including non-boolean truthy
  values, retaining `false` and `nil` predicate results, preserving order,
  preserving retained object identity, empty upstream completion, all-rejected
  completion, laziness, `Flow.reject` and `Source#reject` missing block
  validation, pull counts for rejected prefixes, defensive pulls after
  completion, close propagation after normal completion and early sink
  completion, predicate failure propagation, upstream failure propagation,
  upstream close propagation after predicate and upstream failures, cleanup
  close failure precedence, and `Source#reject`.
- RBS validation for public signatures.
- Existing Minitest suite and RuboCop.

## Open Questions

None.
