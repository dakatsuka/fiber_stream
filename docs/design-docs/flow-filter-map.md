# Flow.filter_map

## Status

Accepted

## Context

FiberStream already has pure linear `Flow.map`, which emits one transformed
value for every upstream element, and `Flow.select`, which drops elements whose
predicate result is falsey while passing accepted original elements unchanged.
`Flow.filter_map` combines the two user-visible operations: it transforms an
input element, emits the transform result when it is truthy, and drops the input
when the transform result is `false` or `nil`.

This flow should remain a small pull-driven linear stage. It does not need a
scheduler, queue, async boundary, or resource ownership contract.

## Goals

- Implement `Flow.filter_map` as a small private pull stage.
- Add `Source#filter_map` as the matching convenience method.
- Preserve pull-driven backpressure, ordering, laziness, and cleanup contracts.
- Keep validation consistent with other block-based flows.
- Keep the falsey-result behavior aligned with Ruby truthiness.

## Non-Goals

- Emitting falsey transform results.
- Predicate-only filtering of original elements.
- Multi-output flat-mapping.
- Async, buffered, parallel, or Ractor-backed execution.
- New public runtime abstractions.

## Proposed Design

`Flow.filter_map` validates that a block is present and stores an attach
callable that returns `Pull.filter_map(upstream, transform)`.

`Pull::FilterMap` stores `@upstream`, `@transform`, `@closed`, and `@done`.
On each `next`:

1. Return `DONE` immediately if closed or already done.
2. Pull one upstream element.
3. If upstream returns `DONE`, mark done and return `DONE`.
4. Call the transform block with the upstream element.
5. If the block result is truthy, return that result.
6. If the block result is `false` or `nil`, discard it and continue the loop.

The loop means one downstream demand may pull multiple upstream elements, just
like `Flow.select`, but upstream still advances only in response to downstream
demand. The stage never buffers rejected results and emits at most one output
per input.

Transform exceptions and upstream exceptions propagate as stream failures. The
stage does not close upstream inside `next` for falsey results because
filtering does not terminate the stream. `Source#run_with` continues to own
materialized-chain cleanup and primary error precedence.

`close` is idempotent and propagates upstream.

## Contracts

- `Flow.filter_map` requires a block and raises `ArgumentError` when missing.
- `Source#filter_map` requires a block through `Flow.filter_map` delegation and
  raises `ArgumentError` when missing.
- `Flow.filter_map` calls the block once per upstream element observed by the
  stage.
- Truthy block results are emitted downstream as transformed values.
- `false` and `nil` block results are dropped and are never emitted.
- Output order matches accepted input order.
- A single downstream demand may pull multiple upstream elements.
- Rejected results are discarded immediately and not buffered.
- The stage emits at most one downstream element for each upstream element.
- If upstream completes before another truthy result, downstream completes
  normally.
- After completion, later defensive pulls return `DONE` without pulling
  upstream.
- Transform and upstream failures propagate from `Source#run_with`.
- Transform and upstream failures remain primary over later cleanup close
  failures from `Source#run_with`.
- `Source#filter_map` delegates to `Flow.filter_map` and returns a new
  `Source`.
- Public APIs never expose `Pull::DONE`.

Public API:

```rbs
class Flow[In, Out]
  def self.filter_map: [In, Out] () { (In) -> (Out | false | nil) } -> Flow[In, Out]
end

class Source[Elem]
  def filter_map: [Out] () { (Elem) -> (Out | false | nil) } -> Source[Out]
end
```

RBS cannot express that `Out` excludes `false` and `nil`. The signature accepts
falsey transform results as possible block returns, while the runtime contract
defines those values as drop signals rather than emitted output values.

Internal API:

```ruby
Pull.filter_map(upstream, transform)
```

## Alternatives Considered

### Implement With `Flow.map` And `Flow.select`

A naive composition such as `Flow.map { ... }.via(Flow.select { |value| value
})` matches the broad behavior, but it creates two stages for a common
operation and makes the falsey-result contract an accidental interaction
between separate flows. A dedicated stage gives the API a direct contract,
keeps tests focused, and avoids materializing an extra pull wrapper.

### Drop Only `nil`

Some APIs use `nil` as the only absence marker. Ruby's `filter_map` drops both
`false` and `nil`, and FiberStream's existing filtering APIs use Ruby
truthiness. Dropping both falsey values keeps the operation predictable for Ruby
users.

### Emit Original Elements

Emitting original elements would duplicate `Flow.select`. `filter_map` should
emit transformed values so users can parse, normalize, or project accepted
inputs in one stage.

## Third-Party Review

Context-free design review found the runtime behavior and pull-stage design
coherent and implementable. It identified two documentation issues:

- RBS cannot enforce that `Out` excludes falsey values, so the product and
  design docs now state that falsey exclusion is a runtime contract and the RBS
  signature is an approximation.
- `Source#filter_map` missing-block behavior was implied by delegation but not
  explicit, so both the requirements and validation list now call it out.

## Validation

- Unit tests for emitting truthy transformed values, dropping `false` and
  `nil`, preserving order, empty upstream completion, all-rejected completion,
  laziness, `Flow.filter_map` and `Source#filter_map` missing block validation,
  pull counts for rejected prefixes, defensive pulls after completion, close
  propagation, transform failure propagation, upstream failure propagation,
  cleanup close failure precedence, and `Source#filter_map`.
- RBS validation for public signatures.
- Existing Minitest suite and RuboCop.

## Open Questions

None.
