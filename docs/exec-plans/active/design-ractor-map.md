# Design Flow.ractor_map

## Status

Active

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
- Scheduler interaction, worker failure normalization, output transfer, and
  worker termination remain open until spike results are recorded.

## Contract First

Proposed public APIs:

- `FiberStream::Flow.ractor_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`
- `FiberStream::Source#ractor_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`

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

## Steps

- [x] Explore: inspect existing code, specs, design docs, references, and tests.
- [x] Draft product spec and design doc.
- [ ] Spike: test Ractor result waiting under Async and determine whether a
      coordinator thread is required.
- [ ] Spike: test worker exception transport and decide whether to re-raise
      directly or normalize to `RactorMapError`.
- [ ] Spike: test `:copy` and `:move` transfer behavior for representative
      inputs and outputs.
- [ ] Spike: test worker shutdown and termination options.
- [ ] Design review: request sub-agent review and incorporate feedback.
- [ ] Finalize product spec and design doc as accepted.
- [ ] Add an ADR recording the accepted public contract and rejected
      alternatives.
- [ ] Static checks: run available documentation and code checks.

## Decisions

- Use `ractor_map` rather than adding Ractor options to `parallel_map`.
- Preserve ordered output for the first public API.
- Require `Ractor.shareable_proc` style mapper blocks via `Ractor.shareable?`
  validation.
- Keep `workers` required.
- Default `input_transfer` and `output_transfer` to `:copy`.

## Verification

Pending.

## Completion Notes

Pending.

## Commit

Pending.
