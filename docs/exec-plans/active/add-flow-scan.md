# Add Flow.scan

## Status

Active

## Objective

Add `Flow.scan(initial) { |accumulator, element| ... }` and
`Source#scan(initial) { |accumulator, element| ... }` as lazy,
pull-driven operators that emit intermediate accumulator values using the same
reducer contract as `Sink.fold`.

## Context

- Product spec: `docs/product-specs/flow-scan.md`
- Design doc: `docs/design-docs/flow-scan.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Related terminal API: `docs/product-specs/minimum-linear-pipeline.md`

## Clarifications

- `Flow.scan` emits one accumulator per upstream element and does not emit
  `initial` before the first element.
- Empty upstream completes without emitting values.
- For non-empty upstream, the final emitted accumulator must equal
  `Sink.fold(initial, reducer)` over the same input sequence.

## Contract First

- Add `Flow.scan(initial, &block)` with `Sink.fold`-style reducer semantics.
- Add `Source#scan(initial, &block)` as the convenience method with the same
  block validation.
- Add private `Pull.scan(upstream, initial, reducer)`.
- Add RBS signatures:
  `def self.scan: [Elem, Acc] (Acc initial) { (Acc, Elem) -> Acc } -> Flow[Elem, Acc]`
  and
  `def scan: [Acc] (Acc initial) { (Acc, Elem) -> Acc } -> Source[Acc]`.
- Document public interfaces with block comments.

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: request sub-agent review and incorporate feedback.
- [ ] Red: write failing behavior-focused tests, with unit test files organized
      per module.
- [ ] Green: implement the smallest change that satisfies the tests.
- [ ] Refactor: improve structure while keeping tests green.
- [ ] Static checks: run formatters and static analysis tools, then fix findings.
- [ ] Code review: request sub-agent review after implementation.
- [ ] Re-review: fix review findings and repeat review until it passes.

## Decisions

None yet.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_scan_test.rb`
- `bundle exec rbs validate`
- `bundle exec rubocop`

## Completion Notes

Pending.

## Commit

Pending.
