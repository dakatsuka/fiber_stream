# Add Flow.scan

## Status

Completed

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
- [x] Red: write failing behavior-focused tests, with unit test files organized
      per module.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Implement `Flow.scan` as a dedicated private synchronous pull stage instead
  of encoding the behavior through `Flow.map` and closure state.
- Do not emit the initial accumulator. Each emitted value is the result of
  applying the reducer to one upstream element.
- Assign and emit the reducer result directly, preserving mutable accumulator
  aliasing and matching `Sink.fold` assignment semantics.
- Keep `Source#scan` as a convenience wrapper over `Source#via(Flow.scan)`.

## Verification

- Red check: `bundle exec ruby -Itest test/fiber_stream/flow_scan_test.rb`
  failed before implementation because `Flow.scan` and `Source#scan` were not
  defined.
- `bundle exec ruby -Itest test/fiber_stream/flow_scan_test.rb`: 18 runs, 36
  assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rake test`: 520 runs, 1233 assertions, 0 failures, 0 errors,
  0 skips.
- `bundle exec rbs validate`: passed.
- `bundle exec rubocop`: 84 files inspected, no offenses detected.
- Context-free implementation review passed with no findings.

## Completion Notes

Implemented `Flow.scan(initial) { |accumulator, element| ... }`,
`Source#scan(initial) { |accumulator, element| ... }`, and private
`Pull::Scan`. The stage emits one updated accumulator per upstream element,
does not emit the initial accumulator, keeps one accumulator reference, and
uses the existing `Source#run_with` cleanup path for failure precedence.

Added RBS signatures, README/API documentation, changelog entry, and focused
tests covering running accumulators, source convenience, laziness, immediate
upstream pull counts, empty input, fold correspondence, mutable accumulator
aliasing, no rollback of in-place mutation on reducer failure, upstream
failure propagation, reducer failure propagation, close propagation, and
cleanup failure precedence.

## Commit

`feat: add Flow.scan`
