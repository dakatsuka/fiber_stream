# Add Flow.tap

## Status

Completed

## Objective

Add `Flow.tap { |element| ... }` as a lazy pass-through observing stage for
middle-of-pipeline side effects.

## Context

- Product spec: `docs/product-specs/flow-tap.md`
- Design doc: `docs/design-docs/flow-tap.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Related terminal API: `docs/product-specs/sink-foreach.md`

## Clarifications

- `tap` observes values in the middle of a pipeline and passes them through
  unchanged.
- The observer block return value is ignored.
- `Sink.foreach` remains the terminal side-effect API.

## Contract First

- Add `Flow.tap { |element| ... }` with missing block validation.
- Add private `Pull.tap(upstream, observer)` and `Pull::Tap`.
- Add RBS signatures:
  `def self.tap: [Elem] () { (Elem) -> void } -> Flow[Elem, Elem]`.
- Document public interfaces with block comments.

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Draft product spec, design doc, and execution plan.
- [x] Design review: request sub-agent review and incorporate feedback.
- [x] Red: write failing behavior-focused tests, with unit test files organized
      per module.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix
      findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use a dedicated private pull stage rather than encoding the operation with
  `Flow.map`.
- Keep `tap` synchronous and scheduler-free.
- Preserve original object identity and ignore observer return values.
- Do not add `Source#tap`, because it would override Ruby's inherited
  `Object#tap` with incompatible lazy per-element behavior.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_tap_test.rb`: 16 runs, 37
  assertions, 0 failures, 0 errors.
- `bundle exec rake test`: 572 runs, 1352 assertions, 0 failures, 0 errors.
- `bundle exec rbs validate`: passed.
- `bundle exec rubocop`: 90 files inspected, no offenses detected.
- Implementation code review: no runtime/API contract issues found after
  verifying that `Source#tap` remains inherited `Object#tap`.

## Completion Notes

Implemented `Flow.tap` as a synchronous pull stage that observes each emitted
element before passing the original value downstream unchanged. The API does
not add `Source#tap`; sources keep Ruby's inherited `Object#tap` behavior.

## Commit

`feat: add Flow.tap`
