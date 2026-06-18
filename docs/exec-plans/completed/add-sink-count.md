# Add Sink.count

## Status

Completed

## Objective

Add `FiberStream::Sink.count` as a terminal sink that consumes the complete
stream and returns the number of elements observed without storing them.

## Context

- Product spec: `docs/product-specs/minimum-linear-pipeline.md`
- Design doc: `docs/design-docs/linear-pull-runtime.md`
- Existing sink implementation: `lib/fiber_stream/sink.rb`
- Existing sink tests: `test/fiber_stream/sink_test.rb`

## Clarifications

- User asked to proceed with design and implementation for `Sink.count`.
- `Sink.count` should be a simple terminal operation, not a flow, and should
  preserve the existing linear pull runtime.

## Contract First

Public API:

```ruby
FiberStream::Sink.count
```

RBS shape:

```rbs
module FiberStream
  class Sink[In, Mat]
    def self.count: [Elem] () -> Sink[Elem, Integer]
  end
end
```

Update `sig/fiber_stream.rbs` with this public signature and run RBS
validation.

Source comment:

```ruby
# Creates a sink that counts all stream elements.
#
# The sink consumes upstream until normal completion and returns the number of
# elements observed. It does not store consumed elements.
```

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

- Extend the existing minimum linear pipeline spec instead of creating a new
  product spec because `Sink.count` is a narrow terminal materialization API.
- Extend the existing linear pull runtime design instead of adding a new design
  document because no new runtime boundary, scheduling, ownership, or topology
  behavior is introduced.
- Return `Integer`, with empty streams returning `0`.
- Consume the complete upstream stream and rely on existing `Source#run_with`
  cleanup for normal completion and failures.
- Count all public stream elements, including `nil` and `false`; only the
  private `Pull::DONE` sentinel ends the stream.
- Added staged red tests before implementation; the first targeted test run
  failed because `Sink.count` was not yet defined.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/sink_test.rb`
  - Red before implementation: failed with `NoMethodError` for missing
    `Sink.count`.
  - Green after implementation: 27 runs, 44 assertions, 0 failures, 0 errors.
- `bundle exec rbs validate`
  - Passed.
- `bundle exec rubocop`
  - Passed, 98 files inspected, no offenses detected.
- `bundle exec rake test`
  - Passed, 709 runs, 1596 assertions, 0 failures, 0 errors.

## Completion Notes

Implemented `FiberStream::Sink.count`, added public RBS coverage, documented
the API in repository specs and user-facing docs, and covered normal counting,
empty streams, falsey elements, flow composition, full consumption, and
laziness.

Design review requested a falsey-element test, laziness coverage, RBS
verification, and one design-doc consistency fix; all were incorporated.
Implementation review found no behavioral bugs and requested verification notes
in this plan; this completed plan records them.

## Commit

Not committed in this task.
