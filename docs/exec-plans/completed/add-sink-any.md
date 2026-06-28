# Add Sink.any?

## Status

Completed

## Objective

Add `FiberStream::Sink.any? { |element| ... }` as a terminal predicate sink
that returns whether any upstream element matches without consuming values after
the answer is known.

## Context

- Product spec: `docs/product-specs/sink-any.md`
- Design doc: `docs/design-docs/linear-pull-runtime.md`
- Closest existing implementation: `FiberStream::Sink.find`
- Existing sink tests: `test/fiber_stream/sink_test.rb`

## Clarifications

- User asked to commit the completed specification work first, then proceed
  with an execution plan and implementation.
- `Sink.any?` should be a terminal sink, not a flow.
- `Sink.any?` should mirror `Sink.find` early completion, Ruby truthiness,
  failure propagation, cleanup precedence, scheduler independence, and no
  buffering, while returning `true` or `false`.
- `Sink.any?` requires a block. The no-block `Enumerable#any?` truthiness form
  is intentionally out of scope.

## Contract First

Public API:

```ruby
FiberStream::Sink.any? { |element| truthy_or_falsey }
```

RBS shape:

```rbs
module FiberStream
  class Sink[In, Mat]
    def self.any?: [Elem] () { (Elem) -> boolish } -> Sink[Elem, bool]
  end
end
```

Source comment:

```ruby
# Creates a sink that returns whether any element matches a predicate.
#
# The sink pulls upstream until the block returns a truthy value or upstream
# completes. It returns `true` after a truthy predicate result and `false` when
# no element matches.
```

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: request context-free review and incorporate feedback.
- [x] Red: write failing behavior-focused tests, with unit test files organized
      per module.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix
      findings.
- [x] Code review: request context-free review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use a dedicated boolean sink rather than recommending `!!Sink.find`, because
  `Sink.find` intentionally returns matching `nil` and `false` elements and is
  therefore ambiguous for existence checks.
- Keep `Sink.any?` block-required to match existing predicate sink constructors
  and avoid adding `Enumerable#any?`'s no-block truthiness form in this narrow
  change.
- Implement directly in `lib/fiber_stream/sink.rb` beside `Sink.find` rather
  than composing public sinks, so early completion and falsey matching elements
  remain explicit.

## Verification

- `mise exec -- ruby -Itest test/fiber_stream/sink_test.rb`
  - Red before implementation: failed with `NoMethodError` for missing
    `Sink.any?`.
  - Green after implementation: 64 runs, 110 assertions, 0 failures, 0 errors,
    0 skips.
- `mise exec -- bundle exec rbs validate`
  - Passed.
- `mise exec -- bundle exec rubocop`
  - Passed, 103 files inspected, no offenses detected.
- `mise exec -- bundle exec rake`
  - Passed, 777 runs, 1734 assertions, 0 failures, 0 errors, 0 skips.
  - Also ran `bundle exec rbs validate` and `bundle exec rubocop`
    successfully.
- `mise exec -- ruby -Itest test/dev/doc_index_check_test.rb`
  - Passed, 2 runs, 4 assertions, 0 failures, 0 errors, 0 skips.

## Completion Notes

Implemented `FiberStream::Sink.any?`, added the public RBS signature, documented
the API in the README, and covered normal boolean results, empty input,
truthiness, matching `nil` and `false` elements, early completion, construction
laziness, missing blocks, identity sentinel behavior, predicate failures,
upstream failures, and cleanup close failure precedence.

Code review found no behavioral issues. The reviewer noted a residual test gap
for direct predicate construction laziness; that coverage was added and final
re-review reported no findings.

## Commit

```text
feat(sink): add Sink.any?
```
