# Add Sink.all?

## Status

Completed

## Objective

Add `FiberStream::Sink.all? { |element| ... }` as a terminal predicate sink
that returns whether every upstream element matches without consuming values
after the answer is known.

## Context

- Product spec: `docs/product-specs/sink-all.md`
- Design doc: `docs/design-docs/linear-pull-runtime.md`
- Closest existing implementation: `FiberStream::Sink.any?`
- Existing sink tests: `test/fiber_stream/sink_test.rb`

## Clarifications

- `Sink.all?` should be a terminal sink, not a flow.
- `Sink.all?` should mirror `Sink.any?` early completion, Ruby truthiness,
  failure propagation, cleanup precedence, scheduler independence, and no
  buffering, while returning `true` only when no predicate result is falsey.
- `Sink.all?` returns `true` for empty upstream, matching Ruby's universal
  predicate behavior.
- `Sink.all?` requires a block. The no-block `Enumerable#all?` truthiness form
  is intentionally out of scope.

## Contract First

Public API:

```ruby
FiberStream::Sink.all? { |element| truthy_or_falsey }
```

RBS shape:

```rbs
module FiberStream
  class Sink[In, Mat]
    def self.all?: [Elem] () { (Elem) -> boolish } -> Sink[Elem, bool]
  end
end
```

Source comment:

```ruby
# Creates a sink that returns whether all elements match a predicate.
#
# The sink pulls upstream until the block returns false or nil, or upstream
# completes. It returns `false` after a falsey predicate result and `true` when
# every predicate result is truthy, including for empty upstream.
```

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: use `.agents/reviews/design-review.md` and incorporate
      feedback.
- [x] Red: write failing behavior-focused tests, with unit test files organized
      per module.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix
      findings.
- [x] Code review: use `.agents/reviews/code-review.md` after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Keep `Sink.all?` block-required to match `Sink.any?` and avoid adding
  `Enumerable#all?`'s no-block truthiness form in this narrow change.
- Implement directly in `lib/fiber_stream/sink.rb` beside `Sink.any?`, so early
  completion and boolean return semantics remain explicit.
- Design review found the product spec sound and identified runtime design doc
  drift. Updated `docs/design-docs/linear-pull-runtime.md` before
  implementation.
- Code review found no behavioral issues. The first pass identified
  documentation and execution-plan drift; both were fixed and re-review passed
  with no findings.

## Verification

- `ruby -Itest test/fiber_stream/sink_test.rb`
  - Red before implementation: failed with `NoMethodError` for missing
    `Sink.all?`.
  - Green after implementation: 83 runs, 144 assertions, 0 failures, 0 errors,
    0 skips.
- `bundle exec rbs validate`
  - Passed.
- `ruby -Itest test/dev/doc_index_check_test.rb`
  - Passed, 2 runs, 4 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rake`
  - Passed finally, 796 runs, 1768 assertions, 0 failures, 0 errors, 0 skips.
  - Also ran `bundle exec rbs validate` and `bundle exec rubocop`
    successfully.
  - One intermediate run hit an existing Ractor unordered-map test failure; the
    targeted test passed on immediate rerun and the final full default gate
    passed.
- `bundle exec rake verify:full`
  - Initially failed at `docs:build` because `npm` was not installed in this
    environment: `npm run docs:build` exited with status 127.
  - After Node/npm were made available through `mise` and website dependencies
    were installed with `mise exec -- npm ci`, passed: 796 runs, 1768
    assertions, 0 failures, 0 errors, 0 skips; RBS validation passed; RuboCop
    passed; VitePress documentation build completed successfully.

## Completion Notes

Implemented `FiberStream::Sink.all?`, added the public RBS signature,
documented the API in README and website reference docs, and covered universal
predicate semantics, empty input, truthiness, falsey `nil` and `false`
elements, early completion, construction laziness, missing blocks, identity
sentinel behavior, predicate failures, upstream failures, and cleanup close
failure precedence.

## Commit

```text
feat(sink): add Sink.all?
```
