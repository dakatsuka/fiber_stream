# Add IO Source

## Status

Completed

## Objective

Add `FiberStream::Source.io` as a scheduler-aware source that emits byte chunks
from an existing IO-like object while preserving pull backpressure and explicit
resource ownership.

## Context

- Product spec: `docs/product-specs/io-source.md`
- Design doc: `docs/design-docs/io-source.md`
- ADR: `docs/design-docs/adr/0004-io-source.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Async boundary design: `docs/design-docs/async-boundary.md`
- Buffer boundary design: `docs/design-docs/buffer-boundary.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Clarifications

- The first IO source reads chunks from an existing IO-like object.
- File opening, line splitting, framing, and IO sinks are deferred.
- Existing IO is not closed by default. `close: true` gives FiberStream close
  responsibility for that materialization.
- `Source.io` is not replayable; repeated materializations read from the same
  IO object's current state.
- IO reads require a scheduler-backed non-blocking fiber context on demand.
- The source stays pull-only; users compose `Flow.async` or
  `Flow.buffer(count)` when they want producer-fiber execution or prefetch.

## Contract First

Add public comments and RBS for:

```ruby
FiberStream::Source.io(io, chunk_size: 16 * 1024, close: false)
```

The method returns `Source[String]`, validates its arguments at construction,
and performs no IO until demand.

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: request sub-agent review and incorporate feedback.
- [x] Red: write failing behavior-focused tests in
      `test/fiber_stream/source_io_test.rb`.
- [x] Green: implement `Source.io` and the private pull source.
- [x] Refactor: keep IO close/error handling small and aligned with existing
      pull stages.
- [x] Static checks: run formatters and static analysis tools, then fix
      findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use byte chunks as the first IO source element type.
- Use `readpartial(chunk_size)` as the read primitive.
- Require both `Fiber.scheduler` and a non-blocking current fiber before IO
  reads.
- Default to `close: false`; use `close: true` for explicit ownership.
- Preserve read failure over close failure when both occur.
- Preserve scheduler validation failure over close failure when both occur.
- Deliver close failure after early downstream completion.
- Preserve downstream failure over close failure when both occur.
- Raise `TypeError` when `readpartial` returns a non-`String`.

## Verification

Final commands:

- `bundle exec ruby -Itest test/fiber_stream/source_io_test.rb`
  - 25 runs, 70 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rake test`
  - 100 runs, 211 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rbs validate`
  - Passed
- `bundle exec rubocop`
  - 16 files inspected, no offenses detected
- `bundle exec rake`
  - Test task: 100 runs, 211 assertions, 0 failures, 0 errors, 0 skips
  - RBS validation passed
  - RuboCop: 16 files inspected, no offenses detected

## Completion Notes

Implemented `FiberStream::Source.io(io, chunk_size: 16 * 1024, close: false)`
as a scheduler-aware pull source that emits byte `String` chunks from an
existing IO-like object. Added public RBS, README status updates, behavior tests
for scheduler requirements, chunking, replayability, ownership, EOF, close/error
precedence, non-`String` return validation, and cleanup. Code review found no
runtime correctness issues; README buffer status and extra `close: false`
regression tests were fixed, and final re-review found no issues.

## Commit

Pending until committed.
