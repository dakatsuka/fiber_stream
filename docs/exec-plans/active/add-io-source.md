# Add IO Source

## Status

Active

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
- [ ] Red: write failing behavior-focused tests in
      `test/fiber_stream/source_io_test.rb`.
- [ ] Green: implement `Source.io` and the private pull source.
- [ ] Refactor: keep IO close/error handling small and aligned with existing
      pull stages.
- [ ] Static checks: run formatters and static analysis tools, then fix
      findings.
- [ ] Code review: request sub-agent review after implementation.
- [ ] Re-review: fix review findings and repeat review until it passes.

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

Pending implementation.

## Completion Notes

Pending implementation.

## Commit

Pending implementation.
