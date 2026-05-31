# Add IO Sink

## Status

Completed

## Objective

Add `FiberStream::Sink.io` as a scheduler-aware sink that writes byte chunks to
an existing IO-like object while preserving pull backpressure and explicit
flush and close ownership.

## Context

- Product spec: `docs/product-specs/io-sink.md`
- Design doc: `docs/design-docs/io-sink.md`
- ADR: `docs/design-docs/adr/0005-io-sink.md`
- IO source design: `docs/design-docs/io-source.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Async boundary design: `docs/design-docs/async-boundary.md`
- Buffer boundary design: `docs/design-docs/buffer-boundary.md`
- References:
  - `docs/references/ruby-fiber-and-tooling.md`
  - `docs/references/socketry-io-stream.md`

## Clarifications

- The first IO sink writes `String` chunks to an existing IO-like object.
- Ruby core IO objects are the primary output target.
- File opening, line formatting, framing, byte-count materialization, retry
  policies, and duplex helpers are deferred.
- Existing IO is not closed by default. `close: true` gives FiberStream close
  responsibility for that materialization.
- Existing IO is not flushed by default. `flush: true` flushes only after normal
  upstream completion.
- The sink returns the number of successful write calls.
- IO-like objects must provide complete-write-or-raise behavior for `write`.
- Repeated materializations write to the same IO object state.
- IO operations require a scheduler-backed non-blocking fiber context.
- `io-stream` remains an optional caller-provided wrapper, not a runtime
  dependency.

## Contract First

Add public comments and RBS for:

```ruby
FiberStream::Sink.io(io, close: false, flush: false)
```

The method returns `Sink[String, Integer]`, validates its arguments at
construction, performs no IO until materialization, and returns the count of
successful write calls.

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: request sub-agent review and incorporate feedback.
- [x] Red: write failing behavior-focused tests in
      `test/fiber_stream/sink_io_test.rb`.
- [x] Green: implement `Sink.io`.
- [x] Refactor: keep IO write, flush, close, and error precedence handling
      small and aligned with `Source.io`.
- [x] Static checks: run formatters and static analysis tools, then fix
      findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use `Sink.io(io, close: false, flush: false)` rather than `Sink.write`.
- Accept any IO-like object that responds to the required methods.
- Treat Ruby core IO objects as the primary integration path.
- Require upstream elements to be `String` values.
- Count successful write calls rather than bytes.
- Ignore `io.write` return values.
- Require complete-write-or-raise `write` semantics from IO-like objects.
- Preserve scheduler, upstream, type, and write failures over close failures.
- Deliver normal-completion flush and close failures as stream failures.
- Keep `io-stream` optional and caller-managed.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/sink_io_test.rb`
  - 39 runs, 117 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rake test`
  - 139 runs, 328 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rbs validate`
  - Passed
- `bundle exec rubocop`
  - 17 files inspected, no offenses detected
- `bundle exec rake`
  - Test task: 139 runs, 328 assertions, 0 failures, 0 errors, 0 skips
  - RBS validation passed
  - RuboCop: 17 files inspected, no offenses detected

## Completion Notes

Implemented `FiberStream::Sink.io(io, close: false, flush: false)` as a
scheduler-aware sink that writes `String` chunks to an existing IO-like object.
Added public RBS, README status updates, behavior tests for scheduler
requirements, Ruby core pipe writes, chunk-count materialization, repeated
materialization, flush/close ownership, close/error precedence, non-`String`
element validation, and cleanup. Code review found no runtime correctness
issues; additional terminal scheduler and no-flush failure tests were added,
and final re-review found no issues.

## Commit

```text
feat: add IO sink

Implement Sink.io so FiberStream can materialize String chunks into existing
IO-like objects while preserving pull backpressure, scheduler requirements,
flush behavior, close ownership, and close/error precedence.

Add RBS, README status updates, accepted docs status, completed execution-plan
notes, and behavior coverage for Ruby core IO, scheduler validation, flushing,
cleanup, and repeated materialization.

Co-Authored-By: OpenAI <codex@openai.com>
```
