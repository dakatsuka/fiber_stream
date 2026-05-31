# Add IO Sink

## Status

Active

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
- [ ] Red: write failing behavior-focused tests in
      `test/fiber_stream/sink_io_test.rb`.
- [ ] Green: implement `Sink.io`.
- [ ] Refactor: keep IO write, flush, close, and error precedence handling
      small and aligned with `Source.io`.
- [ ] Static checks: run formatters and static analysis tools, then fix
      findings.
- [ ] Code review: request sub-agent review after implementation.
- [ ] Re-review: fix review findings and repeat review until it passes.

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

Pending implementation.

## Completion Notes

Pending implementation.

## Commit

Pending implementation.
