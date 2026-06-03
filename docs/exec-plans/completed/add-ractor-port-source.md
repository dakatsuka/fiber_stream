# Add Ractor Port Source

## Status

Completed

## Objective

Add a backpressure-aware `Source.ractor_port` API that lets producer ractors
emit values into FiberStream through typed protocol envelopes.

## Context

- Product spec: `docs/product-specs/ractor-port-source.md`
- Design doc: `docs/design-docs/ractor-port-source.md`
- ADR: `docs/design-docs/adr/0011-ractor-port-source.md`
- References:
  - `docs/references/ruby-ractor.md`
  - `docs/references/akka-stream-actor-source.md`

## Clarifications

- Ractor ingress should be separate from `Source.each`.
- The first Ractor ingress API should be backpressure-aware.
- Ack, complete, data, failure, and cancel messages should be typed `Data`
  envelopes instead of symbols.

## Contract First

- Add `FiberStream::Source.ractor_port`.
- Add `FiberStream::RactorPort` protocol envelope classes.
- Add `FiberStream::RactorPortSourceError`.
- Add structured `RactorPortSourceError` metadata for failure kind and original
  cause.
- Define coordinator shutdown with an internal shutdown port before writing
  tests.
- Add RBS signatures before internal implementation.
- Document public contracts with block comments.

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
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

- Use a dedicated Ractor source API instead of overloading `Source.each`.
- Require a backpressure handshake from the first public API.
- Use typed `Data` envelopes instead of symbols.
- Use shareable producer failure metadata instead of exception objects.
- Keep one-way buffered Ractor ingress out of scope.
- Producer-to-source transfer policy is controlled by the producer's
  `Ractor::Port#send(..., move:)` call because Ruby 4.0.3 receive has no
  `move:` option.
- The producer creates `ack_port`; Ruby only allows `Ractor::Port#receive` from
  the creating ractor.
- Close wakes an outstanding coordinator wait through an internal shutdown
  port instead of relying on `Ractor::Port#close`.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/source_ractor_port_test.rb`: 19
  runs, 57 assertions, 0 failures.
- `bundle exec rake test`: 255 runs, 612 assertions, 0 failures.
- `bundle exec rake rbs`: passed.
- `bundle exec rake rubocop`: 37 files inspected, no offenses.
- Implementation review passed after adding coverage for `cancel: false`,
  `ack_transfer: :move`, receive failure normalization, and stricter Async
  responsiveness.

## Completion Notes

Implemented `Source.ractor_port` with typed `RactorPort` envelopes,
`RactorPortSourceError`, RBS signatures, coordinator-thread Ractor waits,
cooperative ack/cancel backpressure, producer-terminal cancellation
suppression, and focused source tests.

## Commit

`feat: add ractor port source`
