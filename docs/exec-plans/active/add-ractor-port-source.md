# Add Ractor Port Source

## Status

Active

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
- [ ] Red: write failing behavior-focused tests, with unit test files organized
      per module.
- [ ] Green: implement the smallest change that satisfies the tests.
- [ ] Refactor: improve structure while keeping tests green.
- [ ] Static checks: run formatters and static analysis tools, then fix
      findings.
- [ ] Code review: request sub-agent review after implementation.
- [ ] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use a dedicated Ractor source API instead of overloading `Source.each`.
- Require a backpressure handshake from the first public API.
- Use typed `Data` envelopes instead of symbols.
- Use shareable producer failure metadata instead of exception objects.
- Keep one-way buffered Ractor ingress out of scope.

## Verification

Not run yet. This plan is at the design stage.

## Completion Notes

Pending.

## Commit

Pending.
