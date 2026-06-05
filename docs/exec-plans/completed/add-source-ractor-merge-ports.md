# Add Source.ractor_merge_ports

## Status

Completed

## Objective

Add a public `Source.ractor_merge_ports` API that merges multiple
backpressure-aware Ractor producers into one source without requiring a Fiber
scheduler.

## Context

- Product spec: `docs/product-specs/source-ractor-merge-ports.md`
- Design doc: `docs/design-docs/source-ractor-merge-ports.md`
- ADR: `docs/design-docs/adr/0013-source-ractor-merge-ports.md`
- Existing implementation: `lib/fiber_stream/pull/ractor_port_source.rb`
- Related implementation: `lib/fiber_stream/pull/merge.rb`

## Clarifications

- The API is named `Source.ractor_merge_ports`, matching the prior design
  discussion.
- The first API accepts an enumerable of `{ port:, ack_port: }` hashes and
  applies `ack_transfer` / `cancel` uniformly to all producers.
- The API requires at least two port pairs.
- The `ports` enumerable is finite and consumed at construction.
- Data ports and ack ports must be distinct by object identity.

## Contract First

- Add a block comment for `Source.ractor_merge_ports`.
- Add RBS type alias `ractor_port_pair`.
- Add RBS class method signature:
  `Source.ractor_merge_ports(ports, ack_transfer:, cancel:)`.

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: request sub-agent review and incorporate feedback.
- [x] Red: write failing behavior-focused tests in
      `test/fiber_stream/source_ractor_merge_ports_test.rb`.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix
      findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use a coordinator thread and an internal Ractor control port so Ractor waits
  never run in scheduler-managed fibers.
- Use one outstanding ack per active producer so merge can observe ready order
  while keeping bounded per-producer backpressure.
- Reuse `RactorPortSourceError` for all Ractor ingress failures.
- Coordinator-owned producer-terminal state controls cancellation suppression.
- Every ack/cancel send creates a fresh envelope to support `ack_transfer:
  :move`.
- Result-mailbox waits must be scheduler-safe and wake on close.
- Close order is result-mailbox close, coordinator shutdown/wait, then
  cancellation of non-terminal producers so coordinator-observed terminal
  state can suppress cancellation correctly.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/source_ractor_merge_ports_test.rb`
  - 21 runs, 81 assertions, 0 failures
- `bundle exec ruby -Itest test/fiber_stream/source_ractor_port_test.rb`
  - 19 runs, 57 assertions, 0 failures
- `bundle exec ruby -Itest test/fiber_stream/source_merge_test.rb`
  - 23 runs, 53 assertions, 0 failures
- `bundle exec rbs validate`
- `bundle exec rubocop`
  - 71 files inspected, no offenses
- `bundle exec rake test`
  - 416 runs, 991 assertions, 0 failures

## Completion Notes

Added `Source.ractor_merge_ports` with a coordinator-thread pull source,
public RBS signature, product spec, design doc, ADR, README coverage, and
behavior-focused tests. Design and code reviews completed with no remaining
issues.

## Commit

Pending.
