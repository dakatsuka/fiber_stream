# Add Ractor Producer Sources

## Status

Completed

## Objective

Add high-level FiberStream-owned Ractor producer source APIs that hide port
setup and protocol envelopes while preserving existing backpressure,
cancellation, and Ractor ingress error behavior.

## Context

- Product spec: `docs/product-specs/ractor-producer-sources.md`
- Design doc: `docs/design-docs/ractor-producer-sources.md`
- ADR: `docs/design-docs/adr/0016-ractor-producer-sources.md`
- Existing low-level implementation: `lib/fiber_stream/pull/ractor_port_source.rb`
- Existing merge implementation: `lib/fiber_stream/pull/ractor_merge_ports_source.rb`
- Existing protocol envelopes: `lib/fiber_stream/ractor_port.rb`

## Clarifications

- The governing design was completed in git commit `319412aefb0f`.
- Construction validates blocks and options but does not create ports or
  producer ractors.
- High-level producer sources always own their producer ractors and therefore
  always request cooperative cancellation during cleanup.
- The first implementation should reuse the existing single-port and
  merge-port pull sources after owned-producer setup succeeds.

## Contract First

- Add block comments for `Source.ractor_producer` and
  `Source.ractor_merge_producers`.
- Add public `RactorProducer` and `RactorProducerGroup` contracts.
- Extend `RactorPortSourceError` kinds with `:producer_setup`.
- Add RBS signatures for the new source APIs and producer context classes.

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: request sub-agent review and incorporate feedback.
- [x] Red: write failing behavior-focused tests in
      `test/fiber_stream/source_ractor_producer_test.rb` covering:
      construction validation, producer context validation, laziness,
      single-producer happy path, merge happy path, cooperative cancellation,
      producer block failure, manual producer failure, setup failure
      normalization, unexpected producer termination, close during setup,
      same-ack send failure reporting, scheduler responsiveness, and RBS
      contracts.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix
      findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use a setup adapter pull source that starts producers on first demand, waits
  for producer-created acknowledgment ports in a coordinator thread, then
  delegates to `Pull.ractor_port` or `Pull.ractor_merge_ports`.
- Keep setup-port waits, setup-completion waits, delegated low-level result
  waits, delegated close waits, and producer termination waits out of
  scheduler-managed pipeline fibers.
- Monitor producer termination from coordinator-owned threads so monitoring
  does not consume ack permission. Unexpected termination is reported only when
  the source has observed demand for that producer and no terminal producer
  message has already won the race.
- Preserve low-level error precedence after setup by relying on existing
  delegated pull sources for protocol, ack, cancel, and merge behavior.
- Normalize spawn, argument transfer, and setup acknowledgment failures to
  `RactorPortSourceError` kind `:producer_setup`; after a partial setup
  failure, cancel started producers and suppress cleanup failures under the
  setup error.
- Merge producer registration validates required/shareable blocks and
  per-producer transfer at construction, while allowing `transfer: nil` to
  inherit the merge-level default.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/source_ractor_producer_test.rb`
  - 23 runs, 68 assertions, 0 failures
- `bundle exec ruby -Itest test/fiber_stream/source_ractor_port_test.rb`
  - 20 runs, 63 assertions, 0 failures
- `bundle exec ruby -Itest test/fiber_stream/source_ractor_merge_ports_test.rb`
  - 22 runs, 88 assertions, 0 failures
- `bundle exec rbs validate`
- `bundle exec rubocop`
  - 80 files inspected, no offenses
- `bundle exec rake test`
  - 502 runs, 1197 assertions, 0 failures
- Design review:
  - Added explicit scheduler-safety, setup cleanup, monitor/ack coordination,
    and validation test expectations to this plan.
- Initial code review:
  - Fixed close-during-setup cleanup for late setup ack ports.
  - Fixed partial setup failure cleanup for already-started producers.
  - Fixed same-ack fallback double-failure handling.
  - Moved unexpected producer termination reporting into low-level demand
    coordinators so it does not bypass ack permission.
- Re-review:
  - Fixed close/delegate-install races for single and merge producer sources.
  - Fixed setup-thread failure cleanup for unready peers.
  - Final narrow re-review reported no findings.

## Completion Notes

Added `Source.ractor_producer` and `Source.ractor_merge_producers` with public
producer context and merge builder contracts. The implementation starts owned
producer ractors lazily, wires producer-created ack ports through a setup
adapter, delegates normal pulls to the existing low-level Ractor port sources,
and handles setup cleanup, cooperative cancellation, producer failures, and
unexpected producer termination.

## Commit

Pending.
