# Add Flow.throttle

## Status

Completed

## Objective

Add `Flow.throttle`, `Source#throttle`, and `RateLimiter` as a pull-driven
rate-limiting operator with a default local token-bucket implementation and a
custom limiter extension point.

## Context

- Product spec: `docs/product-specs/flow-throttle.md`
- Design doc: `docs/design-docs/flow-throttle.md`
- ADR: `docs/design-docs/adr/0017-flow-throttle-rate-limiter.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Scheduler reference: `docs/references/ruby-fiber-and-tooling.md`

## Clarifications

- The default `RateLimiter` should exist so users can call
  `Flow.throttle(rate:, per:)` without constructing a limiter.
- Users should be able to pass a limiter object to coordinate quotas across
  pipelines.
- `RateLimiter.new(...) { |request| ... }` should allow alternative internal
  policies such as Redis-backed state without FiberStream depending on Redis.
- The throttle stage controls downstream admission. It may pull one upstream
  value before waiting for the limiter, but it must not prefetch farther.
- FiberStream-owned throttle waits should be non-blocking. Construction and
  immediate permit grants do not require a scheduler, but any required sleep
  must validate `Fiber.scheduler` and a non-blocking current fiber.

## Contract First

- Add `FiberStream::RateLimiter` with public contract comments.
- Add `RateLimiter::Request` with readers for `rate`, `per`, `burst`,
  `permits`, and `now`.
- Add `RateLimiter#acquire(permits: 1)`.
- Add scheduler validation before `RateLimiter` performs any FiberStream-owned
  sleep, raising `SchedulerRequiredError` for missing scheduler or blocking
  current fiber.
- Add `Flow.throttle` with keyword-presence tracking so `limiter:` rejects
  explicit `rate:`, `per:`, and `burst:`.
- Add `Source#throttle` with the same keyword behavior.
- Add private `Pull.throttle(upstream, limiter)` and `Pull::Throttle`.
- Add RBS signatures for all public APIs.

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Draft product spec, design doc, ADR, and execution plan.
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

- Use a public `RateLimiter` object rather than hiding sleep logic inside
  `Flow.throttle`.
- Use a token bucket as the default local policy.
- Create a fresh default limiter per materialization for the `rate:` form.
- Default `burst` to `rate`.
- Let custom `RateLimiter` blocks return a wait duration instead of requiring
  them to perform all waiting internally.
- Pull upstream before acquiring a permit so normal completion is not delayed
  by limiter waits.
- Reject impossible `RateLimiter#acquire(permits:)` calls where
  `permits > burst`.
- Require scheduler-backed non-blocking waits for default `RateLimiter`
  sleeps and positive wait durations returned by custom `RateLimiter` blocks.
- Do not require a scheduler for construction or immediate permit grants.
- Do not hold internal token-bucket mutexes while invoking custom limiter
  blocks.
- Treat custom block and custom limiter thread safety as user responsibility.
- Treat custom limiter object waiting as user responsibility, but document that
  it must be scheduler-friendly and must not block the native Ruby thread.
- Recheck throttle closed state after limiter acquisition returns. If closed,
  suppress the pulled value, return completion, and do not refund the acquired
  permit.
- Raise `ArgumentError` for non-finite or non-real numeric wait durations
  returned by custom `RateLimiter` blocks.
- Document steady pacing with `burst: 1`.
- Document rate-limiting a side-effecting stage by placing `throttle` before
  that stage.
- Treat arbitrary custom limiter native-thread blocking as outside
  FiberStream's detectable contract; test failure propagation, not a hanging
  custom limiter.

## Design Review

Context-free design review completed on 2026-06-10. Accepted findings:

- Specify default limiter lifetime for reusable flows and sources.
- Prevent impossible `permits > burst` waits.
- Reject ignored `per:` and `burst:` options with `limiter:`.
- Define custom block concurrency and thread-safety responsibility.
- Clarify cleanup and cancellation behavior during limiter waits.
- Reject non-finite or non-real durations and wait values.
- Define close-during-wait suppression and no-refund behavior after the
  non-blocking scheduler update.
- Add example and validation-plan refinements from third-party design feedback:
  side-effect stage placement, `burst: 1` steady pacing, upstream failure before
  limiter acquisition, and custom limiter responsibility boundaries.

## Verification

- Red check: `bundle exec ruby -Itest test/fiber_stream/rate_limiter_test.rb`
  and `bundle exec ruby -Itest test/fiber_stream/flow_throttle_test.rb`
  failed before implementation because `RateLimiter`, `Flow.throttle`,
  `Source#throttle`, and `Pull.throttle` were not defined.
- `bundle exec ruby -Itest test/fiber_stream/rate_limiter_test.rb`: 12 runs,
  32 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec ruby -Itest test/fiber_stream/flow_throttle_test.rb`: 24 runs,
  50 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rake test`: 556 runs, 1315 assertions, 0 failures, 0 errors,
  0 skips.
- `bundle exec rbs validate`: passed.
- `bundle exec rubocop`: 88 files inspected, no offenses detected.
- Context-free implementation review passed after fixing findings for
  `burst: false` validation and boundary composition cleanup coverage.

## Completion Notes

Implemented `FiberStream::RateLimiter`,
`Flow.throttle(rate:, per:, burst:)`, `Flow.throttle(limiter:)`, and
`Source#throttle(...)`.

The default limiter is a local token bucket with scheduler-backed non-blocking
waits, fresh per-materialization state for the `rate:` form, explicit shared
limiter support through `limiter:`, and custom policy blocks that may return
wait durations. The throttle pull stage preserves order, limits run-ahead to
one pulled value, suppresses values when acquisition fails or the stage closes
while waiting, and delegates cleanup through the existing pull runtime.

Added focused tests for validation, scheduler requirements, limiter sharing,
fresh default limiter materializations, upstream failure behavior, close during
acquisition, boundary composition with `async`, `buffer`, and `parallel_map`,
RBS signatures, README/reference documentation, and changelog coverage.

## Commit

`feat: add Flow.throttle`
