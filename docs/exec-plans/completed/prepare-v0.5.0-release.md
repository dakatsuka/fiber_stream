# Prepare v0.5.0 Release

## Status

Completed

## Objective

Prepare release metadata and user-facing documentation for FiberStream 0.5.0.

## Context

- Changelog: `CHANGELOG.md`
- README: `README.md`
- Website docs: `website/docs/`
- Product specs: `docs/product-specs/`
- Design docs: `docs/design-docs/`
- Version source: `lib/fiber_stream/version.rb`
- Completed work since v0.4.0 includes synchronous flow operators,
  pull-driven rate limiting, and documentation cleanup:
  - `Flow.tap`
  - `Flow.filter_map` / `Source#filter_map`
  - `Flow.reject` / `Source#reject`
  - `Flow.compact` / `Source#compact`
  - `Flow.map_concat` / `Source#map_concat`
  - `Flow.throttle` / `Source#throttle` / `RateLimiter`
  - `Sink.count`

## Clarifications

- User requested release preparation for v0.5.0, including README,
  CHANGELOG, website, and docs inspection and updates.
- Treat this as release preparation only; no runtime behavior changes are
  planned.

## Contract First

No new public API is introduced by this plan. Documentation must reflect the
already-implemented public APIs:

- `FiberStream::Flow.tap { |element| ... }`
- `FiberStream::Flow.filter_map { |element| ... }`
- `Source#filter_map { |element| ... }`
- `FiberStream::Flow.reject { |element| ... }`
- `Source#reject { |element| ... }`
- `FiberStream::Flow.compact`
- `Source#compact`
- `FiberStream::Flow.map_concat { |element| enumerable }`
- `Source#map_concat { |element| enumerable }`
- `FiberStream::Flow.throttle(rate:, per: 1, burst: nil)`
- `FiberStream::Flow.throttle(limiter:)`
- `Source#throttle(rate:, per: 1, burst: nil)`
- `Source#throttle(limiter:)`
- `FiberStream::RateLimiter.new(rate:, per: 1, burst: nil)`
- `FiberStream::RateLimiter#acquire(permits: 1)`
- `FiberStream::Sink.count`

## Steps

- [x] Explore: inspect current release notes, README, website docs, specs,
      design docs, and version metadata.
- [x] Update release metadata and user-facing docs.
- [x] Run focused documentation and library validation commands.
- [x] Request context-free review and address findings.
- [x] Move this plan to completed with verification notes.

## Decisions

- Keep 0.5.0 focused on documentation and release metadata for already
  implemented public APIs.
- Mark completed flow specs and design docs as accepted where execution plans
  and tests already exist.

## Verification

- `bundle exec rake`
  - 709 runs, 1596 assertions, 0 failures, 0 errors, 0 skips.
  - `bundle exec rbs validate` passed.
  - `bundle exec rubocop` inspected 98 files with no offenses.
- `npm run docs:build` from `website/`
  - VitePress build completed.
- `git diff --check`
  - Passed.

## Review

- Context-free review found that `Flow.throttle`, `Source#throttle`, and
  `RateLimiter` were missing from the 0.5.0 changelog and release plan.
- Context-free review found that the website exposed `throttle(limiter:)`
  without a public `RateLimiter` reference page.
- Both findings were addressed by updating the changelog and plan, adding a
  `RateLimiter` website reference page, and linking it from the reference
  sidebar.
- Re-review found only that this execution plan still needed final completion
  notes and a move to `docs/exec-plans/completed/`.

## Completion Notes

Prepared v0.5.0 release notes, bumped release metadata to `0.5.0`, expanded
README and website coverage for synchronous flow operators, rate limiting, and
`Sink.count`, added a public RateLimiter website reference, and promoted
completed flow specs and design docs from Draft to Accepted.

## Commit

Pending.
