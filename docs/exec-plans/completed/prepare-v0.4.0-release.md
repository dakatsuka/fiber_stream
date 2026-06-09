# Prepare v0.4.0 Release

## Status

Completed

## Objective

Prepare user-facing documentation and release metadata for FiberStream 0.4.0.

## Context

- Changelog: `CHANGELOG.md`
- README: `README.md`
- Website docs: `website/docs/`
- Version source: `lib/fiber_stream/version.rb`
- Completed work since v0.3.0 includes `Flow.parallel_unordered_map`,
  FiberStream-owned Ractor producer sources, `Flow.scan`, and Ruby 4.0.5
  project pinning.

## Clarifications

- User requested release preparation for v0.4.0, including CHANGELOG, README,
  website, and docs updates.

## Contract First

No new public API is introduced by this plan. Documentation must reflect the
already-implemented public APIs:

- `FiberStream::Flow.parallel_unordered_map(concurrency:)`
- `Source#parallel_unordered_map(concurrency:)`
- `FiberStream::Source.ractor_producer`
- `FiberStream::Source.ractor_merge_producers`
- `FiberStream::Flow.scan`
- `Source#scan`

## Steps

- [x] Explore: inspect current release notes, README, website docs, and version.
- [x] Update release metadata and user-facing docs.
- [x] Run focused documentation and library validation commands.
- [x] Request context-free review and address findings.
- [x] Move this plan to completed with verification notes.

## Decisions

- Treat v0.4.0 as a documentation and release metadata preparation change; no
  behavior changes are planned.
- Bump `FiberStream::VERSION` and the path gem entry in `Gemfile.lock` to
  `0.4.0` as release metadata, while leaving the gemspec structure unchanged.
- Prefer high-level owned Ractor producer documentation for new user workflows;
  keep low-level Ractor port docs for externally owned producer Ractors.

## Verification

- `bundle exec rake test`
  - 520 runs, 1233 assertions, 0 failures, 0 errors, 0 skips.
- `bundle exec rbs validate`
  - Passed.
- `bundle exec rubocop`
  - 84 files inspected, no offenses detected.
- `npm run docs:build` from `website/`
  - VitePress build completed.
- `git diff --check`
  - Passed.

## Review

- Context-free review found that the active execution plan was not listed in
  `docs/exec-plans/index.md`.
- The plan was completed and moved to `docs/exec-plans/completed/`, and the
  execution-plan index was updated.

## Completion Notes

Prepared v0.4.0 release notes, bumped release metadata to `0.4.0`, updated the
README API surface and examples, expanded website references for
`Flow.parallel_unordered_map`, `Flow.scan`, and owned Ractor producer sources,
updated Ractor source tutorial guidance, and refreshed repository docs for the
current Ruby 4.x validation target.

## Commit

Pending.
