# Prepare v0.6.0 Release

## Status

Completed

## Objective

Prepare release metadata and verify FiberStream 0.6.0 for release.

## Context

- Changelog: `CHANGELOG.md`
- README: `README.md`
- Website docs: `website/docs/`
- Product specs: `docs/product-specs/`
- Design docs: `docs/design-docs/`
- Version source: `lib/fiber_stream/version.rb`
- Release workflow: `.github/workflows/release.yml`
- Completed work since v0.5.0 includes:
  - `Flow.ractor_unordered_map` / `Source#ractor_unordered_map`
  - `Sink.find`
  - `Sink.any?`
  - `Sink.all?`
  - rate-limiting tutorial coverage
  - repository verification and release workflow improvements

## Clarifications

- Treat the requested v0.6.0 release as final release preparation.
- Prepare and verify release metadata, but do not create or push a tag and do
  not publish the gem without an explicit request.
- No runtime behavior changes are planned as part of release preparation.

## Contract First

No new public API is introduced by this plan. Release documentation must
reflect the already-implemented public APIs:

- `FiberStream::Flow.ractor_unordered_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`
- `Source#ractor_unordered_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`
- `FiberStream::Sink.find { |element| ... }`
- `FiberStream::Sink.any? { |element| ... }`
- `FiberStream::Sink.all? { |element| ... }`

## Steps

- [x] Explore: inspect changes since v0.5.0, release metadata, README, website
      docs, specs, design docs, and release automation.
- [x] Update release metadata and release notes.
- [x] Run the full release gate and build the gem.
- [x] Request context-free review and address findings.
- [x] Move this plan to completed with verification notes.

## Decisions

- Keep v0.6.0 focused on already-implemented public APIs and documentation.
- Retain an empty `Unreleased` section above the dated v0.6.0 notes for future
  changes.
- Use the repository date, 2026-07-27, as the release-note date.

## Verification

- `mise exec -- bundle exec rake verify:full`
  - 796 runs, 1768 assertions, 0 failures, 0 errors, 0 skips.
  - `bundle exec rbs validate` passed.
  - `bundle exec rubocop` inspected 103 files with no offenses.
  - The VitePress website build completed.
- `mise exec -- bundle exec gem build fiber_stream.gemspec`
  - Built `fiber_stream-0.6.0.gem`.
  - The artifact contains 61 files, requires Ruby 4.0 or newer, and points its
    source metadata at tag `v0.6.0`.
- Installed the built gem without dependencies into an isolated directory
  under `/tmp`.
  - `require "fiber_stream"` reported version `0.6.0`.
  - A basic mapping pipeline passed.
  - Packaged API smoke tests passed for `Sink.find`, `Sink.any?`, `Sink.all?`,
    and `Source#ractor_unordered_map`.
- `git diff --check`
  - Passed.

## Review

- Context-free review found that the completed metadata update, verification,
  gem build, and artifact checks were not yet recorded and that this plan still
  appeared active.
- The execution record was completed and the plan was moved to `completed/`.
- No other correctness findings were reported. The reviewer confirmed that
  version and lockfile entries, changelog content, gem metadata, source and
  changelog URIs, and packaged runtime files were consistent.
- The review's residual risk was a missing isolated artifact install and
  `require` smoke test. That test had completed after the review began and is
  recorded above.
- Context-free re-review reported no findings after the execution record and
  isolated artifact tests were added.
- The tag-triggered GitHub release and RubyGems trusted-publishing path remain
  untested until `v0.6.0` is pushed.
- Local full verification used Node 26.4.0 while the release workflow uses
  Node 24. The workflow runs the full gate before publishing, but that exact
  runner combination has not yet executed.

## Completion Notes

Prepared v0.6.0 release notes, bumped the version and lockfile metadata to
`0.6.0`, verified the existing README and website coverage for the release's
public APIs, passed the full release gate, built and inspected the gem, and
smoke-tested the packaged public APIs from an isolated installation.

## Commit

`chore(release): prepare v0.6.0`
