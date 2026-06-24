---
name: release-prep
description: Prepare or review FiberStream release readiness, version bumps, changelog and website updates, full verification, gem builds, and tag/publish boundaries. Use when Codex is asked to prepare a release, bump a version, check release readiness, or review release process changes in this repository.
---

# Release Prep

Use this workflow for version bumps, release candidates, or release readiness
reviews.

## Steps

- Confirm the target version and whether the release is a final release or a
  preparatory change.
- Update `lib/fiber_stream/version.rb`, `CHANGELOG.md`, README, examples, and
  website documentation together when the public surface or version changes.
- Check release-sensitive metadata in `fiber_stream.gemspec`, especially source,
  changelog, and RubyGems metadata.
- Run the full local release gate:

```sh
bundle exec rake verify:full
```

- Build the gem when release artifacts are part of the task:

```sh
bundle exec gem build fiber_stream.gemspec
```

- Record verification results in the execution plan or final response.

## Notes

- Release publishing is handled by the `Release` GitHub Actions workflow after a
  version tag is pushed.
- Do not tag or publish unless the user explicitly asks for that action.


