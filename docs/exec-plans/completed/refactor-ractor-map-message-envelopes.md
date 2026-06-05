# Refactor Ractor Map Message Envelopes

## Status

Completed

## Objective

Replace `Flow.ractor_map`'s internal Ractor-crossing tagged array worker
messages with private `Data` envelopes, without changing public APIs,
observable stream behavior, or `RactorMapError` contracts.

## Context

- Product spec: `docs/product-specs/flow-ractor-map.md`
- Design doc: `docs/design-docs/ractor-map.md`
- Ractor reference: `docs/references/ruby-ractor.md`
- Existing implementation: `lib/fiber_stream/pull/ractor_map_boundary.rb`
- Existing tests: `test/fiber_stream/flow_ractor_map_test.rb`

`Source.ractor_port` already uses `Data` envelopes for Ractor-facing protocol
messages. `Flow.ractor_map` still uses positional tagged arrays for worker
jobs, lifecycle messages, and worker results. The public `ractor_map` API does
not expose those messages, so this work is an internal readability and
maintainability refactor.

## Clarifications

- Public APIs stay unchanged:
  `Flow.ractor_map`, `Source#ractor_map`, and `RactorMapError`.
- Product behavior stays unchanged.
- The first refactor target is only the Ractor-crossing worker protocol:
  worker input messages and worker-to-coordinator output messages.
- Main-ractor-only ordered result messages may remain tagged arrays unless a
  very small follow-up conversion is clearly useful during implementation.
- `parallel_map` and other pull boundaries are out of scope.

## Contract First

- Private worker input envelopes:
  - `Job(sequence, value)`
  - `Shutdown()`
- Private worker output envelopes:
  - `Ready(worker_id)`
  - `WorkerValue(worker_id, sequence, value)`
  - `WorkerFailure(worker_id, sequence, kind, cause_class_name, cause_message)`
  - `Stopped(worker_id)`
- All envelopes are implementation-private constants inside
  `FiberStream::Pull::RactorMapBoundary`, and the implementation must mark
  them with `private_constant`.
- Copy and move transfer semantics must remain unchanged. In particular,
  `input_transfer: :move` must not require FiberStream to inspect moved input
  values or the moved `Job` envelope after successfully sending a job to a
  worker. Any needed metadata, especially `sequence`, must be copied to locals
  before send.
- `output_transfer` applies only to `WorkerValue` messages carrying mapped user
  values. `Ready`, `WorkerFailure`, and `Stopped` remain copy-safe control or
  error messages containing copy-safe metadata.
- Failure ordering, bounded admission, shutdown, and coordinator error
  detection must remain unchanged.

## Steps

- [x] Explore: inspect current implementation, specs, design docs, tests, and
      Ruby Ractor reference notes.
- [x] Design review: request context-free sub-agent review and incorporate
      justified feedback before implementation.
- [x] Red: add or adjust focused tests only if the refactor needs regression
      coverage beyond existing behavior tests. Update existing worker-spawner
      test shims that intentionally exercise private worker protocol messages.
- [x] Green: replace worker protocol tagged arrays with private `Data`
      envelopes.
- [x] Refactor: keep conversion helpers and pattern matching small and
      explicit.
- [x] Static checks: run targeted tests and default checks.
- [x] Code review: request context-free sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use `Data.define` instead of structs or hashes to match the `RactorPort`
  envelope style and support Ruby pattern matching.
- Keep the envelope constants scoped to `RactorMapBoundary` so they do not
  become public API; mark them `private_constant`.
- Preserve main-ractor result array messages initially to avoid broadening the
  refactor into all pull boundary internals.

## Design Review

Context-free review on 2026-06-05 found no public API issue with the approach.
The review identified four constraints now incorporated above:

- store sequence metadata before `input_transfer: :move` sends because moving a
  `Job` also moves the envelope object;
- mark envelope constants with `private_constant`;
- apply `output_transfer` only to mapped-value messages, not lifecycle or
  failure metadata messages;
- update private worker-spawner regression test shims that send and receive the
  worker protocol directly.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_ractor_map_test.rb`
  - 25 runs, 68 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rbs validate`
  - passed
- `bundle exec rubocop lib/fiber_stream/pull/ractor_map_boundary.rb test/fiber_stream/flow_ractor_map_test.rb`
  - 2 files inspected, no offenses detected
- `bundle exec rake`
  - 354 runs, 820 assertions, 0 failures, 0 errors, 0 skips
  - `bundle exec rbs validate` passed
  - `bundle exec rubocop` inspected 57 files with no offenses

## Completion Notes

`Flow.ractor_map` now uses private `Data` envelopes for messages that cross the
Ractor worker boundary: `Job`, `Shutdown`, `Ready`, `WorkerValue`,
`WorkerFailure`, and `Stopped`. Main-ractor-only ordered result messages remain
tagged arrays to keep this refactor scoped to the worker protocol.

The implementation preserves copy and move transfer behavior by recording job
sequence metadata before sending `Job` envelopes with `move: true`, and by
applying `output_transfer` only to `WorkerValue` envelopes carrying mapped user
values. Lifecycle and failure messages continue to use copy-safe control
metadata. Existing worker-spawner regression test shims were updated to use the
private envelope classes.

Code review found no code-level behavioral regressions. Documentation findings
around execution-plan status and copy-safe failure metadata wording were fixed.

## Commit

Not requested.
