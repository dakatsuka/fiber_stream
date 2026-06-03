# Harden Ractor Map Coordinator

## Status

Completed

## Objective

Improve `Flow.ractor_map` worker lifecycle robustness by having the coordinator
thread wait on both the shared result port and worker ractors with
`Ractor.select`, so worker termination can be detected even if a worker stops
without publishing the expected lifecycle messages.

## Context

- Product spec: `docs/product-specs/flow-ractor-map.md`
- Design doc: `docs/design-docs/ractor-map.md`
- Existing implementation: `lib/fiber_stream/pull.rb`
- Existing tests: `test/fiber_stream/flow_ractor_map_test.rb`
- Reference: `docs/references/ruby-ractor.md`

## Clarifications

- This is an internal hardening change. Public `Flow.ractor_map` and
  `Source#ractor_map` APIs remain unchanged.
- `Ractor.select` is a blocking Ractor wait API, so it must stay inside the
  coordinator thread and never run in the scheduler-managed pipeline fiber.
- The coordinator should normalize unexpected worker termination to
  `FiberStream::RactorMapError` with kind `:worker_termination`.

## Contract First

- Preserve existing public contracts.
- Preserve ordered delivery and bounded pulled-but-unemitted work.
- Preserve scheduler responsiveness during normal result waits and cleanup
  waits.
- Add regression coverage for coordinator-side worker termination detection.

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: no public design change expected; request code review
      after implementation.
- [x] Red: add failing behavior-focused tests.
- [x] Green: implement coordinator `Ractor.select` and active sequence tracking.
- [x] Refactor: keep worker lifecycle state small and explicit.
- [x] Static checks: run targeted tests and default checks.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Keep `Ractor.select` inside the coordinator thread so scheduler-managed
  pipeline fibers never call blocking Ractor wait APIs directly.
- Include `worker_id` in internal worker `:value` and `:error` result messages
  so the coordinator can clear active-sequence state when a worker publishes a
  result.
- Track active worker sequences in main-ractor state, protected by a mutex
  because downstream admission and coordinator result handling run on different
  threads.
- Treat a worker ractor termination without a result or lifecycle message as a
  normalized `RactorMapError` with kind `:worker_termination`.
- Rescue `Ractor::RemoteError` from coordinator-side `Ractor.select`, choose
  `Ractor::RemoteError#ractor` when available, fall back to the lowest-sequence
  active worker when necessary, and normalize the failure to
  `:worker_termination`.
- Close the ready-worker queue when a coordinator-side worker termination
  failure is observed so downstream admission wakes and can consume the ordered
  error from the result queue.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_ractor_map_test.rb`
  - 25 runs, 68 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rake`
  - 236 runs, 555 assertions, 0 failures, 0 errors, 0 skips
  - `bundle exec rbs validate` passed
  - `bundle exec rubocop` inspected 35 files with no offenses

## Completion Notes

Hardened `Flow.ractor_map` coordinator lifecycle handling by waiting on both
the shared result port and worker ractors with `Ractor.select`. The coordinator
now detects worker termination without lifecycle messages, wakes downstream
admission by closing the ready-worker queue, and emits an ordered
`RactorMapError` with kind `:worker_termination`.

Worker result messages now include `worker_id` for value and error messages so
coordinator-side active sequence tracking can be cleared precisely. If
`Ractor.select` raises `Ractor::RemoteError`, the coordinator uses
`Ractor::RemoteError#ractor` to identify the failed worker and sequence before
falling back to active-sequence inference.

Code review found an unhandled `Ractor::RemoteError` hang path and then a
multi-worker attribution bug. Both were fixed and covered by regression tests.
Re-review found no blocking issues.

## Commit

```text
fix: harden ractor map worker termination handling

Ractor-backed stream mapping should turn unexpected worker termination into an
ordered stream failure instead of hanging or attributing the failure to the
wrong input sequence.

Have the ractor_map coordinator wait on both the shared result port and worker
ractors with Ractor.select, track active worker sequences, normalize
coordinator-side worker termination to RactorMapError, and cover idle and active
worker termination regressions. Update the ractor map design notes for the
worker_id-bearing protocol and RemoteError#ractor attribution.

Co-Authored-By: OpenAI Codex <codex@openai.com>
```
