# Add Flow.ractor_map

## Status

Completed

## Objective

Implement `FiberStream::Flow.ractor_map` as an ordered Ractor-backed mapping
stage with bounded upstream run-ahead, explicit copy/move transfer policies,
normalized Ractor failures, and `Source#ractor_map` convenience support.

## Context

- Product spec: `docs/product-specs/flow-ractor-map.md`
- Design doc: `docs/design-docs/ractor-map.md`
- ADR: `docs/design-docs/adr/0010-flow-ractor-map.md`
- Existing ordered boundary: `lib/fiber_stream/pull.rb`
- Existing tests: `test/fiber_stream/flow_parallel_map_test.rb`

## Clarifications

- The accepted design is the implementation source of truth.
- `ractor_map` does not require `Fiber.scheduler`.
- Upstream pulls stay in the downstream caller's context.
- Blocking Ractor waits are isolated in a coordinator thread.
- Worker and Ractor transfer failures normalize to `FiberStream::RactorMapError`.

## Contract First

- `FiberStream::Flow.ractor_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`
- `FiberStream::Source#ractor_map(workers:, input_transfer: :copy, output_transfer: :copy) { |element| ... }`
- `FiberStream::RactorMapError`

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: accepted design review already completed in
      `completed/design-ractor-map.md`; re-check during implementation only if
      the design has to change.
- [x] Red: write failing behavior-focused tests, with unit test files organized
      per module.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use a per-boundary `Ractor::Port` so independently materialized boundaries do
  not share result channels.
- Keep upstream job admission in downstream `#next`, not in the coordinator
  thread.
- Use `Thread::SizedQueue#push(..., true)` loops in the coordinator so close
  can suppress post-close data/error messages without blocking behind a full
  result queue.
- Treat non-`StandardError` mapper failures as normalized worker failures so a
  worker cannot stop without publishing an ordered stream error.
- Wait for worker shutdown through the coordinator's `:stopped` messages and
  avoid direct `Ractor#value` calls from the pipeline close path.

## Verification

- `bundle exec ruby -Itest test/fiber_stream/flow_ractor_map_test.rb`
  - 22 runs, 57 assertions, 0 failures, 0 errors, 0 skips
- `bundle exec rake`
  - 233 runs, 544 assertions, 0 failures, 0 errors, 0 skips
  - `bundle exec rbs validate` passed
  - `bundle exec rubocop` inspected 30 files with no offenses

## Completion Notes

Implemented `FiberStream::Flow.ractor_map` and `Source#ractor_map` as an
ordered Ractor-backed mapping boundary. The implementation validates shareable
mapper procs, positive worker counts, and copy/move transfer policies; preserves
ordered output and bounded pulled-but-unemitted work; normalizes worker and
Ractor transfer failures to `FiberStream::RactorMapError`; and uses a
coordinator thread to isolate blocking Ractor waits from scheduler-managed
pipeline fibers.

The first code review found that non-`StandardError` worker failures could stop
a worker without publishing an ordered result and that cleanup called
`Ractor#value` directly from the pipeline close path. The implementation now
catches mapper `Exception` failures inside workers, adds regression coverage,
waits through coordinator `:stopped` messages instead of direct Ractor value
waits, and includes an Async cleanup responsiveness test. Re-review found no
blocking issues.

## Commit

```text
feat: add ractor-backed stream mapping

CPU-bound mapping needs an explicit Ractor boundary so users can run
per-element work in parallel while preserving FiberStream's ordered pull
runtime, bounded backpressure, and deterministic cleanup behavior.

Add Flow.ractor_map, Source#ractor_map, RactorMapError, RBS signatures, and
behavior coverage for ordering, transfer policies, worker failures, early
completion, and scheduler responsiveness. The boundary keeps upstream pulls in
the downstream caller context and isolates Ractor waits through a coordinator
thread.

Co-Authored-By: OpenAI Codex <codex@openai.com>
```
