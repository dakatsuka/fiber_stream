# Changelog

## Unreleased

## 0.6.0 - 2026-07-27

### Added

- `Flow.ractor_unordered_map(workers:)` and `Source#ractor_unordered_map` for
  Ractor-backed CPU-bound mapping that emits results in completion order.
- `Sink.find { |element| ... }` for predicate-based terminal search with
  early upstream completion.
- `Sink.any? { |element| ... }` for predicate-based existential checks with
  early upstream completion.
- `Sink.all? { |element| ... }` for predicate-based universal checks with
  early upstream completion.
- Example and benchmark coverage for unordered Ractor-backed mapping.

### Changed

- Added a rate-limiting tutorial covering local, shared in-process,
  database-backed, and Redis-backed quota policies.
- Centralized the local and release verification gates and added documentation
  index validation.

## 0.5.0 - 2026-06-21

### Added

- `Flow.tap { |element| ... }` for lazy pass-through observation without
  changing emitted elements.
- `Flow.filter_map { |element| ... }` and `Source#filter_map` for combined
  transformation and falsey-value dropping.
- `Flow.reject { |element| ... }` and `Source#reject` for complement
  predicate filtering.
- `Flow.compact` and `Source#compact` for nil-only filtering while preserving
  `false`.
- `Flow.map_concat { |element| enumerable }` and `Source#map_concat` for
  one-to-many element expansion.
- `Flow.throttle(...)`, `Source#throttle(...)`, and `RateLimiter` for
  pull-driven rate limiting with optional shared quota state.
- `Sink.count` for counting stream elements without accumulating them.

### Changed

- Expanded README and website reference coverage for the flow operators and
  rate limiter added in 0.5.0.
- Promoted completed flow product specs and design docs from draft to accepted
  status.

## 0.4.0 - 2026-06-09

### Added

- `Flow.parallel_unordered_map(concurrency:)` and
  `Source#parallel_unordered_map(concurrency:)` for scheduler-backed mapping
  that emits results in completion order instead of preserving input order.
- `Source.ractor_producer` for FiberStream-owned single producer Ractors with
  one-outstanding-ack backpressure and cooperative cleanup.
- `Source.ractor_merge_producers` for ready-order fan-in from multiple
  FiberStream-owned producer Ractors without requiring a `Fiber.scheduler`.
- `Flow.scan(initial)` and `Source#scan(initial)` for lazy running
  accumulators using `Sink.fold`-style reducer semantics.

### Changed

- Updated README and website reference coverage for owned Ractor producers,
  unordered parallel mapping, and scan.
- Prefer high-level owned Ractor producer examples in user-facing
  documentation while keeping low-level port APIs documented for externally
  owned producers.
- Updated the project Ruby pin to 4.0.5.

## 0.3.0 - 2026-06-06

### Added

- `Flow.grouped(count)` and `Source#grouped(count)` for fixed-size batches
  with final partial-group emission.
- `Source#merge(source)` for scheduler-backed ready-order merging of two
  sources while preserving each input source's own order.
- `Source.ractor_merge_ports(ports)` for backpressure-aware merging of
  multiple producer Ractor ports without requiring a `Fiber.scheduler`.
- `Flow.split(separator)` and `Source#split(separator)` for delimiter-based
  framing with optional separator retention and per-frame length limits.
- Benchmarks and examples for async IO fanout, stream lifecycle probes, and
  Ractor port merge workflows.

### Changed

- Reworked flow operator tests into focused per-operator test files.
- Expanded README and repository documentation for source merging, Ractor port
  merging, split framing, grouped batches, and runtime safety guidance.
- Clarified that `Flow.lines(max_length: nil)` and
  `Flow.split(max_length: nil)` may buffer one unterminated frame without
  bound, and documented explicit `max_length` usage for untrusted streams.
- Clarified `Source.io` `chunk_size` allocation behavior and Ractor failure
  metadata exposure.

### Fixed

- Deferred `Source#concat` receiver materialization until downstream demand
  reaches the concatenated source.
- Cancelled async and buffer producers when downstream closes early.
- Removed polling from Ractor map enqueue and cleanup paths.
- Re-raised background pipeline process-control exceptions instead of treating
  them as ordinary stream failures.
- Hardened Ractor map worker teardown notifications so secondary send failures
  do not cascade during shutdown.

## 0.2.0 - 2026-06-05

### Added

- `Source#zip(source)` for element-wise pairing of two sources with
  demand-driven materialization and shortest-source completion.
- `Source#concat(source)` for lazy source concatenation.
- `Sink.foreach { |element| ... }` for side-effecting stream consumption
  without accumulating elements.
- `Flow.drop(count)` and `Source#drop(count)` for fixed-prefix dropping.
- `Flow.take_while { |element| ... }` and `Source#take_while { |element| ... }`
  for predicate-based prefix limiting.
- `Flow.drop_while { |element| ... }` and
  `Source#drop_while { |element| ... }` for predicate-based prefix dropping.

### Changed

- Clarified documentation around FiberStream's linear roadmap and Ractor port
  cancellation contract.

## 0.1.0 - 2026-06-03

Initial release.

### Added

- Lazy linear `Source`, `Flow`, `Sink`, and `Pipeline` APIs.
- Pull-based backpressure with `Source.each`, `Flow.map`, `Flow.select`,
  `Flow.take`, `Sink.to_a`, `Sink.first`, and `Sink.fold`.
- Scheduler-aware IO source and sink support.
- Line framing with `Flow.lines`.
- Scheduler-backed async and bounded buffer boundaries.
- Ordered `Flow.parallel_map` and `Flow.ractor_map`.
- Backpressure-aware `Source.ractor_port` with typed Ractor protocol envelopes.
- Background pipeline execution with cancellation support.
- Public RBS signatures.

### Known Limitations

- Only linear pipelines are supported.
- IO and scheduler-backed stages require the caller to provide a Ruby
  `Fiber.scheduler`; FiberStream does not install one.
- Ractor APIs are experimental in Ruby and may change in future Ruby releases.
