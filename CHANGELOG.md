# Changelog

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
