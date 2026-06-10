# ADR 0017: Flow.throttle And RateLimiter

## Status

Draft

## Context

FiberStream users need a way to limit stream throughput without giving
FiberStream responsibility for application-specific quota systems. Local
throttling should be easy, but the API must also allow a process-wide or
service-wide limiter implemented outside FiberStream, including Redis-backed
state.

Existing asynchronous and buffering flows already keep scheduler integration
dependency-free while avoiding native-thread blocking. A throttle should follow
that pattern by requiring scheduler-backed waits without depending on Async,
Redis, or another backend.

## Decision

Add `FiberStream::Flow.throttle` and `Source#throttle` in a future
implementation:

```ruby
FiberStream::Flow.throttle(rate: 10, per: 1)
FiberStream::Flow.throttle(rate: 100, per: 60, burst: 100)
FiberStream::Flow.throttle(limiter: limiter)
```

The `rate:` form creates a fresh default `FiberStream::RateLimiter` for each
materialization. The `limiter:` form accepts any object that responds to
`acquire` and uses the supplied object directly, allowing explicit sharing
across pipelines and materializations. The two forms are mutually exclusive,
and `per:` or `burst:` are valid only with `rate:`. For arbitrary custom
limiter objects, `acquire` must return only after it has acquired the requested
permits; the throttle stage ignores its return value.

Add `FiberStream::RateLimiter` as a public local token-bucket limiter:

```ruby
FiberStream::RateLimiter.new(rate:, per: 1, burst: nil)
FiberStream::RateLimiter.new(rate:, per: 1, burst: nil) { |request| ... }
limiter.acquire(permits: 1)
```

`burst: nil` means `burst == rate`. The token bucket starts full. Users who
need steady pacing can pass `burst: 1`.
`RateLimiter#acquire(permits:)` rejects requests greater than `burst` so the
local token bucket never waits for an impossible permit count.

The custom block is an extension point for alternative policy storage or
coordination. `RateLimiter#acquire` calls the block with a request object. The
block returns `nil` or a non-positive numeric value after acquiring the
permit, returns a positive numeric wait duration when FiberStream should sleep
and retry, or raises to fail the stream. Shared limiters may call custom blocks
concurrently, and those blocks own their external consistency and scheduler
friendliness.

`Flow.throttle` remains a synchronous pull stage. It pulls one upstream value,
returns completion immediately if upstream is done, otherwise acquires one
permit and emits that value. This limits downstream admission, keeps normal
completion prompt, and bounds upstream run-ahead to one value.
If the stage is closed while permit acquisition is waiting, the pulled value is
suppressed after acquisition returns and the acquired permit is not refunded.

When FiberStream needs to sleep for the default token bucket or for a positive
wait duration returned by a `RateLimiter` custom block, it requires an
installed `Fiber.scheduler` from a non-blocking current fiber. Missing
scheduler or blocking current fiber raises `FiberStream::SchedulerRequiredError`
instead of blocking the native Ruby thread. Immediate permit grants do not
require a scheduler.

## Consequences

- Simple local throttling has a compact API.
- Users can share one limiter across pipelines for process-wide coordination.
- Default `rate:` throttles do not accidentally share quota state across
  repeated runs of a reusable `Source` or `Flow`.
- Redis or other external quota systems remain application code, not
  FiberStream dependencies.
- The custom block contract is narrow enough to support atomic external
  read/update operations without exposing Redis-specific concepts.
- Throttling controls side effects after the throttle stage. It does not fully
  prevent upstream source side effects from happening one element ahead.
- The first implementation does not provide named fixed-window,
  sliding-window, or leaky-bucket policies.
- Foreground code without a scheduler can construct throttles and consume
  immediately available burst permits, but it receives `SchedulerRequiredError`
  once FiberStream-owned waiting is required.
- Cancellation of throttle waits is cooperative and depends on scheduler
  interruption support and custom limiter behavior.
- Close during an in-progress wait can consume a permit without emitting the
  already-pulled value. The first API does not provide refund semantics.

## Alternatives Rejected

- Direct sleeps in `Flow.throttle` without a public limiter object: rejected
  because shared and distributed quota policies would need a separate future
  API.
- Requiring `limiter:` for all uses: rejected because common local throttling
  should not require users to construct a separate object.
- Acquiring permits before pulling upstream: rejected because normal completion
  could be delayed by rate-limit waits when no further value exists.
- Bundling Redis support: rejected because backend choice, key structure,
  atomic scripts, clocks, and failure policy are application-specific.
