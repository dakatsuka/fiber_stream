# Flow.throttle

## Status

Accepted

## Problem

Users need a stream operator that limits how quickly elements pass a point in a
pipeline. The first API should work for simple local throttling, while leaving
room for application-owned rate-limit policies such as process-wide,
service-wide, or Redis-backed quota coordination.

## Goals

- Add `FiberStream::Flow.throttle(...)` as a pull-driven rate-limiting flow.
- Add `Source#throttle(...)` as a convenience wrapper.
- Add `FiberStream::RateLimiter` as the default local limiter.
- Let users pass a custom limiter to `Flow.throttle` or `Source#throttle`.
- Let users customize `RateLimiter` behavior with
  `RateLimiter.new(...) { |request| ... }`.
- Preserve lazy construction.
- Preserve element order.
- Preserve pull backpressure; the throttle stage does not prefetch beyond the
  single value currently being emitted.
- Keep limiter waits non-blocking by requiring a scheduler-backed
  non-blocking fiber when FiberStream performs a wait.
- Avoid a runtime dependency on Redis, Async, or any external limiter backend.

## Non-Goals

- Distributed rate limiting in FiberStream itself.
- Redis, database, HTTP, or service-mesh integrations.
- Retry, timeout, circuit-breaker, or quota-class APIs.
- Dropping, sampling, batching, or conflating elements.
- Parallel execution or asynchronous buffering.
- Installing or selecting a `Fiber.scheduler`.
- Starting internal throttle worker fibers.
- Calendar-window quota semantics as the default behavior.

## Requirements

- `FiberStream::Flow.throttle(rate:, per: 1, burst: nil)` creates a throttling
  flow that creates a fresh `FiberStream::RateLimiter` for each materialization.
- `FiberStream::Flow.throttle(limiter:)` creates a throttling flow using a
  user-provided limiter.
- `Source#throttle(...)` is a convenience equivalent to
  `Source#via(FiberStream::Flow.throttle(...))`.
- Constructing `Flow.throttle`, `Source#throttle`, or `RateLimiter` does not
  pull upstream, sleep, call a custom limiter block, or access external
  resources.
- `rate` must be a positive `Integer`.
- `per` must be a positive, finite, real `Numeric` duration in seconds.
- `burst` must be `nil` or a positive `Integer`.
- When `burst` is `nil`, the default local limiter uses `burst: rate`.
- Passing neither `rate:` nor `limiter:` raises `ArgumentError`.
- Passing both `rate:` and `limiter:` raises `ArgumentError`.
- Passing explicit `per:` or `burst:` with `limiter:` raises `ArgumentError`.
  Those options only configure a default limiter created by the `rate:` form.
- A custom `limiter:` must respond to `acquire`.
- Non-conforming limiter objects raise `TypeError`.
- The throttle stage ignores the return value from a custom `limiter:` object.
  The object is expected to return only after it has acquired the requested
  permits, or raise when acquisition fails.
- `RateLimiter#acquire(permits: 1)` waits until the requested permit count has
  been acquired, then returns `nil`.
- `permits` must be a positive `Integer`.
- `permits` must be less than or equal to the limiter's `burst`.
- `RateLimiter#acquire` raises `ArgumentError` when `permits` is greater than
  `burst`.
- A `RateLimiter` custom block returns `nil` or a non-positive numeric value
  when the requested permits have been acquired.
- A `RateLimiter` custom block returns a positive, finite, real numeric value
  when FiberStream should sleep that many seconds and retry the block.
- `RateLimiter#acquire` raises `TypeError` when a custom block returns a
  non-numeric non-`nil` value.
- `RateLimiter#acquire` raises `ArgumentError` when a custom block returns a
  non-finite or non-real numeric value.
- Constructing `Flow.throttle`, `Source#throttle`, or `RateLimiter` does not
  require `Fiber.scheduler`.
- If the default local token bucket can grant permits immediately,
  `RateLimiter#acquire` returns without requiring `Fiber.scheduler`.
- If `RateLimiter#acquire` must wait, it requires an installed
  `Fiber.scheduler` from a non-blocking current fiber before sleeping.
- If no scheduler is installed when `RateLimiter#acquire` must wait,
  `RateLimiter#acquire` raises `FiberStream::SchedulerRequiredError`.
- If a scheduler is installed but the current fiber is blocking when
  `RateLimiter#acquire` must wait, `RateLimiter#acquire` raises
  `FiberStream::SchedulerRequiredError`.
- When a custom `RateLimiter` block returns a positive wait duration,
  FiberStream performs the wait and applies the same scheduler-backed
  non-blocking fiber requirement.
- During normal operation, each value returned by the throttle stage has
  exactly one successful `limiter.acquire(permits: 1)` call.
- A value may be pulled and then suppressed if limiter acquisition fails or if
  the stage is closed while acquisition is waiting.
- When a pulled value is suppressed after a successful permit acquisition,
  FiberStream does not refund that permit. Custom limiters that need refund
  semantics must implement their own higher-level policy outside
  `Flow.throttle`.
- Normal upstream completion is not rate-limited.
- Upstream failures raised before a value is produced are not rate-limited and
  do not call the limiter.
- Upstream values are pulled before the limiter is acquired for that value.
  This keeps completion prompt and limits run-ahead to one in-flight value, but
  it means `Flow.throttle` controls downstream admission rather than upstream
  source side effects.
- To rate-limit side effects in later stages, place `throttle` before those
  stages.
- To coordinate a quota across multiple pipelines or repeated materializations,
  pass the same limiter object to those pipelines with `limiter:`.
- The default local token-bucket `RateLimiter` is thread-safe for normal Ruby
  thread and fiber use.
- When `RateLimiter.new(...) { |request| ... }` is used, FiberStream may call
  that custom block concurrently if the limiter is shared. The block is
  responsible for its own thread safety, scheduler friendliness, and external
  consistency.
- Custom limiters are responsible for their own thread safety, external
  consistency, and scheduler friendliness. A custom limiter that needs to wait
  must do so without blocking the native Ruby thread, or raise its own failure.
- FiberStream does not guarantee immediate cancellation of a throttle wait or
  custom limiter operation. Background pipeline cancellation remains
  cooperative and depends on the active scheduler and user limiter code.
- A custom limiter failure is propagated as a stream failure.
- If a limiter failure occurs after an upstream value has been pulled, that
  value is not emitted.
- If the stage is closed while `limiter.acquire` is waiting, the stage checks
  its closed state after acquisition returns. When closed, it suppresses the
  pulled value and returns completion instead of emitting the value.
- Downstream early completion or downstream failure closes upstream through the
  existing pipeline cleanup path.
- Repeated downstream pulls after completion return completion without
  acquiring limiter permits again.

## Public Contracts

```ruby
FiberStream::Flow.throttle(rate:, per: 1, burst: nil)
FiberStream::Flow.throttle(limiter:)
FiberStream::Source#throttle(rate:, per: 1, burst: nil)
FiberStream::Source#throttle(limiter:)

FiberStream::RateLimiter.new(rate:, per: 1, burst: nil)
FiberStream::RateLimiter.new(rate:, per: 1, burst: nil) { |request| ... }
FiberStream::RateLimiter#acquire(permits: 1)
FiberStream::RateLimiter::Request#rate
FiberStream::RateLimiter::Request#per
FiberStream::RateLimiter::Request#burst
FiberStream::RateLimiter::Request#permits
FiberStream::RateLimiter::Request#now
FiberStream::SchedulerRequiredError
```

Initial RBS shape:

```rbs
module FiberStream
  class RateLimiter
    class Request < Data
      attr_reader rate: Integer
      attr_reader per: Numeric
      attr_reader burst: Integer
      attr_reader permits: Integer
      attr_reader now: Float
    end

    def initialize: (
      rate: Integer,
      ?per: Numeric,
      ?burst: Integer?
    ) ?{ (Request request) -> Numeric? } -> void

    def acquire: (?permits: Integer) -> nil
  end

  class Flow[In, Out]
    def self.throttle: [Elem] (
      ?rate: Integer,
      ?per: Numeric,
      ?burst: Integer?,
      ?limiter: untyped
    ) -> Flow[Elem, Elem]
  end

  class Source[Elem]
    def throttle: (
      ?rate: Integer,
      ?per: Numeric,
      ?burst: Integer?,
      ?limiter: untyped
    ) -> Source[Elem]
  end
end
```

## Examples

Local throttling:

```ruby
require "async"
require "fiber_stream"

Async do
  FiberStream::Source.each(jobs)
    .throttle(rate: 10, per: 1)
    .run_with(FiberStream::Sink.foreach { |job| process(job) })
end.wait
```

Steady one-at-a-time pacing:

```ruby
Async do
  FiberStream::Source.each(jobs)
    .throttle(rate: 10, per: 1, burst: 1)
    .run_with(FiberStream::Sink.foreach { |job| process(job) })
end.wait
```

Rate-limit a later side-effecting stage by placing `throttle` before it:

```ruby
Async do
  FiberStream::Source.each(requests)
    .throttle(rate: 5, per: 1, burst: 1)
    .map { |request| call_api(request) }
    .run_with(FiberStream::Sink.to_a)
end.wait
```

Shared local limiter:

```ruby
limiter = FiberStream::RateLimiter.new(rate: 100, per: 60)

api_a = FiberStream::Source.each(a_jobs).throttle(limiter: limiter)
api_b = FiberStream::Source.each(b_jobs).throttle(limiter: limiter)
```

Repeated materialization with the `rate:` form creates independent limiters:

```ruby
source = FiberStream::Source.each([1]).throttle(rate: 1, per: 60)

source.run_with(FiberStream::Sink.to_a)
source.run_with(FiberStream::Sink.to_a)

# The second run uses a fresh local limiter. Share an explicit limiter when the
# quota must carry across runs.
```

Custom Redis-backed policy:

```ruby
limiter =
  FiberStream::RateLimiter.new(rate: 1_000, per: 60, burst: 1_000) do |request|
    wait_seconds = redis.evalsha(
      SCRIPT_SHA,
      keys: ["global-api-quota"],
      argv: [request.rate, request.per, request.burst, request.permits]
    )

    wait_seconds.positive? ? wait_seconds : nil
  end

FiberStream::Source.each(requests)
  .throttle(limiter: limiter)
  .run_with(FiberStream::Sink.foreach { |request| call_api(request) })
```

The custom block may also perform its own scheduler-friendly waiting and return
`nil` once it has acquired the permit.

## Future Work

- Consider named limiter policies such as fixed-window, sliding-window, or
  leaky-bucket only after user-provided limiter implementations show repeated
  demand for built-in policies.
