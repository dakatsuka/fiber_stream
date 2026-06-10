# Flow.throttle And RateLimiter

## Status

Draft

## Context

FiberStream's current linear pull runtime provides lazy, demand-driven stages,
explicit asynchronous boundaries, and bounded concurrency. Users also need to
pace stream elements without changing the pipeline into a buffered or parallel
runtime. The public API should cover simple local throttling and allow users to
plug in application-specific quota coordination such as Redis-backed global
rate limits.

Governing documents:

- Product spec: `docs/product-specs/flow-throttle.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Async design: `docs/design-docs/async-boundary.md`
- Buffer design: `docs/design-docs/buffer-boundary.md`
- ADR: `docs/design-docs/adr/0017-flow-throttle-rate-limiter.md`
- References: `docs/references/ruby-fiber-and-tooling.md`

## Goals

- Add a pull-driven throttle stage.
- Add a default local `RateLimiter`.
- Allow limiter injection so multiple pipelines can share one quota.
- Allow `RateLimiter.new(...) { |request| ... }` to delegate policy decisions
  to user code or external systems.
- Avoid external backend dependencies.
- Preserve element order and bounded run-ahead.
- Keep waits non-blocking by requiring a scheduler-backed non-blocking fiber
  when FiberStream performs a wait.
- Keep scheduler use environmental rather than managed by FiberStream.

## Non-Goals

- Implement Redis, database, HTTP, or distributed coordination.
- Provide every named rate-limit algorithm in the first API.
- Add buffering, dropping, conflation, or sampling.
- Add timeout or retry policy.
- Add scheduler-managed throttle worker fibers.
- Rate-limit upstream source side effects that occur before the throttle stage.

## Proposed Design

`Flow.throttle` attaches a scheduler-aware synchronous pull stage:

```ruby
downstream_stream = Pull.throttle(upstream_stream, limiter)
```

`Source#throttle(...)` is a convenience method over
`Source#via(Flow.throttle(...))`. It does not introduce a separate runtime
path.

The public construction forms are:

```ruby
FiberStream::Flow.throttle(rate: 10, per: 1)
FiberStream::Flow.throttle(rate: 100, per: 60, burst: 100)
FiberStream::Flow.throttle(limiter: limiter)
```

The `rate:` form stores limiter configuration in the reusable flow definition
and creates a fresh `FiberStream::RateLimiter` each time the flow attaches to a
materialized pull chain. This keeps repeated `run_with` calls independent by
default. Users who need quota state to carry across materializations must
create a limiter explicitly and pass it with `limiter:`.

The `limiter:` form accepts any object that responds to `acquire`. Passing both
`rate:` and `limiter:` is rejected so there is a single source of rate-limit
policy. Passing explicit `per:` or `burst:` with `limiter:` is also rejected
because those options configure only FiberStream-created default limiters.
For arbitrary custom limiter objects, the throttle stage ignores the return
value from `acquire`; the object must return only after the requested permits
have been acquired, or raise when acquisition fails.

The pull stage owns:

- `upstream`, the pull stream before the throttle
- `limiter`, a `RateLimiter` or compatible object
- `closed` and `done` state flags

On `next`, the stage first pulls `upstream.next`. If upstream returns
`Pull::DONE`, the stage marks itself done and returns completion immediately.
If upstream returns a value, the stage calls `limiter.acquire(permits: 1)` and
then returns the value. This means normal completion is never delayed by a
limiter wait. It also means the stage can hold one pulled-but-not-emitted value
while waiting for the limiter, but it never pulls another upstream value until
that value has been emitted.

After `limiter.acquire` returns, `next` rechecks `closed`. If the stage was
closed while waiting, it suppresses the pulled value, marks itself done, and
returns `Pull::DONE` instead of returning the value. The consumed permit is not
refunded. RateLimiter has only acquire semantics; applications that need refund
or reservation cancellation must implement that policy in a custom limiter or
higher-level workflow outside `Flow.throttle`.

This placement deliberately defines `Flow.throttle` as downstream admission
control. Side effects after the throttle are rate-limited. Side effects before
the throttle, including side effects inside the source itself, may happen one
element ahead of permit acquisition. Users who need to limit an API-calling
`map` should place `throttle` before that `map`.

`close` marks the stage closed and closes upstream. There are no internal
fibers, queues, or cancellation requests. Close idempotency and close-error
precedence are inherited from the existing pull-runtime cleanup path.
`Flow.throttle` has no independent way to interrupt a currently executing
limiter wait. If a throttle runs inside the producer fiber of another boundary
such as `Flow.async`, `Flow.buffer`, or parallel map, interruption depends on
that boundary's cooperative cancellation path, Ruby scheduler behavior, and the
limiter implementation.

The stage itself does not start a scheduled fiber. It runs in the caller's
current fiber, and any FiberStream-owned waiting must happen through
scheduler-backed sleep in that current fiber.

## RateLimiter

`FiberStream::RateLimiter` is the default local limiter and a small extension
point. It exposes:

```ruby
FiberStream::RateLimiter.new(rate:, per: 1, burst: nil)
FiberStream::RateLimiter.new(rate:, per: 1, burst: nil) { |request| ... }
limiter.acquire(permits: 1)
```

`rate` is the number of permits that replenish every `per` seconds. `burst`
is the token capacity. When `burst` is `nil`, capacity defaults to `rate`.
The default implementation uses a token bucket based on monotonic time.

The local token bucket starts full. This lets a stream use the configured burst
immediately and then settle into the configured refill rate. Users who want
steady one-at-a-time pacing can pass `burst: 1`.

`RateLimiter#acquire(permits:)` rejects permit counts greater than `burst`.
Without this validation a token bucket capped at `burst` could wait forever for
an impossible request. The throttle stage requests one permit per element, but
the public limiter API still validates larger direct calls.

The default implementation guards mutable token state with a `Mutex` so a
single limiter can be shared by multiple streams in normal Ruby threads and
fibers. Sleeping happens outside the mutex. The limiter uses `Kernel.sleep`,
but only after validating that `Fiber.scheduler` is installed and
`Fiber.current` is non-blocking. If no scheduler is installed, or if the
current fiber is blocking, `RateLimiter#acquire` raises
`SchedulerRequiredError` instead of blocking the native Ruby thread. Immediate
permit grants do not require a scheduler.

Background pipeline cancellation of a sleeping throttle remains cooperative
and depends on scheduler interruption behavior, just like other
scheduler-managed waits.

When a custom block is provided, `RateLimiter#acquire` delegates permit
decisions to that block. For each attempt it builds a
`FiberStream::RateLimiter::Request` with:

- `rate`
- `per`
- `burst`
- `permits`
- `now`, a monotonic timestamp in seconds

The block contract is:

- Return `nil` or a non-positive numeric value when the requested permits have
  been acquired.
- Return a positive numeric value when FiberStream should sleep that many
  seconds and retry the block.
- Raise to fail the stream.

This lets a Redis-backed limiter perform an atomic read/update and either grant
the permit or return the server-computed wait duration. A block that performs
its own waiting can return `nil` after it has acquired the permit, but it is
responsible for doing so without blocking the native Ruby thread.

The default `RateLimiter` validates custom block return values. Non-numeric
non-`nil` return values raise `TypeError`. Negative values are treated as
immediate acquisition. Positive wait values must be finite, real numerics.
Non-finite or non-real numeric values raise `ArgumentError`.

`RateLimiter` does not hold the token-bucket mutex while running a custom
block. A shared limiter with a custom block may call that block concurrently
from multiple streams. The block is responsible for its own thread safety,
scheduler friendliness, external consistency, and idempotency of any external
state updates it performs. When the block returns a positive wait duration,
FiberStream performs that wait and applies the same scheduler-backed
non-blocking fiber requirement as the default token bucket.

## Contracts

- `Flow.throttle(rate:, per:, burst:)` returns `Flow[Elem, Elem]`.
- `Flow.throttle(limiter:)` returns `Flow[Elem, Elem]`.
- `Source#throttle(...)` delegates to `Flow.throttle(...)`.
- The `rate:` form creates a fresh `RateLimiter` per materialization.
- `RateLimiter` is public and can be explicitly shared across materializations
  and pipelines with `limiter:`.
- Throttle construction is lazy.
- `rate` must be a positive `Integer`.
- `per` must be a positive, finite, real `Numeric`.
- `burst` must be `nil` or a positive `Integer`.
- Explicit `per:` or `burst:` with `limiter:` raises `ArgumentError`.
- `limiter` must respond to `acquire`.
- A custom limiter object passed to `Flow.throttle(limiter:)` must complete
  waiting inside `acquire`; its return value is ignored.
- `RateLimiter#acquire(permits:)` requires a positive `Integer` permit count.
- `RateLimiter#acquire(permits:)` raises `ArgumentError` when `permits` is
  greater than `burst`.
- `RateLimiter#acquire` does not require a scheduler when permits are granted
  immediately.
- `RateLimiter#acquire` requires `Fiber.scheduler` and a non-blocking current
  fiber before any FiberStream-owned sleep.
- Missing scheduler or blocking current fiber during a required wait raises
  `FiberStream::SchedulerRequiredError`.
- During normal operation, each value returned by the throttle stage has
  exactly one successful `limiter.acquire(permits: 1)` call.
- Values pulled before limiter failure or closed-during-wait completion are
  suppressed.
- Successful permit acquisitions are not refunded when a pulled value is later
  suppressed because the stage was closed while waiting.
- Upstream normal completion does not call the limiter.
- Upstream pull failures before a value is produced do not call the limiter.
- A limiter failure is a stream failure.
- A value pulled before a limiter failure is suppressed.
- Output order matches input order.
- The stage has at most one pulled-but-not-emitted upstream value.
- The stage does not start fibers or create queues.
- FiberStream-owned throttle waits are scheduler-backed non-blocking waits.
- Custom limiter waits are the limiter implementation's responsibility and
  must not block the native Ruby thread.
- Throttle waits are cooperative cancellation points when the active scheduler
  and limiter implementation make them interruptible.
- Closing the stage closes upstream.
- Repeated pulls after completion return completion without limiter calls.

## Alternatives Considered

### Sleep Directly In Flow.throttle

`Flow.throttle(rate:, per:)` could directly compute sleeps without a public
limiter type. That would cover local throttling but make shared and external
quota policies awkward. A `RateLimiter` object gives users an explicit policy
handle they can share or replace.

### Require A Limiter Object For Every Throttle

`Flow.throttle(limiter:)` only would keep the flow API smaller, but simple use
would be noisy. The `rate:` form is a convenient default while preserving
advanced injection through `limiter:`.

### Make The Custom Block Perform All Waiting

The custom block could be required to block until a permit is acquired. That is
flexible, but it forces every Redis-backed implementation to duplicate retry
sleep loops. Returning a positive wait duration lets FiberStream provide the
loop while still allowing blocks that perform their own waiting.

### Acquire Before Pulling Upstream

Acquiring before `upstream.next` would rate-limit upstream source side effects,
but it would also delay normal completion when the stream has no next value.
The chosen design avoids completion delay and limits run-ahead to one value.

### Add Internal Throttle Fibers

An internal scheduled throttle fiber could isolate waits from the caller's
current fiber, but it would introduce a boundary more like `Flow.async` or
`Flow.buffer`. The first throttle should remain a synchronous pull stage while
requiring scheduler-backed sleep when it needs to wait. Users can combine it
with existing async boundaries when they need concurrent work.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-06-10. Feedback resulted in these
changes:

- Defined `Flow.throttle(rate:)` as creating a fresh `RateLimiter` per
  materialization, with cross-materialization sharing only through explicit
  `limiter:` injection.
- Required `RateLimiter#acquire(permits:)` to reject impossible
  `permits > burst` requests.
- Rejected explicit `per:` and `burst:` options when `limiter:` is supplied.
- Clarified that custom `RateLimiter` blocks may run concurrently and must be
  thread-safe and scheduler-friendly.
- Clarified that throttle has no independent cancellation mechanism while
  waiting inside a limiter.
- Tightened duration validation to finite, real numeric values.

## Validation

- Unit tests for `Flow.throttle` and `Source#throttle` construction,
  validation, laziness, ordering, completion, one-value run-ahead, limiter
  call count, limiter failure propagation, and cleanup.
- Unit tests for `RateLimiter` validation and custom block return contracts.
- Focused timing tests with short intervals proving local limiter pacing while
  keeping duration tolerances broad enough for scheduler and CI variability.
- Tests proving missing scheduler and blocking current fiber raise
  `SchedulerRequiredError` when `RateLimiter` must sleep.
- Tests proving immediate permit grants do not require a scheduler.
- Tests proving positive waits returned by custom `RateLimiter` blocks use the
  same scheduler validation.
- Tests proving upstream pull failures propagate without calling the limiter
  for a value that was never produced.
- Tests proving a shared limiter coordinates multiple streams.
- Tests proving `Flow.throttle(rate:)` and `Source#throttle(rate:)` use fresh
  limiters across repeated materializations.
- Tests proving explicit `per:` or `burst:` with `limiter:` is rejected.
- Tests proving `RateLimiter#acquire(permits:)` rejects `permits > burst`.
- Tests proving custom limiter failures propagate. FiberStream cannot reliably
  detect an arbitrary custom limiter that blocks a native thread; that behavior
  is documented as the custom limiter's responsibility rather than tested with
  a hanging limiter.
- Tests combining throttle before `async`, `buffer`, and parallel map to define
  early close and downstream failure cleanup behavior around in-progress waits.
- RBS validation.
- RuboCop.

## Open Questions

None.
