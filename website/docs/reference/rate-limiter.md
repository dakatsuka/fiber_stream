# RateLimiter

`FiberStream::RateLimiter` is a scheduler-aware token-bucket limiter used by
`Flow.throttle(limiter:)` and `Source#throttle(limiter:)`.

## Constructor

### `RateLimiter.new(rate:, per: 1, burst: nil)`

Creates a limiter that refills `rate` permits every `per` seconds. `burst`
defaults to `rate` and caps the number of stored permits.

```ruby
limiter = FiberStream::RateLimiter.new(rate: 10, per: 1)

FiberStream::Source.each([1, 2, 3])
  .throttle(limiter: limiter)
  .run_with(FiberStream::Sink.to_a)
# => [1, 2, 3]
```

Immediate permit grants do not require a scheduler. When the limiter must wait,
the current fiber must be non-blocking with an installed `Fiber.scheduler`.

### `RateLimiter.new(rate:, per: 1, burst: nil) { |request| ... }`

Creates a limiter with a custom wait policy. The block receives a
`RateLimiter::Request` and returns a wait duration in seconds. Returning `nil`,
zero, or a negative number grants the permits immediately.

```ruby
next_at = 0.0

limiter =
  FiberStream::RateLimiter.new(rate: 100, per: 60) do |request|
    wait = next_at - request.now
    next_at = [request.now, next_at].max + 0.1
    wait
  end
```

## Methods

### `limiter.acquire(permits: 1)`

Acquires permits and returns `nil`. Requests larger than `burst` raise
`ArgumentError` because the token bucket cannot satisfy them.

```ruby
limiter = FiberStream::RateLimiter.new(rate: 2, per: 1)

limiter.acquire
limiter.acquire(permits: 1)
```

## Request

Custom policy blocks receive `RateLimiter::Request` objects with these readers:

- `rate`
- `per`
- `burst`
- `permits`
- `now`
