# Rate Limiting

Use `throttle` immediately before the side effect you want to pace. The simple
`rate:` form is local to one stream materialization. Pass `limiter:` when quota
state must be shared.

## Local pacing

This stream refills five permits per second for downstream service calls.
Setting `burst: 1` produces steady one-at-a-time pacing.

```ruby
require "async"
require "fiber_stream"

jobs = [
  { id: 1, payload: "alpha" },
  { id: 2, payload: "bravo" },
  { id: 3, payload: "charlie" }
]

Async do
  FiberStream::Source.each(jobs)
    .throttle(rate: 5, per: 1, burst: 1)
    .run_with(
      FiberStream::Sink.foreach do |job|
        send_to_saas(job)
      end
    )
end.wait
```

Increase `burst` when short bursts are acceptable. Keep `throttle` before the
network call; stages before the throttle can still run one element ahead.

## Shared in-process quota

Share one limiter object when multiple streams in the same Ruby process should
draw from the same quota.

```ruby
require "async"
require "fiber_stream"

limiter = FiberStream::RateLimiter.new(rate: 100, per: 60, burst: 10)

realtime =
  FiberStream::Source.each(realtime_jobs)
    .throttle(limiter: limiter)

replay =
  FiberStream::Source.each(replay_jobs)
    .throttle(limiter: limiter)

Async do |task|
  task.async do
    realtime.run_with(FiberStream::Sink.foreach { |job| send_to_saas(job) })
  end

  task.async do
    replay.run_with(FiberStream::Sink.foreach { |job| send_to_saas(job) })
  end
end.wait
```

The two streams draw from one bucket that refills 100 permits per minute and
allows up to 10 immediate burst permits in this process.

## Database-backed policy

`RateLimiter` can delegate permit decisions to application-owned state. In this
example a database function atomically records usage and returns the number of
seconds to wait before retrying. Return `nil` or `0` when the permit was
granted.

```ruby
require "async"
require "connection_pool"
require "fiber_stream"
require "pg"

db_pool = ConnectionPool.new(size: 5) do
  PG.connect(ENV.fetch("DATABASE_URL"))
end

limiter =
  FiberStream::RateLimiter.new(rate: 600, per: 60, burst: 600) do |request|
    db_pool.with do |db|
      row =
        db.exec_params(
          "select wait_seconds from acquire_quota($1, $2, $3, $4, $5)",
          [
            "partner-api",
            request.rate,
            request.per,
            request.burst,
            request.permits
          ]
        ).first

      wait = Float(row.fetch("wait_seconds"))
      wait.positive? ? wait : nil
    end
  end

Async do
  FiberStream::Source.each(import_rows)
    .throttle(limiter: limiter)
    .run_with(FiberStream::Sink.foreach { |row| sync_partner(row) })
end.wait
```

Use a scheduler-friendly database client or isolate blocking database work from
the scheduler thread. If the limiter is shared, its block may run concurrently.

## Redis-backed global quota

Use the same Redis key from every server when the whole deployment must stay
within one downstream SaaS quota. The Lua script should atomically grant the
permit or return the wait duration in seconds.

```ruby
require "async"
require "fiber_stream"
require "redis_client"

redis = RedisClient.config(url: ENV.fetch("REDIS_URL")).new_pool(timeout: 1)
script_sha = ENV.fetch("REDIS_RATE_LIMIT_SHA")

global_limiter =
  FiberStream::RateLimiter.new(rate: 10_000, per: 60, burst: 500) do |request|
    redis.with do |client|
      wait =
        client.call(
          "EVALSHA",
          script_sha,
          1,
          "quota:saas:v1",
          request.rate,
          request.per,
          request.burst,
          request.permits
        )

      wait = Float(wait)
      wait.positive? ? wait : nil
    end
  end

Async do
  FiberStream::Source.each(events)
    .throttle(limiter: global_limiter)
    .parallel_unordered_map(concurrency: 20) do |event|
      send_to_saas(event)
    end
    .run_with(FiberStream::Sink.foreach { |response| store_response(response) })
end.wait
```

Every server using `quota:saas:v1` competes for the same external policy. In
this example that policy refills 10,000 permits per minute with a 500-permit
burst. `throttle` controls admission rate; `parallel_unordered_map` controls
how many SaaS calls are in flight inside each process.
