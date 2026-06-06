# Pipeline

`FiberStream::Pipeline` is a reusable runnable stream definition created with
`Source#to(sink)`.

## `pipeline.run`

Runs the pipeline in the current fiber and returns the sink's materialized
value.

```ruby
pipeline =
  FiberStream::Source.each([1, 2, 3])
    .map { |value| value * 2 }
    .to(FiberStream::Sink.to_a)

pipeline.run # => [2, 4, 6]
```

## `pipeline.run_async`

Starts the pipeline in a scheduler-backed background fiber and returns a
`RunningPipeline` handle.

This method requires a `Fiber.scheduler` and a non-blocking current fiber.

```ruby
Async do
  running =
    FiberStream::Source.each([1, 2, 3])
      .to(FiberStream::Sink.to_a)
      .run_async

  running.wait
end.wait
# => [1, 2, 3]
```

## RunningPipeline

### `running.wait`

Waits for completion and returns the materialized value. If the pipeline
failed, `wait` raises the stream failure. If cancellation interrupts background
materialization, `wait` raises `PipelineCancelledError`.

Waiting before completion requires a scheduler-backed non-blocking fiber.
Waiting after completion replays the stored result without requiring a
scheduler.

```ruby
Async do
  running =
    FiberStream::Source.each([1, 2, 3])
      .to(FiberStream::Sink.to_a)
      .run_async

  running.wait
end.wait
# => [1, 2, 3]
```

### `running.cancel`

Requests cancellation and returns the handle. Cancellation is cooperative and
uses the scheduler captured by `run_async`.

If the captured scheduler cannot interrupt fibers, `cancel` raises
`NotImplementedError` without recording a cancellation request.

```ruby
Async do
  running =
    FiberStream::Source.each([1])
      .map { |value| sleep 1; value }
      .to(FiberStream::Sink.to_a)
      .run_async

  running.cancel
  running.cancel_requested?
end.wait
# => true
```

### `running.done?`

Returns whether the background pipeline has completed.

```ruby
Async do
  running =
    FiberStream::Source.each([1])
      .to(FiberStream::Sink.to_a)
      .run_async

  running.wait
  running.done?
end.wait
# => true
```

### `running.cancel_requested?`

Returns whether cancellation has been requested.

```ruby
Async do
  running =
    FiberStream::Source.each([1])
      .map { |value| sleep 1; value }
      .to(FiberStream::Sink.to_a)
      .run_async

  running.cancel
  running.cancel_requested?
end.wait
# => true
```
