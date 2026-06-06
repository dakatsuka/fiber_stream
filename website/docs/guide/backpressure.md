<script setup>
import BackpressureSequence from "../.vitepress/theme/components/BackpressureSequence.vue";
</script>

# Backpressure

FiberStream's default runtime is pull-based. Downstream demand asks for one
element. Each flow pulls only what it needs. The source advances only when
demand reaches it.

<BackpressureSequence />

## Early completion

`Sink.first` completes after one element:

```ruby
FiberStream::Source.each([1, 2, 3])
  .run_with(FiberStream::Sink.first)
# => 1
```

`Flow.take` closes upstream when the limit is reached:

```ruby
FiberStream::Source.each([1, 2, 3])
  .take(2)
  .run_with(FiberStream::Sink.to_a)
# => [1, 2]
```

## Bounded prefetch

`Flow.buffer(count)` allows bounded prefetch. It requires a scheduler when the
stage is demanded.

```ruby
Async do
  FiberStream::Source.each(1..10)
    .buffer(4)
    .run_with(FiberStream::Sink.to_a)
end.wait
```

The buffer stores at most `count` messages. Closing downstream requests
producer cancellation.

## Async mapping

Use `parallel_map` when each mapping operation waits on scheduler-aware IO.
Results are emitted in input order.

```ruby
Async do
  FiberStream::Source.each([1, 2, 3])
    .parallel_map(concurrency: 3) { |id| fetch_record(id) }
    .run_with(FiberStream::Sink.to_a)
end.wait
```

`parallel_map` is not a CPU parallelism primitive. Use `ractor_map` for
CPU-bound work that can run in Ractors.

## Ractor boundaries

`ractor_map`, `ractor_port`, and `ractor_merge_ports` do not require a
`Fiber.scheduler`. They use Ruby Ractors for isolation and CPU parallelism.

Ractor APIs are experimental in Ruby. Treat Ractor-facing code as an explicit
boundary and keep transferred objects shareable or move-only by design.
