# FiberStream Benchmarks

These scripts compare FiberStream with common Ruby patterns. They are practical
development probes, not formal performance claims.

Run from the repository root:

```sh
bundle exec ruby benchmarks/stream_transform.rb
bundle exec ruby benchmarks/latency_overlap.rb
bundle exec ruby benchmarks/heavy_cpu_map.rb
bundle exec ruby benchmarks/stream_lifecycle_probe.rb
bundle exec ruby benchmarks/ractor_map_leak_probe.rb
```

Use smaller settings for quick smoke runs:

```sh
bundle exec ruby benchmarks/stream_transform.rb --items 200 --take 40
bundle exec ruby benchmarks/latency_overlap.rb --items 6 --delay 0.01
bundle exec ruby benchmarks/heavy_cpu_map.rb --items 100 --work 100 --workers 2
bundle exec ruby benchmarks/stream_lifecycle_probe.rb --iterations 10 --warmup 1 --items 8 --sample-every 5
bundle exec ruby benchmarks/ractor_map_leak_probe.rb --iterations 20 --warmup 2 --items 8 --sample-every 5
```

## Scripts

`stream_transform.rb` compares eager `Enumerable`, `Enumerable::Lazy`,
FiberStream's linear pull pipeline, `Flow.parallel_map`, and `Flow.ractor_map`
for ordered map/filter/take work. Use `--work` to increase per-item CPU cost;
small values mostly measure boundary overhead. The `transforms` column shows
how many input items were actually mapped, making eager evaluation versus lazy
or pull-based early termination visible.

`latency_overlap.rb` compares serial delayed work, direct `Async` tasks, and
FiberStream's scheduler-backed parallel and buffered boundaries for
latency-bound work.

`heavy_cpu_map.rb` compares full-input CPU-bound mapping with serial
`Enumerable`, forced `Enumerable::Lazy`, direct `Async`, FiberStream linear,
`parallel_map`, and one or more `ractor_map` worker counts. It writes a CSV and
an SVG bar chart under `benchmarks/results/` by default:

```sh
bundle exec ruby benchmarks/heavy_cpu_map.rb --items 1600 --work 1000 --workers 2,4
```

`ractor_map_leak_probe.rb` is an opt-in lifecycle and memory trend probe for
`Flow.ractor_map`. It repeatedly materializes normal, early-close, failure,
transfer-failure, and move-transfer pipelines, forces GC between samples, and
prints RSS and `GC.stat` deltas relative to the post-warmup baseline plus
absolute boundary, queue, and thread counts. The RSS column uses Linux
`/proc/self/status`; on other platforms it prints `n/a`, and RSS threshold
checks are rejected. Allocation and free counters are cumulative churn
counters, not retained-memory metrics. It is not part of the default test task
because RSS and allocator behavior vary by platform. To turn the probe into a
threshold check, pass explicit limits:

```sh
bundle exec ruby benchmarks/ractor_map_leak_probe.rb \
  --iterations 500 \
  --warmup 50 \
  --max-rss-growth-kb 32768 \
  --max-live-slot-growth 10000
```

`stream_lifecycle_probe.rb` is a broader opt-in lifecycle probe for boundaries
that own background fibers, coordinator threads, queues, ractors, or background
pipeline handles. It covers `Flow.parallel_map`, `Flow.buffer`, `Flow.async`,
`Source.ractor_port`, `Pipeline#run_async`, and `Flow.ractor_map`. Like the
Ractor-specific probe, it reports RSS and `GC.stat` deltas relative to the
post-warmup baseline plus absolute boundary, queue, thread, fiber, and
running-pipeline counts:

```sh
bundle exec ruby benchmarks/stream_lifecycle_probe.rb \
  --iterations 500 \
  --warmup 50 \
  --max-thread-growth 0 \
  --max-boundary-growth 0
```
