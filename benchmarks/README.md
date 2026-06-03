# FiberStream Benchmarks

These scripts compare FiberStream with common Ruby patterns. They are practical
development probes, not formal performance claims.

Run from the repository root:

```sh
bundle exec ruby benchmarks/stream_transform.rb
bundle exec ruby benchmarks/latency_overlap.rb
bundle exec ruby benchmarks/heavy_cpu_map.rb
```

Use smaller settings for quick smoke runs:

```sh
bundle exec ruby benchmarks/stream_transform.rb --items 200 --take 40
bundle exec ruby benchmarks/latency_overlap.rb --items 6 --delay 0.01
bundle exec ruby benchmarks/heavy_cpu_map.rb --items 100 --work 100 --workers 2
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
