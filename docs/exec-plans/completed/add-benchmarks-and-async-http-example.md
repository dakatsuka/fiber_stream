# Add Benchmarks And Async HTTP Example

## Status

Completed

## Objective

Add runnable benchmark scripts that compare common Ruby stream-processing
patterns against FiberStream, and add a practical async HTTP-style example
where concurrent work is visibly faster and easier to reason about than serial
execution.

## Context

- Examples index: `examples/README.md`
- Repository README: `README.md`
- Existing examples: `examples/*.rb`
- FiberStream APIs: `Source.each`, `Flow.async`, `Flow.buffer`,
  `Flow.parallel_map`, `Flow.ractor_map`, `Sink.to_a`, and `Sink.fold`

## Clarifications

- Benchmarks are development examples, not formal performance claims.
- Avoid external network services so examples are deterministic in local and CI
  environments.
- Avoid adding new runtime or development dependencies unless the existing
  dependency set cannot demonstrate the behavior.
- Include direct `Enumerable`, `Enumerable::Lazy`, direct `Async`, and
  FiberStream variants.

## Contract First

- Add a `benchmarks/` directory with runnable scripts and a README.
- Benchmark scripts print Ruby version, scenario settings, elapsed time, and
  relative speed ratios.
- High-load benchmark scripts can write visual artifacts for easier comparison.
- Add a practical HTTP-style example under `examples/` using a local TCP server
  and concurrent Async client tasks.

## Steps

- [x] Explore: inspect examples, dependencies, and current docs.
- [x] Design review: request context-free review of the benchmark/example shape
      before finalizing if design tradeoffs remain.
- [x] Red: add executable scripts first, then run them to expose missing helper
      behavior or portability issues.
- [x] Green: implement benchmark and example scripts.
- [x] Refactor: keep helpers local and scripts readable.
- [x] Static checks: run examples, benchmark smoke tests, and default checks.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Keep benchmark dependencies to Ruby core, `async`, and FiberStream; avoid the
  `benchmark` gem because it is not available in the current bundle on Ruby 4.
- Use local `Process.clock_gettime(Process::CLOCK_MONOTONIC)` helpers for
  elapsed timings.
- Split benchmarks into stream-shape comparisons and latency-overlap
  comparisons so eager/lazy/backpressure/concurrency signals are not mixed into
  one table.
- Use a local loopback HTTP server in `examples/async_http_requests.rb` so the
  sample demonstrates HTTP-style concurrency without external network
  dependencies.
- Label ratios as `slower` and print transform counts in `stream_transform.rb`
  so eager evaluation versus lazy or pull-based early termination is explicit.
- Add `heavy_cpu_map.rb` for full-input CPU-bound work and generate CSV/SVG
  output under `benchmarks/results/`, which is ignored by git.

## Verification

- `bundle exec ruby benchmarks/stream_transform.rb --items 100 --take 20 --work 2`
  - Passed and printed matching results with transform counts.
- `bundle exec ruby benchmarks/latency_overlap.rb --items 4 --delay 0.005`
  - Passed and printed serial, direct Async, `parallel_map`, and `buffer`
    comparisons.
- `bundle exec ruby benchmarks/heavy_cpu_map.rb --items 80 --work 80 --workers 2 --svg /tmp/fiber_stream_heavy_cpu_smoke.svg --csv /tmp/fiber_stream_heavy_cpu_smoke.csv`
  - Passed and generated CSV/SVG output.
- `bundle exec ruby benchmarks/heavy_cpu_map.rb`
  - Passed with default high-load settings.
  - In the verification run, `ractor_map` with 4 workers took about 0.78s,
    while serial, direct Async, FiberStream linear, and `parallel_map` took
    about 1.51s to 1.56s.
  - Wrote `benchmarks/results/heavy_cpu_map.csv` and
    `benchmarks/results/heavy_cpu_map.svg`.
- `bundle exec ruby examples/async_http_requests.rb`
  - Passed with escalated sandbox permissions for local `127.0.0.1` sockets.
  - Serial requests took about 0.46s and FiberStream parallel requests took
    about 0.18s in the verification run.
- `bundle exec rake`
  - 233 runs, 544 assertions, 0 failures, 0 errors, 0 skips
  - `bundle exec rbs validate` passed
  - `bundle exec rubocop` inspected 35 files with no offenses

## Completion Notes

Added three benchmark scripts and one practical async HTTP example.

`benchmarks/stream_transform.rb` compares eager `Enumerable`,
`Enumerable::Lazy`, FiberStream linear, `parallel_map`, and `ractor_map`
pipelines over the same ordered map/filter/take result. It reports elapsed
time, slowdown versus the fastest case, output count, and transform invocation
count so eager evaluation versus lazy or pull-based early termination is
visible.

`benchmarks/latency_overlap.rb` compares serial delayed jobs, direct `Async`
tasks, FiberStream `parallel_map`, and FiberStream `buffer` for latency-bound
work and producer/consumer overlap.

`benchmarks/heavy_cpu_map.rb` compares full-input CPU-bound mapping with serial
`Enumerable`, forced `Enumerable::Lazy`, direct `Async`, FiberStream linear,
`parallel_map`, and `ractor_map` worker counts. It writes CSV and SVG output so
the difference is visible without reading only the terminal table.

`examples/async_http_requests.rb` starts a local loopback HTTP server and
compares serial requests with FiberStream `parallel_map` requests. The example
keeps response ordering while showing elapsed time drop from summed request
latencies to roughly the slowest individual request.

Code review found no blocking issues. It recommended clarifying eager versus
lazy benchmark effects and renaming the ratio column; both were fixed.

## Commit

```text
docs: add stream benchmarks and async HTTP example

Users need runnable comparisons that show how FiberStream relates to ordinary
Enumerable, Enumerable::Lazy, direct Async tasks, and FiberStream's own async
boundaries in different workload shapes.

Add benchmark scripts for ordered stream transforms, high-load CPU-bound maps,
latency-bound parallel work, and producer/consumer overlap. Add a local HTTP
request example that demonstrates serial waits versus FiberStream parallel_map
without depending on external network services, and document the new scripts in
README files.

Co-Authored-By: OpenAI Codex <codex@openai.com>
```
