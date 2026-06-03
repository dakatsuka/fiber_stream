# FiberStream Examples

Run examples from the repository root with Bundler:

```sh
bundle exec ruby examples/basic_pipeline.rb
bundle exec ruby examples/composable_pipeline.rb
bundle exec ruby examples/line_processing.rb
bundle exec ruby examples/file_copy.rb
bundle exec ruby examples/backpressure_buffer.rb
bundle exec ruby examples/background_execution.rb
bundle exec ruby examples/ractor_map_hashing.rb
bundle exec ruby examples/ractor_port_source.rb
bundle exec ruby examples/async_http_requests.rb
```

`basic_pipeline.rb` uses only in-memory values and does not require an async
scheduler.

`composable_pipeline.rb` demonstrates reusable flow pipelines, sink
composition, and runnable `Source#to(...).run` pipelines.

`line_processing.rb` demonstrates `Source#lines` over arbitrary String chunks.

`file_copy.rb` uses Ruby core `File` objects with `Source.io` and `Sink.io`.
IO examples require a scheduler-backed non-blocking fiber, so they run inside
an `Async do ... end.wait` block provided by the `async` gem.

`backpressure_buffer.rb` prints timestamped producer and consumer events. The
unbuffered run stays demand-driven, while the buffered run allows bounded
prefetch. The `produced_ahead` counter includes queued values plus in-flight
producer and consumer work, so it can be larger than the configured queue size
without becoming unbounded.

`background_execution.rb` starts a runnable pipeline with `Pipeline#run_async`
and uses the returned handle to wait for the background materialized value while
the foreground fiber keeps doing scheduler-managed work.

`ractor_map_hashing.rb` hashes independent payloads in Ractor workers. It uses
`Ractor.shareable_proc`, preserves input order, and opts into
`input_transfer: :move` because the input records are not reused after the
pipeline runs.

`ractor_port_source.rb` demonstrates a producer Ractor connected to
`Source.ractor_port`. The producer creates its acknowledgment port, waits for
`RactorPort::Ack`, and sends one typed `RactorPort::Element` per downstream
demand.

`async_http_requests.rb` starts a local HTTP server and compares serial
requests with FiberStream `parallel_map` requests. It keeps responses ordered
while overlapping independent network waits.
