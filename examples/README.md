# FiberStream Examples

Run examples from the repository root with Bundler:

```sh
bundle exec ruby examples/basic_pipeline.rb
bundle exec ruby examples/composable_pipeline.rb
bundle exec ruby examples/file_copy.rb
bundle exec ruby examples/backpressure_buffer.rb
```

`basic_pipeline.rb` uses only in-memory values and does not require an async
scheduler.

`composable_pipeline.rb` demonstrates reusable flow pipelines, sink
composition, and runnable `Source#to(...).run` pipelines.

`file_copy.rb` uses Ruby core `File` objects with `Source.io` and `Sink.io`.
IO examples require a scheduler-backed non-blocking fiber, so they run inside
the `Sync` helper provided by the `async` gem.

`backpressure_buffer.rb` prints timestamped producer and consumer events. The
unbuffered run stays demand-driven, while the buffered run allows bounded
prefetch. The `produced_ahead` counter includes queued values plus in-flight
producer and consumer work, so it can be larger than the configured queue size
without becoming unbounded.
