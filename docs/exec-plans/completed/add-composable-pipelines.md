# Add Composable Pipelines

## Status

Completed

## Objective

Add linear composition APIs for reusable flow pipelines, sink composition, and
runnable source-to-sink pipelines.

## Context

- Product spec: `docs/product-specs/composable-pipelines.md`
- Design doc: `docs/design-docs/composable-pipelines.md`
- ADR: `docs/design-docs/adr/0006-composable-pipelines.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Async boundary design: `docs/design-docs/async-boundary.md`
- Buffer boundary design: `docs/design-docs/buffer-boundary.md`
- IO source design: `docs/design-docs/io-source.md`
- IO sink design: `docs/design-docs/io-sink.md`

## Clarifications

- The requested direction is Akka Streams-like composability for reusable flow
  pipelines, runnable pipelines, and sink composition.
- The first implementation remains linear and does not include graph DSLs.

## Contract First

Add public contracts and source comments for:

```ruby
FiberStream::Flow#via(flow)
FiberStream::Flow#to(sink)
FiberStream::Source#to(sink)
FiberStream::Pipeline#run
```

Initial RBS shape:

```rbs
module FiberStream
  class Source[Elem]
    def to: [Mat] (Sink[Elem, Mat] sink) -> Pipeline[Mat]
  end

  class Flow[In, Out]
    def via: [Next] (Flow[Out, Next] flow) -> Flow[In, Next]
    def to: [Mat] (Sink[Out, Mat] sink) -> Sink[In, Mat]
  end

  class Pipeline[Mat]
    def run: () -> Mat
  end
end
```

## Steps

- [x] Explore: inspect existing source, sink, flow, pull runtime, specs, and
      design docs.
- [x] Design review: request sub-agent review and incorporate feedback.
- [x] Red: write failing behavior-focused tests, with unit test files
      organized per module.
- [x] Green: implement the smallest change that satisfies the tests.
- [x] Refactor: improve structure while keeping tests green.
- [x] Static checks: run formatters and static analysis tools, then fix
      findings.
- [x] Code review: request sub-agent review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use `Flow#via(flow)` for reusable flow pipelines.
- Use `Flow#to(sink)` for sink composition instead of `Sink#via(flow)`.
- Use a named `FiberStream::Pipeline` class for `Source#to(sink)`.
- Keep `Source#run_with(sink)` as the direct materialization API.
- Do not add graph topology or flow convenience instance methods in this
  implementation slice.
- Context-free design review on 2026-05-31 reported no findings.
- Implement `Flow#via` by composing the existing attach functions. If the
  second flow fails during attach, close the first attached stream and preserve
  the attach failure as primary.
- Implement `Flow#to` as a sink wrapper that closes its internally attached
  stream after success, failure, and early completion.
- Implement `Source#to` with a named `FiberStream::Pipeline` whose `#run`
  delegates to `Source#run_with`.
- Context-free code review on 2026-05-31 reported no findings.

## Verification

- `bundle exec rake test`
- `bundle exec rbs validate`
- `bundle exec rubocop`
- `bundle exec rake`
- `bundle exec ruby -Itest test/fiber_stream/composable_pipeline_test.rb`
- `bundle exec ruby examples/basic_pipeline.rb`
- `bundle exec ruby examples/composable_pipeline.rb`
- `bundle exec ruby examples/file_copy.rb`
- `bundle exec ruby examples/backpressure_buffer.rb`

## Completion Notes

Implemented linear composability APIs:

- `FiberStream::Flow#via(flow)` for reusable flow pipelines.
- `FiberStream::Flow#to(sink)` for sink composition.
- `FiberStream::Source#to(sink)` and `FiberStream::Pipeline#run` for runnable
  pipelines.

Added RBS signatures, behavior-focused tests, README status updates, and a
new `examples/composable_pipeline.rb` script that demonstrates reusable flow
pipelines, sink composition, and runnable pipeline materialization.

## Commit

Pending commit:

```text
feat: add composable pipelines
```
