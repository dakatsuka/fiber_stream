# Add Composable Pipelines

## Status

Active

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
- [ ] Red: write failing behavior-focused tests, with unit test files
      organized per module.
- [ ] Green: implement the smallest change that satisfies the tests.
- [ ] Refactor: improve structure while keeping tests green.
- [ ] Static checks: run formatters and static analysis tools, then fix
      findings.
- [ ] Code review: request sub-agent review after implementation.
- [ ] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use `Flow#via(flow)` for reusable flow pipelines.
- Use `Flow#to(sink)` for sink composition instead of `Sink#via(flow)`.
- Use a named `FiberStream::Pipeline` class for `Source#to(sink)`.
- Keep `Source#run_with(sink)` as the direct materialization API.
- Do not add graph topology or flow convenience instance methods in this
  implementation slice.
- Context-free design review on 2026-05-31 reported no findings.

## Verification

Planned:

- `bundle exec rake test`
- `bundle exec rbs validate`
- `bundle exec rubocop`
- `bundle exec rake`
- Manual examples or README snippets for reusable flow pipelines, sink
  composition, and runnable pipelines.

## Completion Notes

Pending implementation.

## Commit

Pending.
