# Composable Pipelines

## Status

Accepted

## Context

FiberStream's current public API supports linear stream construction:

```ruby
source.via(flow).via(flow).run_with(sink)
```

This is already compositional at the source level, but users cannot yet name a
flow chain independently, package a preprocessing flow with a sink, or separate
source/sink wiring from execution. FiberStream should first strengthen this
existing linear model before expanding the runtime shape.

Governing documents:

- Product spec: `docs/product-specs/composable-pipelines.md`
- Existing design: `docs/design-docs/linear-pull-runtime.md`
- Async design: `docs/design-docs/async-boundary.md`
- Buffer design: `docs/design-docs/buffer-boundary.md`
- IO source design: `docs/design-docs/io-source.md`
- IO sink design: `docs/design-docs/io-sink.md`

## Goals

- Add reusable flow pipeline composition.
- Add sink composition through a flow.
- Add a runnable pipeline object for source-to-sink wiring.
- Preserve lazy construction.
- Preserve pull-based backpressure and one-materialization-at-a-time state.
- Keep existing `Source#run_with(sink)` behavior.
- Keep the design linear and small enough to implement without changing the
  pull runtime model.

## Non-Goals

- Fan-in, fan-out, merge, broadcast, zip, cycles, or dynamic topology.
- Materialized-value combination strategies.
- Scheduler ownership or background pipeline execution.
- New data transformation operators.

## Proposed Design

### Reusable Flow Pipelines

`Flow#via(flow)` returns a new `Flow` definition that attaches the receiver
flow first and the argument flow second:

```ruby
normalized =
  Flow.map(&:strip)
    .via(Flow.select { |line| !line.empty? })

source.via(normalized).run_with(sink)
```

The operation is definition-level composition. It does not attach to an
upstream pull stream, call user blocks, start `Flow.async` or
`Flow.buffer(count)`, or close anything. It only stores enough behavior to
attach both flows later during materialization.

Conceptually, composition is:

```ruby
Flow.new do |upstream|
  second.attach(first.attach(upstream))
end
```

The existing `Flow` implementation already represents a flow as an attach
function from upstream pull stream to downstream pull stream. Composing flows
therefore does not require a new runtime stage. The composed flow remains a
definition, and each materialization creates fresh per-stage pull objects.

Ordering follows source-level composition:

```ruby
source.via(flow_a.via(flow_b)).run_with(sink)
```

is behaviorally equivalent to:

```ruby
source.via(flow_a).via(flow_b).run_with(sink)
```

Validation remains narrow: `Flow#via` accepts only `FiberStream::Flow` and
raises `TypeError` for other objects.

### Sink Composition

`Flow#to(sink)` returns a new `Sink` definition:

```ruby
line_output =
  Flow.map { |line| "#{line}\n" }
    .to(Sink.io(output, flush: true))

source.run_with(line_output)
```

The resulting sink accepts the flow's input type and returns the wrapped sink's
materialized value. It is equivalent to inserting the receiver flow immediately
before the sink at materialization time.

The composed sink must own the flow chain it materializes. Its internal run
operation attaches the receiver flow to the upstream stream, runs the wrapped
sink against that attached stream, and closes the attached stream in an ensure
block. Closing the attached stream is required for early-completion sinks such
as `Sink.first`, and for `Flow.async` or `Flow.buffer(count)` boundaries that
may have producer state to release.

Close/error precedence matches `Source#run_with`:

- If the wrapped sink fails, that failure is primary.
- If attached-stream close also fails after a wrapped sink failure, the close
  failure is suppressed.
- If the wrapped sink succeeds and attached-stream close fails, the close
  failure is delivered.

The outer `Source#run_with` still closes the materialized source chain after
the composed sink returns. Pull stream `close` operations must remain
idempotent, so double-close during nested composition is safe and expected.

`Flow#to` validates that the argument is a `FiberStream::Sink` and raises
`TypeError` otherwise. Construction remains lazy and performs no upstream
pulls, IO operations, scheduler validation, or cleanup.

### Runnable Pipeline

`Source#to(sink)` returns a `FiberStream::Pipeline` definition:

```ruby
pipeline =
  Source.io(input, close: true)
    .via(parse)
    .to(Sink.io(output, close: true, flush: true))

pipeline.run
```

The pipeline object stores the source and sink definitions. It does not
materialize them until `Pipeline#run` is called. `Pipeline#run` delegates to the
same materialization path as `Source#run_with(sink)` and returns the sink's
materialized value.

`Source#run_with(sink)` remains the direct convenience API. The following are
behaviorally equivalent:

```ruby
source.run_with(sink)
source.to(sink).run
```

Repeated `Pipeline#run` calls create new materializations from the same source
and sink definitions. Repeatability is not guaranteed by `Pipeline` itself; it
inherits the replayability and resource ownership semantics of the endpoints.
For example, a pipeline built from `Source.each([1, 2, 3])` and `Sink.to_a` is
normally repeatable, while a pipeline built from `Source.io(file, close: true)`
is not replayable after the file reaches EOF or closes.

`Source#to` validates that the argument is a `FiberStream::Sink` and raises
`TypeError` otherwise.

### Public Class Shape

`Pipeline` is a small public class because `Source#to` returns it. It should
not expose the source, sink, or pull internals. The initial public method is
only `#run`.

Implementation should add a new file, likely `lib/fiber_stream/pipeline.rb`,
and require it from `lib/fiber_stream.rb`.

### Type Shape

The intended type flow is:

```rbs
class Flow[In, Out]
  def via: [Next] (Flow[Out, Next] flow) -> Flow[In, Next]
  def to: [Mat] (Sink[Out, Mat] sink) -> Sink[In, Mat]
end

class Source[Elem]
  def to: [Mat] (Sink[Elem, Mat] sink) -> Pipeline[Mat]
end

class Pipeline[Mat]
  def run: () -> Mat
end
```

The first implementation should not add flow convenience instance methods such
as `Flow#map` or `Flow#select`. Users can compose with explicit `via` calls.
Keeping the first slice small leaves room to add fluent convenience methods
after the core composition contracts settle.

## Contracts

- `Flow#via(flow)` returns a composed `Flow`.
- `Flow#via` validates the argument as `FiberStream::Flow`.
- `Flow#via` applies the receiver before the argument flow.
- `Flow#via` is lazy and has no runtime side effects at construction.
- A composed flow creates fresh pull state for each materialization.
- `Flow#to(sink)` returns a composed `Sink`.
- `Flow#to` validates the argument as `FiberStream::Sink`.
- `Flow#to` is lazy and has no runtime side effects at construction.
- A composed sink materializes the receiver flow before the wrapped sink.
- A composed sink returns the wrapped sink's materialized value.
- A composed sink closes its attached flow chain after success, failure, and
  early sink completion.
- A composed sink preserves primary-error precedence over close failures.
- `Source#to(sink)` returns a `FiberStream::Pipeline`.
- `Source#to` validates the argument as `FiberStream::Sink`.
- `Source#to` is lazy and has no runtime side effects at construction.
- `Pipeline#run` materializes the source and sink and returns the sink's
  materialized value.
- `Source#to(sink).run` is behaviorally equivalent to
  `Source#run_with(sink)`.
- `Source#run_with(sink)` remains public and supported.
- Public APIs never expose `Pull::DONE`.

## Alternatives Considered

### Add Non-Linear Topology First

Non-linear composition is more general, but FiberStream's runtime and docs are
currently linear. Adding branching topology now would force decisions about
ports, shape typing, fan-in/fan-out, scheduling, cancellation across branches,
and materialized-value combination. Linear composition solves the current reuse
problem without changing the pull runtime model.

### Add Flow Convenience Instance Methods Immediately

Methods such as `Flow#map`, `Flow#select`, and `Flow#buffer` would make fluent
flow pipelines shorter. They are not necessary for the core composition
contract and increase the first public API surface. The first slice should
prove `Flow#via`; convenience methods can follow later if examples show enough
friction.

### Make `Source#to` Return An Anonymous Runnable Object

An anonymous object would keep the class list smaller, but `Source#to` exposes
a user-visible value. A named `FiberStream::Pipeline` gives RBS, docs, errors,
and future extensions a stable home while keeping the public API narrow.

### Add `Sink#via(flow)` For Sink Composition

Sink-side composition could read naturally as `sink.via(flow)`, but the order
is easier to mistake because data flows from source through flow into sink.
`Flow#to(sink)` keeps the direction left-to-right and mirrors
`Source#to(sink)`.

## Third-Party Review

Reviewed by a context-free sub-agent on 2026-05-31. No findings were reported.
The review specifically checked public API coherence, linear composition
semantics, sink-composition cleanup/error precedence, replayability framing,
RBS shape, and fit with the existing pull attach model.

## Validation

- Unit tests for `Flow#via` argument validation and lazy construction.
- Unit tests proving `flow_a.via(flow_b)` is equivalent to
  `source.via(flow_a).via(flow_b)`.
- Unit tests proving reusable composed flows do not share per-materialization
  state.
- Unit tests for `Flow#to` argument validation and lazy construction.
- Unit tests proving composed sinks return wrapped sink materialized values.
- Unit tests proving composed sinks close attached flow chains after success,
  failure, and early sink completion.
- Unit tests proving composed sink primary-error precedence over close
  failures.
- Unit tests for `Source#to` argument validation and lazy construction.
- Unit tests proving `Source#to(sink).run` matches `Source#run_with(sink)`.
- Unit tests proving repeated `Pipeline#run` follows existing source/sink
  repeatability semantics.
- RBS validation.
- RuboCop.
- README or examples updates showing reusable flow pipelines, sink
  composition, and runnable pipelines.

## Open Questions

None.
