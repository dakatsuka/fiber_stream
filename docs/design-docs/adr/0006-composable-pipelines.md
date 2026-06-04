# ADR 0006: Composable Pipelines

## Status

Accepted

## Context

FiberStream currently supports linear pipelines through
`Source#via(flow).run_with(sink)`. Users now need Akka Streams-like
composability for the linear case: reusable flow pipelines, sinks with
preprocessing attached, and source-to-sink runnable definitions. The library
should add these without committing to branching topology or background
execution.

## Decision

Add three linear composition APIs:

```ruby
FiberStream::Flow#via(flow)
FiberStream::Flow#to(sink)
FiberStream::Source#to(sink)
```

`Flow#via(flow)` returns a reusable flow definition that applies the receiver
first and the argument flow second. It is lazy and is behaviorally equivalent
to repeated `Source#via` calls.

`Flow#to(sink)` returns a composed sink. The composed sink attaches the flow to
its upstream, runs the wrapped sink, returns the wrapped sink's materialized
value, and closes the attached flow chain after success, failure, or early sink
completion. Primary failures from the wrapped sink win over close failures.

`Source#to(sink)` returns a named `FiberStream::Pipeline` object. `Pipeline#run`
materializes the stored source and sink definitions and returns the sink's
materialized value. `Source#to(sink).run` is behaviorally equivalent to
`Source#run_with(sink)`, and `Source#run_with` remains supported.

All construction APIs validate their argument types and remain lazy. The first
slice remains linear only. It does not add fan-in/fan-out, materialized-value
combining, background execution, or flow convenience instance methods.

## Consequences

- Users can name and reuse flow pipelines independent of a source.
- Users can package flow preprocessing with sinks.
- Users can construct runnable source-to-sink pipelines before execution.
- The design reuses the existing pull attach model and does not require new
  runtime stage types.
- Composed sinks must carefully close their internally attached flow chain to
  avoid leaks with early completion and async/buffer boundaries.
- Public API surface grows by one class and three methods.
- Branching composition is outside this decision.

## Alternatives Rejected

- Adding branching topology before linear composition.
- Adding fluent `Flow#map`, `Flow#select`, `Flow#take`, `Flow#async`, or
  `Flow#buffer` convenience methods in the first slice.
- Returning an anonymous runnable object from `Source#to`.
- Adding `Sink#via(flow)` instead of `Flow#to(sink)`.
