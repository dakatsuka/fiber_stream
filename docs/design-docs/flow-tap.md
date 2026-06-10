# Flow.tap

## Status

Accepted

## Context

The linear pull runtime already supports value transformation with
`Flow.map`, filtering with `Flow.select`, and terminal side effects with
`Sink.foreach`. Users also need a middle-of-pipeline observation stage that
runs side effects while passing values through unchanged.

Governing documents:

- Product spec: `docs/product-specs/flow-tap.md`
- Existing runtime design: `docs/design-docs/linear-pull-runtime.md`
- Related terminal API: `docs/product-specs/sink-foreach.md`

## Goals

- Implement `Flow.tap { ... }` as a private synchronous pull stage.
- Preserve pull-driven demand, value identity, and ordering.
- Keep failure and cleanup behavior aligned with existing synchronous flows.

## Non-Goals

- Asynchronous side effects.
- Side-effect retry, timeout, batching, or recovery policies.
- Transforming or filtering values.
- Public exposure of internal pull stages.

## Proposed Design

`Flow.tap { |element| ... }` validates that a block is present and stores an
attach callable that returns:

```ruby
Pull.tap(upstream, block)
```

`Pull::Tap` keeps:

- `upstream`, the pull stream before the stage
- `observer`, the user block
- `closed` and `done` flags

On each `next`:

1. Return `DONE` if closed or already done.
2. Pull one upstream value.
3. If upstream returns `DONE`, mark done and return `DONE`.
4. Call the observer block with the upstream value.
5. Ignore the block return value.
6. Return the original upstream value unchanged.

If the observer block raises, the current value is not emitted and the
exception propagates through `Source#run_with`. Existing materialized-chain
cleanup closes upstream and preserves the primary failure over cleanup errors.

`close` is idempotent and closes upstream. The stage does not add queues,
fibers, scheduler requirements, or buffering.

## Contracts

- `Flow.tap { ... }` returns `Flow[Elem, Elem]`.
- Missing blocks raise `ArgumentError`.
- Construction is lazy.
- Each emitted value causes exactly one observer block call.
- The observer return value is ignored.
- The original object is emitted unchanged.
- Upstream completion does not call the observer.
- Upstream failures before a value is produced do not call the observer.
- Observer failures fail the stream and suppress the current value.
- After completion, later defensive pulls return `DONE` without pulling
  upstream or calling the observer.
- The stage has no scheduler requirement.
- Closing the stage closes upstream.

Public API:

```ruby
class Flow[In, Out]
  def self.tap: [Elem] () { (Elem) -> void } -> Flow[Elem, Elem]
end
```

`Source#tap` is not part of this design. Ruby's `Object#tap` is commonly used
to eagerly inspect or configure an object, and `Source` instances inherit that
method today. A lazy per-element `Source#tap` would be source-specific behavior
with the same spelling as the standard object helper.

Internal API:

```ruby
Pull.tap(upstream, observer)
```

## Alternatives Considered

### Use `Flow.map`

Users can write `Flow.map { |value| side_effect(value); value }`, but this
mixes observation and transformation semantics and makes it easier to
accidentally return the wrong value. A dedicated `tap` stage documents the
intent and preserves identity by construction.

### Add Only `Sink.foreach`

`Sink.foreach` already covers terminal side effects, but it cannot observe a
stream in the middle of a flow chain while allowing later stages to continue.
`Flow.tap` and `Sink.foreach` serve different positions in the pipeline.

## Third-Party Review

A context-free design review supported the pull-stage algorithm and raised one
blocking API concern: `Source#tap` would override Ruby's `Object#tap` with
incompatible lazy per-element behavior. This design keeps `Flow.tap` and does
not add a source convenience method.

## Validation

- Unit tests for pass-through behavior, laziness, block
  call ordering, ignored block return values, object identity preservation,
  missing block validation, upstream completion, upstream failure before value,
  observer failure propagation, repeated pulls after completion, and close
  propagation.
- Unit tests for downstream early completion with `Sink.first`, downstream
  failure cleanup, observer failure precedence over close failures, and no
  extra observer calls after early completion.
- RBS validation confirming only `Flow.tap` is added.
- Existing Minitest suite and RuboCop.

## Open Questions

None.
