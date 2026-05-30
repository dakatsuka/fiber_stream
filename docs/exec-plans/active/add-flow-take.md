# Add Flow.take

## Status

Active

## Objective

Add `FiberStream::Flow.take(count)` as the first flow-side early-completion
operation.

## Context

- Product spec: `docs/product-specs/minimum-linear-pipeline.md`
- Design doc: `docs/design-docs/linear-pull-runtime.md`
- ADR: `docs/design-docs/adr/0001-initial-linear-pull-runtime.md`

## Clarifications

- `Flow.take(count)` passes through at most `count` elements.
- `Flow.take(0)` completes without pulling upstream and closes upstream on the
  first downstream demand.
- `Flow.take(count)` forwards the count-th element, closes upstream during that
  same pull, and completes later downstream pulls.
- `Flow.take(count)` raises `ArgumentError` when `count` is negative.
- `Flow.take(count)` raises `TypeError` when `count` is not an `Integer`.
- When `Flow.take(count)` reaches its limit, it closes upstream.
- `Flow.take` itself remains lazy; construction does not evaluate upstream.
- Public RBS signatures are required.

## Contract First

Public API:

- `FiberStream::Flow.take(count)`

Initial RBS shape:

```rbs
class FiberStream::Flow[In, Out]
  def self.take: [Elem] (Integer count) -> Flow[Elem, Elem]
end
```

Contract comments must document:

- maximum emitted element count
- `take(0)` upstream non-evaluation
- `take(0)` upstream close on first downstream demand
- negative count error behavior
- non-Integer count error behavior
- upstream close after the limit is reached

## Steps

- [x] Explore: inspect existing code, specs, design docs, and tests.
- [x] Design review: request sub-agent review and incorporate feedback.
- [ ] Red: write failing behavior-focused tests.
- [ ] Green: implement the smallest change that satisfies the tests.
- [ ] Refactor: improve structure while keeping tests green.
- [ ] Static checks: run formatters and static analysis tools, then fix findings.
- [ ] Code review: request sub-agent review after implementation.
- [ ] Re-review: fix review findings and repeat review until it passes.

## Decisions

- `Flow.take` is a flow-side early-completion stage rather than a sink.
- `take(0)` must not pull upstream, which makes it a useful backpressure
  regression test.
- Reaching the limit closes upstream immediately during the pull that returns
  the count-th element instead of waiting for `Source#run_with` cleanup.
- Runtime count validation should raise `TypeError` for non-Integer values and
  `ArgumentError` for negative Integer values.

## Verification

Planned commands:

- `bundle exec rake test`
- `bundle exec rbs validate`
- `bundle exec rubocop`

## Completion Notes

Pending.

## Commit

Pending.
