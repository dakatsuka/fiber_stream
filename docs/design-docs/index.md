# Design Docs
This directory is the source of truth for FiberStream's technical design.

## Current Documents

- [Background Execution](background-execution.md)
- [Async Boundary](async-boundary.md)
- [Buffer Boundary](buffer-boundary.md)
- [Composable Pipelines](composable-pipelines.md)
- [Flow.drop](flow-drop.md)
- [Flow.grouped](flow-grouped.md)
- [Flow.take_while](flow-take-while.md)
- [Flow.drop_while](flow-drop-while.md)
- [Flow.lines](flow-lines.md)
- [Flow.split](flow-split.md)
- [Sink.foreach](foreach-sink.md)
- [IO Sink](io-sink.md)
- [IO Source](io-source.md)
- [Linear Pull Runtime](linear-pull-runtime.md)
- [Parallel Map](parallel-map.md)
- [Parallel Unordered Map](parallel-unordered-map.md)
- [Ractor Map](ractor-map.md)
- [Ractor Producer Sources](ractor-producer-sources.md)
- [Ractor Port Source](ractor-port-source.md)
- [Source.concat](source-concat.md)
- [Source.merge](source-merge.md)
- [Source.ractor_merge_ports](source-ractor-merge-ports.md)
- [Source.zip](source-zip.md)

## Architecture Decision Records

- [ADR 0016: Ractor Producer Sources](adr/0016-ractor-producer-sources.md)
- [ADR 0015: Flow.parallel_unordered_map](adr/0015-flow-parallel-unordered-map.md)
- [ADR 0014: Flow.split](adr/0014-flow-split.md)
- [ADR 0013: Source.ractor_merge_ports](adr/0013-source-ractor-merge-ports.md)
- [ADR 0012: Source.merge](adr/0012-source-merge.md)
- [ADR 0011: Ractor Port Source](adr/0011-ractor-port-source.md)
- [ADR 0009: Background Execution](adr/0009-background-execution.md)
- [ADR 0010: Flow.ractor_map](adr/0010-flow-ractor-map.md)
- [ADR 0008: Flow.parallel_map](adr/0008-flow-parallel-map.md)
- [ADR 0006: Composable Pipelines](adr/0006-composable-pipelines.md)
- [ADR 0007: Flow.lines](adr/0007-flow-lines.md)
- [ADR 0005: IO Sink](adr/0005-io-sink.md)
- [ADR 0004: IO Source](adr/0004-io-source.md)
- [ADR 0003: Flow.buffer Boundary](adr/0003-flow-buffer-boundary.md)
- [ADR 0002: Flow.async Boundary](adr/0002-flow-async-boundary.md)
- [ADR 0001: Initial Linear Pull Runtime](adr/0001-initial-linear-pull-runtime.md)

## When To Add Or Update A Design Doc
Create or update a design document when a change affects:

- module boundaries or package structure
- public APIs or long-lived internal interfaces
- concurrency, resource ownership, cancellation, or error handling
- parser, encoder, protocol, or network behavior
- performance, observability, reliability, or security posture

For a major specification change, update the relevant design document and add an ADR that records the decision, alternatives, and consequences.

## Suggested Design Doc Template

```markdown
# Title

## Status

Draft | Accepted | Superseded

## Context

What problem exists, what constraints matter, and what prior documents apply?

## Goals

What must this design achieve?

## Non-Goals

What is intentionally out of scope?

## Proposed Design

Describe the architecture, interfaces, and important behaviors.

## Contracts

List public APIs, function signatures, types, and invariants that must be stable
enough to implement against. Public contracts must be documented with block
comments in source files.

## Alternatives Considered

List credible alternatives and why they were not chosen.

## Third-Party Review

Record feedback from a context-free sub-agent review and how the design changed
before implementation.

## Validation

How will tests, benchmarks, examples, or reviews prove this design works?

## Open Questions

List unresolved decisions that block or shape implementation.
```
