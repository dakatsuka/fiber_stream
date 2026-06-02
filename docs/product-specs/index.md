# Product Specs
Product specs define externally visible behavior and user expectations.

## Current Specs

- [Background Execution](background-execution.md)
- [Flow.ractor_map](flow-ractor-map.md)
- [Flow.parallel_map](flow-parallel-map.md)
- [Flow.async](flow-async.md)
- [Flow.buffer](flow-buffer.md)
- [Flow.lines](flow-lines.md)
- [Composable Pipelines](composable-pipelines.md)
- [IO Sink](io-sink.md)
- [IO Source](io-source.md)
- [Minimum Linear Pipeline](minimum-linear-pipeline.md)

## When To Add Or Update A Product Spec
Create or update a product spec when work affects:

- public API behavior
- compatibility promises
- examples, tutorials, or user-facing workflows;
- release criteria.

Implementation should not silently invent product behavior. If behavior matters to users, capture it here before or during implementation.

## Product Spec Template

```markdown
# Title

## Status

Draft | Accepted | Superseded

## Problem

What user need or product requirement does this address?

## Goals

What must be true for users?

## Non-Goals

What is explicitly out of scope?

## Requirements

Specific behavior, compatibility, and error handling requirements.

## Public Contracts

User-visible APIs, function signatures, types, and invariants that design and
implementation must preserve.

## Examples

Representative usage or protocol examples.

## Open Questions

Unresolved product decisions. Ask clarifying questions instead of proceeding by
assumption when these affect implementation.
```
