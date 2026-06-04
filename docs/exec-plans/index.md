# Execution Plans
Execution plans are first-class development artifacts for substantial work.

## Directories
- `active/`: plans currently being implemented.
- `completed/`: finished plans with final notes and verification results.

## Active Plans

None.

## Completed Plans

- [Add Source.concat](completed/add-source-concat.md)
- [Harden Ractor Map Coordinator](completed/harden-ractor-map-coordinator.md)
- [Add Ractor Port Source](completed/add-ractor-port-source.md)
- [Add Benchmarks And Async HTTP Example](completed/add-benchmarks-and-async-http-example.md)
- [Add Flow.ractor_map](completed/add-flow-ractor-map.md)
- [Design Flow.ractor_map](completed/design-ractor-map.md)
- [Add Background Execution](completed/add-background-execution.md)
- [Add Flow.parallel_map](completed/add-flow-parallel-map.md)
- [Add Composable Pipelines](completed/add-composable-pipelines.md)
- [Add Flow.lines](completed/add-flow-lines.md)
- [Add Flow.buffer](completed/add-flow-buffer.md)
- [Add Flow.async](completed/add-flow-async.md)
- [Add IO Sink](completed/add-io-sink.md)
- [Add IO Source](completed/add-io-source.md)
- [Add Flow.take](completed/add-flow-take.md)
- [Add Flow.select](completed/add-flow-select.md)
- [Add Sink.fold](completed/add-sink-fold.md)
- [Initial Linear Pipeline](completed/initial-linear-pipeline.md)

## When To Create A Plan
Create an execution plan when work spans multiple files, introduces a subsystem, changes public behavior, or requires staged verification.

Small local fixes may be completed without a checked-in plan if the relevant product spec and design docs are already clear.

## Plan Template

```markdown
# Title

## Status

Active | Completed | Abandoned

## Objective

What outcome should exist when this plan is complete?

## Context

Which specs, design docs, ADRs, and references govern this work?

## Clarifications

List questions asked before implementation and the answers that removed
ambiguity. Do not proceed on unclear instructions by guessing.

## Contract First

List public APIs, function signatures, types, and contract comments to create
before internal implementation.

## Steps

- [ ] Explore: inspect existing code, specs, design docs, and tests.
- [ ] Design review: request sub-agent review and incorporate
      feedback.
- [ ] Red: write failing behavior-focused tests, with unit test files organized
      per module.
- [ ] Green: implement the smallest change that satisfies the tests.
- [ ] Refactor: improve structure while keeping tests green.
- [ ] Static checks: run formatters and static analysis tools, then fix findings.
- [ ] Code review: request sub-agent review after implementation.
- [ ] Re-review: fix review findings and repeat review until it passes.

## Decisions

Record implementation decisions made during the work.

## Verification

List test commands, static analysis commands, format commands, examples, or
manual checks.

## Completion Notes

Summarize what changed and any follow-up work.

## Commit

Record the Conventional Commits-compliant commit message used for the work.
```
