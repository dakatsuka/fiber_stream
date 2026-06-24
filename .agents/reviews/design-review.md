# Design Review

Use this workflow after drafting or updating a product spec, design document, or
ADR, and before implementation begins.

## Inputs

- The user request or execution plan objective.
- The relevant product spec, design document, ADR, and external references.
- Any open questions or rejected alternatives that materially shape the design.

## Sub-Agent Prompt

Ask a context-free sub-agent to review the design without relying on the current
conversation:

```text
Context-free design review task. In this repository, review the proposed design
for [feature/change]. Do not edit files. Focus on missing requirements,
unclear contracts, concurrency/resource ownership risks, compatibility risks,
and testability gaps. Return findings ordered by severity with file references.
If the design is sound, say so and note residual risks or assumptions.
```

Add the specific document paths and any relevant constraints to the prompt.

## Pass Criteria

- Blocking findings are fixed in the spec, design doc, ADR, or execution plan.
- Non-blocking findings are either incorporated or explicitly rejected with a
  short rationale.
- The final review outcome is recorded in the document's `Third-Party Review`
  section or the execution plan's `Decisions` / `Completion Notes` section.

