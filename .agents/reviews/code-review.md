# Code Review

Use this workflow after implementation and before final verification or commit.

## Inputs

- The user request or execution plan objective.
- The changed files and the governing specs, design docs, ADRs, and references.
- The verification commands already run and their results.

## Sub-Agent Prompt

Ask a context-free sub-agent for a code-review pass:

```text
Context-free code review task. In this repository, review the current changes
for [feature/fix]. Do not edit files. Prioritize bugs, behavioral regressions,
missing tests, contract drift, concurrency/resource cleanup issues, and
documentation mismatches. Return findings first, ordered by severity, with
file and line references. If there are no findings, say so and identify any
remaining test gaps or residual risk.
```

Add the specific changed files, relevant docs, and verification results to the
prompt. Do not include expected answers or prior conclusions unless they are
required to reproduce the review context.

## Pass Criteria

- Blocking findings are fixed.
- The review is repeated until no blocking findings remain.
- The final review result is recorded in the execution plan when one exists.

