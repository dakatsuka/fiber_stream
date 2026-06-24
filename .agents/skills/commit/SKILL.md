---
name: commit
description: >-
  Create a well-formed git commit from current changes using session history for
  rationale and summary; use when asked to commit, prepare a commit message, or
  finalize staged work.
---

# Commit

## Goals

- Produce a commit that reflects the actual code changes and the session
  context.
- Follow repository commit conventions, especially Conventional Commits.
- Include both summary and rationale in the body for non-trivial changes.

## Inputs

- Codex session history for intent and rationale.
- `git status`, `git diff`, and `git diff --staged` for actual changes.
- Repo-specific commit conventions from `AGENTS.md` or nearby instructions.

## Steps

1. Read session history to identify scope, intent, and rationale.
2. Inspect the working tree and staged changes:

   ```sh
   git status --short
   git diff
   git diff --staged
   ```

3. Identify the intended commit scope. Do not stage unrelated user changes.
4. Stage only intended paths with explicit `git add <path>...`. Use
   `git add -A` only when the user explicitly wants every current change in the
   commit or the inspected tree shows that every change belongs to the same
   commit.
5. Sanity-check newly added files. If anything looks generated, random,
   sensitive, likely ignored, or unrelated, flag it to the user before
   committing.
6. If staging is incomplete or includes unrelated files, fix the index or ask
   for confirmation.
7. Choose a Conventional Commits type and optional scope that match the change
   (for example, `feat(scope): ...`, `fix(scope): ...`,
   `refactor(scope): ...`, `docs(scope): ...`, `test(scope): ...`, or
   `chore(scope): ...`).
8. Write a subject line in imperative mood, no trailing period, and keep it at
   72 characters or fewer when practical.
9. For non-trivial changes, write a body that explains why first, then what:
   - Rationale and trade-offs.
   - Summary of key changes.
   - Tests or validation run, or an explicit note if not run.
10. Add trailers only when appropriate. Do not add `Co-authored-by` unless the
    user requested it or the repository convention requires it.
11. Wrap body lines at 72 characters.
12. Create the commit message with a temp file and use `git commit -F <file>` so
    newlines are literal. Avoid `git commit -m` with escaped `\n`.
13. Commit only when the message matches the staged changes. If the staged diff
    includes unrelated files or the message describes work that is not staged,
    fix the index or revise the message before committing.

## Output

- A single commit created with `git commit` whose message reflects the session.
- A short final note with the commit hash and validation commands included in
  the commit body.

## Template

Type and scope are examples only; adjust to fit the repo and changes.

```text
<type>(<scope>): <short summary>

Rationale:
- <why this change is needed>
- <trade-offs or constraints>

Summary:
- <what changed>
- <what changed>

Tests:
- <command or "not run (reason)">
```
