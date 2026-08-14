# Implementation plan convention

This directory contains living execution plans for substantial features and
cross-cutting changes. The authoritative product contract remains
`docs/project_specs.md`; plans translate that contract into work but do not
override it.

## Creating and maintaining a plan

1. Record the repository state and the specification sections in scope before
   proposing changes.
2. Split work into dependency-ordered milestones and small checkpoints. Every
   checkpoint must leave the repository runnable and tested.
3. For each milestone state its goal, prerequisites, deliverables, likely
   files, public-interface impact, tests, validation, acceptance criteria,
   risks, and explicit non-goals.
4. Record architectural choices in a decision log. Mark each choice stable or
   provisional, list credible alternatives, and revisit provisional choices
   only when implementation evidence changes the trade-off.
5. Treat documented schemas, CLI semantics, portable states, and JSON output as
   public contracts. Update examples, contract tests, and
   `docs/project_specs.md` together when those contracts change.
6. Update status and discovered issues in the same change that advances the
   plan. Do not mark a checkpoint or milestone complete until its acceptance
   criteria and validation commands pass.
7. Preserve architectural invariants explicitly: domain models do not import
   concrete infrastructure; Runs are not jobs; Tasks are not array indices;
   target configuration is separate from experiment configuration; every
   stochastic task has an explicit seed; and every submitted Run has an
   isolated source snapshot.
8. Prefer the smallest abstraction justified by current backends. Record
   deferred generalization instead of designing speculative plugin systems.

## Status notation

- `[ ]` not started
- `[~]` in progress (include owner/session and next action)
- `[x]` complete (include validation evidence in the progress log)
- `[!]` blocked (describe the blocker and the decision or access needed)

Only one checkpoint should normally be `[~]` at a time. A milestone is complete
only when all its checkpoints and milestone-level acceptance criteria are
complete.

## Validation baseline

Use the checks relevant to the change. Once the Python scaffold exists, the
normal baseline is:

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy src
```

Also run the minimal local example for execution changes, fixed-seed checks for
task construction/execution changes, JSON contract tests for public-output
changes, and opt-in system tests only when the required target is available.
Record skipped checks and why; never imply that an unavailable shoal system test
passed.

## Closing a plan

Before declaring a plan complete, reconcile its acceptance criteria with the
specification, resolve or deliberately carry forward every discovered issue,
record final validation evidence, and name the next concrete task. Completed
plans remain in `.agent/plans/` as an implementation record.
