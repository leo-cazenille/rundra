# M20 journal reconciliation and aggregate-state repair

## Objective

Make worker-pool status deterministic while Slurm dependencies, active journal
publication, retries, and older persisted aggregate state overlap.

## Invariants

- Missing journal files are an empty event set while the scheduler remains
  queued or running.
- Identical events visible through multiple atomic journal fragments are
  idempotent.
- Two different terminal outcomes for the same Task attempt are corruption and
  fail reconciliation explicitly.
- A higher compact-worker attempt supersedes lower attempts; lower-attempt
  outcomes remain provenance but cannot override the latest attempt.
- Run-level portable state is derived from durable Task state. Run-level native
  state is one common Task native state or `MIXED`.
- ETA is absent until the completed sample is both large and representative;
  terminal or undersampled Runs do not retain stale estimates.

## Implementation

1. Merge materialized and compact journal events by Task and attempt rather
   than rejecting harmless duplicate visibility.
2. Add bounded native-state counts to the compact SQLite Task store and repair
   aggregate state before terminal short-circuiting.
3. Share one conservative progress-estimate helper between materialized and
   compact worker reconciliation.
4. Add fake-Slurm and persistence regressions for pending dependencies,
   duplicate fragments, retries, contradictions, stale aggregate state, and
   ETA sampling.
5. Update the architectural specification and agent guidance.

## Validation

- Focused launch, fake-Slurm, lifecycle, Task-store, and agent-guide tests.
- Ruff formatting/lint and mypy.
- Full default test suite; real-cluster tests remain separately gated.
