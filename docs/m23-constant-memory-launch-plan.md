# M23 constant-memory launch plan

## Goal

Remove the remaining controller-side logical-Task expansion from large Slurm
worker-pool launches. Seed parsing, execution planning, initial Run persistence,
staging, scheduler submission, and receipts must remain bounded by parameter-set
and worker count rather than logical Task count.

## Implementation

1. Preserve inclusive CLI seed ranges as `SeedRange` values through launch
   resolution. Expand them only for legacy execution paths.
2. Select the target execution policy before constructing a materialized plan.
   Eligible large worker pools use their scalable plan directly and carry one
   immutable `ExpandedConfig` per parameter set.
3. Create the SQLite TaskSpace and version-4 compact Run record before staging.
   Scheduler acceptance no longer triggers a materialized-to-compact migration
   for this path.
4. Keep small Runs, targets without a compact Task store, non-worker-pool plans,
   and non-contiguous configured seed lists on the existing materialized path.

## Acceptance

- A 1,000-Task worker-pool request contains at most ten preview units.
- The pre-submission JSON Run record contains no materialized Tasks.
- Existing scheduler manifests, receipts, task paging, waiting, cancellation,
  and result-shard retrieval continue to use the same version-4 contracts.
