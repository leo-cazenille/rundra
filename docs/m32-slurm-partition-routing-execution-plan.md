# M32 Slurm partition routing execution plan

## Objective

Make duration-partitioned Slurm sites usable through one portable target while
keeping exact partition names in operator configuration. M31 scratch execution
remains the data-locality contract.

## Changes

- Target v11 declares ordered CPU/GPU partition routes and finite time limits.
- Pure planning selects and records the shortest compatible route and rejects
  missing walltime or native-partition policy bypasses.
- Scientific, worker-pool, preparation, and probe submissions share one routing
  transform. Routed Runs persist as RunRecord v7.
- `doctor --connect --scheduler-inventory` returns bounded structured Slurm
  partition data without submitting work.

## Verification

Unit tests cover strict parsing, deterministic selection, policy rejection,
inventory parsing, and doctor validation. The anonymous Docker Slurm suite
provides short/long CPU and synthetic GPU partitions and verifies routing with
allocation-local scratch. No real site names or credentials are tracked.
