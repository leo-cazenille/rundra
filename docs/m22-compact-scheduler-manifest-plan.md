# M22 compact scheduler manifest plan

## Objective

Remove logical Task count from worker-pool scheduler request, command manifest,
submission result, and staged configuration size.

## Contract

1. A backend-neutral compact scheduler request carries TaskSpace, one command
   template per parameter set, logical and worker resources, and bounded worker
   scale.
2. Slurm workers derive ordinal, Task ID, parameter-set ordinal, and seed using
   integer arithmetic. Manifest size is independent of seed count.
3. Dynamic command values are substituted only into argument-vector templates;
   scientific literals remain shell-quoted and the manifest never uses `eval`.
4. Compact scheduler submissions return root and worker identities only. The
   Task-state sidecar expands the ordinal-modulo-worker mapping transactionally.
5. Existing explicit arrays and small bundled submissions remain unchanged.

## Deferred boundary

The CLI still creates a materialized execution plan and pre-submission RunRecord
before selecting compact persistence. A later milestone must move compact
selection ahead of seed expansion to make launch memory fully constant-size.
