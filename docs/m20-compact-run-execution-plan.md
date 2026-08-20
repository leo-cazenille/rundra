# M20 compact Run execution plan

## Objective

Connect the existing version-4 `CompactRun` and SQLite Task-state model to real
large Slurm worker-pool execution without changing smaller Run schemas.

## Implemented milestones

1. Select compact persistence automatically for worker-pool Runs with at least
   1,000 logical Tasks.
2. After scheduler acceptance, initialize every Task's scheduler identity in a
   mode-0600 SQLite sidecar and atomically compact the JSON RunRecord.
3. Restrict Run-store compaction to a one-way, validated v1-v3 to v4 operation
   preserving Run identity, target, provenance, experiment, Task IDs, seeds,
   and lifecycle state.
4. Reconcile active and terminal worker journals into SQLite for status, wait,
   cancellation, throughput, ETA, pagination, and retrieval.
5. Cover the full fake-Slurm path with 1,000 Tasks and assert that the durable
   RunRecord remains below 100 KB.

## Compatibility

- Runs below 1,000 Tasks retain their existing v1-v3 records.
- OpenPBS and non-worker-pool execution retain materialized records.
- Existing v1-v4 readers and records remain valid.
- Submission still uses the existing scheduler adapter and safety receipts.

## Deferred work

Scheduler request/manifest construction and submission receipts remain
transiently materialized. A later milestone can make those paths
constant-memory after defining a compact command/config manifest that preserves
parameter-set provenance and interrupted-submission recovery.
