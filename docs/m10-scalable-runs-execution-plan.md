# M10 - Scalable Runs from Multi-Array to 100M Tasks

## Objective

Scale one logical Rundra Run from ordinary Slurm arrays to 100 million
individually identifiable Tasks without running computation on the SSH
controller, materializing every Task in memory, or adding a persistent daemon.

Medium Runs use multiple bounded arrays with one logical Task per element.
Extreme Runs use a bounded Slurm worker pool whose compute-node workers execute
deterministic sequential leases, checkpoint, and requeue across allocation
windows. Scientific failures are recorded and workers continue; only
interrupted infrastructure attempts are retried.

## Public contracts

- Add target configuration version 3 with strict execution policy: hard Task
  and confirmation limits, active-task and array bounds, worker-pool settings,
  retry/requeue bounds, output-shard size, and automatic retrieval threshold.
- Add `--execution-strategy auto|multi-array|worker-pool`,
  `--retrieval all|manifest|none`, and exact `--confirm-tasks N` launch controls.
- Add Plan and RunRecord version 4. Preserve strict v1-v3 readers and output
  without adding fields to old formats.
- Represent large parameter/seed products as a compact deterministic TaskSpace.
  Add paginated `rundr tasks` rather than returning unbounded Task arrays.
- Version-4 artifacts may identify an ordinary file or an indexed member of an
  immutable tar shard.

## Implementation milestones

1. Add compact seed ranges, TaskSpace ordinal derivation, v4 planning summaries,
   strict target-v3 policy parsing, and launch safety validation.
2. Partition medium Runs into deterministic arrays no larger than the configured
   and probed Slurm bound. Persist every root and task range, throttle aggregate
   concurrency, batch scheduler queries, cancel roots, and recover submission
   roots from a remote append-only ledger.
3. Replace monolithic v4 Task state with a per-Run SQLite sidecar and compact
   JSON summary. Support transactional state batches, aggregate counts, direct
   Task lookup, and bounded pages while retaining JsonRunStore v1-v3 behavior.
4. Stage a framework-owned Python worker for single-node/single-task resources.
   Assign deterministic strided shards, enforce logical Task walltime, continue
   after scientific failures, checkpoint sole-writer journals, and requeue near
   allocation expiry within target policy.
5. Seal each completed lease into an indexed, immutable uncompressed tar shard.
   Implement all, manifest-only, none, and selected retrieval without unbounded
   rsync arguments; extraction and verification occur on bigfish, never
   fishvision.
6. Extend status, inspect, logs, fetch, cancel, progress, JSON contracts, doctor,
   documentation, and examples for multi-root and worker Runs.

## Failure and safety semantics

- Plans stay pure and report exact logical count, strategy, scheduler batches or
  workers, concurrency, resource totals, retrieval policy, and bounded previews.
- Above the target threshold, submission requires an exact matching
  `--confirm-tasks`. No CLI option can exceed the target hard cap.
- Partial multi-array submission cancels known roots and durably records the
  cleanup. The remote ledger permits status recovery after client interruption.
- Worker outputs become visible only after a complete shard is atomically
  published. Interruption replays only the unsealed lease. Scientific nonzero
  exits and timeouts are never retried automatically.
- Fishvision is limited to staging, retrieval, probes, and Slurm lifecycle
  commands. Preparation, workers, packing, applications, tests, and analysis run
  only in scheduler allocations on Shoal nodes.

## Verification

- Unit-test Task ordinal boundaries through 99,999,999, constant-memory plans,
  v1-v3 compatibility, target-v3 rejection, confirmation gates, deterministic
  partitioning, throttles, ledger recovery, bounded query/cancel, worker coverage,
  checkpoint/requeue, failure continuation, shard verification, pagination, and
  selective retrieval.
- Integration-test fake SSH/Slurm execution for 20,000 multi-array Tasks and a
  simulated 100-million-Task worker Run without materializing all Tasks.
- Add gated Shoal tests for multiple array roots and worker requeue recovery.
  Launch the 20,000-Task Pogosim campaign only after pure plan inspection and
  exact confirmation; assert accounting contains only `shoal1` through `shoal8`.
- Run pytest, Ruff check/format, mypy, schema contracts, local reproducibility,
  and explicitly authorized Shoal tests.
