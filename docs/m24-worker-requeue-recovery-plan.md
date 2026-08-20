# M24 worker requeue recovery plan

## Goal

Allow a compact Slurm worker-pool array element to resume safely after Slurm
requeues it. Recovery must preserve exact logical Task identity, never retry a
Task with a durable scientific outcome, and enforce site-owned retry limits.

## Contract

- Compact scheduler requests carry non-negative `infrastructure_retry_limit`
  and `requeue_limit` values selected from target policy.
- Slurm restart attempts use `SLURM_RESTART_COUNT`; values above the configured
  worker requeue limit are rejected before computation.
- Every attempt writes immutable, attempt-specific worker journals and result
  shards. A restarted lane scans only its own prior immutable journals.
- A Task with a durable FINISH event is skipped. A Task with START but no FINISH
  is an infrastructure interruption and may run again up to the configured
  per-Task limit. Exhaustion records exit code 125 without launching the
  scientific command.
- Journal schema v2 records the attempt on START and FINISH events. Readers
  continue accepting schema-v1 and legacy two-column journals.

## Acceptance

- Replaying a completed worker attempt launches no scientific Tasks twice.
- An interrupted Task resumes with an incremented durable attempt.
- Retry exhaustion is explicit and terminal.
- Journal and scheduler request sizes remain independent of logical Task count.
