# M19 scalable agent operations execution plan

## Objective

Keep agent interactions bounded and truthful for long or large Runs while
making preparation inputs and compact result analysis explicit.

## Milestones

1. Add project schema v4 with explicit definition context and retain v3
   whole-snapshot compatibility. Remove resource ceilings from image-content
   identity.
2. Extend Slurm worker journals with continuous start/finish events and derive
   active workers, running Tasks, throughput, and ETA during reconciliation.
3. Bound public JSON for large materialized Runs without relabeling historical
   durable RunRecords as compact. Preserve paginated Task access.
4. Publish a verified Python result-shard reader and migrate the multiprocessing
   analysis example to consume archives without extraction.
5. Add atomic terminal notification files plus topic-scoped CLI and MCP agent
   guidance.
6. Freeze the v19 CLI surface, update public documentation, run the complete
   unit/contract/format/lint/type validation, and retain system tests as gated.

## Compatibility and safety

- Project v1-v3 behavior remains unchanged; only v4 requires context.
- Existing RunRecord readers and records remain valid.
- Notification files contain no credentials and reject symlinks/conflicts.
- Archive reads require immutable checksum and indexed member verification.
- Worker telemetry is observational and never triggers retry or resubmission.

## Deferred migration

Execution still materializes scheduler Tasks in current RunRecords. A future
milestone may connect compact TaskSpace execution and sparse durable state end
to end; M19 does not claim that migration merely by changing output schemas.
