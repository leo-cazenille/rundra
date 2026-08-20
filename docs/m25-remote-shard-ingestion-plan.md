# M25 remote result-shard ingestion plan

## Goal

Turn fetched compact result shards into verified, durable per-Task retrieval
state without materializing the complete TaskSpace on the client.

## Contract

- Compact sharded fetches represent an all-Task selection implicitly and derive
  aggregate retrieval state from SQLite counts.
- Archive checksums and bounded indexes are verified on the client or compute
  host, never on an SSH controller configured as a non-computation gateway.
- One SQLite transaction validates every indexed Task against its durable Task
  identity, terminal execution state, and exit code. Cross-shard duplicate
  ownership, conflicting outcomes, missing coverage, and unsafe indexes abort
  the transaction.
- Successful ingestion records each Task's owning shard and transitions only
  proven Tasks to retrieved. Repeated ingestion of the same shard is idempotent.
- Attempt shards containing no newly completed Tasks are valid index-only
  archives and do not claim Task coverage.
- Reference-mode retrieval retains its existing semantics because no archive is
  copied locally for verification.

## Acceptance

- Full compact archive fetch does not construct an all-Task tuple or call
  `SqliteTaskStore.all_states()`.
- Missing, duplicated, corrupt, or outcome-mismatched shards do not partially
  commit retrieval success.
- Selected extraction and legacy/materialized fetch schemas remain unchanged.
