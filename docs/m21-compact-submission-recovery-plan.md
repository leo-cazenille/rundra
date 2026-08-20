# M21 compact submission recovery plan

## Objective

Keep scheduler-submission recovery bounded for large worker-pool Runs while
preserving Rundra's no-duplicate-submission guarantee.

## Contract

1. Existing version-1 and version-2 receipts remain readable and unchanged.
2. Compact Runs begin a strict version-3 receipt containing only TaskSpace and
   sidecar metadata; it never contains per-Task identifiers.
3. The SQLite sidecar transaction records every Task scheduler identity and the
   bounded root scheduler IDs before the receipt transitions to accepted.
4. `resume` may promote a pending compact receipt only when that transaction is
   complete, then atomically compact and submit the RunRecord.
5. Missing or partial scheduler identity state remains an unknown outcome and
   cannot trigger automatic resubmission.

## Deferred boundary

Scheduler request and command-manifest construction still materialize the
execution plan. A later milestone can generate those structures incrementally
without changing this recovery contract.
