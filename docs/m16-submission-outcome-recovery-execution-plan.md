# M16 - Durable submission outcomes and recovery

## Incident and objective

Run `run_db049dcfd365418eb5061904c8b5a684` exposed two separate gaps. Its
192 logical Tasks requested 4 GiB each and were bundled into eight workers with
24 slots, producing a 96 GiB request per worker on 63,000 MB nodes. Slurm
returned a definitive nonzero submission status and accepted no worker job, but
Rundra classified the result as `SUBMISSION_OUTCOME_UNKNOWN`, left the Run in
`STAGING`, and made `resume`, `wait`, and `purge` unable to finish it.

M16 will reject target-declared impossible worker resources before staging and
represent scheduler submission outcomes precisely enough to recover safely
without creating duplicate jobs.

## Safety invariants

- Never automatically resubmit when a scheduler may have accepted work.
- A scheduler command that completed with a documented nonzero rejection is a
  definitive failure, not an unknown outcome.
- A transport interruption during scheduler contact or zero-exit output that
  cannot be parsed remains unknown.
- Receipt and RunRecord updates are atomic, idempotent, and safe after process
  interruption at every persistence boundary.
- External stderr, command values, credentials, and environment values remain
  redacted. Structured diagnostics may expose only backend, phase, exit status,
  and portable failure classification.
- Existing version-1 receipts remain readable and are never silently rewritten
  or treated as proof that scheduler contact occurred.

## Milestones

### 1. Portable scheduler outcome semantics

Add scheduler-neutral typed outcomes at the scheduler port boundary:

- `rejected`: the scheduler command completed and accepted no job;
- `uncertain`: scheduler acceptance cannot be determined safely;
- `accepted`: complete scheduler roots and per-Task identities are available.

Slurm and OpenPBS adapters will classify failures by execution boundary. Local
manifest/chunk publication failures and nonzero `sbatch`/`qsub` results are
rejections. Transport exceptions during the final scheduler-contact command and
unparseable zero-exit scheduler output are uncertain. Backend exceptions retain
their concrete types but expose the portable classification to orchestration.

Split combined adapter error messages so manifest publication and scheduler
rejection are distinguishable. Preserve only safe fields such as
`backend=slurm`, `phase=scheduler_submit`, and `exit_code=1`.

### 2. Version-2 submission receipts

Introduce a strict version-2 receipt with an explicit outcome and phase instead
of inferring scheduler contact from `started_at`. It records:

- Run and exact Task identities;
- attempt start and last-transition timestamps;
- `pending`, `accepted`, `rejected`, `uncertain`, or `operator_resolved`;
- safe backend/phase/failure classification;
- complete root and per-Task scheduler identities only for `accepted`.

Receipt transitions use compare-and-replace publication and directory fsync.
Readers support versions 1 and 2. A pending version-1 receipt remains ambiguous;
its current duplicate-prevention behavior is preserved with corrected wording.

### 3. Correct orchestration transitions

Handle typed adapter outcomes before the generic exception boundary:

- `rejected`: persist a rejected receipt, mark every nonterminal Task and the
  Run `FAILED`, and return `SCHEDULER_SUBMISSION_FAILED`;
- `uncertain`: persist an uncertain receipt when possible, retain `STAGING`, and
  return `SUBMISSION_OUTCOME_UNKNOWN` with an explicit operator action;
- `accepted`: persist scheduler identities before transitioning the RunRecord
  to `SUBMITTED` as today.

`rundr resume` adopts accepted receipts, finalizes rejected receipts
idempotently, reports already durable Runs as `found`, and blocks only genuinely
uncertain or legacy-pending attempts. `wait` and `cancel` return an actionable
submission-state error instead of attempting scheduler queries without durable
identities.

### 4. Explicit operator resolution

Add a non-submitting lifecycle operation for cases where an operator has
verified that no scheduler job exists:

```text
rundr resolve-submission RUN_ID --not-submitted --confirm RUN_ID
```

The command is valid only for a `STAGING` Run with an uncertain or legacy
pending receipt and no scheduler identities. Exact Run-ID confirmation is
mandatory. It records `operator_resolved`, marks the Run and Tasks `FAILED`, and
allows normal inspection and purge. It never retries submission, cancels work,
or accepts a scheduler ID supplied by the user. JSON, help, agent-guide, and MCP
interfaces expose the same guarded operation.

### 5. Explicit aggregate worker resource ceilings

Add a new target execution-policy version with an optional, site-owned
`max_memory_per_worker`. Planning already computes exact aggregate worker
resources from logical Task memory and effective slots. Reject a plan before
staging when that aggregate exceeds the configured ceiling, reporting logical
memory, slots, aggregate memory, and ceiling.

Do not infer node memory, topology, exclusivity, or heterogeneous partition
capacity. Existing targets retain current behavior. The Shoal target can set a
conservative ceiling below its 63,000 MB node capacity, while callers remain
free to reduce slots or justified per-Task memory.

### 6. Documentation and compatibility

Update the architectural specification, CLI reference, troubleshooting,
agent-guide, MCP tutorial, target examples, schemas, and changelog. Keep Run
record schemas unchanged where the separate receipt can carry recovery state;
version any public JSON documents that gain fields or operations.

## Test plan

- Unit-test strict receipt-v2 parsing, every legal transition, atomic
  replacement, concurrent mutation, corruption, and version-1 compatibility.
- Inject Slurm and OpenPBS failures before scheduler contact, nonzero scheduler
  rejection, final-command transport loss, malformed zero-exit output, receipt
  completion failure, and RunRecord-update interruption.
- Assert rejection produces a terminal failed Run and uncertainty remains
  nonterminal without scheduler queries or automatic resubmission.
- Test accepted-receipt adoption, rejected-receipt finalization, legacy pending
  wording, repeated resume, and already-durable discovery.
- Test operator resolution confirmation, invalid states, existing scheduler
  identities, idempotency, JSON/MCP parity, and subsequent purge eligibility.
- Test worker-memory ceilings at, below, and above the exact boundary across
  explicit and default worker-slot selection. Confirm old target versions remain
  compatible and planning remains network-free.
- Extend fake SSH/Slurm and Docker OpenPBS system tests with deterministic
  rejection and ambiguous-transport scenarios. Keep real-cluster rejection
  tests separately gated and bounded.
- Run the complete unit/integration suite, Ruff, mypy, contract checks,
  distribution audit, and one authorized Shoal acceptance after implementation.

## Current incident disposition

Preparation job `2437` completed and no worker job was accepted. The later
active Run `run_377cdd76dc7646aba883b6371968b09a` and its jobs `2439`/`2440`
were cancelled through Rundra. The Shoal user queue was empty afterward. The
original incident Run remains a stranded `STAGING` record until M16 or an
explicitly authorized manual migration resolves it.
