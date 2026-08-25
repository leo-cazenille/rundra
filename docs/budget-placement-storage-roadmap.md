# Budget, placement, Campaign, and storage roadmap

## Status

This document preserves design decisions discussed after M30. Except for the
small M31 proposal below, these are deferred ideas rather than implemented or
committed public interfaces. Exact schemas and version numbers may change when
each feature receives its own execution plan.

The broad design was intentionally split because accounting, placement,
multi-target identity, result custody, and node-local execution have different
failure models and should not be introduced in one milestone.

## Deferred feature summary

### Target usage budgets

- Targets may reference a shared budget group so aliases using the same cluster
  account cannot bypass one another's limits.
- A budget may cap concurrent jobs, CPUs, and GPUs plus daily and monthly CPU-
  or GPU-hour use. Any limit may be infinite.
- Slurm is the first finite-budget implementation. Usage includes all jobs for
  the authenticated user, optionally restricted to a configured account, not
  only jobs submitted by Rundra.
- Daily and monthly limits should support calendar and rolling windows.
  Running work is not cancelled after crossing a cumulative limit; later
  submissions are blocked.
- Admission needs a target-side locked reservation ledger to close the interval
  between a successful probe and scheduler visibility.
- Probe failures are closed by default. An explicit assume-available policy may
  proceed only with a recorded warning.
- A future `rundr usage` operation should expose capacity, measured use,
  reservations, limits, and remaining headroom as bounded structured output.

### Automatic target placement

- A target must explicitly opt into automatic selection. Existing explicit
  target workflows remain exact and unchanged.
- `run`, `submit`, and `plan` may accept repeated candidate targets and a
  `single` or `split` placement mode.
- Live selection filters incompatible or over-budget targets, then ranks
  candidates by compatible free slots, budget headroom, configured priority,
  and stable target name.
- A network-free plan describes candidates and deferred live decisions without
  claiming a selected target or cache hit.
- Single placement may submit to a compatible queue when no slot is currently
  free. Split placement degrades to a single target with a warning when fewer
  than two targets have positive capacity.

### Multi-target Campaigns

- Split placement creates a Campaign parent containing ordinary single-target
  child Runs. It does not turn one Run into a cross-target record.
- Campaign Tasks retain deterministic global identities while children retain
  their normal target-local scheduler identities.
- Distribution uses deterministic weighted round-robin based on admissible live
  capacity. Budget reservations are acquired in stable target order.
- Failure to submit one child triggers best-effort cancellation of submitted
  siblings and preserves every record for diagnosis.
- Run lifecycle commands should eventually accept Run or Campaign IDs and
  aggregate status, waiting, tasks, logs, fetching, cancellation, recovery, and
  purge operations.

### Storage-limited targets

- Target policy must distinguish durable shared storage, Rundra caches,
  allocation-local scratch, and externally owned image candidates.
- Image, source, and compiled-build caches may independently be persistent or
  Run-scoped. Rundra never deletes external user-owned candidates.
- A target without durable result storage needs a configured target-visible
  durable sink and a bounded scheduler finalizer submitted with an `afterany`
  dependency.
- The finalizer atomically delivers a complete reproducibility bundle before
  deleting any Rundra-owned ephemeral workspace or Run-scoped cache.
- Failed delivery preserves the last available copy and marks custody at risk.
  An explicit idempotent `rundr finalize ID` operation retries delivery; Rundra
  never reports success or deletes the last copy merely because a retention
  deadline passed.
- Suggested custody states are EPHEMERAL, FINALIZER_PENDING, DELIVERING,
  DURABLE, CLEANED, AT_RISK, and LOST.

### Scratch-first Slurm target design

Some Slurm clusters provide CPU and GPU partition families divided by maximum
walltime. Shared home storage may be suitable for Rundra control data,
persistent caches, and retrieved results, but not as an execution working
directory. CPU allocations may expose node-local scratch through
`SLURM_TMPDIR`; GPU allocations may expose a separate fast scratch root through
`SLURM_GPUTMPDIR`.

The eventual automatic-routing design uses one logical target. Every scheduler
request declares a resource class and explicit walltime. Rundra picks the
shortest compatible configured partition before considering live capacity.
Exact partition, GPU GRES, constraint, account, and QOS names remain operator
configuration and must not be inferred from hardware totals.

Published cluster totals are capacity information, not user budgets. Budget
limits therefore remain infinite until an administrator or account owner
provides actual usage limits.

## Recommended next milestone: M31 node-local Slurm scratch

### Objective

Run Slurm Tasks from allocation-local storage while retaining the existing
single-target selection, scheduler submission, Run identity, and retrieval
model. This is the smallest independently useful part of the roadmap and makes
Rundra suitable for sites that prohibit computation directly from shared home
storage.

### Configuration and behavior

- Add one optional target execution-storage policy for Slurm targets. It selects
  a scheduler-provided environment variable separately for CPU and GPU jobs.
- Accept only environment-variable selectors, initially `SLURM_TMPDIR` and
  `SLURM_GPUTMPDIR`; do not accept arbitrary shell expressions or fallback
  paths.
- Stage the sealed source snapshot, effective config, and verified SIF into
  scratch once per allocation or worker. Native execution stages source and
  config without an image.
- Run scheduled source compilation and definition-image preparation from the
  same allocation-local scratch policy. Publish verified source, build, and
  image cache entries to shared storage only after successful preparation.
- Verify the copied SIF SHA-256 before starting scientific Tasks.
- Give each Task an isolated runtime and output directory below the allocation
  scratch root.
- After each Task, seal and atomically copy its outputs and status metadata back
  to the existing durable Run workspace. A required copy-back failure fails the
  Task and cannot be rendered as scientific success.
- Stage once per worker-pool allocation rather than once per bundled Task.
- Use tightly scoped cleanup traps for Rundra-created scratch paths. Never
  recursively remove the scratch root supplied by the scheduler.
- Keep preparation cache identities, result fetching, target selection, and
  RunRecord semantics unchanged. Budget accounting, automatic partition
  routing, Campaigns, and scheduler finalizers are out of M31.

Example conceptual target addition:

```yaml
execution_storage:
  type: slurm_scratch
  cpu_environment: SLURM_TMPDIR
  gpu_environment: SLURM_GPUTMPDIR
  stage_image: true
  copy_back: task
```

The final field names and target schema version must be fixed in the M31
execution plan after mapping every current Slurm script path.

### Safety and observability

- `plan` remains network-free and reports whether scratch execution is enabled,
  which assets are staged, and that Task outputs are copied back immediately.
- Missing, relative, root, symlink-escaped, or non-writable scratch directories
  fail before scientific execution.
- `doctor --scheduler-probe` validates the configured variable and performs a
  reversible write/copy-back/delete probe inside a bounded Slurm allocation.
- Preparation, staging, digest verification, Task execution, copy-back, and
  cleanup failures remain distinguishable in logs and structured errors.
- Run provenance records the selected scratch policy and variable name, but not
  the allocation-specific temporary path after cleanup.

### Verification

- Unit-test policy parsing, safe scratch-path validation, generated CPU/GPU
  scripts, digest mismatch, per-Task isolation, atomic copy-back, and cleanup.
- Test single-Task arrays and worker pools with fake Slurm adapters, including a
  later worker failure after earlier Task outputs were preserved.
- Extend Docker Slurm tests with a node-local scratch mount and assertions that
  scientific commands execute there while durable outputs return to the Run
  workspace.
- Add a synthetic Docker Slurm target with shared persistent storage, separate
  CPU and GPU scratch mounts, and policy checks that reject scientific or
  preparation computation from the shared workspace.
- Exercise prebuilt images, definition-image preparation, application builds,
  arrays, and worker pools in Docker. Assert that caches and Task outputs are
  copied back while allocation-local paths are removed.
- Keep any real-cluster acceptance configuration outside the repository. A
  bounded acceptance test may consume an externally supplied target and must
  confirm that neither scientific nor preparation computation runs from shared
  home storage.

## Later milestone order

After M31, implement the remaining designs independently:

1. Slurm usage measurement and read-only `rundr usage` reporting.
2. Budget admission and reservation enforcement.
3. Automatic single-target selection and duration-based partition routing.
4. Storage-limited target finalization and result custody.
5. Multi-target Campaigns and split placement.

Each milestone must retain pure planning, structured agent output, scheduler
policy enforcement, and focused commits with its own schema and system-test
gates.
