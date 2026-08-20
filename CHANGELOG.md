# Changelog

All notable user-visible changes are recorded here.

## [Unreleased]

### Added

- RunRecord version 5 persists the resolved retrieval destination and an
  explicit materialized/compact Run kind independently of preparation and
  parameter-sweep capabilities.

### Fixed

- A later `fetch RUN_ID` now reuses the destination resolved by `submit` unless
  an explicit fetch destination overrides it.

## [0.1.2] - 2026-08-20

### Added

- Actual Run submission now records the selected container runtime and its
  bounded version identifier when available, without making pure planning or
  target diagnosis execute a version probe.
- Compact archive fetch now transactionally ingests verified result-shard
  indexes into per-Task retrieval state without materializing all Task IDs.
- Compact Slurm workers now recover from scheduler requeues using immutable
  attempt journals, skip durably finished Tasks, and enforce target-owned worker
  and per-Task infrastructure retry limits.
- Large Slurm worker-pool launches now preserve compact seed ranges, use bounded
  preview plans, and create version-4 Run records before staging without
  materializing every logical Task.
- Compact worker-pool Runs use constant-size scheduler requests, bounded worker
  submissions, ordinal-driven Slurm manifests, and one staged config per
  parameter set instead of per-Task command and configuration expansion.
- Large compact submissions use bounded version-3 receipts and can resume an
  interrupted post-acceptance RunRecord compaction from their Task-state sidecar.
- Slurm worker-pool submissions with at least 1,000 logical Tasks now persist a
  version-4 `CompactRun` plus a per-Run SQLite Task-state sidecar.
- Run stores expose a narrowly validated, atomic one-way materialized-to-v4
  compaction capability after scheduler acceptance.

- Project schema version 4 gives Apptainer definition builds an explicit context
  allowlist, so unrelated source changes no longer invalidate image caches.
- `wait --notify-file PATH` atomically publishes one private terminal JSON
  document for external agent supervisors without polling transcript output.
- `rundr agent-guide --topic TOPIC`, `--list-topics`, and MCP `get_guidance`
  provide bounded workflow-specific instructions.
- `rundra.artifacts.open_result_shard` verifies and reads indexed result members
  directly from compact output archives.

- `--progress-interval` throttles interactive redraws and `wait --notify`
  emits one credential-free terminal completion alert.

- Submission receipts record explicit accepted, rejected, uncertain, and
  operator-resolved outcomes. `rundr resolve-submission` and the equivalent MCP
  tool can close an uncertain Run only after exact operator confirmation that
  no scheduler job exists.
- Target schema version 7 can enforce a site-owned aggregate
  `max_memory_per_worker` ceiling during pure planning.

### Changed

- `status`, `wait`, `cancel`, `tasks`, and result retrieval reconcile compact
  worker-pool Runs through scheduler worker observations and Task journals
  without restoring per-Task maps to the JSON RunRecord.

- Large Run and status JSON responses are bounded and report aggregate TaskSpace
  and worker progress instead of serializing thousands of Task details.
- Slurm worker lanes journal Task start and finish events continuously; status
  can report active workers, running Tasks, measured throughput, and ETA.

- Progress output deduplicates unchanged observations, terminal transitions
  remain immediate, and captured `--json --progress` warns agent callers about
  transcript growth. The agent guide recommends silent blocking or renewable
  JSON waits.

- Portable lifecycle help, target errors, setup guidance, and agent instructions
  now describe both Slurm and OpenPBS without renaming backend-specific strategy
  or native-resource fields.

### Fixed

- The local multiprocessing integration test now respects the CPU affinity
  exposed by constrained CI runners while retaining parallel-process coverage.
- Definitive Slurm and OpenPBS submission rejection now fails the registered
  Run instead of stranding it as an unknown outcome; genuinely ambiguous and
  partial submissions remain blocked against duplicate retry.

## [0.1.1] - 2026-08-19

### Added

- `rundr --version` and `rundr version` report the installed distribution
  version from package metadata.
- `rundr agent-guide` links to the public PyPI overview while retaining
  self-contained, version-aware baseline instructions.
- An opt-in Docker Compose system harness exercises the complete remote stack
  and a 1,000-Task worker-pool Run on two local Slurm compute containers.
- An OpenPBS scheduler backend supports arrays, lifecycle reconciliation,
  cancellation, native resource rendering, and Dockerized system coverage.
- A gated Docker Slurm cgroup-v2 harness verifies memory enforcement and
  durable out-of-memory Task classification.
- Agent capability diagnostics report the filesystem and network permissions
  needed before local or remote execution, including explicitly audited access
  to target-visible storage.
- Explicit worker scaling distributes bounded Task slots across requested
  scheduler workers without creating one scheduler job per experiment Task.
- Interrupted scheduler submissions can be recovered from durable receipts
  with `rundr resume` without creating a duplicate Run.
- The optional MCP adapter supports authenticated Streamable HTTP, durable
  submission recovery, and bounded Run and Task discovery.

### Changed

- Manual release-workflow dispatches publish only to TestPyPI; published GitHub
  releases publish directly to PyPI after rebuilding and validating artifacts.
- `rundr list --json` returns compact, paginated Run summaries by default and
  expands Task details only when explicitly requested.
- Automatic retrieval detects jointly visible target storage and can publish a
  reference manifest instead of transferring large result trees unnecessarily.
- Long-running lifecycle commands provide bounded progress reporting, default
  retrieval destinations, and `--last` selection for interactive use.

### Fixed

- Slurm array observations are reconciled consistently across scheduler output
  forms.
- OpenPBS memory requests are rendered per `select` chunk.
- Terminal rsync-backed Runs preserve automatic shared-filesystem retrieval
  selection instead of being forced into a bulk copy.
- Submission progress reaches completion when durable asynchronous submission
  succeeds.

## [0.1.0] - 2026-08-18

### Added

- Portable version-1 experiment, target, project-launch, and user-launch YAML.
- Human and deterministic JSON forms of `validate`, `plan`, `targets`, `run`,
  `submit`, `status`, `list`, `logs`, `fetch`, `inspect`, and `cancel`.
- Explicit or generated seeds, inclusive multi-seed ranges, durable effective
  configuration, deterministic Task identities, and replayable launch
  resolution.
- Synchronous local execution using the native or Apptainer runtime.
- SSH/Slurm/rsync/Apptainer execution with isolated source snapshots,
  asynchronous submission, Slurm arrays, status reconciliation with or without
  usable `sacct`, per-Task logs, cancellation, and idempotent retrieval.
- Versioned RunRecords with independent computation/retrieval state, artifacts,
  scheduler identities, bounded Git provenance, and concurrency-safe updates.
- Checked version-1 CLI, JSON, and RunRecord contract fixtures plus explicitly
  gated Shoal CPU, GPU, failure, array, preflight, and disconnected-lifecycle
  tests.

### Security

- Local subprocesses use argument vectors without a shell; the SSH and sbatch
  shell boundaries use centralized serialization and constrained native values.
- OpenSSH configuration and host verification remain under normal user/site
  policy; Rundra stores no authentication credentials.
- Configuration and RunRecord credential fields, unsafe SSH destinations,
  symlinked retrieval paths, and unsafe workspace/native-option values are
  rejected. Infrastructure errors omit external stderr and command values.

### Compatibility

- The documented CLI, YAML, JSON, state, identifier, and RunRecord surfaces are
  frozen for v0.1. Human formatting and physical storage layouts are not.
- Rundra v0.1 exposes no supported Python API; all `rundra.*` imports remain
  internal and unstable.

### Known limitations

- Only Python 3.12 is supported.
- Local execution is synchronous; local `submit` returns `ASYNC_UNAVAILABLE`.
- The only remote execution stack is SSH/Slurm/rsync/Apptainer. Rundra requires
  no persistent daemon and does not implement PBS, LSF, Kubernetes, or Globus.
- Container digest/runtime-version provenance is not yet captured. Scientific
  reproducibility still depends on preserving external source, image, and
  environment inputs.
