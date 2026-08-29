# Changelog

All notable user-visible changes are recorded here.

## [Unreleased]

### Fixed

- Preparation cache receipts are trusted only for sealed, non-writable SIF
  entries. Legacy writable entries receive one full SHA-256 verification and
  are sealed before receipt migration. Scratch preparation tests also preserve
  target cache and image-search paths outside the allocation-local Run copy.

## [0.1.7] - 2026-08-29

### Added

- Zero-configuration local launches derive an adjacent `config.yaml`, source
  root, destination, packaged local target, and random seed when the project
  does not provide `rundra.yaml` or explicit launch values.
- Local targets execute multi-Task Runs through bounded worker pools and, when
  execution limits are omitted, automatically use the available logical CPUs.
  Named profiles are also derived automatically from configured targets.
- Plans preview the number and total byte size of staged source files before a
  Run is created. Default synchronization excludes generated result trees,
  temporary directories, Python caches, and SIF/SIMG images to avoid recursively
  staging large prior outputs.
- Codex doctor audits use a private two-command nonce handshake to prove that
  the selected Run store survives sandbox process boundaries. Doctor JSON v4
  returns the exact verification command and an actionable persistent-path
  fallback.
- Push and pull-request CI install the built wheel into a clean Python 3.12
  environment and execute a 40-worker local Run through the installed `rundr`
  entry point.
- Content-addressed Apptainer image caches publish atomic versioned receipts
  containing the verified digest and byte size. Legacy entries are measured
  once and migrated automatically.

### Changed

- Human, agent, target, scheduler, schema, and release documentation now
  reflects the current 0.1 command surface and backend capabilities.
- Local multi-Task progress advances from durable Task completions instead of
  jumping from preparation directly to a terminal Run state.
- Valid trusted image receipts avoid repeatedly hashing multi-gigabyte SIF
  files during preparation and offline/cache probes. Missing or inconsistent
  receipts still require a complete SHA-256 measurement before trust.

### Fixed

- Explicit cross-target launches no longer inherit target-specific `workers`
  or `task_slots_per_worker` values from the project or its selected profile.
  The selected target's defaults apply unless the CLI requests scale directly.
- `status` retries transient bundled-journal transport failures once, while
  `wait` and `await` tolerate a configurable bounded number of consecutive
  failed snapshots instead of aborting a healthy long Run after one SSH/read
  interruption. Malformed and contradictory journals still fail immediately.
- Local execution policy is optional, while explicit policy ceilings remain
  enforced. Custom local targets and zero-configuration launches can run
  multiple Tasks without scheduler-specific recovery settings.
- Packaged local target YAML is valid, local retrieval retains declared output
  pattern matching, and task-specific output paths prevent seeds from
  overwriting one another.
- Existing plan JSON contracts retain their declared schema versions while new
  plans expose staging previews through the current schema.

### Agent action required

- Upgrade the installed tool, verify `rundr version`, refresh managed guidance
  with `rundr agent-guide --write AGENTS.md`, and rerun `rundr doctor --agent
  codex --json` before submitting work.
- Review the staged-source preview in `rundr plan`. Projects that intentionally
  keep required inputs under normally generated names such as `results`,
  `outputs`, or `tmp`, or as `.sif`/`.simg` files, must declare an appropriate
  project synchronization policy rather than relying on implicit whole-tree
  staging.

## [0.1.6] - 2026-08-28

### Added

- `rundr await` blocks on one or several Run IDs and emits one compact terminal
  result, allowing agent harnesses to wait without model-driven polling.
- HTCondor joins Slurm and OpenPBS as a scheduler backend for detached Task
  clusters on explicitly shared workspaces.
- Target schemas version 10 and 11 add scheduler-provided allocation scratch,
  read-only Slurm partition inventory, and target-owned walltime/resource
  routing.
- Remote targets may select a compatible `singularity` executable while keeping
  the portable Apptainer runtime contract.
- Scheduler capabilities are derived from a central registry and exposed in
  target-shaped JSON for capability-driven agent planning.
- OpenPBS supports compact worker-pool arrays with concurrent lanes,
  preparation dependencies, durable journals, and bounded scheduler roots.
- Remote offline doctor probes honor `--prepare-location` and verify target
  source and prebuilt-image cache inputs when connected.

### Fixed

- Added `doctor --offline` cache-readiness checks so cold pinned Git or image
  caches fail before Run creation with actionable remediation codes.
- Slurm lifecycle queries fall back to accounting when a completed job has
  already disappeared from `squeue`.
- OpenPBS cancellation preserves an acknowledged cancelled state, and compact
  shard retrieval retries incomplete visibility instead of publishing partial
  coverage.
- Scientific jobs with an unsatisfied preparation dependency are cancelled
  after preparation failure instead of remaining queued indefinitely.
- Image-only remote preparation works without an application build recipe,
  and scratch preparation preserves sealed framework metadata.
- Parameter-sweep Tasks retain their routed resources, including walltime and
  partition selection.
- Singularity 3.8 launches use compatible execution flags.
- Terminal compact Runs clear stale active worker and running Task counters.
- Archive extraction has an independent durable transaction state, so an
  interrupted `--extract` no longer masquerades as a completed extraction or
  rolls back verified shard retrieval.
- Worker status tolerates dependency-pending Runs without journals and merges
  identical events visible through overlapping atomic journal fragments.
- Run aggregates self-heal from durable Task state, compact retries supersede
  older attempts, and undersampled or terminal Runs no longer retain noisy ETA.
- CLI `--fetch-mode` survives launch-layer overlay and is persisted in new Run
  records.

### Agent action required

- Upgrade the installed tool, verify `rundr version`, refresh the managed
  `AGENTS.md` section with `rundr agent-guide --write AGENTS.md`, and rerun
  `rundr doctor --agent codex --json`. Existing target files remain explicit;
  adopt newer target schema fields only when the site requires them.

## [0.1.4] - 2026-08-21

### Added

- RunRecord version 6 records typed fetch mode and definition-image recipe
  identity while keeping pending and verified image identities distinct.
- `rundra.artifacts.open_result_set` provides one read-only Python interface for
  materialized results and shared-filesystem reference manifests.
- A separately gated cold/warm prepared-submission acceptance test verifies
  scheduled image construction, cache reuse, result references, and execution
  on Shoal compute nodes.

### Changed

- Public schema support and current-version declarations now use one central
  registry.

### Fixed

- Plain `agent-guide --list-topics` and `--topic` output now renders guidance
  content instead of a path-oriented `None` placeholder.
- Completed remote preparation now atomically persists the verified image
  digest before publishing a successful state, including cancellation races.

## [0.1.3] - 2026-08-20

### Added

- RunRecord version 5 persists the resolved retrieval destination and an
  explicit materialized/compact Run kind independently of preparation and
  parameter-sweep capabilities.
- Prepared plan JSON reports the policy-derived `selected_location` alongside
  the original preparation location request.

### Fixed

- A later `fetch RUN_ID` now reuses the destination resolved by `submit` unless
  an explicit fetch destination overrides it.
- Automatic definition-image preparation now honors target-authorized build
  locations and selects a scheduled target build when local builds are not
  permitted.

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
