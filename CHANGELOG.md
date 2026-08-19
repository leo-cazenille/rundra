# Changelog

All notable user-visible changes are recorded here.

## [Unreleased]

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

### Changed

- Manual release-workflow dispatches publish only to TestPyPI; published GitHub
  releases publish directly to PyPI after rebuilding and validating artifacts.

### Fixed

- Slurm array observations are reconciled consistently across scheduler output
  forms.
- OpenPBS memory requests are rendered per `select` chunk.

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
