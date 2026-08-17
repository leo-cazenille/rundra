# M8: Agent Setup, Derived Destinations, and Native YAML Sweeps

## Summary

Reduce agent-session friction without weakening SSH security, derive retrieval
destinations from config names, add generic YAML-native parameter sweeps
inspired by Pogobatch, and keep scientific analysis user-owned but directly
runnable.

Implementation will be split into working commits: specification, destination
handling, diagnostics, sweep domain/parser, sweep execution, and Pogosim
example/analysis migration.

## Key Changes

### Agent setup and diagnostics

- Add `rundr doctor [EXPERIMENT] [--target NAME] [--targets-file PATH]
  [--connect] [--json]`.
- Static checks validate configuration readability, target resolution, required
  local executables, effective OpenSSH configuration, host-key file
  accessibility, and `SSH_AUTH_SOCK` availability.
- `--connect` performs non-mutating batch-mode SSH, remote capability,
  workspace visibility, and writable-ancestor checks without submitting jobs
  or creating remote state.
- Return structured check results with `pass`, `warning`, or `fail`; redact
  credentials and private-key paths.
- Document persistent agent setup: user-level target configuration, readable
  SSH config and known-hosts files, inherited agent socket, network access, and
  safe fallbacks when sandbox policy cannot expose authentication.
- Explicitly prohibit copying private keys into projects, disabling host
  verification, or adding a Rundr credential broker/daemon.

### Derived retrieval destinations

- Preserve precedence for explicit CLI, profile, project, and user
  destinations.
- When no destination is configured, derive
  `PROJECT_ROOT/retrieved/<config-stem>`.
- Without an adjacent project file, use
  `CURRENT_WORKING_DIRECTORY/retrieved/<config-stem>`.
- Report the resolved path with launch source `built_in`; configured
  destinations retain their existing source.
- Repeated runs of the same config intentionally refresh the same destination.
  `fetch --destination` remains explicit.
- Move the Pogosim configuration layout under
  `examples/pogosim-shoal/conf/`, remove its fixed project destination, and
  update documentation/tests for the derived paths.

### Generic YAML sweeps

- Treat a YAML file passed through the existing `--config` interface as a sweep
  only when it contains a strict top-level `_rundr` block:

  ```yaml
  _rundr:
    version: 1
    seeds: "0:19"
  ```

- `_rundr.seeds` accepts one non-negative integer or inclusive `START:STOP`.
  CLI `--seed`, `--seeds`, or `--random-seed` overrides it; otherwise existing
  launch defaults and generated-seed behavior remain.
- Support the Pogobatch-compatible marker subset:
  - `batch_options`
  - `batch_options_range`
  - `batch_hierarchical_options`
- Expand dimensions deterministically in YAML traversal/value order using a
  Cartesian product.
- Simple markers replace their containing value. Hierarchical choices merge
  the selected mapping into the containing mapping; `default` is excluded from
  sweep choices and `name` labels the dimension in provenance.
- Strip `_rundr` and all expansion markers from each effective application
  config.
- Do not implement Pogobatch result merging, `result_filename_format`, retry
  policy, or application-specific columns.
- `plan`, `run`, and `submit` automatically use the expanded task set; no new
  sweep command or sweep CLI parameters are added.
- Keep one Run and one Slurm array containing `parameter sets x seeds`.
  Progress, cancellation, status, logs, and retrieval continue operating
  through stable Task IDs.
- Stage one immutable effective config per Task and map each scheduler-array
  entry to its Task ID, seed, and config path.
- Permit repeated seeds across parameter sets; task identity becomes
  `(TaskId, parameter-set identity, seed)`.
- Add deterministic parameter-set IDs, chosen dimension values,
  effective-config hashes, and Task mappings to plan, inspect, RunRecord, and
  retrieved `metadata/tasks.json`.
- Introduce plan/RunRecord/affected JSON schema version 3 for task-specific
  configs. Unswept projects continue emitting unchanged v1/v2 documents, and
  readers support all three versions.

### Pogosim and analysis example

- Replace the two MSD configs with `conf/msd-120s.yaml`, containing
  hierarchical `ballistic` and `long_tumble` choices plus 20 default seeds.
- The complete study becomes one command:

  ```bash
  uv run rundr run examples/pogosim-shoal/experiment.yaml \
    --config examples/pogosim-shoal/conf/msd-120s.yaml \
    --progress
  ```

- Retrieve under `retrieved/msd-120s`, with Task metadata identifying regime
  and seed.
- Move the analyzer under an analysis directory, add PEP 723 `pyarrow`
  metadata, accept retrieval/output paths as arguments, and write derived
  JSON/CSV outside `retrieved`.
- Keep PyArrow out of Rundr's runtime dependencies and retain derived analysis
  outside the execution core.

## Test Plan

- Test destination precedence, project-root and cwd derivation, config stems,
  repeat retrieval, launch provenance, and unchanged explicit destinations.
- Test doctor static/live success, missing files, missing or inaccessible agent
  sockets, host-key failures, authentication failures, remote capability
  failures, JSON contracts, redaction, and no remote mutation.
- Unit-test strict `_rundr` parsing, seed precedence, all three marker forms,
  ranges, hierarchical merging, deterministic Cartesian ordering, malformed or
  empty markers, and marker stripping.
- Test duplicate seeds across parameter sets, task-specific config hashes,
  v1/v2 compatibility, v3 serialization, and rejection of unsupported schema
  fields.
- Test local and fake SSH/Slurm sweep execution, array manifests, per-task
  configs, progress totals, cancellation, partial failures, retrieval metadata,
  and preparation-cache reuse across all variants.
- Integration-test the one-command Pogosim sweep and MSD analyzer.
- Run the full unit suite, Ruff checks, formatter check, mypy, JSON contract
  tests, minimal local execution, fixed-seed reproducibility, and explicitly
  authorized Shoal sweep smoke test.

## Assumptions and Defaults

- Existing configured destinations remain exact paths; only the built-in
  fallback changes.
- Sweep YAML remains valid application YAML only after Rundr materializes and
  strips framework markers.
- Hierarchical `name` is provenance metadata and is not injected into the
  application config.
- Raw outputs remain separated by Task; Rundr does not aggregate scientific
  datasets.
- Scheduler task-count and resource-policy limits remain authoritative.
- Sandbox permission changes are controlled by the agent platform, not Rundr;
  diagnostics and documentation make failures actionable but never bypass
  them.
- The unrelated untracked repository-root `rundra.yaml` is not modified or
  committed.
