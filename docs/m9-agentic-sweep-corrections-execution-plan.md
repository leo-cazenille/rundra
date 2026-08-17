# M9: Agentic Sweep Correctness and Transport Efficiency

## Objective

Correct the defects observed during the first end-to-end parameterized Pogosim
study while preserving the portable experiment, transport, staging, scheduler,
and preparation boundaries.

## Milestones

### 1. Task-specific execution correctness

- Translate each staged Task config path into its corresponding read-only
  container input path instead of always using `input/config.yaml`.
- Unit-test generated commands and fake Slurm array manifests with distinct
  parameter-set configs.
- Show parameter-set IDs and choices in sweep-oriented human plan/run/status
  output so repeated seeds are unambiguous.

### 2. Explicit OpenSSH configuration

- Extend SSH target configuration with optional `executable` and `config_file`
  fields, restricted to user/target configuration rather than experiment YAML.
- Validate local absolute config paths and safe executable values without
  reading or recording credential contents.
- Pass the same SSH selection to transport, doctor, rsync upload/retrieval, and
  reconstructed lifecycle adapters.
- Keep host verification enabled and continue using argument arrays with
  `shell=False`.

### 3. Stable progress and batched staging

- Keep one immutable progress total of `6 + expanded Task count` after sweep
  resolution; preparation events must not reset it to seed count.
- Materialize all Task configs and `metadata/tasks.json` in one local temporary
  directory and transfer that directory in one rsync invocation.
- Preserve atomic remote workspace allocation, exact task paths, input sealing,
  and failure cleanup semantics.

### 4. Warm target preparation reuse without controller source acquisition

- Publish a target-side immutable preparation index after successful remote
  preparation. Its key derives from the pinned source recipe, image digest,
  canonical build recipe, target scope, and platform fingerprint.
- The index records the verified source-content digest, build key, prepared
  source cache path, image path, output hashes, and platform fingerprint.
- Before cloning/fetching a pinned Git source, probe the exact target index. A
  valid warm hit reuses the prepared source in place and stages it into the Run
  workspace server-side; no GitHub access or source upload is required.
- Never use this optimization for explicit mutable `--source-root`, `--rebuild`,
  local preparation, mismatched fingerprints, corrupt indexes, or missing cache
  entries. Cold behavior remains unchanged.
- Validate index/cache entries under the existing target cache locks and never
  trust user-owned filenames or symlinks.

## Contracts and documentation

- Update target-schema examples and the architectural specification for SSH
  selection and warm preparation indexes.
- Preserve v1/v2/v3 experiment, plan, RunRecord, and lifecycle JSON documents;
  transport options are explicit target provenance rather than credentials.
- Add regression tests for each observed failure and retain all existing
  compatibility fixtures.

## Validation

- Run the complete test suite, Ruff lint/format, and mypy.
- Run the minimal fixed-seed local example twice and compare outputs.
- With explicit authorization, run the 40-Task Pogosim sweep on Shoal and assert
  paired raw files differ between regimes and ballistic MSD exceeds long-tumble
  MSD at long times.
