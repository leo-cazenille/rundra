# Rundra

Rundra is planned as a portable experiment-execution framework for
reproducible scientific computing and agentic research. The authoritative
product and architecture specification is in
[`docs/project_specs.md`](docs/project_specs.md).

The project, GitHub repository, Python package, and PyPI distribution are named
`rundra`; the command-line executable is `rundr`. The intended primary domain is
`rundra.ai`, with `rundr.ai` redirecting to it. Neither domain is reserved yet.

## Development status

M0 through M5 are complete; M6 release hardening is in progress. The checked
Shoal path has passed separately gated CPU, GPU, controlled-failure, and
three-element Slurm-array system tests. M6.1 audits every public CLI operation,
common `--json` placement, deterministic output, structured usage errors, and
process exit semantics. M6.2 statically validates executable target stacks,
container/GPU/resource compatibility, and scheduler-native options before
execution, while making `plan` safety and staging behavior explicit.
M6.3 hardens Run persistence with per-Run writer locking, mandatory optimistic
updates, retryable conflict reporting, and concurrent status/fetch/cancel stress
coverage; readers remain lock-free over atomically replaced JSON records.
M6.4 audits every subprocess and the SSH/sbatch shell boundaries, restricts SSH
destinations and Slurm native tokens, rejects symlinked fetch destinations,
redacts adapter diagnostics, and enforces the no-credential invariant at both
configuration and RunStore boundaries without weakening normal SSH host-key
verification.

Rundra has
portable domain and configuration models,
deterministic planning, isolated local staging, durable versioned Run records,
shell-free local execution, Apptainer command construction, Git provenance,
artifact retrieval, and common human/JSON lifecycle interfaces. The checked
minimal experiment runs through the same planner, ports, orchestration service,
and persistence path intended for later remote execution.

M3 adds deterministic inspectable sbatch scripts, parsable submission, durable
job references, `squeue`/`sacct` reconciliation, synchronous waiting,
asynchronous `submit`, cancellation, and normalized remote logs. Portable
resource fields and a narrow `resources.native.slurm` allowlist are translated
without allowing native options to override framework-managed directives.
Normal tests use scripted transports and do not invoke an installed Slurm.

M2.1 adds a typed OpenSSH transport adapter that honors normal user SSH
configuration, agent authentication, jump-host configuration, and host-key
verification. SSH/Slurm/rsync/Apptainer targets are now wired through `run` and
`submit`; site behavior is validated only by separately enabled system tests.

M2.2 centralizes the unavoidable remote-shell serialization boundary. Literal
arguments, environment values, and working directories are POSIX-shell quoted;
diagnostics expose only structurally redacted command summaries.

M2.3–M2.6 add validated isolated remote workspace allocation and rsync upload
of live working trees plus exact effective configuration. Successfully uploaded
source/input snapshots are sealed read-only. Independent idempotent rsync
retrieval collects output, logs, and metadata while keeping transfer state
separate from computation state.

The default integration suite validates this remote transport/staging path with
executable SSH and rsync shims. It requires neither a network connection nor an
installed scheduler. Real-cluster checks remain explicitly opt-in.

A non-secret Shoal target template and its configuration guidance are in
[`docs/shoal.md`](docs/shoal.md). The template is setup documentation, not a
portable site default; recorded bounded validation evidence is documented
separately in that guide.

M1E adds strict project launch profiles, optional user defaults, deterministic
resolution precedence, concise `run`/`plan` commands, and generated seeds that
are displayed and durably recorded before execution.

Local execution is synchronous and local `submit` remains unavailable. Slurm
targets support durable asynchronous `submit`, new-process `status`/`logs`, and
idempotent `cancel`. An explicit `native` runtime supports only an all-local
target and an experiment without a container request; remote experiments use
the `apptainer` runtime.

Implementation progress is tracked in
[`.agent/plans/v0.1.md`](.agent/plans/v0.1.md).

## Development setup

The project requires Python 3.12 and uses
[`uv`](https://docs.astral.sh/uv/) for environment and dependency management.

```bash
uv sync
uv run rundr --help
```

Run the required development checks with:

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy src
```

Do not install project dependencies globally or use a different package
manager.

## Minimal local example

Install the checked local target at the standard user location once:

```bash
mkdir -p ~/.config/rundra
cp examples/minimal/targets.yaml ~/.config/rundra/targets.yaml
```

The adjacent `examples/minimal/rundra.yaml` profile supplies the config, target,
source root, and destination. Plan or run it concisely:

```bash
uv run rundr plan examples/minimal/experiment.yaml
uv run rundr run examples/minimal/experiment.yaml
```

When no seed is configured, Rundra generates a non-negative 63-bit integer
before planning, displays it, passes it to the application, and stores it in the
RunRecord. An independent `plan` and `run` generate different values. Replay a
reported seed explicitly when reproducibility is required:

```bash
uv run rundr run examples/minimal/experiment.yaml --seed 17
```

The fully explicit agent-oriented form remains supported:

```bash
uv run rundr run examples/minimal/experiment.yaml \
  --config examples/minimal/config.yaml \
  --seed 17 \
  --target local \
  --targets-file examples/minimal/targets.yaml \
  --source-root examples/minimal \
  --destination examples/minimal/retrieved
```

For the same source, effective config, seed, Python 3.12 environment, and
runtime, repeated runs must produce byte-identical
`retrieved/results/result.json`. The integration suite runs this example twice
and checks that criterion. It does not claim byte identity across different
Python/runtime versions.

Use `--random-seed` to override a fixed seed supplied by a project profile or
user default. Explicit `--seeds START:STOP` uses an inclusive range in `plan`:
`0:2` produces Tasks for seeds 0, 1, and 2. Task order follows seed-request
order, IDs are stable zero-based ordinals, and duplicate seeds are rejected.
M5.1 fixes these logical semantics. M5.2 makes grouping inspectable:
for two or more homogeneous Tasks on a Slurm target, `plan --json` reports
`strategy: "slurm_array"`, one ordered Task group, and an explicit Task
ID/seed/zero-based-array-index mapping. M5.3–M5.6 add bounded safely quoted
array manifests, submission, per-Task reconciliation/logs/results/retrieval,
cancellation, deterministic aggregation, and real-cluster partial-failure
evidence.

## Launch configuration

Rundra automatically checks for `rundra.yaml` beside the experiment. Use
`--project-file` for any other location and `--profile` to select a non-default
profile. Relative project paths are resolved against the project file:

```yaml
version: 1
default_profile: local
profiles:
  local:
    config: config.yaml
    target: local
    source_root: .
    destination: retrieved
```

Optional user defaults live in `~/.config/rundra/config.yaml`; target backend
definitions remain in their separate target file:

```yaml
version: 1
defaults:
  target: local
  targets_file: targets.yaml
  data_dir: records
```

Paths in user defaults are relative to the user configuration file. Resolution
precedence is CLI → selected project profile → project defaults → user defaults
→ built-ins. `plan --json` and `run --json` expose the effective values and the
source selected for each field under `launch`.

## Inspecting without execution

The checked example can be inspected without executing an experiment:

```bash
uv run rundr validate examples/minimal/experiment.yaml
uv run rundr plan examples/minimal/experiment.yaml \
  --config examples/minimal/config.yaml \
  --seeds 0:1 \
  --target local \
  --targets-file examples/minimal/targets.yaml
uv run rundr targets --targets-file examples/minimal/targets.yaml
```

Human plans summarize resources, native options, execution strategy, staging,
and the offline safety boundary. With `--json`, the plan additionally exposes
normalized `resources` and `native_options`, structured `staging` and
`validation`, and a `safety` object confirming that planning contacts no target,
creates no workspace or Run, and submits nothing. Capability validation here is
static: live SSH, scheduler, rsync, and Apptainer availability is checked only
when execution or an explicit preflight is requested.

Lifecycle commands use `~/.local/share/rundra/runs` by default; pass
`--data-dir` to select another record store. Synchronous `run` executes one
seed or an inclusive seed range, while `source_root` and `destination` may come
from the same launch-resolution layers.

Add `--json` to obtain the version-1 machine-readable contracts documented in
[`docs/schemas/`](docs/schemas/). Authentication comes only from external
transport mechanisms: credentials must never be placed in experiment files,
target files, opaque scientific configuration, command arguments, or run data.

`--json` may also precede the command, for example `rundr --json status
RUN_ID`. Successful operations exit 0. Structured usage/operation failures
exit 1. Exit 2 is reserved for `run` returning a durable failed or cancelled
experiment result; querying that Run successfully with `status` still exits 0.
