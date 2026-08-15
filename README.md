# Rundra

Rundra is planned as a portable experiment-execution framework for
reproducible scientific computing and agentic research. The authoritative
product and architecture specification is in
[`docs/project_specs.md`](docs/project_specs.md).

The project, GitHub repository, Python package, and PyPI distribution are named
`rundra`; the command-line executable is `rundr`. The intended primary domain is
`rundra.ai`, with `rundr.ai` redirecting to it. Neither domain is reserved yet.

## Development status

M1 and M1E are complete. Rundra has portable domain and configuration models,
deterministic planning, isolated local staging, durable versioned Run records,
shell-free local execution, Apptainer command construction, Git provenance,
artifact retrieval, and common human/JSON lifecycle interfaces. The checked
minimal experiment runs through the same planner, ports, orchestration service,
and persistence path intended for later remote execution.

M1E adds strict project launch profiles, optional user defaults, deterministic
resolution precedence, concise `run`/`plan` commands, and generated seeds that
are displayed and durably recorded before execution.

Local execution is synchronous. `submit` reports `ASYNC_UNAVAILABLE` until a
durable asynchronous backend exists. An explicit `native` runtime supports only
an all-local target and an experiment without a container request; experiments
that declare a container image require the `apptainer` runtime.

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
user default. Explicit `--seeds START:STOP` remains available to `plan`; the
multi-Task execution lifecycle is scheduled for M5.

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

Lifecycle commands use `~/.local/share/rundra/runs` by default; pass
`--data-dir` to select another record store. Synchronous `run` executes exactly
one seed, supplied or generated, while `source_root` and `destination` may come
from the same launch-resolution layers.

Add `--json` to obtain the version-1 machine-readable contracts documented in
[`docs/schemas/`](docs/schemas/). Authentication comes only from external
transport mechanisms: credentials must never be placed in experiment files,
target files, opaque scientific configuration, command arguments, or run data.
