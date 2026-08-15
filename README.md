# Rundra

Rundra is planned as a portable experiment-execution framework for
reproducible scientific computing and agentic research. The authoritative
product and architecture specification is in
[`docs/project_specs.md`](docs/project_specs.md).

The project, GitHub repository, Python package, and PyPI distribution are named
`rundra`; the command-line executable is `rundr`. The intended primary domain is
`rundra.ai`, with `rundr.ai` redirecting to it. Neither domain is reserved yet.

## Development status

M1 is complete. Rundra has portable domain and configuration models,
deterministic planning, isolated local staging, durable versioned Run records,
shell-free local execution, Apptainer command construction, Git provenance,
artifact retrieval, and common human/JSON lifecycle interfaces. The checked
minimal experiment runs through the same planner, ports, orchestration service,
and persistence path intended for later remote execution.

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

Run the checked example with an explicit seed:

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
`--data-dir` to select another record store. `run` accepts one `--seed`, a
project `--source-root`, and a deterministic or explicit `--destination`.

Add `--json` to obtain the version-1 machine-readable contracts documented in
[`docs/schemas/`](docs/schemas/). Authentication comes only from external
transport mechanisms: credentials must never be placed in experiment files,
target files, opaque scientific configuration, command arguments, or run data.
