# Rundra

Rundra is planned as a portable experiment-execution framework for
reproducible scientific computing and agentic research. The authoritative
product and architecture specification is in
[`docs/project_specs.md`](docs/project_specs.md).

The project, GitHub repository, Python package, and PyPI distribution are named
`rundra`; the command-line executable is `rundr`. The intended primary domain is
`rundra.ai`, with `rundr.ai` redirecting to it. Neither domain is reserved yet.

## Development status

The repository contains the M0 foundation: portable domain values, strict
version-1 YAML loaders, pure deterministic planning, narrow backend contracts,
and fake-driven contract tests. M1.1 adds strict versioned RunRecord
serialization and atomic, collision-safe local JSON persistence. The CLI can
validate configuration, inspect a non-submitting plan, and list configured
targets. Experiment staging, execution, and concrete infrastructure backends
are not implemented yet.

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

## Non-executing CLI

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

Add `--json` to obtain the version-1 machine-readable contracts documented in
[`docs/schemas/`](docs/schemas/). Authentication comes only from external
transport mechanisms: credentials must never be placed in experiment files,
target files, opaque scientific configuration, command arguments, or run data.
