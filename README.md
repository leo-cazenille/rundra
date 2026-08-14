# shoal-run

`shoal-run` is planned as a portable experiment-execution framework for
reproducible scientific computing and agentic research. The authoritative
product and architecture specification is in
[`docs/project_specs.md`](docs/project_specs.md).

## Development status

The repository currently contains the M0.1 Python/tooling scaffold and the M0.2
portable domain values. The CLI exposes a help screen, but YAML configuration
loading, experiment planning and execution, persistence, and infrastructure
backends are not implemented yet.

Implementation progress is tracked in
[`.agent/plans/v0.1.md`](.agent/plans/v0.1.md).

## Development setup

The project requires Python 3.12 and uses
[`uv`](https://docs.astral.sh/uv/) for environment and dependency management.

```bash
uv sync
uv run shoal-run --help
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
