#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy src
