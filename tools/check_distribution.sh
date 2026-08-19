#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

if [ "$#" -eq 0 ]; then
    echo "usage: tools/check_distribution.sh ARTIFACT..." >&2
    exit 2
fi

wheel=
for artifact in "$@"; do
    if [ ! -f "$artifact" ]; then
        echo "distribution artifact does not exist: $artifact" >&2
        exit 2
    fi
    case "$artifact" in
        *.whl)
            if [ -n "$wheel" ]; then
                echo "expected exactly one wheel artifact" >&2
                exit 2
            fi
            wheel=$artifact
            ;;
    esac
done
if [ -z "$wheel" ]; then
    echo "no wheel artifact was supplied" >&2
    exit 2
fi

uv run python tools/audit_distribution.py "$@"
uv run twine check "$@"

temporary=$(mktemp -d "${TMPDIR:-/tmp}/rundra-wheel-smoke.XXXXXX")
trap 'rm -rf "$temporary"' EXIT INT TERM
uv venv "$temporary/venv" --python 3.12
uv pip install --python "$temporary/venv/bin/python" "$wheel"
"$temporary/venv/bin/rundr" --version
"$temporary/venv/bin/rundr" help
