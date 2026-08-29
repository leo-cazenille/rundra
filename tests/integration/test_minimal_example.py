from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path

from rundra.persistence import JsonRunStore

_ROOT = Path(__file__).parents[2]
_EXAMPLE = _ROOT / "examples/minimal"


def _run_example(
    *,
    targets: Path,
    records: Path,
    destination: Path,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "rundr",
            "run",
            str(_EXAMPLE / "experiment.yaml"),
            "--config",
            str(_EXAMPLE / "config.yaml"),
            "--seed",
            "17",
            "--target",
            "local",
            "--targets-file",
            str(targets),
            "--source-root",
            str(_EXAMPLE),
            "--destination",
            str(destination),
            "--data-dir",
            str(records),
            "--json",
        ],
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _restore_writes(root: Path) -> None:
    if not root.exists():
        return
    for path in (root, *root.rglob("*")):
        if not path.is_symlink():
            os.chmod(path, stat.S_IMODE(path.stat().st_mode) | stat.S_IWUSR)


def test_minimal_local_example_is_byte_reproducible_for_a_fixed_seed(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""\
version: 1
targets:
  local:
    transport: {{type: local}}
    scheduler: {{type: local}}
    staging: {{type: local}}
    container: {{type: native}}
    workspace: {workspace}
""",
        encoding="utf-8",
    )
    records = tmp_path / "records"

    first = _run_example(
        targets=targets,
        records=records,
        destination=tmp_path / "first",
    )
    second = _run_example(
        targets=targets,
        records=records,
        destination=tmp_path / "second",
    )

    try:
        assert first.returncode == 0, first.stderr or first.stdout
        assert second.returncode == 0, second.stderr or second.stdout
        first_document = json.loads(first.stdout)
        second_document = json.loads(second.stdout)
        assert first_document["run"]["run_id"] != second_document["run"]["run_id"]
        first_result = (tmp_path / "first/results/result-17.json").read_bytes()
        second_result = (tmp_path / "second/results/result-17.json").read_bytes()
        assert first_result == second_result
        assert json.loads(first_result) == {
            "population_size": 100,
            "samples": [
                0.5219839097124932,
                0.8066907771186791,
                0.9604947743238768,
            ],
            "seed": 17,
        }
        stored = JsonRunStore(records).list()
        assert len(stored) == 2
        assert all(record.run.state.value == "SUCCEEDED" for record in stored)
        assert all(record.run.tasks[0].seed == 17 for record in stored)
        assert all(record.git_commit is not None for record in stored)
    finally:
        for run_root in (workspace / "runs").glob("run_*"):
            _restore_writes(run_root / "source")
            _restore_writes(run_root / "input")
