from __future__ import annotations

import json
import math
import os
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[2]
_EXAMPLE = _ROOT / "examples/python-multiprocessing"


def test_python_multiprocessing_local_run_and_analysis(tmp_path: Path) -> None:
    affinity_getter = getattr(os, "sched_getaffinity", None)
    available_cpus = (
        len(affinity_getter(0))
        if affinity_getter is not None
        else (os.cpu_count() or 1)
    )
    if available_cpus < 2:
        pytest.skip("multiprocessing integration requires at least two visible CPUs")
    source_config = json.loads((_EXAMPLE / "config.json").read_text(encoding="utf-8"))
    processes = min(source_config["processes"], available_cpus)
    source_config["processes"] = processes
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(source_config, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
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
    destination = tmp_path / "retrieved"
    completed = subprocess.run(
        (
            "rundr",
            "run",
            str(_EXAMPLE / "experiment-local.yaml"),
            "--config",
            str(config),
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
            str(tmp_path / "records"),
            "--json",
        ),
        cwd=_ROOT,
        capture_output=True,
        check=False,
        text=True,
        timeout=300,
    )

    task_stderr = "\n".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in workspace.rglob("*.stderr")
    )
    assert completed.returncode == 0, "\n".join(
        value for value in (completed.stderr, completed.stdout, task_stderr) if value
    )
    run_document = json.loads(completed.stdout)
    assert run_document["run"]["state"] == "SUCCEEDED"
    result_source = destination / "results/result.json"
    result = json.loads(result_source.read_text(encoding="utf-8"))
    assert result["seed"] == 17
    assert result["processes"] == processes
    assert result["intervals"] == 2_000_000
    assert result["allocated_cpus"] >= processes
    assert abs(result["pi_estimate"] - math.pi) <= 1e-10
    partitions = result["partitions"]
    assert [item["ordinal"] for item in partitions] == list(range(processes))
    assert len({item["pid"] for item in partitions}) == processes
    assert partitions[0]["start"] == 0
    assert partitions[-1]["stop"] == 2_000_000
    assert all(
        left["stop"] == right["start"]
        for left, right in zip(partitions, partitions[1:], strict=False)
    )

    summary_source = tmp_path / "derived/summary.json"
    analyzed = subprocess.run(
        (
            sys.executable,
            str(_EXAMPLE / "analyze.py"),
            "--input",
            str(destination),
            "--output",
            str(summary_source),
            "--expected-processes",
            str(processes),
        ),
        cwd=_ROOT,
        capture_output=True,
        check=False,
        text=True,
        timeout=60,
    )
    assert analyzed.returncode == 0, analyzed.stderr or analyzed.stdout
    summary = json.loads(summary_source.read_text(encoding="utf-8"))
    assert summary["tasks"] == 1
    assert summary["processes"] == processes
    assert summary["seeds"] == [17]
    assert summary["hosts"] == [result["host"]]
    assert summary["maximum_absolute_error"] <= 1e-10
