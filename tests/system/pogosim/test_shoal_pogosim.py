"""Opt-in cold/warm Pogosim preparation test for the Shoal Slurm target."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from rundra.domain.models import RunId, Target
from rundra.persistence import JsonRunStore

pytestmark = [pytest.mark.shoal_system, pytest.mark.shoal_pogosim]

_REPOSITORY_ROOT = Path(__file__).parents[3]
_EXPERIMENT = _REPOSITORY_ROOT / "examples/pogosim-shoal/experiment.yaml"
_SEEDS = (0, 1, 2)


def _invoke_cli(
    arguments: tuple[str, ...],
    *,
    timeout: float = 30 * 60,
) -> dict[str, object]:
    completed = subprocess.run(
        (sys.executable, "-m", "rundra", *arguments, "--json"),
        cwd=_REPOSITORY_ROOT,
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="replace",
        shell=False,
        timeout=timeout,
    )
    if completed.returncode != 0:
        pytest.fail(
            f"rundr {arguments[0]} exited {completed.returncode}: "
            f"{completed.stdout.strip() or completed.stderr.strip()}"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        pytest.fail(
            f"rundr {arguments[0]} returned invalid JSON: "
            f"{completed.stdout.strip() or completed.stderr.strip()}"
        )
        raise AssertionError from error
    if not isinstance(value, dict):
        pytest.fail(f"rundr {arguments[0]} returned a non-object JSON document")
    return value


def _assert_feather_signature(path: Path) -> None:
    with path.open("rb") as stream:
        header = stream.read(6)
        stream.seek(-6, 2)
        footer = stream.read(6)
    assert header == b"ARROW1", f"missing Arrow header: {path}"
    assert footer == b"ARROW1", f"missing Arrow footer: {path}"


def _run_once(
    *,
    target_name: str,
    targets_source: Path,
    data_dir: Path,
    destination: Path,
) -> tuple[RunId, JsonRunStore]:
    document = _invoke_cli(
        (
            "run",
            str(_EXPERIMENT),
            "--seeds",
            "0:2",
            "--target",
            target_name,
            "--targets-file",
            str(targets_source),
            "--destination",
            str(destination),
            "--data-dir",
            str(data_dir),
        )
    )
    run = document.get("run")
    if not isinstance(run, dict):
        pytest.fail("run result has no Run payload")
    assert run["state"] == "SUCCEEDED"
    assert run["seeds"] == list(_SEEDS)
    run_id = RunId(run["run_id"])
    return run_id, JsonRunStore(data_dir)


def _assert_completed_run(
    run_id: RunId,
    store: JsonRunStore,
    data_dir: Path,
    destination: Path,
    *,
    image_action: str,
    build_action: str,
) -> None:
    record = store.load(run_id)
    assert record.preparation is not None
    assert record.preparation.image_action == image_action
    assert record.preparation.build_action == build_action
    assert len(record.preparation.build_outputs) == 1
    assert record.preparation.build_outputs[0].executable is True
    assert len(record.scheduler_job_ids) == 1
    assert record.preparation.builder_scheduler_id is not None
    assert record.preparation.builder_scheduler_id not in record.scheduler_job_ids

    status_document = _invoke_cli(("status", str(run_id), "--data-dir", str(data_dir)))
    status = status_document.get("status")
    assert isinstance(status, dict)
    assert status["tasks"] == {"total": 3, "succeeded": 3}
    assert isinstance(status.get("preparation"), dict)

    preparation_logs = _invoke_cli(
        ("logs", str(run_id), "--preparation", "--data-dir", str(data_dir))
    )
    assert isinstance(preparation_logs.get("preparation_logs"), dict)

    feather_files = sorted(destination.rglob("data.feather"))
    assert len(feather_files) == 3
    for feather_file in feather_files:
        _assert_feather_signature(feather_file)


def test_cold_then_warm_three_seed_pogosim_run_on_shoal(
    tmp_path: Path,
    shoal_target: Target,
    shoal_targets_source: Path,
    shoal_target_name: str,
) -> None:
    assert shoal_target.name == shoal_target_name
    cold_id, cold_store = _run_once(
        target_name=shoal_target_name,
        targets_source=shoal_targets_source,
        data_dir=tmp_path / "cold-records",
        destination=tmp_path / "cold-results",
    )
    _assert_completed_run(
        cold_id,
        cold_store,
        tmp_path / "cold-records",
        tmp_path / "cold-results",
        image_action="pull_image",
        build_action="build_and_publish",
    )

    warm_id, warm_store = _run_once(
        target_name=shoal_target_name,
        targets_source=shoal_targets_source,
        data_dir=tmp_path / "warm-records",
        destination=tmp_path / "warm-results",
    )
    _assert_completed_run(
        warm_id,
        warm_store,
        tmp_path / "warm-records",
        tmp_path / "warm-results",
        image_action="reuse_image_cache",
        build_action="reuse_build_cache",
    )
