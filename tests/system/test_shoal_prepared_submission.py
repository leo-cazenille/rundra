from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from rundra.artifacts import open_result_set
from rundra.domain.models import RunId, Target
from rundra.persistence import JsonRunStore

pytestmark = [pytest.mark.shoal_system, pytest.mark.shoal_prepared_submission]
_ROOT = Path(__file__).parents[2]
_EXAMPLE = _ROOT / "examples/python-multiprocessing"
_SHOAL_HOSTS = {f"shoal{index}" for index in range(1, 9)}


def _invoke(arguments: tuple[str, ...], *, timeout: float = 900) -> dict[str, object]:
    completed = subprocess.run(
        (sys.executable, "-m", "rundra", *arguments, "--json"),
        cwd=_ROOT,
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="replace",
        shell=False,
        timeout=timeout,
    )
    try:
        document = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        pytest.fail(
            f"rundr {arguments[0]} returned invalid JSON: "
            f"{completed.stdout.strip() or completed.stderr.strip()}"
        )
        raise AssertionError from error
    assert completed.returncode == 0, completed.stderr or document
    assert isinstance(document, dict)
    assert document["ok"] is True
    return document


def _prepare_source(root: Path) -> Path:
    source = root / "source"
    shutil.copytree(_EXAMPLE, source)
    definition = source / "python.def"
    definition.write_text(
        definition.read_text(encoding="utf-8")
        + f"\n%labels\n    org.rundra.acceptance-key {uuid4().hex}\n",
        encoding="utf-8",
    )
    return source


def _prepare_target(source: Path, destination: Path, target_name: str) -> Path:
    document = yaml.safe_load(source.read_text(encoding="utf-8"))
    document["version"] = 8
    target = document["targets"][target_name]
    target.setdefault("preparation", {})["definition_build"] = {
        "allowed_locations": ["target"],
        "mode": "fakeroot",
        "max_resources": {
            "cpus_per_task": 2,
            "memory": "2GiB",
            "walltime": "00:20:00",
        },
    }
    destination.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return destination


def _submit(
    experiment: Path,
    common: tuple[str, ...],
    records: Path,
) -> RunId:
    submitted = _invoke(
        ("submit", str(experiment), *common, "--data-dir", str(records))
    )
    run = submitted["run"]
    assert isinstance(run, dict)
    return RunId(str(run["run_id"]))


def _wait(run_id: RunId, records: Path) -> None:
    waited = _invoke(
        ("wait", str(run_id), "--timeout", "900", "--data-dir", str(records))
    )
    wait = waited["wait"]
    assert isinstance(wait, dict)
    assert wait["terminal"] is True
    status = wait["status"]
    assert isinstance(status, dict)
    assert status["state"] == "SUCCEEDED"
    assert status["tasks"] == {"total": 1, "succeeded": 1}


def test_shoal_cold_then_warm_definition_preparation(
    tmp_path: Path,
    shoal_target: Target,
    shoal_targets_source: Path,
    shoal_target_name: str,
) -> None:
    del shoal_target
    source = _prepare_source(tmp_path)
    targets = _prepare_target(
        shoal_targets_source, tmp_path / "targets.yaml", shoal_target_name
    )
    records = tmp_path / "records"
    experiment = source / "prepared/experiment.yaml"
    common = (
        "--config",
        str(source / "config.json"),
        "--seed",
        "17",
        "--target",
        shoal_target_name,
        "--targets-file",
        str(targets),
        "--source-root",
        str(source),
        "--workers",
        "1",
        "--task-slots-per-worker",
        "1",
    )

    cold_id = _submit(experiment, common, records)
    _wait(cold_id, records)
    cold = JsonRunStore(records).load(cold_id)
    assert cold.format_version == 6
    assert cold.preparation is not None
    assert cold.preparation.image_action == "build_definition_image"
    assert cold.preparation.builder_scheduler_id is not None
    assert cold.preparation.image_recipe_key is not None
    assert cold.preparation.image_sha256 is not None
    assert cold.container_digest == cold.preparation.image_sha256
    assert len(cold.container_digest) == 64
    assert set(cold.allocated_nodes) <= _SHOAL_HOSTS

    destination = Path.home() / ".local/share/rundra/system-test-results" / str(cold_id)
    fetched = _invoke(
        (
            "fetch",
            str(cold_id),
            "--destination",
            str(destination),
            "--mode",
            "reference",
            "--data-dir",
            str(records),
        )
    )
    fetch = fetched["fetch"]
    assert isinstance(fetch, dict)
    assert fetch["retrieval_state"] == "SUCCEEDED"
    result_set = open_result_set(destination)
    result_files = tuple(
        item for item in result_set.iter_files() if item.path.name == "result.json"
    )
    assert result_set.referenced is True
    assert len(result_files) == 1
    result = json.loads(result_files[0].path.read_text(encoding="utf-8"))
    assert result["seed"] == 17
    assert result["host"] in _SHOAL_HOSTS
    assert result["processes"] == 4

    warm_id = _submit(experiment, common, records)
    _wait(warm_id, records)
    warm = JsonRunStore(records).load(warm_id)
    assert warm.format_version == 6
    assert warm.preparation is not None
    assert warm.preparation.image_action == "reuse_definition_image_cache"
    assert warm.preparation.builder_scheduler_id is not None
    assert warm.preparation.image_recipe_key == cold.preparation.image_recipe_key
    assert warm.container_digest == cold.container_digest
