from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from pathlib import Path

from rundra.domain.models import ArtifactKind
from rundra.persistence import JsonRunStore, record_from_dict


def _run(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["rundr", *arguments],
        check=False,
        capture_output=True,
        text=True,
    )


def _persist_local_record(tmp_path: Path) -> str:
    document: object = json.loads(
        Path("docs/schemas/run-record-v1.json").read_text(encoding="utf-8")
    )
    original = record_from_dict(document)
    workspace_root = tmp_path / "workspace"
    run_root = workspace_root / "runs" / str(original.run.id)
    logs = run_root / "logs"
    outputs = run_root / "output/results"
    logs.mkdir(parents=True)
    outputs.mkdir(parents=True)
    stdout = logs / "task_000000.stdout"
    stderr = logs / "task_000000.stderr"
    stdout.write_text("cli stdout\n", encoding="utf-8")
    stderr.write_text("cli stderr\n", encoding="utf-8")
    (outputs / "result.json").write_text('{"cli": true}\n', encoding="utf-8")
    artifacts = tuple(
        replace(
            artifact,
            path=(
                stdout
                if artifact.kind is ArtifactKind.STDOUT
                else stderr
                if artifact.kind is ArtifactKind.STDERR
                else artifact.path
            ),
        )
        for artifact in original.artifacts
    )
    record = replace(
        original,
        run=replace(
            original.run,
            target=replace(original.run.target, workspace=workspace_root),
        ),
        artifacts=artifacts,
    )
    JsonRunStore(tmp_path / "records").create(record)
    return str(record.run.id)


def test_persisted_lifecycle_commands_work_from_new_cli_processes(
    tmp_path: Path,
) -> None:
    run_id = _persist_local_record(tmp_path)
    data_arguments = ("--data-dir", str(tmp_path / "records"), "--json")

    status = _run("status", run_id, *data_arguments)
    listed = _run("list", *data_arguments)
    logs = _run("logs", run_id, "--task", "0", *data_arguments)
    inspected = _run("inspect", run_id, *data_arguments)
    fetched = _run(
        "fetch",
        run_id,
        "--destination",
        str(tmp_path / "retrieved"),
        *data_arguments,
    )

    assert all(
        result.returncode == 0 and result.stderr == ""
        for result in (status, listed, logs, inspected, fetched)
    )
    assert json.loads(status.stdout)["status"]["state"] == "SUCCEEDED"
    assert json.loads(listed.stdout)["runs"][0]["run_id"] == run_id
    assert json.loads(logs.stdout)["logs"]["stderr"] == "cli stderr\n"
    assert json.loads(inspected.stdout)["record"]["run"]["id"] == run_id
    assert json.loads(fetched.stdout)["fetch"]["retrieval_state"] == "SUCCEEDED"
    assert (tmp_path / "retrieved/results/result.json").is_file()


def test_submit_and_invalid_run_id_are_structured_cli_failures(tmp_path: Path) -> None:
    unavailable = _run(
        "submit",
        "examples/minimal/experiment.yaml",
        "--config",
        "examples/minimal/config.yaml",
        "--seed",
        "1",
        "--target",
        "local",
        "--targets-file",
        "examples/minimal/targets.yaml",
        "--json",
    )
    invalid = _run(
        "status",
        "invalid",
        "--data-dir",
        str(tmp_path / "records"),
        "--json",
    )

    assert unavailable.returncode == 1
    unavailable_error = json.loads(unavailable.stdout)["error"]
    assert unavailable_error["code"] == "ASYNC_UNAVAILABLE"
    assert "detached scheduler" in unavailable_error["message"]
    assert invalid.returncode == 1
    assert json.loads(invalid.stdout)["error"]["code"] == "INVALID_RUN_ID"


def test_local_run_persists_native_runtime_provenance(tmp_path: Path) -> None:
    data_dir = tmp_path / "records"
    result = _run(
        "run",
        "examples/minimal/experiment.yaml",
        "--config",
        "examples/minimal/config.yaml",
        "--seed",
        "7",
        "--target",
        "local",
        "--targets-file",
        "examples/minimal/targets.yaml",
        "--data-dir",
        str(data_dir),
        "--destination",
        str(tmp_path / "retrieved"),
        "--json",
    )

    assert result.returncode == 0, result.stderr or result.stdout
    record = JsonRunStore(data_dir).list()[0]
    assert record.format_version == 6
    assert record.run_kind == "materialized"
    assert record.retrieval_destination == tmp_path / "retrieved"
    assert record.scheduler_metadata["container_runtime"] == "native"
    assert "container_runtime_version" not in record.scheduler_metadata
