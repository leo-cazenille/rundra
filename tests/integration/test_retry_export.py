from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tarfile
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path

import yaml

from rundra.domain.campaigns import (
    CampaignFailurePolicy,
    CampaignId,
    CampaignLaunchRecord,
    CampaignRecord,
    CampaignSubmissionState,
)
from rundra.domain.models import RunId
from rundra.persistence.campaign_store import JsonCampaignStore


def test_retry_preserves_sparse_seeds_and_sealed_inputs_then_exports(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    experiment = source / "experiment.yaml"
    experiment.write_text(
        """\
version: 1
experiment: {name: retry-test}
command:
  argv: [python3, task.py, "{config}", "{seed}", /workspace/output/result.json]
resources:
  memory: 16MiB
  walltime: "00:01:00"
outputs: {include: [result.json]}
""",
        encoding="utf-8",
    )
    config = source / "config.yaml"
    config.write_text("original: true\n", encoding="utf-8")
    application = source / "task.py"
    application.write_text(
        """\
import json
import sys
from pathlib import Path

seed = int(sys.argv[2])
Path(sys.argv[3]).write_text(json.dumps({"seed": seed, "config": Path(sys.argv[1]).read_text()}))
raise SystemExit(19 if seed in {10, 13} else 0)
""",
        encoding="utf-8",
    )
    targets = tmp_path / "targets.yaml"
    settings = yaml.safe_load(
        files("rundra").joinpath("defaults", "targets.yaml").read_text(encoding="utf-8")
    )
    local = settings["targets"]["local"]
    local["workspace"] = str(tmp_path / "workspaces")
    settings["targets"]["alternate"] = {
        **local,
        "workspace": str(tmp_path / "alternate"),
    }
    targets.write_text(yaml.safe_dump(settings), encoding="utf-8")
    data_dir = tmp_path / "records"
    destination = tmp_path / "retrieved"

    def run_cli(*args: str, expected: int = 0) -> dict:
        completed = subprocess.run(
            (
                sys.executable,
                "-m",
                "rundra",
                *args,
                "--data-dir",
                str(data_dir),
                "--json",
            ),
            cwd=source,
            check=False,
            text=True,
            capture_output=True,
            timeout=60,
        )
        assert completed.returncode == expected, completed.stderr or completed.stdout
        return json.loads(completed.stdout)

    parent = run_cli(
        "run",
        str(experiment),
        "--target",
        "local",
        "--targets-file",
        str(targets),
        "--seeds",
        "10:13",
        "--destination",
        str(destination),
        expected=2,
    )["run"]["run_id"]
    application.write_text("raise SystemExit(99)\n", encoding="utf-8")
    config.write_text("original: false\n", encoding="utf-8")
    retry = (
        "retry",
        parent,
        "--failed",
        "--target",
        "alternate",
        "--targets-file",
        str(targets),
    )
    preview = run_cli(*retry)["retry"]
    assert preview["run_id"] is None
    assert preview["task_count"] == 2
    assert [(row["parent_task_id"], row["seed"]) for row in preview["tasks"]] == [
        ("task_000000", 10),
        ("task_000003", 13),
    ]
    rejected = run_cli(*retry, "--confirm-tasks", "1", expected=1)
    assert rejected["error"]["code"] == "TASK_CONFIRMATION_MISMATCH"
    child = run_cli(*retry, "--confirm-tasks", "2", expected=2)["retry"]["run_id"]
    record = run_cli("inspect", child)["record"]
    assert record["scheduler_metadata"]["retry_parent_run_id"] == parent
    assert [item["seed"] for item in record["run"]["tasks"]] == [10, 13]
    assert set(record["task_exit_codes"].values()) == {19}
    assert {item["config"]["content"] for item in record["run"]["tasks"]} == {
        "original: true\n"
    }
    output = tmp_path / "bundle.tar.gz"
    exported = run_cli(
        "export", child, "--destination", str(output), "--include-results"
    )
    assert exported["export"]["result_count"] == 2
    assert (
        hashlib.sha256(output.read_bytes()).hexdigest()
        == exported["export"]["archive_sha256"]
    )
    with tarfile.open(output, "r:gz") as archive:
        manifest = json.load(archive.extractfile("manifest.json"))
        assert manifest["run_ids"] == [child]
        for member, identity in manifest["files"].items():
            payload = archive.extractfile(member).read()
            assert len(payload) == identity["size_bytes"]
            assert hashlib.sha256(payload).hexdigest() == identity["sha256"]
        lineage = json.load(archive.extractfile(f"runs/{child}/retry-lineage.json"))
        assert lineage["parent_run_id"] == parent
        assert [item["parent_task_id"] for item in lineage["tasks"]] == [
            "task_000000",
            "task_000003",
        ]
        assert {
            json.load(archive.extractfile(member))["seed"]
            for member in manifest["files"]
            if "/results/" in member
        } == {10, 13}
    collision = run_cli("export", child, "--destination", str(output), expected=1)
    assert collision["error"]["code"] == "EXPORT_DESTINATION_EXISTS"

    campaign_id = CampaignId.new()
    JsonCampaignStore(data_dir).create(
        CampaignRecord(
            format_version=2,
            framework_version="0.1.7",
            id=campaign_id,
            name="export-two-runs",
            source=experiment,
            experiment_source=experiment,
            created_at=datetime.now(UTC),
            on_submit_failure=CampaignFailurePolicy.STOP,
            allow_duplicate_tasks=True,
            launches=(
                CampaignLaunchRecord(
                    "original",
                    RunId(parent),
                    "local",
                    4,
                    destination,
                    CampaignSubmissionState.SUBMITTED,
                ),
                CampaignLaunchRecord(
                    "followup",
                    RunId(child),
                    "alternate",
                    2,
                    Path(record["retrieval_destination"]),
                    CampaignSubmissionState.SUBMITTED,
                ),
            ),
        )
    )
    assert (
        run_cli(
            "retry",
            str(campaign_id),
            "--failed",
            "--launch",
            "original",
            "--targets-file",
            str(targets),
        )["retry"]["parent_run_id"]
        == parent
    )
    campaign_output = tmp_path / "campaign.tar.gz"
    result = run_cli("export", str(campaign_id), "--destination", str(campaign_output))
    assert result["export"]["run_count"] == 2
    assert result["export"]["result_count"] == 0
    with tarfile.open(campaign_output, "r:gz") as archive:
        campaign_manifest = json.load(archive.extractfile("manifest.json"))
        assert campaign_manifest["run_ids"] == [parent, child]
        assert [item["name"] for item in campaign_manifest["campaign"]["launches"]] == [
            "original",
            "followup",
        ]
        assert all("/results/" not in name for name in archive.getnames())
