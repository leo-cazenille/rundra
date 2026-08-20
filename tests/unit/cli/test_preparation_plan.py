from __future__ import annotations

import hashlib
import json
from pathlib import Path

from rundra.cli.main import main

_ROOT = Path(__file__).parents[3]


def test_v2_plan_describes_preparation_without_performing_it(
    tmp_path: Path, capsys: object
) -> None:
    experiment = tmp_path / "experiment.yaml"
    experiment.write_text(
        """\
version: 1
experiment: {name: prepared-plan}
command:
  argv: [bin/model, "{config}", "{seed}"]
container: {image: application.sif}
resources: {}
""",
        encoding="utf-8",
    )
    (tmp_path / "config.yaml").write_text("value: 1\n", encoding="utf-8")
    digest = hashlib.sha256(b"not-created").hexdigest()
    (tmp_path / "rundra.yaml").write_text(
        f"""\
version: 2
defaults:
  config: config.yaml
  target: local
preparation:
  source:
    git:
      url: https://invalid.example.test/project.git
      revision: 0123456789abcdef0123456789abcdef01234567
  image:
    name: application.sif
    uri: library://invalid/image:v1
    sha256: {digest}
  build:
    argv: [make, model]
    outputs: [{{path: bin/model, executable: true}}]
    resources:
      cpus_per_task: 1
      memory: 1GiB
      walltime: "00:05:00"
""",
        encoding="utf-8",
    )
    targets = tmp_path / "targets.yaml"
    workspace = tmp_path / "workspace"
    targets.write_text(
        f"""\
version: 1
targets:
  local:
    transport: {{type: local}}
    scheduler: {{type: local}}
    staging: {{type: local}}
    container: {{type: apptainer}}
    workspace: {workspace}
""",
        encoding="utf-8",
    )

    exit_code = main(
        (
            "plan",
            str(experiment),
            "--seed",
            "7",
            "--targets-file",
            str(targets),
            "--offline",
            "--rebuild",
            "--json",
        )
    )
    captured = capsys.readouterr()  # type: ignore[attr-defined]
    document = json.loads(captured.out)

    assert exit_code == 0
    assert document["format_version"] == 2
    preparation = document["plan"]["preparation"]
    assert preparation["source"]["mode"] == "git"
    assert preparation["image"]["sha256"] == digest
    assert preparation["strategy"]["offline"] is True
    assert preparation["strategy"]["rebuild"] is True
    assert preparation["strategy"]["cache_hits_known"] is False
    assert preparation["strategy"]["requested_location"] == "auto"
    assert preparation["strategy"]["selected_location"] == "local"
    assert "fetch_git_commit" not in preparation["strategy"]["possible_actions"]
    assert preparation["safety"] == {
        "builds": False,
        "contacts_target": False,
        "fetches_git": False,
        "pulls_image": False,
    }
    assert not workspace.exists()


def test_v3_plan_resolves_project_working_tree_source_root(capsys: object) -> None:
    example = _ROOT / "examples/python-multiprocessing"

    exit_code = main(
        (
            "plan",
            str(example / "prepared/experiment.yaml"),
            "--profile",
            "local",
            "--targets-file",
            str(example / "targets-local.yaml"),
            "--seed",
            "7",
            "--json",
        )
    )
    captured = capsys.readouterr()  # type: ignore[attr-defined]
    document = json.loads(captured.out)

    assert exit_code == 0
    preparation = document["plan"]["preparation"]
    assert preparation["source"] == {
        "git": None,
        "mode": "working_tree",
        "root": str(example),
    }
    assert preparation["image"]["kind"] == "definition"
    assert preparation["strategy"]["selected_location"] == "local"


def test_definition_plan_reports_target_only_auto_selection(
    tmp_path: Path, capsys: object
) -> None:
    example = _ROOT / "examples/python-multiprocessing"
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        """\
version: 8
targets:
  cluster:
    transport: {type: ssh, host: fake-cluster}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /remote/rundra
    preparation:
      definition_build:
        allowed_locations: [target]
        mode: unprivileged
        max_resources:
          cpus_per_task: 4
          memory: 4GiB
          walltime: "00:30:00"
    execution:
      hard_task_limit: 1000
      confirmation_threshold: 100
      max_active_tasks: 40
      max_concurrent_jobs: 8
      max_array_size: 100
      output_shard_tasks: 100
      automatic_retrieval_threshold: 100
      max_memory_per_worker: 4GiB
      worker_pool:
        activation_threshold: 100
        max_workers: 2
        default_workers: 1
        tasks_per_lease: 10
        default_task_slots_per_worker: 1
        max_task_slots_per_worker: 4
        infrastructure_retry_limit: 1
        requeue_limit: 2
""",
        encoding="utf-8",
    )

    exit_code = main(
        (
            "plan",
            str(example / "prepared/experiment.yaml"),
            "--target",
            "cluster",
            "--targets-file",
            str(targets),
            "--seed",
            "7",
            "--workers",
            "1",
            "--task-slots-per-worker",
            "1",
            "--json",
        )
    )
    captured = capsys.readouterr()  # type: ignore[attr-defined]
    document = json.loads(captured.out)

    assert exit_code == 0, document
    strategy = document["plan"]["preparation"]["strategy"]
    assert strategy["requested_location"] == "auto"
    assert strategy["selected_location"] == "target"
