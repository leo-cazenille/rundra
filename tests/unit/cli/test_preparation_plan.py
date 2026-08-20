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
