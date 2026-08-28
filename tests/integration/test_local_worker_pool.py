from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).parents[2]


def _rundr(*arguments: str, timeout: float = 120) -> dict[str, object]:
    deployed = os.environ.get("RUNDRA_LOCAL_DEPLOYMENT_EXECUTABLE")
    command = (
        (deployed, *arguments, "--json")
        if deployed is not None
        else (sys.executable, "-m", "rundra", *arguments, "--json")
    )
    completed = subprocess.run(
        command,
        cwd=_ROOT,
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    document: object = json.loads(completed.stdout)
    assert isinstance(document, dict)
    return document


def test_local_run_executes_forty_tasks_with_forty_workers(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "task.py").write_text(
        """\
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--config", type=Path, required=True)
parser.add_argument("--seed", type=int, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps({"seed": args.seed}) + "\\n", encoding="utf-8")
""",
        encoding="utf-8",
    )
    experiment = tmp_path / "experiment.yaml"
    experiment.write_text(
        """\
version: 1
experiment: {name: local-forty-worker-smoke}
command:
  argv:
    - python3
    - task.py
    - --config
    - "{config}"
    - --seed
    - "{seed}"
    - --output
    - /workspace/output/result.json
resources:
  cpus_per_task: 1
  memory: 16MiB
  walltime: "00:01:00"
outputs:
  include: [result.json]
""",
        encoding="utf-8",
    )
    config = tmp_path / "config.yaml"
    config.write_text("value: local-worker-pool\n", encoding="utf-8")
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""\
version: 8
targets:
  local-forty:
    transport: {{type: local}}
    scheduler: {{type: local}}
    staging: {{type: local}}
    container: {{type: native}}
    workspace: {tmp_path / "workspace"}
    execution:
      hard_task_limit: 1000
      confirmation_threshold: 1000
      max_active_tasks: 40
      max_concurrent_jobs: 40
      max_array_size: 1000
      output_shard_tasks: 1000
      automatic_retrieval_threshold: 1000
      max_memory_per_worker: 1GiB
      worker_pool:
        activation_threshold: 2
        default_workers: 1
        max_workers: 40
        tasks_per_lease: 1
        default_task_slots_per_worker: 1
        max_task_slots_per_worker: 1
        infrastructure_retry_limit: 0
        requeue_limit: 0
""",
        encoding="utf-8",
    )
    common = (
        str(experiment),
        "--config",
        str(config),
        "--target",
        "local-forty",
        "--targets-file",
        str(targets),
        "--source-root",
        str(source),
        "--seeds",
        "0:39",
        "--workers",
        "40",
        "--task-slots-per-worker",
        "1",
    )

    planned = _rundr("plan", *common)
    scheduling = planned["plan"]["scheduling"]  # type: ignore[index]
    assert scheduling["worker_count"] == 40
    assert scheduling["concurrent_task_capacity"] == 40

    destination = tmp_path / "retrieved"
    completed = _rundr(
        "run",
        *common,
        "--destination",
        str(destination),
        "--data-dir",
        str(tmp_path / "records"),
    )

    run = completed["run"]
    assert isinstance(run, dict)
    assert run["state"] == "SUCCEEDED"
    assert run["tasks"] == 40
    results = tuple(destination.rglob("result.json"))
    assert len(results) == 40
    assert {
        json.loads(path.read_text(encoding="utf-8"))["seed"] for path in results
    } == set(range(40))
