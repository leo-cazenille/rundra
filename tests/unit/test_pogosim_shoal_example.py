import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from rundra.cli.operations import plan_operation
from rundra.config.launch import load_project_launch

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_ROOT = REPOSITORY_ROOT / "examples" / "pogosim-shoal"


def _load_yaml(name: str) -> dict[str, Any]:
    value = yaml.safe_load((EXAMPLE_ROOT / name).read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_pogosim_experiment_passes_the_public_validator() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "rundra",
            "validate",
            str(EXAMPLE_ROOT / "experiment.yaml"),
            "--json",
        ],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    output = json.loads(completed.stdout)
    assert output["ok"] is True
    assert output["operation"] == "validate"


def test_pogosim_three_seed_plan_is_one_slurm_array(tmp_path: Path) -> None:
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        """\
version: 1
targets:
  shoal:
    transport:
      type: ssh
      host: fishvision
    scheduler:
      type: slurm
    staging:
      type: rsync
    container:
      type: apptainer
    workspace: /shoalhome/tester/rundra
""",
        encoding="utf-8",
    )

    result = plan_operation(
        EXAMPLE_ROOT / "experiment.yaml",
        EXAMPLE_ROOT / "conf/default.yaml",
        targets,
        "shoal",
        seeds="0:2",
    )

    assert result.error is None
    assert result.value is not None
    plan = result.value.plan
    assert plan.strategy == "slurm_array"
    assert len(plan.groups) == 1
    assert [unit.seed for unit in plan.units] == [0, 1, 2]
    assert [
        (str(item.task_id), item.seed, item.array_index) for item in plan.array_mapping
    ] == [
        ("task_000000", 0, 0),
        ("task_000001", 1, 1),
        ("task_000002", 2, 2),
    ]


def test_pogosim_experiment_is_a_bounded_headless_cpu_workload() -> None:
    experiment = _load_yaml("experiment.yaml")

    assert experiment["version"] == 1
    assert experiment["experiment"]["name"] == "pogosim-run-and-tumble"

    argv = experiment["command"]["argv"]
    assert argv[0] == "examples/run_and_tumble/run_and_tumble"
    assert "pogobatch" not in argv
    assert argv.count("{config}") == 1
    assert argv.count("{seed}") == 1
    assert {"-g", "-q", "-nr", "--seed"}.issubset(argv)

    container = experiment["container"]
    assert container["gpu"] is False
    assert str(container["image"]).endswith(".sif")

    resources = experiment["resources"]
    assert resources == {
        "nodes": 1,
        "tasks": 1,
        "cpus_per_task": 1,
        "gpus_per_task": 0,
        "memory": "1GiB",
        "walltime": "00:15:00",
    }
    assert experiment["outputs"]["include"] == ["data.feather", "console.txt"]


def test_pogosim_config_has_no_seed_and_writes_raw_outputs_only() -> None:
    config = _load_yaml("conf/default.yaml")

    assert "seed" not in config
    assert config["GUI"] is False
    assert config["simulation_time"] == 10.0
    assert config["objects"]["robots"]["nb"] == 50
    assert config["enable_data_logging"] is True
    assert config["save_video_period"] == -1.0

    output_root = "/workspace/output/"
    assert config["data_filename"] == f"{output_root}data.feather"
    assert config["console_filename"] == f"{output_root}console.txt"
    assert str(config["frames_name"]).startswith(output_root)


def test_pogosim_profile_keeps_site_policy_out_of_the_example() -> None:
    project_config = _load_yaml("rundra.yaml")
    loaded = load_project_launch(EXAMPLE_ROOT / "rundra.yaml")
    profile = project_config["profiles"]["shoal"]

    assert project_config["default_profile"] == "shoal"
    assert project_config["version"] == 2
    assert loaded.preparation is not None
    assert loaded.default_profile == "shoal"
    assert profile["target"] == "shoal"
    assert project_config["defaults"]["config"] == "conf/default.yaml"
    assert "destination" not in project_config["defaults"]
    assert "targets_file" not in profile
    assert "account" not in profile
    assert "partition" not in profile
    assert "qos" not in profile


def test_pogosim_guide_pins_source_and_uses_the_stable_prebuilt_image() -> None:
    guide = (EXAMPLE_ROOT / "README.md").read_text(encoding="utf-8")

    assert "fe012bb58ef17eae2155b9904bc3eedb650a86bc" in guide
    assert "library://leo.cazenille/pogosim/pogosim-full:v0.10.10" in guide
    assert "apptainer build" not in guide
    assert "4005aa26696ca542f1bb462d46085b13ab56f2b51eb4c27f3483c6761995dfd8" in guide


def test_pogosim_guide_uses_adjacent_project_discovery_for_one_command_run() -> None:
    guide = (EXAMPLE_ROOT / "README.md").read_text(encoding="utf-8")

    assert (
        "uv run rundr run examples/pogosim-shoal/experiment.yaml --seeds 0:2" in guide
    )
    assert "--project-file" not in guide


def test_pogosim_recipe_pins_build_inputs_and_declared_output() -> None:
    project = _load_yaml("rundra.yaml")
    preparation = project["preparation"]

    assert preparation["source"]["git"]["revision"] == (
        "fe012bb58ef17eae2155b9904bc3eedb650a86bc"
    )
    assert preparation["image"]["sha256"] == (
        "4005aa26696ca542f1bb462d46085b13ab56f2b51eb4c27f3483c6761995dfd8"
    )
    assert preparation["build"]["argv"] == [
        "make",
        "-C",
        "examples/run_and_tumble",
        "clean",
        "sim",
    ]
    assert preparation["build"]["outputs"] == [
        {
            "path": "examples/run_and_tumble/run_and_tumble",
            "executable": True,
        }
    ]


def test_pogosim_msd_sweep_is_two_parameter_sets_across_twenty_seeds() -> None:
    config = _load_yaml("conf/msd-120s.yaml")
    hierarchy = config["parameters"]["batch_hierarchical_options"]

    assert config["_rundr"] == {"version": 1, "seeds": "0:19"}
    assert config["simulation_time"] == 120.0
    assert hierarchy["name"] == "regime"
    assert set(hierarchy) == {"name", "default", "ballistic", "long_tumble"}
