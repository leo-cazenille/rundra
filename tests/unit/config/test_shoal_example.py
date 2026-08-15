from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).parents[3]


def test_checked_shoal_target_example_matches_supported_reference_path() -> None:
    from rundra.config.targets import load_targets

    targets = load_targets(_REPOSITORY_ROOT / "examples/shoal/targets.yaml")

    target = targets["shoal"]
    assert target.transport.kind == "ssh"
    assert target.transport.options == {"host": "fishvision"}
    assert target.scheduler.kind == "slurm"
    assert target.staging.kind == "rsync"
    assert target.container.kind == "apptainer"
    assert str(target.workspace) == "/shoalhome/YOUR_USERNAME/.rundra"
