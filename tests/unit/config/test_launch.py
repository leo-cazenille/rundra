from __future__ import annotations

from pathlib import Path

import pytest

from rundra.config.errors import ConfigError
from rundra.config.launch import (
    discover_project_launch,
    load_project_launch,
)


def test_project_launch_loads_profiles_and_resolves_declared_paths(
    tmp_path: Path,
) -> None:
    source = tmp_path / "project" / "rundra.yaml"
    source.parent.mkdir()
    source.write_text(
        """\
version: 1
default_profile: local
defaults:
  target: local
  destination: common-results
profiles:
  local:
    config: config.yaml
    seed: 17
    source_root: .
    destination: retrieved
""",
        encoding="utf-8",
    )

    launch = load_project_launch(source)

    assert launch.version == 1
    assert launch.source == source.resolve()
    assert launch.project_root == source.parent.resolve()
    assert launch.default_profile == "local"
    assert launch.defaults.target == "local"
    assert launch.defaults.destination == (source.parent / "common-results").resolve()
    profile = launch.profiles["local"]
    assert profile.config == (source.parent / "config.yaml").resolve()
    assert profile.seed == 17
    assert profile.source_root == source.parent.resolve()
    assert profile.destination == (source.parent / "retrieved").resolve()
    with pytest.raises(TypeError):
        launch.profiles["other"] = profile


def test_project_launch_discovery_is_adjacent_or_explicit(tmp_path: Path) -> None:
    root = tmp_path / "project"
    nested = root / "experiments" / "one"
    nested.mkdir(parents=True)
    experiment = nested / "experiment.yaml"
    experiment.touch()
    root_launch = root / "rundra.yaml"
    root_launch.write_text("version: 1\ndefaults: {target: local}\n", encoding="utf-8")

    assert discover_project_launch(experiment) is None
    explicit = discover_project_launch(experiment, project_file=root_launch)
    assert explicit is not None
    assert explicit.source == root_launch.resolve()

    adjacent = nested / "rundra.yaml"
    adjacent.write_text("version: 1\ndefaults: {target: local}\n", encoding="utf-8")
    discovered = discover_project_launch(experiment)
    assert discovered is not None
    assert discovered.source == adjacent.resolve()


@pytest.mark.parametrize(
    "content, code, path",
    [
        (
            "version: 2\ndefaults: {target: local}\n",
            "UNSUPPORTED_VERSION",
            ("version",),
        ),
        ("version: 1\nunknown: true\n", "UNKNOWN_FIELD", ("unknown",)),
        ("version: 1\ntoken: no\n", "FORBIDDEN_FIELD", ("token",)),
        ("version: 1\ndefaults: []\n", "INVALID_TYPE", ("defaults",)),
        (
            "version: 1\ndefaults: {password: no}\n",
            "FORBIDDEN_FIELD",
            ("defaults", "password"),
        ),
        (
            "version: 1\nprofiles: {local: {backend: native}}\n",
            "UNKNOWN_FIELD",
            ("profiles", "local", "backend"),
        ),
        (
            "version: 1\ndefault_profile: missing\nprofiles: {local: {target: local}}\n",
            "UNKNOWN_PROFILE",
            ("default_profile",),
        ),
        ("version: 1\n", "EMPTY_LAUNCH_CONFIG", ()),
    ],
)
def test_project_launch_rejects_invalid_or_sensitive_fields(
    tmp_path: Path,
    content: str,
    code: str,
    path: tuple[str | int, ...],
) -> None:
    source = tmp_path / "rundra.yaml"
    source.write_text(content, encoding="utf-8")

    with pytest.raises(ConfigError) as caught:
        load_project_launch(source)

    assert caught.value.code == code
    assert caught.value.path == path


def test_explicit_missing_project_launch_is_an_actionable_error(tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"

    with pytest.raises(ConfigError) as caught:
        discover_project_launch(tmp_path / "experiment.yaml", project_file=missing)

    assert caught.value.code == "CONFIG_NOT_FOUND"
    assert caught.value.source == missing.resolve()
