from __future__ import annotations

from pathlib import Path

import pytest

from rundra.config.errors import ConfigError
from rundra.config.launch import (
    LaunchResolutionError,
    LaunchValues,
    discover_project_launch,
    discover_user_launch,
    load_project_launch,
    load_user_launch,
    resolve_launch,
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
    seed: -17
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
    assert profile.seed == -17
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
            "MISSING_FIELD",
            ("preparation",),
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


def test_user_launch_defaults_are_strict_and_resolve_paths(tmp_path: Path) -> None:
    source = tmp_path / "user" / "config.yaml"
    source.parent.mkdir()
    source.write_text(
        """\
version: 1
defaults:
  target: local
  targets_file: targets.yaml
  data_dir: records
  destination: results
  seed: 9
""",
        encoding="utf-8",
    )

    user = load_user_launch(source)

    assert user.source == source.resolve()
    assert user.defaults.target == "local"
    assert user.defaults.seed == 9
    assert user.defaults.targets_file == (source.parent / "targets.yaml").resolve()
    assert user.defaults.data_dir == (source.parent / "records").resolve()
    assert user.defaults.destination == (source.parent / "results").resolve()
    assert discover_user_launch(tmp_path / "absent.yaml") is None


@pytest.mark.parametrize(
    "content, code, path",
    [
        ("version: 1\n", "MISSING_FIELD", ("defaults",)),
        ("version: 1\ndefaults: {}\n", "EMPTY_LAUNCH_CONFIG", ("defaults",)),
        (
            "version: 1\ndefaults: {profiles: local}\n",
            "UNKNOWN_FIELD",
            ("defaults", "profiles"),
        ),
        (
            "version: 1\ndefaults: {api_key: nope}\n",
            "FORBIDDEN_FIELD",
            ("defaults", "api_key"),
        ),
    ],
)
def test_user_launch_rejects_invalid_documents(
    tmp_path: Path,
    content: str,
    code: str,
    path: tuple[str | int, ...],
) -> None:
    source = tmp_path / "config.yaml"
    source.write_text(content, encoding="utf-8")

    with pytest.raises(ConfigError) as caught:
        load_user_launch(source)

    assert caught.value.code == code
    assert caught.value.path == path


def test_launch_resolution_uses_documented_field_precedence(tmp_path: Path) -> None:
    project_source = tmp_path / "project" / "rundra.yaml"
    project_source.parent.mkdir()
    project_source.write_text(
        """\
version: 1
default_profile: local
defaults:
  target: project-target
  destination: project-results
profiles:
  local:
    config: local.yaml
    seed: 17
""",
        encoding="utf-8",
    )
    user_source = tmp_path / "user.yaml"
    user_source.write_text(
        """\
version: 1
defaults:
  target: user-target
  targets_file: targets.yaml
  data_dir: records
""",
        encoding="utf-8",
    )

    resolved = resolve_launch(
        cli=LaunchValues(target="cli-target"),
        project=load_project_launch(project_source),
        user=load_user_launch(user_source),
        builtins=LaunchValues(source_root=tmp_path / "builtin-source"),
    )

    assert resolved.profile == "local"
    assert resolved.values.target == "cli-target"
    assert resolved.values.seed == 17
    assert resolved.values.config == (project_source.parent / "local.yaml").resolve()
    assert (
        resolved.values.destination
        == (project_source.parent / "project-results").resolve()
    )
    assert resolved.values.targets_file == (tmp_path / "targets.yaml").resolve()
    assert resolved.values.data_dir == (tmp_path / "records").resolve()
    assert resolved.values.source_root == tmp_path / "builtin-source"
    assert resolved.sources == {
        "source_root": "built_in",
        "target": "cli",
        "targets_file": "user",
        "data_dir": "user",
        "destination": "project",
        "config": "project_profile:local",
        "seed": "project_profile:local",
    }
    with pytest.raises(TypeError):
        resolved.sources["seed"] = "other"


def test_launch_resolution_rejects_an_unknown_requested_profile(
    tmp_path: Path,
) -> None:
    source = tmp_path / "rundra.yaml"
    source.write_text(
        "version: 1\nprofiles: {local: {target: local}}\n", encoding="utf-8"
    )

    with pytest.raises(LaunchResolutionError) as caught:
        resolve_launch(project=load_project_launch(source), profile="missing")

    assert caught.value.code == "PROFILE_NOT_FOUND"
