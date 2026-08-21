from pathlib import Path

import pytest

from rundra.config.errors import ConfigError
from rundra.config.launch import LaunchValues, load_project_launch, resolve_launch


def _project(version: int, fetch_mode: str) -> str:
    return f"""\
version: {version}
defaults:
  fetch_mode: {fetch_mode}
preparation:
  source:
    git:
      url: https://example.test/project.git
      revision: "{"01" * 20}"
  image:
    name: image.sif
    prebuilt:
      uri: library://example/image:v1
      sha256: {"ab" * 32}
"""


def test_project_v5_accepts_fetch_mode(tmp_path: Path) -> None:
    source = tmp_path / "rundra.yaml"
    source.write_text(_project(5, "copy"), encoding="utf-8")

    project = load_project_launch(source)

    assert project.defaults.fetch_mode == "copy"


def test_legacy_project_rejects_fetch_mode(tmp_path: Path) -> None:
    source = tmp_path / "rundra.yaml"
    source.write_text(_project(4, "copy"), encoding="utf-8")

    with pytest.raises(ConfigError) as error:
        load_project_launch(source)

    assert error.value.code == "UNKNOWN_FIELD"


def test_project_v5_rejects_invalid_fetch_mode(tmp_path: Path) -> None:
    source = tmp_path / "rundra.yaml"
    source.write_text(_project(5, "stream"), encoding="utf-8")

    with pytest.raises(ConfigError) as error:
        load_project_launch(source)

    assert error.value.code == "INVALID_VALUE"


def test_launch_overlay_preserves_cli_fetch_mode() -> None:
    resolved = resolve_launch(
        builtins=LaunchValues(fetch_mode="auto"),
        cli=LaunchValues(fetch_mode="copy"),
    )

    assert resolved.values.fetch_mode == "copy"
    assert resolved.sources["fetch_mode"] == "cli"
