from pathlib import Path

import pytest

from rundra.mcp_server import ServerSettings, build_server


def test_mcp_settings_confine_tool_paths(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    settings = ServerSettings(
        root,
        tmp_path / "records",
        tmp_path / "targets.yaml",
        (root,),
    )

    assert settings.path("config.yaml") == root / "config.yaml"
    with pytest.raises(ValueError, match="outside"):
        settings.path("../secret")


def test_mcp_server_exposes_guarded_lifecycle_tools(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    settings = ServerSettings(
        root, tmp_path / "records", tmp_path / "targets.yaml", (root,)
    )

    server = build_server(settings)

    assert server.name == "Rundra"
