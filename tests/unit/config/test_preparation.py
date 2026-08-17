from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from rundra.config.errors import ConfigError
from rundra.config.launch import load_project_launch
from rundra.domain.preparation import build_cache_key, source_recipe_identity

_REVISION = "0123456789abcdef0123456789abcdef01234567"
_DIGEST = "ab" * 32


def _document(*, overrides: str = "") -> str:
    return f"""\
version: 2
defaults:
  config: config.yaml
  target: local
preparation:
  source:
    git:
      url: https://example.test/project.git
      revision: {_REVISION}
  image:
    name: application.sif
    uri: library://example/application:v1
    sha256: {_DIGEST}
  build:
    argv: [make, -C, simulation]
    outputs:
      - path: simulation/model
        executable: true
    cache_scope: target
    resources:
      cpus_per_task: 2
      memory: 2GiB
      walltime: "00:15:00"
{overrides}"""


def test_project_v2_loads_strict_preparation_recipe(tmp_path: Path) -> None:
    source = tmp_path / "rundra.yaml"
    source.write_text(_document(), encoding="utf-8")

    project = load_project_launch(source)

    assert project.version == 2
    assert project.preparation is not None
    preparation = project.preparation
    assert preparation.source.revision == _REVISION
    assert preparation.image.sha256 == _DIGEST
    assert preparation.build is not None
    assert preparation.build.argv == ("make", "-C", "simulation")
    assert preparation.build.resources.cpus_per_task == 2
    assert preparation.build.resources.memory_bytes == 2 * 1024**3
    assert preparation.build.resources.walltime == timedelta(minutes=15)


@pytest.mark.parametrize(
    ("replacement", "code", "path"),
    [
        (_REVISION, "INVALID_VALUE", ("preparation", "source", "git", "revision")),
        (_DIGEST, "INVALID_VALUE", ("preparation", "image", "sha256")),
        ("simulation/model", "INVALID_VALUE", ("preparation", "image", "name")),
        ("target", "INVALID_VALUE", ("preparation", "build", "cache_scope")),
    ],
)
def test_project_v2_rejects_unpinned_or_unsafe_values(
    tmp_path: Path,
    replacement: str,
    code: str,
    path: tuple[str | int, ...],
) -> None:
    content = _document()
    if replacement == _REVISION:
        content = content.replace(_REVISION, "main")
    elif replacement == _DIGEST:
        content = content.replace(_DIGEST, "unverified")
    elif replacement == "simulation/model":
        content = content.replace("name: application.sif", "name: ../application.sif")
    else:
        content = content.replace("cache_scope: target", "cache_scope: host")
    source = tmp_path / "rundra.yaml"
    source.write_text(content, encoding="utf-8")

    with pytest.raises(ConfigError) as caught:
        load_project_launch(source)

    assert caught.value.code == code
    assert caught.value.path == path


def test_project_v2_rejects_embedded_git_credentials(tmp_path: Path) -> None:
    source = tmp_path / "rundra.yaml"
    source.write_text(
        _document().replace(
            "https://example.test/project.git",
            "https://token@example.test/project.git",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_project_launch(source)

    assert caught.value.code == "FORBIDDEN_VALUE"
    assert caught.value.path == ("preparation", "source", "git", "url")


def test_preparation_identities_and_build_keys_are_deterministic(
    tmp_path: Path,
) -> None:
    source = tmp_path / "rundra.yaml"
    source.write_text(_document(), encoding="utf-8")
    preparation = load_project_launch(source).preparation
    assert preparation is not None and preparation.build is not None

    identity = source_recipe_identity(preparation.source)
    first = build_cache_key(
        source_digest="11" * 32,
        image_digest=preparation.image.sha256,
        build=preparation.build,
        builder_scope="local",
        platform_fingerprint="linux-x86_64",
    )
    second = build_cache_key(
        source_digest="11" * 32,
        image_digest=preparation.image.sha256,
        build=preparation.build,
        builder_scope="local",
        platform_fingerprint="linux-x86_64",
    )

    assert len(identity) == 64
    assert first == second
    assert len(first) == 64
