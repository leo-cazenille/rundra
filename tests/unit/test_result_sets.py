import json
from collections.abc import Callable
from pathlib import Path

import pytest

from rundra.artifacts import ResultSetError, open_result_set


def _run_tree(root: Path) -> Path:
    for task_id in ("task_000000", "task_000001"):
        result = root / "output" / task_id / "results"
        result.mkdir(parents=True)
        (result / "value.json").write_text("{}\n", encoding="utf-8")
    (root / "metadata").mkdir()
    (root / "logs").mkdir()
    return root


def _reference(destination: Path, run_root: Path) -> Path:
    destination.mkdir()
    manifest = destination / "rundra-reference.json"
    manifest.write_text(
        json.dumps(
            {
                "format_version": 1,
                "kind": "rundra-shared-reference",
                "immutable": True,
                "run_root": str(run_root),
                "output_root": str(run_root / "output"),
                "metadata_root": str(run_root / "metadata"),
                "log_root": str(run_root / "logs"),
                "patterns": ["results/**"],
            }
        ),
        encoding="utf-8",
    )
    return manifest


def test_open_materialized_result_set_filters_by_task(tmp_path: Path) -> None:
    result_set = open_result_set(_run_tree(tmp_path / "retrieved"))

    files = result_set.iter_files("task_000001")

    assert result_set.referenced is False
    assert [item.relative_path.as_posix() for item in files] == [
        "task_000001/results/value.json"
    ]
    assert files[0].task_id is not None
    assert files[0].task_id.value == "task_000001"


def test_open_reference_result_set_reads_without_copying(tmp_path: Path) -> None:
    run_root = _run_tree(tmp_path / "run")
    destination = tmp_path / "retrieved"
    manifest = _reference(destination, run_root)

    result_set = open_result_set(manifest)

    assert result_set.referenced is True
    assert len(result_set.iter_files()) == 2
    assert list(destination.iterdir()) == [manifest]


@pytest.mark.parametrize(
    "change",
    [
        lambda document, _: document.__setitem__("kind", "other"),
        lambda document, _: document.__setitem__("immutable", False),
        lambda document, _: document.__setitem__("output_root", "relative"),
        lambda document, source: document.__setitem__(
            "output_root", str(source.parent / "outside")
        ),
        lambda document, _: document.__setitem__("patterns", ["../secret"]),
    ],
)
def test_reference_manifest_rejects_invalid_contract(
    tmp_path: Path, change: Callable[[dict[str, object], Path], None]
) -> None:
    run_root = _run_tree(tmp_path / "run")
    manifest = _reference(tmp_path / "retrieved", run_root)
    document: dict[str, object] = json.loads(manifest.read_text(encoding="utf-8"))
    change(document, run_root)
    manifest.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ResultSetError):
        open_result_set(manifest)


def test_result_set_ignores_symlinked_output_files(tmp_path: Path) -> None:
    root = _run_tree(tmp_path / "retrieved")
    (root / "output" / "task_000000" / "results" / "link").symlink_to(
        tmp_path / "outside"
    )

    files = open_result_set(root).iter_files()

    assert len(files) == 2
