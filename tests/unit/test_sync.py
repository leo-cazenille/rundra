from pathlib import Path

from rundra.sync import DEFAULT_SYNC_EXCLUDES, preview_source_snapshot


def test_preview_source_snapshot_applies_defaults_and_custom_exclusions(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "main.py").write_bytes(b"1234")
    (source / "data").mkdir()
    (source / "data" / "input.bin").write_bytes(b"123456")
    (source / "results").mkdir()
    (source / "results" / "large.bin").write_bytes(b"x" * 100)
    (source / ".git").mkdir()
    (source / ".git" / "index").write_bytes(b"x" * 200)

    preview = preview_source_snapshot(source, ("results/",))

    assert preview.file_count == 2
    assert preview.size_bytes == 10
    assert preview.exact is True
    assert preview.excluded_patterns == (*DEFAULT_SYNC_EXCLUDES, "results")
    assert [(str(item.path), item.size_bytes) for item in preview.largest_entries] == [
        ("data", 6),
        ("main.py", 4),
    ]


def test_preview_source_snapshot_skips_nested_local_workspace(tmp_path: Path) -> None:
    source = tmp_path / "source"
    workspace = source / ".custom-workspace"
    workspace.mkdir(parents=True)
    (workspace / "old-copy.bin").write_bytes(b"x" * 100)
    (source / "input.bin").write_bytes(b"123")

    preview = preview_source_snapshot(source, (), workspace_root=workspace)

    assert preview.file_count == 1
    assert preview.size_bytes == 3


def test_preview_source_snapshot_marks_symlink_estimate_non_exact(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "target.bin").write_bytes(b"123")
    (source / "link.bin").symlink_to("target.bin")

    preview = preview_source_snapshot(source, ())

    assert preview.file_count == 2
    assert preview.symlink_entries == 1
    assert preview.exact is False


def test_default_sync_excludes_generated_result_and_container_content(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "main.py").write_bytes(b"123")
    for directory in ("results", "outputs", "tmp"):
        generated = source / directory
        generated.mkdir()
        (generated / "large.bin").write_bytes(b"x" * 100)
    (source / "container.sif").write_bytes(b"x" * 100)
    (source / "container.simg").write_bytes(b"x" * 100)

    preview = preview_source_snapshot(source, ())

    assert preview.file_count == 1
    assert preview.size_bytes == 3
    assert tuple(str(item.path) for item in preview.largest_entries) == ("main.py",)
