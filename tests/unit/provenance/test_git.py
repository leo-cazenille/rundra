from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from rundra.provenance import GitProvenance, GitProvenanceCapture, ProvenanceProvider


def _git(repo: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )


def _repository(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Rundra Tests")
    _git(repo, "config", "user.email", "tests@example.invalid")
    (repo / "tracked.txt").write_text("original\n", encoding="utf-8")
    _git(repo, "add", "tracked.txt")
    _git(repo, "commit", "-m", "initial")
    return repo


def test_git_provenance_captures_clean_commit_branch_and_dirty_state(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    capture = GitProvenanceCapture()

    provenance = capture.capture(repo)

    assert isinstance(capture, ProvenanceProvider)
    assert provenance.commit == _git(repo, "rev-parse", "HEAD").stdout.strip()
    assert provenance.branch == "main"
    assert provenance.dirty is False
    assert provenance.diff is None


def test_git_provenance_captures_a_bounded_tracked_patch_and_untracked_dirt(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    (repo / "tracked.txt").write_text("changed\n", encoding="utf-8")
    (repo / "untracked.txt").write_text("untracked\n", encoding="utf-8")

    provenance = GitProvenanceCapture(max_diff_bytes=4096).capture(repo)

    assert provenance.dirty is True
    assert provenance.diff is not None
    assert "-original" in provenance.diff
    assert "+changed" in provenance.diff
    assert "untracked.txt" not in provenance.diff


def test_git_provenance_omits_an_oversized_patch_without_hiding_dirty_state(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    (repo / "tracked.txt").write_text("changed by a long patch\n", encoding="utf-8")

    provenance = GitProvenanceCapture(max_diff_bytes=8).capture(repo)

    assert provenance.dirty is True
    assert provenance.diff is None


def test_git_provenance_omits_a_patch_with_common_credential_markers(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    (repo / "tracked.txt").write_text("api_token=must-not-persist\n", encoding="utf-8")

    provenance = GitProvenanceCapture().capture(repo)

    assert provenance.dirty is True
    assert provenance.diff is None


def test_git_provenance_handles_detached_non_repository_and_missing_git(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _repository(tmp_path)
    _git(repo, "checkout", "--detach", "HEAD")
    detached = GitProvenanceCapture().capture(repo)

    assert detached.commit is not None
    assert detached.branch is None
    assert detached.dirty is False
    assert GitProvenanceCapture().capture(tmp_path / "not-a-repo") == GitProvenance()

    import rundra.provenance.git as git_provenance

    monkeypatch.setattr(git_provenance.shutil, "which", lambda executable: None)
    assert GitProvenanceCapture().capture(repo) == GitProvenance()

    def fail_which(executable: str) -> str | None:
        raise OSError("PATH unavailable")

    monkeypatch.setattr(git_provenance.shutil, "which", fail_which)
    assert GitProvenanceCapture().capture(repo) == GitProvenance()


@pytest.mark.parametrize("limit", [0, -1, True, "10"])
def test_git_provenance_rejects_invalid_patch_limits(limit: object) -> None:
    with pytest.raises((TypeError, ValueError), match="max_diff_bytes"):
        GitProvenanceCapture(max_diff_bytes=limit)  # type: ignore[arg-type]
