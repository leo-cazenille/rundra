from __future__ import annotations

import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePath

from rundra.provenance.base import GitProvenance

_SENSITIVE_PATCH_MARKERS = (
    b"api_key",
    b"api-token",
    b"api_token",
    b"authorization:",
    b"password",
    b"private_key",
    b"secret",
    b"-----begin private key-----",
)


@dataclass(frozen=True, slots=True)
class _CommandOutput:
    returncode: int
    content: bytes | None
    size: int


class GitProvenanceCapture:
    """Capture bounded Git provenance using argument-array subprocesses."""

    def __init__(
        self,
        *,
        executable: str = "git",
        max_diff_bytes: int = 1024 * 1024,
        timeout_seconds: float = 10.0,
    ) -> None:
        if type(executable) is not str or not executable.strip():
            raise ValueError("Git executable must be a nonblank string")
        if "\x00" in executable:
            raise ValueError("Git executable must not contain NUL")
        if type(max_diff_bytes) is not int:
            raise TypeError("max_diff_bytes must be an integer")
        if max_diff_bytes <= 0:
            raise ValueError("max_diff_bytes must be positive")
        if type(timeout_seconds) not in (int, float):
            raise TypeError("timeout_seconds must be numeric")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._executable = executable
        self._max_diff_bytes = max_diff_bytes
        self._timeout_seconds = float(timeout_seconds)

    def capture(self, source_root: PurePath) -> GitProvenance:
        """Return available values, or an empty snapshot when Git is unavailable."""
        if not isinstance(source_root, PurePath):
            raise TypeError("Git provenance source_root must be a PurePath")
        try:
            executable = shutil.which(self._executable)
        except OSError:
            return GitProvenance()
        if executable is None:
            return GitProvenance()
        source = Path(str(source_root)).expanduser()
        repository = self._run(
            executable, source, ("rev-parse", "--is-inside-work-tree"), 16
        )
        if repository.returncode != 0 or repository.content != b"true\n":
            return GitProvenance()

        commit = self._text_value(
            self._run(executable, source, ("rev-parse", "HEAD"), 256)
        )
        branch = self._text_value(
            self._run(
                executable,
                source,
                ("symbolic-ref", "--quiet", "--short", "HEAD"),
                4096,
            )
        )
        status = self._run(
            executable,
            source,
            ("status", "--porcelain=v1", "--untracked-files=normal"),
            1,
        )
        dirty = status.size > 0 if status.returncode == 0 else None
        diff = None
        if dirty is True and commit is not None:
            patch = self._run(
                executable,
                source,
                ("diff", "--binary", "--no-ext-diff", "--no-color", "HEAD", "--"),
                self._max_diff_bytes,
            )
            if (
                patch.returncode == 0
                and patch.content
                and not _looks_sensitive(patch.content)
            ):
                try:
                    diff = patch.content.decode("utf-8")
                except UnicodeDecodeError:
                    diff = None
        return GitProvenance(commit=commit, branch=branch, dirty=dirty, diff=diff)

    def _run(
        self,
        executable: str,
        source: Path,
        arguments: tuple[str, ...],
        limit: int,
    ) -> _CommandOutput:
        try:
            with tempfile.TemporaryFile() as output:
                completed = subprocess.run(
                    (executable, "-C", str(source), *arguments),
                    stdout=output,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    shell=False,
                    timeout=self._timeout_seconds,
                )
                size = output.tell()
                content = None
                if size <= limit:
                    output.seek(0)
                    content = output.read()
                return _CommandOutput(completed.returncode, content, size)
        except (OSError, subprocess.SubprocessError):
            return _CommandOutput(1, None, 0)

    @staticmethod
    def _text_value(output: _CommandOutput) -> str | None:
        if output.returncode != 0 or output.content is None:
            return None
        try:
            value = output.content.decode("utf-8").strip()
        except UnicodeDecodeError:
            return None
        return value or None


def _looks_sensitive(patch: bytes) -> bool:
    lowered = patch.lower()
    return any(marker in lowered for marker in _SENSITIVE_PATCH_MARKERS)
