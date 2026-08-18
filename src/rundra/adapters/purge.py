from __future__ import annotations

import os
import shutil
import stat
from pathlib import Path, PurePath, PurePosixPath

from rundra.domain.models import Command
from rundra.domain.purge import PurgeOutcome, PurgeRequest, PurgeResult
from rundra.ports import Transport


class PurgeError(RuntimeError):
    """An exact Run path could not be purged safely."""


class LocalPurger:
    def purge(self, request: PurgeRequest, *, dry_run: bool = False) -> PurgeResult:
        run_root, target, tombstone = _local_paths(request)
        _reject_symlink(target, "purge target")
        _reject_symlink(tombstone, "purge tombstone")
        if target.exists() and tombstone.exists():
            raise PurgeError("Purge target and tombstone both exist")
        if dry_run:
            outcome = (
                PurgeOutcome.PLANNED
                if target.exists()
                else PurgeOutcome.RESUMED
                if tombstone.exists()
                else PurgeOutcome.ALREADY_ABSENT
            )
            return PurgeResult(target, tombstone, outcome, "local")
        outcome = PurgeOutcome.ALREADY_ABSENT
        if target.exists():
            target.rename(tombstone)
            outcome = PurgeOutcome.PURGED
        elif tombstone.exists():
            outcome = PurgeOutcome.RESUMED
        if tombstone.exists():
            _make_directories_writable(tombstone)
            shutil.rmtree(tombstone)
        if request.scope.value == "workspace" and run_root.exists():
            raise PurgeError("Run workspace remained after workspace purge")
        return PurgeResult(target, tombstone, outcome, "local")


class SSHPurger:
    def __init__(self, transport: Transport) -> None:
        if not isinstance(transport, Transport):
            raise TypeError("SSHPurger transport must implement Transport")
        self._transport = transport

    def purge(self, request: PurgeRequest, *, dry_run: bool = False) -> PurgeResult:
        run_root, target, tombstone = _remote_paths(request)
        result = self._transport.run(
            Command(
                (
                    "/bin/sh",
                    "-c",
                    _REMOTE_PURGE_SCRIPT,
                    "rundra-purge",
                    str(request.target_workspace),
                    str(request.run_id),
                    request.scope.value,
                    "dry-run" if dry_run else "purge",
                )
            )
        )
        if result.exit_code != 0:
            raise PurgeError(
                f"Remote purge failed with safe exit code {result.exit_code}"
            )
        try:
            outcome = PurgeOutcome(result.stdout.strip())
        except ValueError as error:
            raise PurgeError("Remote purge returned an invalid outcome") from error
        return PurgeResult(target, tombstone, outcome, "ssh")


def _local_paths(request: PurgeRequest) -> tuple[Path, Path, Path]:
    workspace_root = Path(str(request.target_workspace)).expanduser().resolve()
    if workspace_root == Path("/"):
        raise PurgeError("Target workspace must be non-root")
    expected = workspace_root / "runs" / str(request.run_id)
    run_root = Path(str(request.run_root)).expanduser().resolve()
    if run_root != expected:
        raise PurgeError("Run workspace does not match the persisted target")
    if request.scope.value == "outputs":
        return run_root, run_root / "output", run_root / ".rundra-purge-output"
    return run_root, run_root, run_root.parent / f".{request.run_id}.rundra-purge"


def _remote_paths(request: PurgeRequest) -> tuple[PurePath, PurePath, PurePath]:
    workspace_root = PurePosixPath(request.target_workspace)
    if not workspace_root.is_absolute() or workspace_root == PurePosixPath("/"):
        raise PurgeError("Target workspace must be absolute and non-root")
    run_root = workspace_root / "runs" / str(request.run_id)
    if PurePosixPath(request.run_root) != run_root:
        raise PurgeError("Run workspace does not match the persisted target")
    if request.scope.value == "outputs":
        return run_root, run_root / "output", run_root / ".rundra-purge-output"
    return run_root, run_root, run_root.parent / f".{request.run_id}.rundra-purge"


def _reject_symlink(path: Path, name: str) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return
    if stat.S_ISLNK(mode):
        raise PurgeError(f"{name} must not be a symlink")


def _make_directories_writable(root: Path) -> None:
    for directory, names, _files in os.walk(root, topdown=True, followlinks=False):
        path = Path(directory)
        path.chmod(stat.S_IMODE(path.stat().st_mode) | stat.S_IRWXU)
        names[:] = [name for name in names if not (path / name).is_symlink()]


_REMOTE_PURGE_SCRIPT = """\
set -eu
workspace=$1
run_id=$2
scope=$3
mode=$4
[ "${workspace#/}" != "$workspace" ] && [ "$workspace" != / ] || exit 64
case "$run_id" in run_[0123456789abcdef][0123456789abcdef]*) ;; *) exit 64 ;; esac
run="$workspace/runs/$run_id"
case "$scope" in
  outputs) target="$run/output"; tombstone="$run/.rundra-purge-output" ;;
  workspace) target="$run"; tombstone="$workspace/runs/.$run_id.rundra-purge" ;;
  *) exit 64 ;;
esac
[ ! -L "$run" ] && [ ! -L "$target" ] && [ ! -L "$tombstone" ] || exit 65
if [ -e "$target" ] && [ -e "$tombstone" ]; then exit 75; fi
if [ -e "$target" ]; then outcome=planned
elif [ -e "$tombstone" ]; then outcome=resumed
else outcome=already_absent
fi
if [ "$mode" = dry-run ]; then printf '%s\\n' "$outcome"; exit 0; fi
if [ -e "$target" ]; then mv -- "$target" "$tombstone"; outcome=purged; fi
if [ -e "$tombstone" ]; then
  find -P "$tombstone" -type d -exec chmod u+rwx -- {} +
  rm -rf -- "$tombstone"
fi
printf '%s\\n' "$outcome"
"""
