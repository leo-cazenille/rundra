from __future__ import annotations

import os
import shutil
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path

from rundra.adapters._remote_shell import serialize_remote_command
from rundra.config.errors import ConfigError
from rundra.config.targets import load_targets
from rundra.domain.models import Command, Target
from rundra.results import OperationError, OperationResult

_STATUSES = frozenset({"pass", "warning", "fail"})


@dataclass(frozen=True, slots=True)
class DoctorCheck:
    name: str
    status: str
    message: str

    def __post_init__(self) -> None:
        if not self.name or not self.message:
            raise ValueError("Doctor checks require a name and message")
        if self.status not in _STATUSES:
            raise ValueError("Doctor check status is unsupported")


@dataclass(frozen=True, slots=True)
class DoctorValue:
    source: Path
    target: Target
    checks: tuple[DoctorCheck, ...]
    connected: bool

    @property
    def ready(self) -> bool:
        return all(check.status != "fail" for check in self.checks)


def doctor_operation(
    targets_file: Path,
    target_name: str | None,
    *,
    connect: bool = False,
) -> OperationResult[DoctorValue]:
    """Diagnose target access without creating remote state or scheduler work."""
    source = targets_file.expanduser().resolve()
    if target_name is None or not target_name.strip():
        return OperationResult.failure(
            "doctor",
            OperationError(
                "DOCTOR_TARGET_REQUIRED",
                "Select a target directly or through an experiment project",
            ),
        )
    try:
        targets = load_targets(source)
    except ConfigError as error:
        return OperationResult.failure(
            "doctor",
            OperationError(
                error.code,
                error.message,
                {"source": str(error.source), "path": error.path},
            ),
        )
    if target_name not in targets:
        return OperationResult.failure(
            "doctor",
            OperationError(
                "TARGET_NOT_FOUND",
                f"Target '{target_name}' is not defined",
                {"source": str(source)},
            ),
        )
    target = targets[target_name]
    checks = [DoctorCheck("target_config", "pass", "target configuration is valid")]
    if target.transport.kind == "local":
        checks.append(_local_workspace_check(Path(target.workspace)))
    else:
        host = str(target.transport.options["host"])
        checks.extend(_ssh_static_checks(host))
        if connect:
            checks.append(_ssh_connect_check(host))
    return OperationResult.success(
        "doctor", DoctorValue(source, target, tuple(checks), connect)
    )


def _local_workspace_check(workspace: Path) -> DoctorCheck:
    candidate = workspace
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    if candidate.is_dir() and os.access(candidate, os.W_OK):
        return DoctorCheck(
            "workspace", "pass", "workspace or nearest existing ancestor is writable"
        )
    return DoctorCheck(
        "workspace", "fail", "workspace has no writable existing ancestor"
    )


def _ssh_static_checks(host: str) -> list[DoctorCheck]:
    checks: list[DoctorCheck] = []
    for executable in ("ssh", "rsync"):
        available = shutil.which(executable) is not None
        checks.append(
            DoctorCheck(
                f"executable_{executable}",
                "pass" if available else "fail",
                f"{executable} is available"
                if available
                else f"{executable} is missing",
            )
        )
    socket_value = os.environ.get("SSH_AUTH_SOCK")
    if socket_value is None:
        checks.append(
            DoctorCheck(
                "ssh_agent",
                "warning",
                "SSH_AUTH_SOCK is unset; another external authentication method is required",
            )
        )
    else:
        socket_path = Path(socket_value)
        available = False
        try:
            available = stat.S_ISSOCK(socket_path.stat().st_mode) and os.access(
                socket_path, os.R_OK | os.W_OK
            )
        except OSError:
            pass
        checks.append(
            DoctorCheck(
                "ssh_agent",
                "pass" if available else "warning",
                "SSH agent socket is accessible"
                if available
                else "SSH_AUTH_SOCK is not an accessible socket",
            )
        )
    checks.extend(_ssh_configuration_checks(host))
    return checks


def _ssh_configuration_checks(host: str) -> list[DoctorCheck]:
    if shutil.which("ssh") is None:
        return []
    try:
        completed = subprocess.run(
            ("ssh", "-G", "--", host),
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
            shell=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return [
            DoctorCheck(
                "ssh_config", "fail", "OpenSSH configuration could not be resolved"
            )
        ]
    if completed.returncode != 0:
        return [DoctorCheck("ssh_config", "fail", "OpenSSH configuration is invalid")]
    checks = [DoctorCheck("ssh_config", "pass", "OpenSSH configuration resolves")]
    known_hosts: list[Path] = []
    for line in completed.stdout.splitlines():
        key, separator, value = line.partition(" ")
        if separator and key.lower() == "userknownhostsfile":
            known_hosts.extend(Path(item).expanduser() for item in value.split())
    readable = any(path.is_file() and os.access(path, os.R_OK) for path in known_hosts)
    checks.append(
        DoctorCheck(
            "known_hosts",
            "pass" if readable else "warning",
            "an OpenSSH user known-hosts file is readable"
            if readable
            else "no configured user known-hosts file is currently readable",
        )
    )
    return checks


def _ssh_connect_check(host: str) -> DoctorCheck:
    tools = ("rsync", "sbatch", "squeue", "scancel", "scontrol", "apptainer")
    script = 'for tool in "$@"; do command -v "$tool" >/dev/null || exit 20; done'
    command = Command(("sh", "-c", script, "rundr-doctor", *tools))
    try:
        completed = subprocess.run(
            (
                "ssh",
                "-T",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=10",
                "--",
                host,
                serialize_remote_command(command),
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
            shell=False,
        )
    except subprocess.TimeoutExpired:
        return DoctorCheck("connect", "fail", "SSH connection timed out")
    except OSError:
        return DoctorCheck("connect", "fail", "SSH client could not be executed")
    if completed.returncode == 0:
        return DoctorCheck(
            "connect",
            "pass",
            "batch-mode SSH authentication and remote capabilities succeeded",
        )
    message = (
        "one or more required remote executables are missing"
        if completed.returncode == 20
        else "batch-mode SSH authentication or connectivity failed"
    )
    return DoctorCheck("connect", "fail", message)
