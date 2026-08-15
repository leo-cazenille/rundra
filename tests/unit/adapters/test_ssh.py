from __future__ import annotations

import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import PurePosixPath

import pytest

from rundra.adapters import SSHTransport as PublicSSHTransport
from rundra.adapters.ssh import (
    SSHCommandError,
    SSHExecutionError,
    SSHTransport,
    SSHUnavailableError,
)
from rundra.domain.models import Command
from rundra.ports import Transport


def test_ssh_transport_check_uses_the_configured_openssh_executable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def which(executable: str) -> str:
        calls.append(executable)
        return "/usr/bin/ssh"

    monkeypatch.setattr(shutil, "which", which)

    capability = SSHTransport("fishvision", executable="openssh").check()

    assert capability.name == "ssh"
    assert capability.version is None
    assert calls == ["openssh"]


def test_ssh_transport_is_exported_from_the_adapter_package() -> None:
    assert PublicSSHTransport is SSHTransport


def test_ssh_transport_reports_an_unavailable_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(shutil, "which", lambda executable: None)

    with pytest.raises(SSHUnavailableError, match="was not found on PATH"):
        SSHTransport("cluster-alias").check()


def test_ssh_transport_reports_client_search_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_search(executable: str) -> None:
        raise OSError("PATH unavailable")

    monkeypatch.setattr(shutil, "which", fail_search)

    with pytest.raises(SSHUnavailableError, match="Could not search"):
        SSHTransport("cluster-alias").check()


@pytest.mark.parametrize(
    ("host", "error"),
    [
        ("", ValueError),
        ("   ", ValueError),
        ("cluster\x00alias", ValueError),
        (object(), TypeError),
    ],
)
def test_ssh_transport_rejects_invalid_hosts(
    host: object, error: type[Exception]
) -> None:
    with pytest.raises(error, match="SSH host"):
        SSHTransport(host)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("executable", "error"),
    [("", ValueError), ("ssh\x00client", ValueError), (object(), TypeError)],
)
def test_ssh_transport_rejects_invalid_executables(
    executable: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error, match="SSH executable"):
        SSHTransport("cluster", executable=executable)  # type: ignore[arg-type]


def test_ssh_transport_runs_exact_openssh_argv_and_returns_typed_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[str, ...], dict[str, object]]] = []

    def run(
        argv: tuple[str, ...], **options: object
    ) -> subprocess.CompletedProcess[str]:
        calls.append((argv, options))
        return subprocess.CompletedProcess(argv, 7, "remote output\n", "remote error\n")

    monkeypatch.setattr(subprocess, "run", run)
    command = Command(
        (
            "python",
            "script with spaces.py",
            "$(touch /tmp/not-created)",
            "quote'and\nnewline",
        ),
        environment={
            "MODE": "value with spaces",
            "TOKEN": "secret'line\nvalue",
        },
        working_directory=PurePosixPath("/remote/work dir"),
    )

    before = datetime.now(UTC)
    result = SSHTransport("cluster-alias", executable="/usr/bin/ssh").run(command)
    after = datetime.now(UTC)

    assert isinstance(SSHTransport("cluster-alias"), Transport)
    assert calls == [
        (
            (
                "/usr/bin/ssh",
                "-T",
                "--",
                "cluster-alias",
                "cd -- '/remote/work dir' && exec env -- "
                "'MODE=value with spaces' "
                "'TOKEN=secret'\"'\"'line\nvalue' "
                "python 'script with spaces.py' "
                "'$(touch /tmp/not-created)' "
                "'quote'\"'\"'and\nnewline'",
            ),
            {
                "capture_output": True,
                "check": False,
                "encoding": "utf-8",
                "errors": "replace",
                "shell": False,
            },
        )
    ]
    assert result.command is command
    assert result.exit_code == 7
    assert result.stdout == "remote output\n"
    assert result.stderr == "remote error\n"
    assert before <= result.started_at <= result.finished_at <= after


def test_ssh_transport_does_not_leak_command_or_environment_on_start_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_start(argv: tuple[str, ...], **options: object) -> None:
        raise OSError("process unavailable")

    monkeypatch.setattr(subprocess, "run", fail_start)
    command = Command(
        ("credential-command", "super-secret-value"),
        environment={"API_TOKEN": "another-secret-value"},
    )

    with pytest.raises(SSHExecutionError) as captured:
        SSHTransport("safe-host-alias").run(command)

    diagnostic = str(captured.value)
    assert "safe-host-alias" in diagnostic
    assert "credential-command" not in diagnostic
    assert "super-secret-value" not in diagnostic
    assert "API_TOKEN" not in diagnostic
    assert "another-secret-value" not in diagnostic


@pytest.mark.parametrize(
    "command",
    [
        Command(("program", "bad\x00argument")),
        Command(("program",), environment={"BAD=NAME": "value"}),
        Command(("program",), environment={"NAME": "bad\x00value"}),
        Command(("program",), working_directory=PurePosixPath("/bad\x00directory")),
    ],
)
def test_ssh_transport_rejects_unrepresentable_remote_values(command: Command) -> None:
    with pytest.raises(SSHCommandError):
        SSHTransport("cluster").run(command)


@pytest.mark.parametrize("value", ["not-a-command", object()])
def test_ssh_transport_rejects_non_commands(value: object) -> None:
    with pytest.raises(TypeError, match="Command"):
        SSHTransport("cluster").run(value)  # type: ignore[arg-type]
