from __future__ import annotations

import shutil

import pytest

from rundra.adapters.ssh import SSHTransport, SSHUnavailableError


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
