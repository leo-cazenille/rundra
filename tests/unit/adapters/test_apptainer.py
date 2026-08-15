from __future__ import annotations

from pathlib import PurePosixPath

import pytest

from rundra.adapters.apptainer import (
    ApptainerConfigurationError,
    ApptainerRuntime,
    ApptainerUnavailableError,
)
from rundra.domain.models import Command
from rundra.ports import BindMount, ContainerRequest, ContainerRuntime


def _request(
    *,
    command: Command | None = None,
    gpu: bool = False,
    binds: tuple[BindMount, ...] = (),
) -> ContainerRequest:
    return ContainerRequest(
        command=command or Command(("python", "main.py")),
        image=PurePosixPath("/images/project image.sif"),
        gpu=gpu,
        binds=binds,
    )


def test_apptainer_runtime_builds_a_shell_free_cpu_command() -> None:
    runtime = ApptainerRuntime()
    request = _request(
        command=Command(
            (
                "python",
                "main.py",
                "--label",
                "value with spaces",
                "; touch /tmp/not-executed",
            )
        )
    )

    command = runtime.build_command(request)

    assert isinstance(runtime, ContainerRuntime)
    assert command.argv == (
        "apptainer",
        "exec",
        "--cleanenv",
        "--no-eval",
        "/images/project image.sif",
        "python",
        "main.py",
        "--label",
        "value with spaces",
        "; touch /tmp/not-executed",
    )
    assert command.environment == {}
    assert command.working_directory is None


def test_apptainer_runtime_adds_gpu_binds_and_container_working_directory() -> None:
    request = _request(
        command=Command(
            ("python", "main.py"),
            working_directory=PurePosixPath("/workspace/runtime"),
        ),
        gpu=True,
        binds=(
            BindMount(
                PurePosixPath("/runs/one/source"),
                PurePosixPath("/workspace/source"),
                read_only=True,
            ),
            BindMount(
                PurePosixPath("/runs/one/output"),
                PurePosixPath("/workspace/output"),
                read_only=False,
            ),
        ),
    )

    command = ApptainerRuntime(executable="singularity").build_command(request)

    assert command.argv == (
        "singularity",
        "exec",
        "--cleanenv",
        "--no-eval",
        "--nv",
        "--bind",
        "/runs/one/source:/workspace/source:ro",
        "--bind",
        "/runs/one/output:/workspace/output:rw",
        "--cwd",
        "/workspace/runtime",
        "/images/project image.sif",
        "python",
        "main.py",
    )


def test_apptainer_runtime_passes_exact_environment_without_argv_interpolation() -> (
    None
):
    request = _request(
        command=Command(
            ("python", "main.py"),
            environment={
                "MODE": "test,one=two",
                "LITERAL": "$(touch /tmp/not-executed)",
            },
        )
    )

    command = ApptainerRuntime().build_command(request)

    assert command.environment == {
        "APPTAINERENV_LITERAL": "$(touch /tmp/not-executed)",
        "APPTAINERENV_MODE": "test,one=two",
    }
    assert "$(touch /tmp/not-executed)" not in command.argv
    assert command.argv[2:4] == ("--cleanenv", "--no-eval")


def test_singularity_compatibility_uses_its_native_environment_prefix() -> None:
    request = _request(
        command=Command(("python", "main.py"), environment={"MODE": "test"})
    )

    command = ApptainerRuntime(executable="/opt/bin/singularity").build_command(request)

    assert command.environment == {"SINGULARITYENV_MODE": "test"}


@pytest.mark.parametrize("name", ["BAD-NAME", "1INVALID", "PREPEND_PATH"])
def test_apptainer_runtime_rejects_environment_names_it_cannot_preserve(
    name: str,
) -> None:
    request = _request(command=Command(("python",), environment={name: "value"}))

    with pytest.raises(ApptainerConfigurationError, match="environment variable"):
        ApptainerRuntime().build_command(request)


@pytest.mark.parametrize(
    "bind",
    [
        BindMount(PurePosixPath("relative"), PurePosixPath("/container")),
        BindMount(PurePosixPath("/host"), PurePosixPath("relative")),
        BindMount(PurePosixPath("/host:part"), PurePosixPath("/container")),
        BindMount(PurePosixPath("/host"), PurePosixPath("/container,part")),
    ],
)
def test_apptainer_runtime_rejects_unrepresentable_bind_paths(
    bind: BindMount,
) -> None:
    with pytest.raises(ApptainerConfigurationError, match="bind"):
        ApptainerRuntime().build_command(_request(binds=(bind,)))


def test_bind_mount_and_container_request_defensively_copy_and_validate() -> None:
    bind = BindMount(
        PurePosixPath("/host"), PurePosixPath("/container"), read_only=False
    )
    supplied = [bind]
    request = ContainerRequest(
        Command(("python",)),
        PurePosixPath("image.sif"),
        False,
        supplied,
    )
    supplied.clear()

    assert request.binds == (bind,)
    with pytest.raises(ValueError, match="unique container destinations"):
        ContainerRequest(
            Command(("python",)),
            PurePosixPath("image.sif"),
            False,
            (bind, bind),
        )
    with pytest.raises(TypeError, match="read_only"):
        BindMount(PurePosixPath("/host"), PurePosixPath("/container"), 1)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "container_request",
    [
        ContainerRequest(
            Command(("python",)), PurePosixPath("-looks-like-an-option"), False
        ),
        ContainerRequest(
            Command(("python",), working_directory=PurePosixPath("relative")),
            PurePosixPath("image.sif"),
            False,
        ),
        ContainerRequest(
            Command(("python", "bad\x00argument")),
            PurePosixPath("image.sif"),
            False,
        ),
    ],
)
def test_apptainer_runtime_rejects_unsafe_request_values(
    container_request: ContainerRequest,
) -> None:
    with pytest.raises(ApptainerConfigurationError):
        ApptainerRuntime().build_command(container_request)


def test_apptainer_capability_check_uses_path_without_executing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rundra.adapters.apptainer as apptainer

    calls: list[str] = []

    def resolve(executable: str) -> str | None:
        calls.append(executable)
        return "/opt/apptainer/bin/apptainer"

    monkeypatch.setattr(apptainer.shutil, "which", resolve)

    capability = ApptainerRuntime().check()

    assert capability.name == "apptainer"
    assert capability.version is None
    assert calls == ["apptainer"]


def test_apptainer_capability_check_reports_an_actionable_missing_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rundra.adapters.apptainer as apptainer

    monkeypatch.setattr(apptainer.shutil, "which", lambda executable: None)

    with pytest.raises(ApptainerUnavailableError, match="not found on PATH"):
        ApptainerRuntime().check()


@pytest.mark.parametrize("executable", ["", "   ", "bad\x00name"])
def test_apptainer_runtime_rejects_invalid_executable_names(executable: str) -> None:
    with pytest.raises((TypeError, ValueError), match="executable"):
        ApptainerRuntime(executable=executable)
