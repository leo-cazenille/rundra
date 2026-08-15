from __future__ import annotations

from pathlib import PurePosixPath

import pytest

from rundra.adapters.native import NativeRuntime, NativeRuntimeError
from rundra.domain.models import Command
from rundra.ports import BindMount, ContainerRequest, ContainerRuntime


def _request(
    *, image: PurePosixPath | None = None, gpu: bool = False
) -> ContainerRequest:
    return ContainerRequest(
        command=Command(
            (
                "python",
                "main.py",
                "--config=/workspace/input/config.yaml",
                "; touch /tmp/not-executed",
            ),
            environment={
                "MODE": "literal",
                "OUTPUT": "/workspace/output/result.json",
            },
            working_directory=PurePosixPath("/workspace/source/project"),
        ),
        image=image,
        gpu=gpu,
        binds=(
            BindMount(
                PurePosixPath("/runs/one/source"),
                PurePosixPath("/workspace/source"),
            ),
            BindMount(
                PurePosixPath("/runs/one/input"),
                PurePosixPath("/workspace/input"),
            ),
            BindMount(
                PurePosixPath("/runs/one/output"),
                PurePosixPath("/workspace/output"),
                read_only=False,
            ),
        ),
    )


def test_native_runtime_maps_semantic_paths_without_executing_or_using_a_shell() -> (
    None
):
    runtime = NativeRuntime()

    command = runtime.build_command(_request())

    assert isinstance(runtime, ContainerRuntime)
    assert runtime.check().name == "native"
    assert command.argv == (
        "python",
        "main.py",
        "--config=/runs/one/input/config.yaml",
        "; touch /tmp/not-executed",
    )
    assert command.environment == {
        "MODE": "literal",
        "OUTPUT": "/runs/one/output/result.json",
    }
    assert command.working_directory == PurePosixPath("/runs/one/source/project")


@pytest.mark.parametrize(
    "container_request",
    [
        _request(image=PurePosixPath("image.sif")),
        _request(gpu=True),
        ContainerRequest(
            Command(
                ("python",),
                working_directory=PurePosixPath("/outside/workspace"),
            ),
            image=None,
            gpu=False,
        ),
        ContainerRequest(Command(("python",)), image=None, gpu=False),
    ],
)
def test_native_runtime_rejects_container_features_and_unmapped_working_directories(
    container_request: ContainerRequest,
) -> None:
    with pytest.raises(NativeRuntimeError):
        NativeRuntime().build_command(container_request)
