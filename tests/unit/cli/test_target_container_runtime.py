from pathlib import PurePosixPath

from rundra.cli.operations import _execution_adapters
from rundra.domain.models import BackendConfig, Command, Target
from rundra.ports import ContainerRequest


def test_execution_adapters_use_target_container_executable() -> None:
    target = Target(
        name="local-singularity",
        transport=BackendConfig("local"),
        scheduler=BackendConfig("local"),
        staging=BackendConfig("local"),
        container=BackendConfig("apptainer", {"executable": "singularity"}),
        workspace=PurePosixPath("/tmp/rundra-local-singularity"),
    )

    _, _, runtime, _ = _execution_adapters(target)
    command = runtime.build_command(
        ContainerRequest(
            command=Command(("python3", "main.py"), environment={"MODE": "test"}),
            image=PurePosixPath("/images/application.sif"),
            gpu=False,
        )
    )

    assert command.argv[0] == "singularity"
    assert command.environment == {"SINGULARITYENV_MODE": "test"}
