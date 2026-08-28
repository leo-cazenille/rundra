from datetime import timedelta
from pathlib import PurePosixPath

from rundra.domain.preparation import PreparationImage
from rundra.orchestration import service


def test_prebuilt_image_without_build_uses_bounded_framework_resources() -> None:
    image = PreparationImage(
        PurePosixPath("application.sif"),
        "library://example/application:1",
        "ab" * 32,
    )

    resources = service._remote_preparation_resources(image, None)

    assert resources is not None
    assert resources.cpus_per_task == 1
    assert resources.memory_bytes == 2 * 1024**3
    assert resources.walltime == timedelta(minutes=15)
