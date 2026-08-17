from __future__ import annotations

from dataclasses import replace
from pathlib import PurePosixPath

from rundra.domain.models import ContainerSpec
from rundra.domain.preparation import (
    PreparationRecord,
    PreparedOutput,
)
from rundra.persistence import record_from_dict, record_to_dict
from tests.unit.persistence.test_json_store import _record

_DIGEST = "ab" * 32


def test_version_two_preparation_record_round_trips_strictly() -> None:
    base = _record()
    image = PurePosixPath("/cache/images/application.sif")
    experiment = replace(base.experiment, container=ContainerSpec(image))
    preparation = PreparationRecord(
        source_identity="git-recipe",
        source_digest="cd" * 32,
        source_action="checkout_git_cache",
        image_uri="library://example/application:v1",
        image_sha256=_DIGEST,
        image_path=image,
        image_action="reuse_image_cache",
        resolution_location="local",
        build_cache_key="ef" * 32,
        builder_location="local",
        build_outputs=(PreparedOutput(PurePosixPath("bin/model"), "12" * 32, True),),
        logs=(PurePosixPath("/cache/build.stdout"),),
    )
    record = replace(
        base,
        format_version=2,
        experiment=experiment,
        container_digest=_DIGEST,
        preparation=preparation,
    )

    document = record_to_dict(record)

    assert document["format_version"] == 2
    assert document["preparation"]["source_digest"] == "cd" * 32
    assert document["experiment"]["container"]["image"] == str(image)
    assert record_from_dict(document) == record


def test_version_one_record_shape_does_not_gain_preparation() -> None:
    document = record_to_dict(_record())

    assert document["format_version"] == 1
    assert "preparation" not in document
