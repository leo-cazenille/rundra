from __future__ import annotations

import hashlib
import io
import json
import os
import stat
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from rundra.adapters._local_paths import resolve_write_destination
from rundra.cli.operations import _load_record
from rundra.cli.retained_inputs import RetainedInputError, RetainedInputs
from rundra.cli.retry_operations import LINEAGE_DIRECTORY
from rundra.domain.campaigns import CampaignId
from rundra.domain.models import Artifact, ArtifactKind
from rundra.domain.parameters import ParameterSet
from rundra.domain.records import RunRecord
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.persistence import JsonRunStore, record_to_dict
from rundra.persistence.campaign_store import JsonCampaignStore
from rundra.persistence.errors import RunStoreError
from rundra.results import OperationError, OperationResult


@dataclass(frozen=True, slots=True)
class ExportValue:
    selector: str
    destination: Path
    run_count: int
    result_count: int
    archive_sha256: str


class _DigestReader:
    def __init__(self, stream: BinaryIO) -> None:
        self.stream = stream
        self.digest = hashlib.sha256()

    def read(self, size: int = -1, /) -> bytes:
        block = self.stream.read(size)
        self.digest.update(block)
        return block


@dataclass(slots=True)
class _Bundle:
    archive: tarfile.TarFile
    files: dict[str, dict[str, object]] = field(default_factory=dict)

    def add(self, name: str, stream: BinaryIO, size: int) -> None:
        if name in self.files:
            raise ValueError(f"Duplicate export member {name}")
        entry = tarfile.TarInfo(name)
        entry.mode = 0o600
        entry.size = size
        reader = _DigestReader(stream)
        self.archive.addfile(entry, reader)
        self.files[name] = {"size_bytes": size, "sha256": reader.digest.hexdigest()}

    def add_bytes(self, name: str, content: bytes) -> None:
        self.add(name, io.BytesIO(content), len(content))

    def add_json(self, name: str, value: object) -> None:
        self.add_bytes(
            name,
            (
                json.dumps(
                    value, sort_keys=True, separators=(",", ":"), allow_nan=False
                )
                + "\n"
            ).encode("utf-8"),
        )


def export_operation(
    selector: str, data_dir: Path, destination: Path, *, include_results: bool
) -> OperationResult[ExportValue]:
    """Publish a local, read-only Run or campaign evidence archive."""
    temporary_path: Path | None = None
    try:
        store = JsonRunStore(data_dir)
        campaign_summary = None
        selectors: tuple[str, ...] = (selector,)
        if selector.startswith("campaign_"):
            campaign = JsonCampaignStore(data_dir).load(CampaignId(selector))
            selectors = tuple(str(item.run_id) for item in campaign.launches)
            campaign_summary = {
                "campaign_id": str(campaign.id),
                "name": campaign.name,
                "launches": [
                    {
                        "name": item.name,
                        "run_id": str(item.run_id),
                        "target": item.target,
                        "task_count": item.task_count,
                        "submission_state": item.submission_state.value,
                    }
                    for item in campaign.launches
                ],
            }
        records = []
        for run_id in selectors:
            record, error = _load_record(run_id, store)
            if error is not None:
                return OperationResult.failure("export", error)
            assert record is not None
            records.append(record)
        target = resolve_write_destination(destination)
        if target.exists() or target.is_symlink():
            raise FileExistsError("Export destination already exists")
        target.parent.mkdir(parents=True, exist_ok=True)
        result_count = 0
        with tempfile.NamedTemporaryFile(
            dir=target.parent, prefix=".rundra-export-", delete=False
        ) as stream:
            temporary_path = Path(stream.name)
            with tarfile.open(fileobj=stream, mode="w:gz") as archive:
                bundle = _Bundle(archive)
                for record in records:
                    result_count += _add_run(bundle, record, data_dir, include_results)
                bundle.add_json(
                    "manifest.json",
                    {
                        "format_version": 1,
                        "kind": "rundra_export",
                        "selector": selector,
                        "run_ids": list(selectors),
                        "campaign": campaign_summary,
                        "results_included": include_results,
                        "files": dict(bundle.files),
                    },
                )
            stream.flush()
            os.fsync(stream.fileno())
        with temporary_path.open("rb") as payload:
            digest = hashlib.file_digest(payload, "sha256").hexdigest()
        os.link(temporary_path, target)
        return OperationResult.success(
            "export", ExportValue(selector, target, len(records), result_count, digest)
        )
    except FileExistsError as error:
        return OperationResult.failure(
            "export", OperationError("EXPORT_DESTINATION_EXISTS", str(error))
        )
    except RetainedInputError as error:
        return OperationResult.failure(
            "export", OperationError("EXPORT_INPUTS_UNAVAILABLE", str(error))
        )
    except (
        OSError,
        RuntimeError,
        RunStoreError,
        ValueError,
        tarfile.TarError,
    ) as error:
        return OperationResult.failure(
            "export", OperationError("EXPORT_FAILED", str(error))
        )
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _add_run(
    bundle: _Bundle, record: RunRecord, data_dir: Path, include_results: bool
) -> int:
    prefix = f"runs/{record.run.id}"
    config_files: set[str] = set()

    def add_config(content: str) -> str:
        data = content.encode("utf-8")
        name = f"configs/{hashlib.sha256(data).hexdigest()}.yaml"
        if name not in config_files:
            bundle.add_bytes(f"{prefix}/{name}", data)
            config_files.add(name)
        return name

    task_document = record_to_dict(record)
    bundle.add_json(f"{prefix}/experiment.json", task_document["experiment"])
    if record.is_compact:
        inputs = RetainedInputs(record)
        assert record.task_space is not None
        parameters = []
        for index in range(record.task_space.parameter_set_count):
            config = inputs.compact_config(index)
            parameters.append(
                {
                    "ordinal": index,
                    "config": add_config(config.config.content),
                    "parameter_set": _parameter_document(config.parameter_set),
                }
            )
        task_table: object = {
            "task_space": task_document["task_space"],
            "parameters": parameters,
        }
    else:
        task_table = {
            "tasks": [
                {
                    "task_id": str(task.id),
                    "seed": task.seed,
                    "state": task.state.value,
                    "config": add_config(task.config.content),
                    "parameter_set": _parameter_document(task.parameter_set),
                }
                for task in record.run.tasks
            ]
        }
    bundle.add_json(f"{prefix}/tasks.json", task_table)
    preparation = record.preparation
    parent_id = record.scheduler_metadata.get("retry_parent_run_id")
    bundle.add_json(
        f"{prefix}/provenance.json",
        {
            "run_id": str(record.run.id),
            "target": record.run.target.name,
            "scheduler": record.run.target.scheduler.kind,
            "scheduler_job_ids": list(record.scheduler_job_ids),
            "framework_version": record.framework_version,
            "run_record_version": record.format_version,
            "state": record.run.state.value,
            "retrieval_state": record.run.retrieval_state.value,
            "created_at": record.run.created_at.isoformat(),
            "git_commit": record.git_commit,
            "git_dirty": record.git_dirty,
            "container_digest": record.container_digest,
            "container_runtime": record.scheduler_metadata.get("container_runtime"),
            "container_runtime_version": record.scheduler_metadata.get(
                "container_runtime_version"
            ),
            "prepared_source_identity": None
            if preparation is None
            else preparation.source_identity,
            "prepared_source_sha256": None
            if preparation is None
            else preparation.source_digest,
            "prepared_image_uri": None
            if preparation is None
            else preparation.image_uri,
            "retry_parent_run_id": parent_id,
        },
    )
    if parent_id is not None:
        lineage = data_dir / LINEAGE_DIRECTORY / f"{record.run.id}.json"
        if lineage.is_file() and not lineage.is_symlink():
            bundle.add_bytes(f"{prefix}/retry-lineage.json", lineage.read_bytes())
    artifacts = tuple(
        item
        for item in record.artifacts
        if item.kind
        in {
            ArtifactKind.RAW_RESULT,
            ArtifactKind.OUTPUT_SHARD,
            ArtifactKind.REFERENCE_MANIFEST,
        }
    )
    if include_results and (
        record.run.state
        not in {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }
        or record.run.retrieval_state is not RetrievalState.SUCCEEDED
        or not any(item.kind is ArtifactKind.RAW_RESULT for item in artifacts)
    ):
        raise RetainedInputError(
            "Materialized result files are unavailable. Fetch the saved Run with --mode copy --extract first."
        )
    root = (
        None
        if record.retrieval_destination is None
        else Path(record.retrieval_destination)
    )
    result_index = []
    included: set[str] = set()
    for item in sorted(artifacts, key=lambda artifact: str(artifact.path)):
        path = Path(item.path)
        relative = (
            None
            if root is None or not path.is_relative_to(root)
            else path.relative_to(root)
        )
        member = None
        if include_results and item.kind is ArtifactKind.RAW_RESULT:
            if (
                root is None
                or relative is None
                or not relative.parts
                or ".." in relative.parts
            ):
                raise RetainedInputError(
                    "A saved result is outside the recorded materialized destination"
                )
            member = f"results/{relative.as_posix()}"
            if member not in included:
                _add_result(bundle, f"{prefix}/{member}", root, relative, item)
                included.add(member)
        result_index.append(
            {
                "kind": item.kind.value,
                "task_id": None if item.task_id is None else str(item.task_id),
                "path": None if relative is None else relative.as_posix(),
                "size_bytes": item.size_bytes,
                "member": member,
            }
        )
    bundle.add_json(f"{prefix}/results.json", {"artifacts": result_index})
    return len(included)


def _add_result(
    bundle: _Bundle, member: str, root: Path, relative: Path, item: Artifact
) -> None:
    path = root
    if root.resolve(strict=True) != root:
        raise RetainedInputError(
            "The materialized result root contains a symbolic link"
        )
    for component in relative.parts:
        path /= component
        if path.is_symlink():
            raise RetainedInputError(
                "A materialized result path contains a symbolic link"
            )
    if not path.is_file():
        raise RetainedInputError("A recorded materialized result file is missing")
    with path.open("rb") as stream:
        status = os.fstat(stream.fileno())
        if not stat.S_ISREG(status.st_mode) or (
            item.size_bytes is not None and item.size_bytes != status.st_size
        ):
            raise RetainedInputError(
                "A materialized result differs from its saved index"
            )
        bundle.add(member, stream, status.st_size)


def _parameter_document(parameter: ParameterSet | None) -> dict[str, object] | None:
    return (
        None
        if parameter is None
        else {"id": parameter.id, "choices": dict(parameter.choices)}
    )
