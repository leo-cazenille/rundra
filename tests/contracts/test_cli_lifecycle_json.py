from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path, PurePosixPath

import pytest

from rundra.cli.operations import (
    ArtifactsValue,
    CancelValue,
    FetchValue,
    InspectValue,
    LaunchResolutionValue,
    ListRunsValue,
    LogsValue,
    RunValue,
    StatusValue,
    TaskStatusValue,
)
from rundra.cli.render import result_document
from rundra.domain.models import ArtifactKind
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.persistence import record_from_dict
from rundra.results import OperationResult

_SCHEMAS = Path("docs/schemas")
_RECORD_DOCUMENT: object = json.loads(
    (_SCHEMAS / "run-record-v1.json").read_text(encoding="utf-8")
)
_RECORD = record_from_dict(_RECORD_DOCUMENT)
_STATUS = StatusValue(
    run_id=_RECORD.run.id,
    experiment=_RECORD.run.experiment_name,
    target=_RECORD.run.target.name,
    state=_RECORD.run.state,
    retrieval_state=_RECORD.run.retrieval_state,
    task_counts={"SUCCEEDED": 1},
    native_state=_RECORD.native_state,
    scheduler_job_ids=_RECORD.scheduler_job_ids,
    task_details=(
        TaskStatusValue(
            _RECORD.run.tasks[0].id,
            17,
            ExecutionState.SUCCEEDED,
            RetrievalState.SUCCEEDED,
            native_id="local-0123456789abcdef0123456789abcdef",
            native_state="EXITED",
            exit_code=0,
        ),
    ),
)
_LOGS = LogsValue(
    run_id=_RECORD.run.id,
    task_id=_RECORD.run.tasks[0].id,
    stdout="seed=17 done\n",
    stderr="",
    stdout_path=PurePosixPath(
        "/workspaces/runs/run_0123456789abcdef0123456789abcdef/logs/task_000000.stdout"
    ),
    stderr_path=PurePosixPath(
        "/workspaces/runs/run_0123456789abcdef0123456789abcdef/logs/task_000000.stderr"
    ),
)
_RAW_ARTIFACTS = tuple(
    artifact
    for artifact in _RECORD.artifacts
    if artifact.kind is ArtifactKind.RAW_RESULT
)
_RUN_LAUNCH = LaunchResolutionValue(
    None,
    {
        "config": "examples/minimal/config.yaml",
        "seed": 17,
        "target": "local",
        "targets_file": "examples/minimal/targets.yaml",
        "source_root": "/work/minimal",
        "destination": "/work/minimal/retrieved",
        "data_dir": "/work/records",
    },
    {
        "config": "cli",
        "seed": "cli",
        "target": "cli",
        "targets_file": "cli",
        "source_root": "cli",
        "destination": "cli",
        "data_dir": "cli",
    },
)
_SUBMITTED_RUN = replace(
    _RECORD.run,
    target=replace(
        _RECORD.run.target,
        name="shoal",
        scheduler=replace(_RECORD.run.target.scheduler, kind="slurm"),
    ),
    state=ExecutionState.SUBMITTED,
    retrieval_state=RetrievalState.NOT_REQUESTED,
    tasks=tuple(
        replace(task, state=ExecutionState.SUBMITTED) for task in _RECORD.run.tasks
    ),
)
_SUBMITTED_RECORD = replace(
    _RECORD,
    run=_SUBMITTED_RUN,
    scheduler_job_ids=("18372",),
    started_at=None,
    completed_at=None,
    native_state=None,
    scheduler_metadata={},
    task_scheduler_ids={_RECORD.run.tasks[0].id: "18372"},
    task_native_states={},
    task_retrieval_states={_RECORD.run.tasks[0].id: RetrievalState.NOT_REQUESTED},
    task_exit_codes={},
    artifacts=_RECORD.artifacts[:2],
)
_CANCELLED_STATUS = StatusValue(
    run_id=_RECORD.run.id,
    experiment="minimal",
    target="shoal",
    state=ExecutionState.CANCELLED,
    retrieval_state=RetrievalState.NOT_REQUESTED,
    task_counts={"CANCELLED": 1},
    native_state="CANCELLED",
    scheduler_job_ids=("18372",),
    task_details=(
        TaskStatusValue(
            _RECORD.run.tasks[0].id,
            17,
            ExecutionState.CANCELLED,
            RetrievalState.NOT_REQUESTED,
            native_id="18372",
            native_state="CANCELLED",
            exit_code=0,
        ),
    ),
)


@pytest.mark.parametrize(
    ("result", "contract"),
    [
        (
            OperationResult.success("run", RunValue(_RECORD, _RUN_LAUNCH)),
            "run-success-v1.json",
        ),
        (
            OperationResult.success("submit", RunValue(_SUBMITTED_RECORD)),
            "submit-success-v1.json",
        ),
        (OperationResult.success("status", _STATUS), "status-success-v1.json"),
        (
            OperationResult.success("list", ListRunsValue((_STATUS,))),
            "list-success-v2.json",
        ),
        (OperationResult.success("logs", _LOGS), "logs-success-v1.json"),
        (
            OperationResult.success(
                "fetch",
                FetchValue(
                    _RECORD.run.id,
                    PurePosixPath("retrieved"),
                    _RECORD.run.retrieval_state,
                    _RAW_ARTIFACTS,
                    (_RECORD.run.tasks[0].id,),
                ),
            ),
            "fetch-success-v1.json",
        ),
        (
            OperationResult.success("cancel", CancelValue(_CANCELLED_STATUS)),
            "cancel-success-v1.json",
        ),
    ],
)
def test_lifecycle_json_matches_checked_contract(
    result: OperationResult[object],
    contract: str,
) -> None:
    expected = json.loads((_SCHEMAS / contract).read_text(encoding="utf-8"))

    assert result_document(result) == expected


def test_inspect_embeds_the_checked_run_record_contract() -> None:
    document = result_document(
        OperationResult.success("inspect", InspectValue(_RECORD))
    )

    assert document == {
        "format_version": 1,
        "ok": True,
        "operation": "inspect",
        "record": _RECORD_DOCUMENT,
    }


def test_summary_documents_omit_unbounded_detail() -> None:
    status_document = result_document(
        OperationResult.success(
            "status",
            replace(_STATUS, task_details=(), task_details_included=False),
        )
    )
    inspect_document = result_document(
        OperationResult.success("inspect", InspectValue(_RECORD, summary=True))
    )
    fetch_document = result_document(
        OperationResult.success(
            "fetch",
            FetchValue(
                _RECORD.run.id,
                PurePosixPath("retrieved"),
                _RECORD.run.retrieval_state,
                (),
                (_RECORD.run.tasks[0].id,),
                artifact_total=len(_RAW_ARTIFACTS),
                artifacts_included=False,
            ),
        )
    )

    assert status_document["status"]["task_details"] == []
    assert not status_document["status"]["task_details_included"]
    assert not isinstance(inspect_document["record"]["artifacts"], list)
    assert fetch_document["fetch"]["artifact_total"] == 1
    assert not fetch_document["fetch"]["artifacts_included"]
    assert fetch_document["fetch"]["artifacts"] == []


def test_artifacts_document_is_paginated() -> None:
    document = result_document(
        OperationResult.success(
            "artifacts",
            ArtifactsValue(
                run_id=_RECORD.run.id,
                total=len(_RECORD.artifacts),
                offset=0,
                limit=2,
                artifacts=_RECORD.artifacts[:2],
            ),
        )
    )

    assert document["artifacts"]["total"] == len(_RECORD.artifacts)
    assert document["artifacts"]["offset"] == 0
    assert document["artifacts"]["limit"] == 2
    assert document["artifacts"]["next_offset"] == 2
    assert len(document["artifacts"]["items"]) == 2
