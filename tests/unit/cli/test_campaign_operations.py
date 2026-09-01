from __future__ import annotations

from pathlib import Path

from rundra.cli.campaign_operations import (
    CampaignChildRecovery,
    CampaignLaunchPlanValue,
    campaign_inspect_operation,
    campaign_plan_operation,
    campaign_resume_operation,
    campaign_status_operation,
    campaign_submit_operation,
)
from rundra.domain.campaigns import (
    CampaignFailurePolicy,
    CampaignId,
    CampaignSubmissionState,
)
from rundra.domain.models import RunId
from rundra.persistence.campaign_store import JsonCampaignStore
from rundra.results import OperationError, OperationResult


def _write_inputs(
    tmp_path: Path, *, campaigns: str, target_kind: str = "slurm"
) -> Path:
    experiment = tmp_path / "experiment.yaml"
    experiment_text = """\
version: 1
experiment: {name: campaign-test}
command:
  argv: [python3, task.py, --config, '{config}', --seed, '{seed}']
resources:
  nodes: 1
  tasks: 1
  cpus_per_task: 1
  gpus_per_task: 0
  memory: 1GiB
  walltime: 00:05:00
"""
    if target_kind != "local":
        experiment_text += "container:\n  image: application.sif\n"
    experiment_text += "outputs: {include: ['results/**']}\n"
    experiment.write_text(experiment_text, encoding="utf-8")
    (tmp_path / "config.yaml").write_text("value: 1\n", encoding="utf-8")
    targets = tmp_path / "targets.yaml"
    if target_kind == "local":
        target = """\
    transport: {type: local}
    scheduler: {type: local}
    staging: {type: local}
    container: {type: native}
"""
    else:
        target = """\
    transport: {type: ssh, host: cluster}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
"""
    targets.write_text(
        f"version: 1\ntargets:\n  cluster:\n{target}    workspace: {tmp_path / 'workspace'}\n",
        encoding="utf-8",
    )
    (tmp_path / "rundra.yaml").write_text(
        f"""\
version: 7
defaults:
  config: config.yaml
  target: cluster
campaigns:
{campaigns}
""",
        encoding="utf-8",
    )
    return experiment


def test_campaign_plan_aggregates_detached_launches(tmp_path: Path) -> None:
    experiment = _write_inputs(
        tmp_path,
        campaigns="""\
  split:
    launches:
      - {name: first, seeds: '0:3'}
      - {name: second, seeds: '4:7'}
""",
    )

    result = campaign_plan_operation(
        experiment,
        campaign_name="split",
        targets_file=tmp_path / "targets.yaml",
    )

    assert result.ok and result.value is not None
    assert result.value.total_tasks == 8
    assert result.value.total_concurrent_task_capacity == 8
    assert [item.name for item in result.value.launches] == ["first", "second"]
    assert (
        result.value.launches[0].destination
        == (tmp_path / "retrieved/split/first").resolve()
    )


def test_campaign_plan_rejects_overlapping_logical_tasks(tmp_path: Path) -> None:
    experiment = _write_inputs(
        tmp_path,
        campaigns="""\
  overlap:
    launches:
      - {name: first, seeds: '0:3'}
      - {name: second, seeds: '2:5'}
""",
    )

    result = campaign_plan_operation(
        experiment,
        campaign_name="overlap",
        targets_file=tmp_path / "targets.yaml",
    )

    assert not result.ok and result.error is not None
    assert result.error.code == "DUPLICATE_CAMPAIGN_TASKS"
    assert result.error.details["task_count"] == 2


def test_campaign_plan_warns_when_duplicates_are_explicitly_allowed(
    tmp_path: Path,
) -> None:
    experiment = _write_inputs(
        tmp_path,
        campaigns="""\
  overlap:
    allow_duplicate_tasks: true
    launches:
      - {name: first, seed: 2}
      - {name: second, seed: 2}
""",
    )

    result = campaign_plan_operation(
        experiment,
        campaign_name="overlap",
        targets_file=tmp_path / "targets.yaml",
    )

    assert result.ok and result.value is not None
    assert result.value.warnings == (
        "duplicate logical Tasks allowed between first and second: 1",
    )


def test_campaign_plan_rejects_synchronous_local_target(tmp_path: Path) -> None:
    experiment = _write_inputs(
        tmp_path,
        target_kind="local",
        campaigns="""\
  local:
    launches:
      - {name: local, seed: 1}
""",
    )

    result = campaign_plan_operation(
        experiment,
        campaign_name="local",
        targets_file=tmp_path / "targets.yaml",
    )

    assert not result.ok and result.error is not None
    assert result.error.code == "CAMPAIGN_TARGET_NOT_DETACHED"


def test_standalone_campaign_uses_explicit_destination_root(tmp_path: Path) -> None:
    experiment = _write_inputs(
        tmp_path,
        campaigns="""\
  unused:
    launches: [{name: unused, seed: 0}]
""",
    )
    campaign = tmp_path / "campaign.yaml"
    campaign.write_text(
        """\
kind: campaign
version: 1
name: standalone
experiment: experiment.yaml
project_file: rundra.yaml
launches:
  - {name: cluster, seeds: '10:11'}
""",
        encoding="utf-8",
    )

    result = campaign_plan_operation(
        campaign,
        destination=tmp_path / "collected",
        targets_file=tmp_path / "targets.yaml",
    )

    assert experiment.is_file()
    assert result.ok and result.value is not None
    assert (
        result.value.launches[0].destination
        == (tmp_path / "collected/cluster").resolve()
    )


def test_campaign_submit_reserves_ids_and_cancels_prior_children(
    tmp_path: Path,
) -> None:
    experiment = _write_inputs(
        tmp_path,
        campaigns="""\
  cancel-on-failure:
    launches:
      - {name: first, seed: 1}
      - {name: second, seed: 2}
      - {name: third, seed: 3}
""",
    )
    planned = campaign_plan_operation(
        experiment,
        campaign_name="cancel-on-failure",
        targets_file=tmp_path / "targets.yaml",
        data_dir=tmp_path / "records",
    )
    assert planned.ok and planned.value is not None
    submitted: list[RunId] = []
    cancelled: list[RunId] = []

    def submitter(
        launch: CampaignLaunchPlanValue,
        run_id: RunId,
        confirmed: int | None,
    ) -> OperationResult[RunId]:
        submitted.append(run_id)
        if len(submitted) == 2:
            return OperationResult.failure(
                "submit", OperationError("SCHEDULER_SUBMISSION_FAILED", "rejected")
            )
        return OperationResult.success("submit", run_id)

    def canceller(run_id: RunId, data_dir: Path) -> OperationResult[object]:
        cancelled.append(run_id)
        return OperationResult.success("cancel", object())

    result = campaign_submit_operation(
        planned.value,
        submitter=submitter,
        canceller=canceller,
        campaign_id_factory=lambda: CampaignId(
            "campaign_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ),
        framework_version="test",
    )

    assert not result.ok and result.error is not None
    record = JsonCampaignStore(tmp_path / "records").load(
        CampaignId("campaign_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    )
    assert len(set(submitted)) == 2
    assert cancelled == [submitted[0]]
    assert [item.submission_state for item in record.launches] == [
        CampaignSubmissionState.CANCELLED,
        CampaignSubmissionState.FAILED,
        CampaignSubmissionState.NOT_ATTEMPTED,
    ]


def test_campaign_submit_unknown_outcome_halts_without_policy_action(
    tmp_path: Path,
) -> None:
    experiment = _write_inputs(
        tmp_path,
        campaigns="""\
  uncertain:
    on_submit_failure: continue
    launches:
      - {name: first, seed: 1}
      - {name: second, seed: 2}
""",
    )
    planned = campaign_plan_operation(
        experiment,
        campaign_name="uncertain",
        targets_file=tmp_path / "targets.yaml",
        data_dir=tmp_path / "records",
    )
    assert planned.ok and planned.value is not None

    def submitter(
        launch: CampaignLaunchPlanValue,
        run_id: RunId,
        confirmed: int | None,
    ) -> OperationResult[RunId]:
        return OperationResult.failure(
            "submit", OperationError("SUBMISSION_OUTCOME_UNKNOWN", "uncertain")
        )

    campaign_id = CampaignId("campaign_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    result = campaign_submit_operation(
        planned.value,
        submitter=submitter,
        campaign_id_factory=lambda: campaign_id,
        framework_version="test",
    )

    assert not result.ok and result.error is not None
    assert result.error.code == "CAMPAIGN_SUBMISSION_OUTCOME_UNKNOWN"
    assert result.error.details["run_id"]
    record = JsonCampaignStore(tmp_path / "records").load(campaign_id)
    assert record.on_submit_failure is CampaignFailurePolicy.CONTINUE
    assert [item.submission_state for item in record.launches] == [
        CampaignSubmissionState.UNKNOWN,
        CampaignSubmissionState.PENDING,
    ]

    submitted: list[RunId] = []

    def resumer(
        run_id: RunId, data_dir: Path
    ) -> OperationResult[CampaignChildRecovery]:
        return OperationResult.success(
            "resume", CampaignChildRecovery(run_id, True, "resumed")
        )

    def resumed_submitter(
        launch: CampaignLaunchPlanValue,
        run_id: RunId,
        confirmed: int | None,
    ) -> OperationResult[RunId]:
        submitted.append(run_id)
        return OperationResult.success("submit", run_id)

    resumed = campaign_resume_operation(
        planned.value,
        campaign_id,
        resumer=resumer,
        submitter=resumed_submitter,
    )

    assert resumed.ok and resumed.value is not None
    assert submitted == [record.launches[1].run_id]
    assert [item.submission_state for item in resumed.value.record.launches] == [
        CampaignSubmissionState.SUBMITTED,
        CampaignSubmissionState.SUBMITTED,
    ]


def test_campaign_status_reports_unknown_without_querying_missing_child_run(
    tmp_path: Path,
) -> None:
    experiment = _write_inputs(
        tmp_path,
        campaigns="""\
  uncertain:
    launches: [{name: first, seed: 1}]
""",
    )
    planned = campaign_plan_operation(
        experiment,
        campaign_name="uncertain",
        targets_file=tmp_path / "targets.yaml",
        data_dir=tmp_path / "records",
    )
    assert planned.ok and planned.value is not None

    def submitter(
        launch: CampaignLaunchPlanValue,
        run_id: RunId,
        confirmed: int | None,
    ) -> OperationResult[RunId]:
        return OperationResult.failure(
            "submit", OperationError("SUBMISSION_OUTCOME_UNKNOWN", "uncertain")
        )

    campaign_id = CampaignId("campaign_cccccccccccccccccccccccccccccccc")
    campaign_submit_operation(
        planned.value,
        submitter=submitter,
        campaign_id_factory=lambda: campaign_id,
        framework_version="test",
    )

    status = campaign_status_operation(campaign_id, tmp_path / "records")
    inspected = campaign_inspect_operation(campaign_id, tmp_path / "records")

    assert status.ok and status.value is not None
    assert status.value.state == "UNKNOWN"
    assert status.value.task_counts == {"unknown": 1}
    assert inspected.ok and inspected.value is not None
    assert inspected.value.record.id == campaign_id
