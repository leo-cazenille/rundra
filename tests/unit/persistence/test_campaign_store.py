from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from rundra.domain.campaigns import (
    CampaignFailurePolicy,
    CampaignId,
    CampaignLaunchRecord,
    CampaignRecord,
    CampaignSubmissionState,
)
from rundra.domain.models import RunId
from rundra.persistence import (
    JsonCampaignStore,
    campaign_record_from_dict,
    campaign_record_to_dict,
)


def _record() -> CampaignRecord:
    return CampaignRecord(
        format_version=1,
        framework_version="0.1.7",
        id=CampaignId("campaign_0123456789abcdef0123456789abcdef"),
        name="dual",
        source=PurePosixPath("/work/campaign.yaml"),
        experiment_source=PurePosixPath("/work/experiment.yaml"),
        created_at=datetime(2026, 9, 1, tzinfo=UTC),
        on_submit_failure=CampaignFailurePolicy.CANCEL,
        allow_duplicate_tasks=False,
        launches=(
            CampaignLaunchRecord(
                "shoal",
                RunId("run_0123456789abcdef0123456789abcdef"),
                "shoal",
                32,
                PurePosixPath("/work/results/shoal"),
            ),
        ),
    )


def test_campaign_record_round_trips_and_updates_atomically(tmp_path: Path) -> None:
    record = _record()
    store = JsonCampaignStore(tmp_path)

    store.create(record)
    loaded = store.load(record.id)
    updated = replace(
        loaded,
        launches=(
            replace(
                loaded.launches[0], submission_state=CampaignSubmissionState.SUBMITTED
            ),
        ),
    )
    store.update(updated, expected=loaded)

    assert store.load(record.id) == updated
    assert store.list() == (updated,)
    assert campaign_record_from_dict(campaign_record_to_dict(updated)) == updated
