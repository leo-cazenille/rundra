from __future__ import annotations

from datetime import UTC, datetime
from pathlib import PurePosixPath

from rundra.domain.campaigns import (
    CampaignFailurePolicy,
    CampaignId,
    CampaignLaunchRecord,
    CampaignRecord,
)
from rundra.domain.models import RunId
from rundra.domain.placement import PlacementDecision, PlacementTargetDecision
from rundra.persistence.campaign_store import (
    campaign_record_from_dict,
    campaign_record_to_dict,
)


def test_campaign_record_v2_round_trips_automatic_placement() -> None:
    observed = datetime(2026, 9, 1, 12, tzinfo=UTC)
    placement = PlacementDecision(
        "balanced",
        "available_capacity",
        observed,
        (
            PlacementTargetDecision(
                "shoal",
                True,
                "selected",
                partition="cpu",
                utilization_percent=25,
                idle_cpus=24,
                planned_capacity=32,
                usable_capacity=24,
                assigned_seed_start=0,
                assigned_seed_stop=7,
            ),
            PlacementTargetDecision(
                "busy", False, "utilization threshold reached", utilization_percent=95
            ),
        ),
    )
    record = CampaignRecord(
        2,
        "test",
        CampaignId("campaign_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        "automatic",
        PurePosixPath("/records/campaign-plans/automatic.yaml"),
        PurePosixPath("/project/experiment.yaml"),
        observed,
        CampaignFailurePolicy.CANCEL,
        False,
        (
            CampaignLaunchRecord(
                "shoal",
                RunId("run_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                "shoal",
                8,
                PurePosixPath("/project/results/shoal"),
            ),
        ),
        placement=placement,
    )

    restored = campaign_record_from_dict(campaign_record_to_dict(record))

    assert restored == record
    assert restored.placement is not None
    assert restored.placement.selected_targets == ("shoal",)
