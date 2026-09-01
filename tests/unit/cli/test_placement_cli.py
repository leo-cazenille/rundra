from __future__ import annotations

from rundra.cli.main import build_parser


def test_plan_accepts_automatic_placement_and_candidate_targets() -> None:
    arguments = build_parser().parse_args(
        (
            "plan",
            "experiment.yaml",
            "--seeds",
            "0:99",
            "--placement",
            "auto",
            "--candidate-target",
            "shoal",
            "--candidate-target",
            "isircluster",
        )
    )

    assert arguments.placement == "auto"
    assert arguments.candidate_targets == ["shoal", "isircluster"]


def test_doctor_accepts_seed_range_for_automatic_placement() -> None:
    arguments = build_parser().parse_args(
        (
            "doctor",
            "experiment.yaml",
            "--seeds",
            "0:99",
            "--placement",
            "auto",
            "--connect",
        )
    )

    assert arguments.seeds == "0:99"
    assert arguments.connect is True
