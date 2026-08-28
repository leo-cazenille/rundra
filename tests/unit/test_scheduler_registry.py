from __future__ import annotations

import pytest

from rundra.scheduler_registry import (
    scheduler_capabilities,
    scheduler_capabilities_document,
    scheduler_kinds,
    scheduler_required_tools,
)


def test_builtin_scheduler_capabilities_are_explicit() -> None:
    assert not scheduler_capabilities("local").detached_submission
    assert not scheduler_capabilities("local").arrays
    assert scheduler_capabilities("local").materialized_worker_pool
    assert not scheduler_capabilities("local").compact_worker_pool
    assert not scheduler_capabilities("local").bundled_worker_pool
    assert scheduler_capabilities("slurm").scheduler_requeue_recovery
    assert scheduler_capabilities("pbs").compact_worker_pool
    assert not scheduler_capabilities("pbs").scheduler_requeue_recovery
    assert scheduler_capabilities_document("pbs") == {
        "arrays": True,
        "compact_worker_pool": True,
        "dependencies": True,
        "detached_submission": True,
        "scheduler_probe": True,
        "scheduler_requeue_recovery": False,
    }
    assert scheduler_capabilities_document("htcondor") == {
        "arrays": True,
        "compact_worker_pool": False,
        "dependencies": False,
        "detached_submission": True,
        "scheduler_probe": True,
        "scheduler_requeue_recovery": False,
    }
    assert scheduler_kinds() == frozenset({"local", "slurm", "pbs", "htcondor"})
    assert scheduler_required_tools("htcondor") == (
        "condor_submit",
        "condor_q",
        "condor_history",
        "condor_rm",
        "condor_version",
    )


def test_unknown_scheduler_has_no_implicit_fallback() -> None:
    with pytest.raises(ValueError, match="Unsupported scheduler backend"):
        scheduler_capabilities("unknown")
