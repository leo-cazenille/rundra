from __future__ import annotations

import pytest

from rundra.scheduler_registry import (
    scheduler_capabilities,
    scheduler_capabilities_document,
)


def test_builtin_scheduler_capabilities_are_explicit() -> None:
    assert not scheduler_capabilities("local").detached_submission
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


def test_unknown_scheduler_has_no_implicit_fallback() -> None:
    with pytest.raises(ValueError, match="Unsupported scheduler backend"):
        scheduler_capabilities("unknown")
