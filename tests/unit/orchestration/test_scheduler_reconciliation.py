from rundra.domain.states import ExecutionState
from rundra.orchestration.service import _monotonic_observed_state


def test_scheduler_observations_do_not_regress_portable_execution_state() -> None:
    assert (
        _monotonic_observed_state(ExecutionState.RUNNING, ExecutionState.SUBMITTED)
        is ExecutionState.RUNNING
    )
    assert (
        _monotonic_observed_state(ExecutionState.RUNNING, ExecutionState.QUEUED)
        is ExecutionState.RUNNING
    )
    assert (
        _monotonic_observed_state(ExecutionState.QUEUED, ExecutionState.SUBMITTED)
        is ExecutionState.QUEUED
    )
    assert (
        _monotonic_observed_state(ExecutionState.RUNNING, ExecutionState.SUCCEEDED)
        is ExecutionState.SUCCEEDED
    )
