from __future__ import annotations

import pytest


def test_execution_transition_table_is_explicit_and_complete() -> None:
    from rundra.domain.states import ExecutionState, validate_execution_transition

    allowed = {
        ("CREATED", "STAGING"),
        ("CREATED", "FAILED"),
        ("CREATED", "CANCELLED"),
        ("STAGING", "SUBMITTED"),
        ("STAGING", "RUNNING"),
        ("STAGING", "FAILED"),
        ("STAGING", "CANCELLED"),
        ("SUBMITTED", "QUEUED"),
        ("SUBMITTED", "RUNNING"),
        ("SUBMITTED", "SUCCEEDED"),
        ("SUBMITTED", "FAILED"),
        ("SUBMITTED", "CANCELLED"),
        ("SUBMITTED", "UNKNOWN"),
        ("QUEUED", "RUNNING"),
        ("QUEUED", "SUCCEEDED"),
        ("QUEUED", "FAILED"),
        ("QUEUED", "CANCELLED"),
        ("QUEUED", "UNKNOWN"),
        ("RUNNING", "SUCCEEDED"),
        ("RUNNING", "FAILED"),
        ("RUNNING", "CANCELLED"),
        ("RUNNING", "UNKNOWN"),
        ("UNKNOWN", "SUBMITTED"),
        ("UNKNOWN", "QUEUED"),
        ("UNKNOWN", "RUNNING"),
        ("UNKNOWN", "SUCCEEDED"),
        ("UNKNOWN", "FAILED"),
        ("UNKNOWN", "CANCELLED"),
    }
    for current in ExecutionState:
        for target in ExecutionState:
            if current is target or (current.value, target.value) in allowed:
                validate_execution_transition(current, target)
            else:
                with pytest.raises(ValueError):
                    validate_execution_transition(current, target)


def test_retrieval_transition_table_is_explicit_and_complete() -> None:
    from rundra.domain.states import RetrievalState, validate_retrieval_transition

    allowed = {
        ("NOT_REQUESTED", "PENDING"),
        ("PENDING", "SUCCEEDED"),
        ("PENDING", "FAILED"),
        ("FAILED", "PENDING"),
    }
    for current in RetrievalState:
        for target in RetrievalState:
            if current is target or (current.value, target.value) in allowed:
                validate_retrieval_transition(current, target)
            else:
                with pytest.raises(ValueError):
                    validate_retrieval_transition(current, target)


@pytest.mark.parametrize("validator", ["execution", "retrieval"])
def test_transition_validators_reject_non_enum_inputs(validator: str) -> None:
    from rundra.domain.states import (
        validate_execution_transition,
        validate_retrieval_transition,
    )

    function = (
        validate_execution_transition
        if validator == "execution"
        else validate_retrieval_transition
    )
    with pytest.raises(TypeError, match="state"):
        function("CREATED", "RUNNING")


def test_execution_state_contains_exactly_the_portable_v01_states() -> None:
    try:
        from rundra.domain.states import ExecutionState
    except ModuleNotFoundError:
        pytest.fail("ExecutionState is not implemented")

    assert {state.value for state in ExecutionState} == {
        "CREATED",
        "STAGING",
        "SUBMITTED",
        "QUEUED",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
        "UNKNOWN",
    }


@pytest.mark.parametrize(
    "states",
    [
        ("CREATED", "STAGING", "RUNNING", "SUCCEEDED"),
        ("CREATED", "STAGING", "SUBMITTED", "QUEUED", "RUNNING", "FAILED"),
        ("SUBMITTED", "UNKNOWN", "RUNNING", "CANCELLED"),
        ("RUNNING", "RUNNING", "SUCCEEDED"),
    ],
)
def test_execution_transition_validation_accepts_supported_lifecycles(
    states: tuple[str, ...],
) -> None:
    try:
        from rundra.domain.states import (
            ExecutionState,
            validate_execution_transition,
        )
    except ImportError:
        pytest.fail("Execution transition validation is not implemented")

    portable_states = tuple(ExecutionState(state) for state in states)
    for current, target in zip(portable_states[:-1], portable_states[1:], strict=True):
        validate_execution_transition(current, target)


@pytest.mark.parametrize(
    "current, target",
    [
        ("CREATED", "RUNNING"),
        ("STAGING", "QUEUED"),
        ("QUEUED", "STAGING"),
        ("SUCCEEDED", "RUNNING"),
        ("FAILED", "RUNNING"),
        ("CANCELLED", "QUEUED"),
    ],
)
def test_execution_transition_validation_rejects_invalid_lifecycles(
    current: str,
    target: str,
) -> None:
    from rundra.domain.states import ExecutionState, validate_execution_transition

    with pytest.raises(ValueError, match=f"{current}.*{target}"):
        validate_execution_transition(ExecutionState(current), ExecutionState(target))


def test_retrieval_state_is_distinct_and_allows_failed_fetch_retry() -> None:
    try:
        from rundra.domain.states import (
            RetrievalState,
            validate_retrieval_transition,
        )
    except ImportError:
        pytest.fail("Retrieval state validation is not implemented")

    assert {state.value for state in RetrievalState} == {
        "NOT_REQUESTED",
        "PENDING",
        "SUCCEEDED",
        "FAILED",
    }
    validate_retrieval_transition(
        RetrievalState.NOT_REQUESTED,
        RetrievalState.PENDING,
    )
    validate_retrieval_transition(RetrievalState.PENDING, RetrievalState.FAILED)
    validate_retrieval_transition(RetrievalState.FAILED, RetrievalState.PENDING)
    validate_retrieval_transition(RetrievalState.PENDING, RetrievalState.SUCCEEDED)


def test_retrieval_state_rejects_reopening_a_successful_fetch() -> None:
    from rundra.domain.states import RetrievalState, validate_retrieval_transition

    with pytest.raises(ValueError, match="SUCCEEDED.*PENDING"):
        validate_retrieval_transition(
            RetrievalState.SUCCEEDED,
            RetrievalState.PENDING,
        )
