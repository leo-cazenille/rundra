from collections.abc import Sequence
from enum import StrEnum


class ExecutionState(StrEnum):
    """Portable lifecycle state shared by Runs and Tasks."""

    CREATED = "CREATED"
    STAGING = "STAGING"
    SUBMITTED = "SUBMITTED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


class RetrievalState(StrEnum):
    """Portable result-transfer state, independent from computation state."""

    NOT_REQUESTED = "NOT_REQUESTED"
    PENDING = "PENDING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


def aggregate_execution_state(states: Sequence[ExecutionState]) -> ExecutionState:
    """Derive one Run state without hiding active Tasks behind terminal outcomes.

    A Run becomes terminal only after every Task is terminal. Mixed terminal
    outcomes then use FAILED > CANCELLED > SUCCEEDED precedence. While work is
    active, RUNNING takes precedence; a completed or failed sibling also keeps
    the aggregate RUNNING while queued/submitted work remains. Otherwise
    QUEUED > SUBMITTED > STAGING > CREATED; UNKNOWN is used when no active
    state can describe the incomplete Task set.
    """
    if not isinstance(states, Sequence) or isinstance(states, (str, bytes)):
        raise TypeError("Aggregate execution states must be a sequence")
    normalized = tuple(states)
    if not normalized:
        raise ValueError("Aggregate execution states must not be empty")
    if any(type(state) is not ExecutionState for state in normalized):
        raise TypeError("Aggregate execution states must contain ExecutionState values")
    terminal = frozenset(
        {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }
    )
    if all(state in terminal for state in normalized):
        if ExecutionState.FAILED in normalized:
            return ExecutionState.FAILED
        if ExecutionState.CANCELLED in normalized:
            return ExecutionState.CANCELLED
        return ExecutionState.SUCCEEDED
    if ExecutionState.RUNNING in normalized or (
        any(
            state in {ExecutionState.SUCCEEDED, ExecutionState.FAILED}
            for state in normalized
        )
        and any(
            state
            in {
                ExecutionState.QUEUED,
                ExecutionState.SUBMITTED,
                ExecutionState.STAGING,
                ExecutionState.CREATED,
            }
            for state in normalized
        )
    ):
        return ExecutionState.RUNNING
    for state in (
        ExecutionState.QUEUED,
        ExecutionState.SUBMITTED,
        ExecutionState.STAGING,
        ExecutionState.CREATED,
    ):
        if state in normalized:
            return state
    return ExecutionState.UNKNOWN


def aggregate_retrieval_state(states: Sequence[RetrievalState]) -> RetrievalState:
    """Summarize per-Task retrieval without claiming partial work is complete."""
    if not isinstance(states, Sequence) or isinstance(states, (str, bytes)):
        raise TypeError("Aggregate retrieval states must be a sequence")
    normalized = tuple(states)
    if not normalized:
        raise ValueError("Aggregate retrieval states must not be empty")
    if any(type(state) is not RetrievalState for state in normalized):
        raise TypeError("Aggregate retrieval states must contain RetrievalState values")
    if all(state is RetrievalState.NOT_REQUESTED for state in normalized):
        return RetrievalState.NOT_REQUESTED
    if all(state is RetrievalState.SUCCEEDED for state in normalized):
        return RetrievalState.SUCCEEDED
    if RetrievalState.FAILED in normalized:
        return RetrievalState.FAILED
    return RetrievalState.PENDING


_ALLOWED_EXECUTION_TRANSITIONS: dict[ExecutionState, frozenset[ExecutionState]] = {
    ExecutionState.CREATED: frozenset(
        {ExecutionState.STAGING, ExecutionState.FAILED, ExecutionState.CANCELLED}
    ),
    ExecutionState.STAGING: frozenset(
        {
            ExecutionState.SUBMITTED,
            ExecutionState.RUNNING,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }
    ),
    ExecutionState.SUBMITTED: frozenset(
        {
            ExecutionState.QUEUED,
            ExecutionState.RUNNING,
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
            ExecutionState.UNKNOWN,
        }
    ),
    ExecutionState.QUEUED: frozenset(
        {
            ExecutionState.RUNNING,
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
            ExecutionState.UNKNOWN,
        }
    ),
    ExecutionState.RUNNING: frozenset(
        {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
            ExecutionState.UNKNOWN,
        }
    ),
    ExecutionState.SUCCEEDED: frozenset(),
    ExecutionState.FAILED: frozenset(),
    ExecutionState.CANCELLED: frozenset(),
    ExecutionState.UNKNOWN: frozenset(
        {
            ExecutionState.SUBMITTED,
            ExecutionState.QUEUED,
            ExecutionState.RUNNING,
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        }
    ),
}

_ALLOWED_RETRIEVAL_TRANSITIONS: dict[RetrievalState, frozenset[RetrievalState]] = {
    RetrievalState.NOT_REQUESTED: frozenset({RetrievalState.PENDING}),
    RetrievalState.PENDING: frozenset(
        {RetrievalState.SUCCEEDED, RetrievalState.FAILED}
    ),
    RetrievalState.SUCCEEDED: frozenset(),
    RetrievalState.FAILED: frozenset({RetrievalState.PENDING}),
}


def validate_execution_transition(
    current: ExecutionState,
    target: ExecutionState,
) -> None:
    """Raise when a portable Run or Task state transition is invalid."""
    if type(current) is not ExecutionState or type(target) is not ExecutionState:
        raise TypeError("Execution state transitions require ExecutionState values")
    if current == target:
        return
    if target not in _ALLOWED_EXECUTION_TRANSITIONS[current]:
        raise ValueError(f"Invalid execution state transition: {current} -> {target}")


def validate_retrieval_transition(
    current: RetrievalState,
    target: RetrievalState,
) -> None:
    """Raise when a result-retrieval state transition is invalid."""
    if type(current) is not RetrievalState or type(target) is not RetrievalState:
        raise TypeError("Retrieval state transitions require RetrievalState values")
    if current == target:
        return
    if target not in _ALLOWED_RETRIEVAL_TRANSITIONS[current]:
        raise ValueError(f"Invalid retrieval state transition: {current} -> {target}")
