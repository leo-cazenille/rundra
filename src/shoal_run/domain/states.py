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
