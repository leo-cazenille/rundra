import pytest

from rundra.results import OperationError, OperationResult


def test_operation_result_represents_exactly_one_success_or_error() -> None:
    success = OperationResult.success("validate", "experiment")
    failure = OperationResult.failure(
        "validate", OperationError("INVALID", "invalid input", {"path": ("name",)})
    )

    assert success.ok and success.value == "experiment"
    assert not failure.ok and failure.error is not None
    assert failure.error.details["path"] == ("name",)
    with pytest.raises(ValueError, match="exactly one"):
        OperationResult[str](operation="validate")
