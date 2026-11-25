from __future__ import annotations

import json
from pathlib import Path

from rundra.cli.operations import RunDiagnosticsValue
from rundra.cli.render import _diagnostics_document
from rundra.domain.models import RunId
from rundra.domain.states import ExecutionState, RetrievalState
from rundra.execution_diagnostics import ExecutionDiagnostic


def test_run_diagnostics_matches_version_one_contract() -> None:
    actual = _diagnostics_document(
        RunDiagnosticsValue(
            RunId("run_0123456789abcdef0123456789abcdef"),
            ExecutionState.FAILED,
            RetrievalState.NOT_REQUESTED,
            {"FAILED": 32},
            ("37",),
            ("cpu02",),
            "apptainer",
            "3.8.4",
            "BUNDLE_JOURNAL_MISSING",
            "available",
            (ExecutionDiagnostic("worker", "37_0", "SCRATCH_SYMLINK", 72, "cpu02"),),
            False,
        )
    )
    fixture = Path(__file__).parents[2] / "docs/schemas/run-diagnostics-v1.json"
    assert actual == json.loads(fixture.read_text(encoding="utf-8"))
