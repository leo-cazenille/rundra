from __future__ import annotations

import json
from pathlib import Path

from rundra.cli.export_operations import ExportValue
from rundra.cli.render import render_json
from rundra.cli.retry_operations import RetryValue
from rundra.domain.models import RunId
from rundra.results import OperationResult

_FIXTURES = Path(__file__).parents[2] / "docs/schemas"
_RUN = RunId("run_0123456789abcdef0123456789abcdef")


def test_retry_preview_matches_documented_shape() -> None:
    preview = RetryValue(
        _RUN,
        "cluster",
        1,
        (("task_000009", "task_000000", 42, "parameter_set_000001"),),
    )
    expected = (_FIXTURES / "retry-preview-v1.json").read_text(encoding="utf-8")
    assert json.loads(
        render_json(OperationResult.success("retry", preview))
    ) == json.loads(expected)


def test_export_result_matches_documented_shape() -> None:
    export = ExportValue(str(_RUN), Path("/local/archive/run.tar.gz"), 1, 0, "a" * 64)
    expected = (_FIXTURES / "export-success-v1.json").read_text(encoding="utf-8")
    assert json.loads(
        render_json(OperationResult.success("export", export))
    ) == json.loads(expected)
