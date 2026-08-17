from __future__ import annotations

from pathlib import Path

from rundra.cli.doctor import doctor_operation


def test_doctor_accepts_a_writable_local_target(tmp_path: Path) -> None:
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""version: 1
targets:
  local:
    transport: {{type: local}}
    scheduler: {{type: local}}
    staging: {{type: local}}
    container: {{type: native}}
    workspace: {tmp_path / "workspace"}
""",
        encoding="utf-8",
    )

    result = doctor_operation(targets, "local")

    assert result.ok and result.value is not None
    assert result.value.ready
    assert [check.status for check in result.value.checks] == ["pass", "pass"]


def test_doctor_reports_missing_target_without_connecting(tmp_path: Path) -> None:
    targets = tmp_path / "targets.yaml"
    targets.write_text("version: 1\ntargets: {}\n", encoding="utf-8")

    result = doctor_operation(targets, "absent", connect=True)

    assert result.error is not None
    assert result.error.code == "TARGET_NOT_FOUND"
