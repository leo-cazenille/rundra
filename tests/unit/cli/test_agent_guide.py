from pathlib import Path

from rundra.cli.agent_guide import GUIDE, agent_guide_operation


def test_agent_guide_print_write_update_and_check(tmp_path: Path) -> None:
    path = tmp_path / "AGENTS.md"
    printed = agent_guide_operation()
    written = agent_guide_operation(write=path)
    path.write_text(
        f"# Existing\n\n{path.read_text(encoding='utf-8')}", encoding="utf-8"
    )
    updated = agent_guide_operation(write=path)
    checked = agent_guide_operation(check=path)

    assert printed.ok and printed.value is not None
    assert printed.value.content == GUIDE
    assert "https://pypi.org/project/rundra/" in printed.value.content
    assert "installed `rundr help`" in printed.value.content
    assert "rundr doctor --agent codex --json" in printed.value.content
    assert written.ok and updated.ok and checked.ok
    assert path.read_text(encoding="utf-8").count("rundra-agent:start") == 1


def test_agent_guide_check_reports_drift_and_rejects_bad_markers(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing.md"
    missing.write_text("# No guide\n", encoding="utf-8")
    malformed = tmp_path / "malformed.md"
    malformed.write_text("<!-- rundra-agent:start -->\n", encoding="utf-8")

    drift = agent_guide_operation(check=missing)
    bad = agent_guide_operation(write=malformed)

    assert drift.error is not None
    assert drift.error.code == "AGENT_GUIDE_OUTDATED"
    assert bad.error is not None
    assert bad.error.code == "AGENT_GUIDE_WRITE_FAILED"
