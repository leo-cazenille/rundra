from pathlib import Path

from rundra.cli.agent_guide import GUIDE, GUIDE_TOPICS, agent_guide_operation
from rundra.cli.main import main


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
    assert "--local-target-access" in printed.value.content
    assert "rundr await RUN_ID... --json" in printed.value.content
    assert "transcript tokens" in printed.value.content
    assert written.ok and updated.ok and checked.ok
    assert path.read_text(encoding="utf-8").count("rundra-agent:start") == 1


def test_agent_guide_exposes_bounded_topics() -> None:
    topics = agent_guide_operation(list_topics=True)
    large = agent_guide_operation(topic="large-runs")
    scratch = agent_guide_operation(topic="scratch")
    unknown = agent_guide_operation(topic="missing")

    assert topics.ok and topics.value is not None
    assert all(name in topics.value.content for name in GUIDE_TOPICS)
    assert large.ok and large.value is not None
    assert "worker-pool" in large.value.content
    assert scratch.ok and scratch.value is not None
    assert "scheduler-provided scratch root" in scratch.value.content
    assert unknown.error is not None
    assert unknown.error.code == "UNKNOWN_GUIDE_TOPIC"


def test_agent_guide_plain_cli_renders_topic_content(capsys: object) -> None:
    topics_exit = main(("agent-guide", "--list-topics"))
    topics_output = capsys.readouterr().out  # type: ignore[attr-defined]
    topic_exit = main(("agent-guide", "--topic", "large-runs"))
    topic_output = capsys.readouterr().out  # type: ignore[attr-defined]

    assert topics_exit == 0
    assert all(name in topics_output for name in GUIDE_TOPICS)
    assert topics_output.strip() != "None"
    assert topic_exit == 0
    assert "worker-pool" in topic_output
    assert topic_output.strip() != "None"


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
