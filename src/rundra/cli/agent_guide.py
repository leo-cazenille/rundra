from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from rundra.results import OperationError, OperationResult

START_MARKER = "<!-- rundra-agent:start -->"
END_MARKER = "<!-- rundra-agent:end -->"

GUIDE = f"""{START_MARKER}
## Rundra experiment execution

- On a new machine or agent session, run `rundr doctor --agent codex --json`
  before attempting an experiment. Apply only the reported permissions, start a
  new agent session, and rerun the audit until `ready` is true.
- Use Rundra for scientific execution; do not invoke SSH, rsync, Slurm, or
  Apptainer directly except while diagnosing an explicit Rundra error.
- Run `rundr doctor EXPERIMENT --connect --agent codex --json` and `rundr plan
  EXPERIMENT` before consuming cluster resources. Use the explicit
  `--scheduler-probe` only when a bounded no-op scheduler submission is wanted.
  Review task count, seeds, resources, concurrency, and retrieval strategy.
- When the client mounts target storage directly, or before cluster system
  tests that use target-resident files, add `--local-target-access`. Shared
  staging enables this audit automatically. Apply the reported workspace,
  preparation-cache, and image-search-path permissions before continuing.
- Use explicit seeds for reproducibility. Above a target safety threshold, pass
  the exact requested `--confirm-tasks N` value only after reviewing the plan.
- Use `rundr help` to discover available operations and the common workflow.
  Use `rundr help COMMAND` for command-specific arguments and options.
- See https://pypi.org/project/rundra/ for installation and overview
  documentation. That page describes the latest release; `rundr version` and
  the installed `rundr help` output are authoritative for the local version.
- Treat help output as guidance only. Use `--json` or Rundra MCP tools for
  structured automation; do not parse human-oriented help text.
- Prefer `rundr submit EXPERIMENT`, then `rundr wait RUN_ID`, then
  `rundr fetch RUN_ID` for long Runs. Use `--destination PATH` only to override
  the configuration-based default. Use `rundr run` only when keeping the client
  attached is appropriate.
- Preserve the Run ID and the exact `--data-dir` used at submission. Lifecycle
  commands must use the same Run store. `--last` is convenient interactively,
  but agents should retain explicit Run IDs to avoid selecting concurrent work.
- Continue an interrupted submit with `rundr resume RUN_ID`. Do not repeat the
  submission as a new Run until Rundra has resolved the recorded scheduler
  outcome; an unknown outcome intentionally blocks automatic resubmission.
- Use `--json` or Rundra MCP tools. Never parse scheduler-native output.
- Use paginated `rundr list --json` Run summaries for discovery and `rundr
  tasks RUN_ID --json` for Task pages. Request `list --include-tasks` only when
  an expanded cross-Run response is specifically needed.
- Run scientific and analysis workloads on the configured execution target or
  an approved workstation, never on a login/controller host.
- Keep raw retrieved results separate from derived analysis outputs.
- Prefer `rundr fetch RUN_ID` with its default auto mode. Rundra verifies shared
  visibility and avoids bulk transfer when safe; use `--mode copy` only when a
  materialized local result tree is required.
- Use `rundr cancel` for active work. Preview deletion with `rundr purge
  RUN_ID --dry-run`; purge only with exact Run-ID confirmation.
- Never place SSH keys, tokens, passwords, or other credentials in experiment,
  project, target, agent, or RunRecord files.
{END_MARKER}
"""


@dataclass(frozen=True, slots=True)
class AgentGuideValue:
    content: str
    action: str
    path: Path | None = None


def agent_guide_operation(
    *, write: Path | None = None, check: Path | None = None
) -> OperationResult[AgentGuideValue]:
    if write is not None and check is not None:
        return OperationResult.failure(
            "agent-guide",
            OperationError("CLI_USAGE_ERROR", "--write and --check are exclusive"),
        )
    if check is not None:
        try:
            current = check.read_text(encoding="utf-8")
            installed = _installed_section(current)
        except (OSError, ValueError) as error:
            return OperationResult.failure(
                "agent-guide", OperationError("AGENT_GUIDE_CHECK_FAILED", str(error))
            )
        if installed != GUIDE.rstrip("\n"):
            return OperationResult.failure(
                "agent-guide",
                OperationError(
                    "AGENT_GUIDE_OUTDATED",
                    f"Rundra instructions in {check} are missing or outdated",
                    {"source": str(check)},
                ),
            )
        return OperationResult.success(
            "agent-guide", AgentGuideValue(GUIDE, "current", check)
        )
    if write is None:
        return OperationResult.success("agent-guide", AgentGuideValue(GUIDE, "printed"))
    try:
        current = write.read_text(encoding="utf-8") if write.exists() else ""
        updated = _merge_guide(current)
        _atomic_write(write, updated)
    except (OSError, ValueError) as error:
        return OperationResult.failure(
            "agent-guide", OperationError("AGENT_GUIDE_WRITE_FAILED", str(error))
        )
    return OperationResult.success(
        "agent-guide", AgentGuideValue(GUIDE, "written", write)
    )


def _installed_section(content: str) -> str | None:
    starts = content.count(START_MARKER)
    ends = content.count(END_MARKER)
    if starts == 0 and ends == 0:
        return None
    if starts != 1 or ends != 1:
        raise ValueError("AGENTS.md contains malformed Rundra markers")
    start = content.index(START_MARKER)
    end = content.index(END_MARKER, start) + len(END_MARKER)
    return content[start:end]


def _merge_guide(content: str) -> str:
    installed = _installed_section(content)
    section = GUIDE.rstrip("\n")
    if installed is None:
        prefix = content.rstrip("\n")
        return f"{prefix}\n\n{section}\n" if prefix else f"{section}\n"
    start = content.index(START_MARKER)
    end = content.index(END_MARKER, start) + len(END_MARKER)
    return f"{content[:start]}{section}{content[end:]}"


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        if path.exists():
            temporary.chmod(path.stat().st_mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
