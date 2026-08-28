from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from rundra.results import OperationError, OperationResult

START_MARKER = "<!-- rundra-agent:start -->"
END_MARKER = "<!-- rundra-agent:end -->"

GUIDE_TOPICS = {
    "setup": "Run doctor --agent codex --json first. Grant only reported paths and network access, restart the agent sandbox when required, then rerun doctor until ready is true.",
    "upgrade": "After installing a new Rundra release, run rundr version, review the release's Agent action required section in CHANGELOG.md or the PyPI changelog, refresh managed instructions with rundr agent-guide --write AGENTS.md, and rerun rundr doctor --agent codex --json before submitting work.",
    "launch": "Validate and plan before submit. Use explicit seeds, review task count/resources/concurrency, retain the returned Run ID and data directory, and submit only once.",
    "large-runs": "Use worker-pool execution only when plan.target.scheduler.capabilities.compact_worker_pool is true. Runs with at least 1,000 Tasks automatically use compact durable Task state; inspect individuals with paginated tasks JSON, use bounded wait calls without progress, retain archive retrieval, and pass an exact confirm-tasks value after plan review. OpenPBS worker targets require requeue_limit 0 because scheduler rerun recovery is not supported.",
    "htcondor": "Use targets version 9 and require the operator to confirm shared_workspace: true only when the access point and execute nodes see the workspace at the same absolute path. HTCondor supports detached Task clusters and arrays, but not Rundra compact workers, preparation dependencies, file transfer, or scheduler rerun recovery. Inspect plan.target.scheduler.capabilities instead of inferring support from the backend name.",
    "lifecycle": "Use submit, await for one or several long Runs, status/tasks, fetch, then purge. Await emits one final compact result and supports all/any aggregate conditions, avoiding model-driven polling. Dependency-pending workers remain queued before journals exist; Rundra merges identical atomic journal fragments and rejects contradictory outcomes. ETA is omitted until at least 20 Tasks, 10% completion, and 60 seconds of evidence. Agents should use explicit Run IDs rather than --last.",
    "results": "Prefer fetch auto. Set project-v5 fetch_mode: copy when downstream analysis requires ordinary files instead of a shared reference manifest. Compact archive fetch verifies exact Task coverage; add --extract only when individual files are required. Keep derived outputs separate.",
    "scratch": "Treat target-v10+ execution_storage as an operator-owned Slurm contract. Plan reports CPU/GPU variables and copy-back; doctor --scheduler-probe verifies the CPU scratch path inside one bounded allocation. Rundra stages source, config, and verified images locally, copies every Task output back before success, and never owns or deletes the scheduler-provided scratch root.",
    "partitions": "Target v11 may declare ordered Slurm partition_routes by CPU/GPU class and maximum walltime. Plan stays offline and selects the shortest compatible route. Use doctor --connect --scheduler-inventory --json for read-only operator onboarding; it submits no job. Never guess a partition or bypass a configured route with native options.",
    "preparation": "Pin acquired images. Definition projects v4+ declare an explicit context include list; Rundra hashes only that context plus the definition for image-cache identity. Scientific jobs use Rundra-owned afterok dependencies and must not be resubmitted while preparation runs.",
    "provenance": "Inspect the Run record after submission. Prepared Runs record the verified image digest; actual launches record container_runtime and container_runtime_version when available. Plan and doctor intentionally do not claim execution-time runtime identity.",
    "recovery": "Definition preparation and scientific work use a durable afterok dependency, so submit does not wait for the image build. After any interrupted submit, resume the same Run ID; never submit a duplicate. Resolve manually only after proving no scheduler job exists.",
}

GUIDE = f"""{START_MARKER}
## Rundra experiment execution

- On a new machine or agent session, run `rundr doctor --agent codex --json`
  before attempting an experiment. Apply only the reported permissions, start a
  new agent session, and rerun the audit until `ready` is true.
- Use Rundra for scientific execution; do not invoke SSH, rsync,
  scheduler-native, or Apptainer commands directly except while diagnosing an
  explicit Rundra error.
- Run `rundr doctor EXPERIMENT --connect --agent codex --json` and `rundr plan
  EXPERIMENT` before consuming cluster resources. Use the explicit
  `--scheduler-probe` only when a bounded no-op scheduler submission is wanted.
  Review task count, seeds, resources, concurrency, and retrieval strategy.
- Do not add `--offline` to a first preparation run. Use `rundr doctor
  EXPERIMENT --offline --json` only when execution must avoid Git fetches and
  image pulls; `ready` then proves the immutable local preparation inputs are
  already cached. Warm a missing cache by running once without `--offline`.
- When the client mounts target storage directly, or before cluster system
  tests that use target-resident files, add `--local-target-access`. Shared
  staging enables this audit automatically. Apply the reported workspace,
  preparation-cache, and image-search-path permissions before continuing.
- Use explicit seeds for reproducibility. Above a target safety threshold, pass
  the exact requested `--confirm-tasks N` value only after reviewing the plan.
- Read scheduler capabilities from structured `targets`, `doctor`, or `plan`
  JSON. Do not infer arrays, dependencies, worker pools, or rerun recovery from
  a scheduler name. OpenPBS worker pools require target `requeue_limit: 0`.
- For target-v10 allocation scratch, review `plan` storage effects and use
  `doctor --scheduler-probe` once when onboarding the target. Never substitute
  a guessed scratch path or treat the scheduler-provided root as durable data.
- For target-v11 Slurm routing, declare an explicit walltime and inspect the
  selected partition in `plan --json`. Operators may use `doctor --connect
  --scheduler-inventory --json` to validate configured routes without submitting
  work; agents must not parse `sinfo` output themselves.
- Use `rundr help` to discover available operations and the common workflow.
  Use `rundr help COMMAND` for command-specific arguments and options.
- Use `rundr agent-guide --list-topics` and `rundr agent-guide --topic TOPIC`
  for bounded workflow-specific guidance instead of rereading this full guide.
- See https://pypi.org/project/rundra/ for installation and overview
  documentation. That page describes the latest release; `rundr version` and
  the installed `rundr help` output are authoritative for the local version.
- After upgrading Rundra, use `rundr agent-guide --topic upgrade`, refresh this
  managed section with `rundr agent-guide --write AGENTS.md`, and rerun doctor
  before submitting work.
- Treat help output as guidance only. Use `--json` or Rundra MCP tools for
  structured automation; do not parse human-oriented help text.
- Prefer `rundr submit EXPERIMENT`, then `rundr wait RUN_ID`, then
  `rundr fetch RUN_ID` for long Runs. Fetch reuses the absolute destination
  persisted by submit; use `--destination PATH` only to override it, such as on
  another workstation. Use `rundr run` only when keeping the client attached
  is appropriate.
- For unattended agents, use `rundr await RUN_ID... --json`: it emits one final
  compact document when all Runs finish. Use `--until any` only for intentional
  first-completion workflows and `--timeout` when the harness imposes a deadline.
  The harness should block on this process rather than wake the model to poll.
  Reserve `--progress` for interactive humans because captured TQDM redraws can
  consume transcript tokens. `--notify-file PATH` adds an atomic aggregate signal.
- Workers waiting on preparation remain `QUEUED` before their status journals
  exist. Rundra merges identical events that overlap during atomic journal
  publication and reports contradictory outcomes as corruption. Do not bypass
  a Rundra journal error by inferring success from scheduler output alone.
- ETA is intentionally absent until at least 20 Tasks and 10 percent of the Run
  have finished over at least 60 seconds. Treat any ETA as an estimate for the
  observed workload mix, not a deadline for heterogeneous Tasks.
- Preserve the Run ID and the exact `--data-dir` used at submission. Lifecycle
  commands must use the same Run store. `--last` is convenient interactively,
  but agents should retain explicit Run IDs to avoid selecting concurrent work.
- Continue an interrupted submit with `rundr resume RUN_ID`. Do not repeat the
  submission as a new Run until Rundra has resolved the recorded scheduler
  outcome; an unknown outcome intentionally blocks automatic resubmission.
  MCP clients use the equivalent `resume_submission` tool.
- If `resume` reports an unknown outcome, inspect the scheduler through an
  approved read-only route. Only after verifying that no job exists, close the
  Run with `rundr resolve-submission RUN_ID --not-submitted --confirm RUN_ID`.
  Never use this command merely because a scheduler query is inconvenient.
  MCP clients use the equivalent `resolve_submission` tool with the same exact
  Run-ID confirmation.
- Use `--json` or Rundra MCP tools. Never parse scheduler-native output.
- Use paginated `rundr list --json` Run summaries for discovery and `rundr
  tasks RUN_ID --json` for Task pages. Request `list --include-tasks` only when
  an expanded cross-Run response is specifically needed.
- Run scientific and analysis workloads on the configured execution target or
  an approved workstation, never on a login/controller host.
- Keep raw retrieved results separate from derived analysis outputs.
- Inspect provenance after submission when runtime identity matters. Prepared
  Runs record the verified image digest; actual launches record
  `container_runtime` and `container_runtime_version` when available. A pure
  plan intentionally does not claim the runtime version that will execute it.
- Prefer `rundr fetch RUN_ID` with its default auto mode. Rundra verifies shared
  visibility and avoids bulk transfer when safe; use `--mode copy` only when a
  materialized local result tree is required. Projects whose analysis always
  needs ordinary files can set `fetch_mode: copy` in version-5 defaults or a
  profile.
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
    *,
    write: Path | None = None,
    check: Path | None = None,
    topic: str | None = None,
    list_topics: bool = False,
) -> OperationResult[AgentGuideValue]:
    selected = sum(
        value
        for value in (
            write is not None,
            check is not None,
            topic is not None,
            list_topics,
        )
    )
    if selected > 1:
        return OperationResult.failure(
            "agent-guide",
            OperationError("CLI_USAGE_ERROR", "agent-guide actions are exclusive"),
        )
    if list_topics:
        content = "\n".join(f"{name}: {GUIDE_TOPICS[name]}" for name in GUIDE_TOPICS)
        return OperationResult.success(
            "agent-guide", AgentGuideValue(content + "\n", "topics")
        )
    if topic is not None:
        guidance = GUIDE_TOPICS.get(topic)
        if guidance is None:
            return OperationResult.failure(
                "agent-guide",
                OperationError(
                    "UNKNOWN_GUIDE_TOPIC", f"Unknown agent-guide topic: {topic}"
                ),
            )
        return OperationResult.success(
            "agent-guide", AgentGuideValue(guidance + "\n", "topic")
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
