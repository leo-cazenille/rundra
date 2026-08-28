<!-- rundra-agent:start -->
## Rundra experiment execution

- On a new machine or agent session, run `rundr doctor --agent codex --json`
  before attempting an experiment. Execute any structured
  `run_store_durability.verification_argv` as a separate command to detect
  command-local filesystem overlays. Apply only the reported permissions, use
  a persistent `--data-dir` when necessary, start a new agent session after
  permission changes, and rerun the audit until `ready` is true.
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
- Prefer `rundr submit EXPERIMENT`, then `rundr await RUN_ID... --json`, then
  `rundr fetch RUN_ID --json` for long Runs. Fetch reuses the absolute
  destination persisted by submit; use `--destination PATH` only to override
  it, such as on another workstation. Use `rundr run` only when keeping the
  client attached is appropriate.
- `rundr wait RUN_ID --progress` is useful for a human watching an interactive
  terminal. Unattended agents should use `await`: it emits one final
  compact document when all Runs finish. Use `--until any` only for intentional
  first-completion workflows and `--timeout` when the harness imposes a deadline.
  The harness should block on this process rather than wake the model to poll.
  Reserve `--progress` for interactive humans because captured TQDM redraws can
  consume transcript tokens. `--notify-file PATH` adds an atomic aggregate signal.
- Workers waiting on preparation remain `QUEUED` before their status journals
  exist. Rundra retries bounded transient journal transport/read failures,
  merges identical events that overlap during atomic publication, and reports
  malformed or contradictory outcomes as corruption. Do not bypass a Rundra
  journal error by inferring success from scheduler output alone.
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
<!-- rundra-agent:end -->
