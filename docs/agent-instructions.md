<!-- rundra-agent:start -->
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
- Use explicit seeds for reproducibility. Above a target safety threshold, pass
  the exact requested `--confirm-tasks N` value only after reviewing the plan.
- Use `rundr help` to discover available operations and the common workflow.
  Use `rundr help COMMAND` for command-specific arguments and options.
- Treat help output as guidance only. Use `--json` or Rundra MCP tools for
  structured automation; do not parse human-oriented help text.
- Prefer `rundr submit EXPERIMENT`, then `rundr wait RUN_ID`, then
  `rundr fetch RUN_ID` for long Runs. Use `--destination PATH` only to override
  the configuration-based default. Use `rundr run` only when keeping the client
  attached is appropriate.
- For one or several unattended Runs, launch `rundr await RUN_ID... --json` once
  and let the harness block on it. It emits one compact final document without
  progress redraws. Do not wake the model every few minutes to poll status.
- Definition-image preparation is submitted with a framework-owned dependency;
  do not keep a separate scheduler watch or resubmit while preparation runs.
  Recover an interrupted client with `rundr resume RUN_ID` or bounded `wait`.
- Preserve the Run ID and the exact `--data-dir` used at submission. Lifecycle
  commands must use the same Run store. `--last` is convenient interactively,
  but agents should retain explicit Run IDs to avoid selecting concurrent work.
- Use `--json` or Rundra MCP tools. Never parse scheduler-native output.
- Run scientific and analysis workloads on the configured execution target or
  an approved workstation, never on a login/controller host.
- Keep raw retrieved results separate from derived analysis outputs.
- Use project-v5 `fetch_mode: copy` when analysis requires a materialized file
  tree; otherwise retain the shared-storage-efficient `auto` default.
- Use `rundr cancel` for active work. Preview deletion with `rundr purge
  RUN_ID --dry-run`; purge only with exact Run-ID confirmation.
- Never place SSH keys, tokens, passwords, or other credentials in experiment,
  project, target, agent, or RunRecord files.
<!-- rundra-agent:end -->
