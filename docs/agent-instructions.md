<!-- rundra-agent:start -->
## Rundra experiment execution

- Use Rundra for scientific execution; do not invoke SSH, rsync, Slurm, or
  Apptainer directly except while diagnosing an explicit Rundra error.
- Run `rundr doctor EXPERIMENT` and `rundr plan EXPERIMENT` before consuming
  cluster resources. Review task count, seeds, resources, concurrency, and
  retrieval strategy.
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
- Preserve the Run ID and the exact `--data-dir` used at submission. Lifecycle
  commands must use the same Run store.
- Use `--json` or Rundra MCP tools. Never parse scheduler-native output.
- Run scientific and analysis workloads on the configured execution target or
  an approved workstation, never on a login/controller host.
- Keep raw retrieved results separate from derived analysis outputs.
- Use `rundr cancel` for active work. Preview deletion with `rundr purge
  RUN_ID --dry-run`; purge only with exact Run-ID confirmation.
- Never place SSH keys, tokens, passwords, or other credentials in experiment,
  project, target, agent, or RunRecord files.
<!-- rundra-agent:end -->
