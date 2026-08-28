# Repository instructions

## Project purpose

This repository implements a portable experiment-execution framework for
scientific computing and agentic research.

The authoritative project specification is:

- `docs/project_specs.md`

Read the relevant parts of that document before making architectural changes.

The implemented execution targets include:

- local execution and remote execution over SSH;
- Slurm, OpenPBS, and HTCondor scheduler backends with explicit capability
  differences;
- native local execution and Apptainer-compatible container runtimes;
- rsync and shared filesystems for staging;
- reproducible experiment runs defined by executable + YAML config + seed;
- structured JSON interfaces suitable for both humans and LLM agents.

The architecture must remain extensible to additional schedulers, transports,
storage systems, container runtimes, and agent interfaces.

## Engineering principles

- Use Python 3.12.
- Use type annotations for all public interfaces.
- Prefer small, composable modules over large framework classes.
- Separate portable domain logic from infrastructure-specific adapters.
- Do not let Slurm, SSH, rsync, or Apptainer concepts leak into core experiment
  models unless they are explicitly represented as backend-specific extensions.
- Prefer standard-library functionality where practical.
- Avoid adding dependencies unless they substantially simplify the system.
- Do not add a persistent daemon unless explicitly required by the specification.
- Do not silently change public schemas or CLI behavior.

## Core architectural boundaries

Keep these concerns separate:

1. experiment/run/task domain models;
2. scheduler adapters;
3. transport adapters;
4. staging/storage adapters;
5. container-runtime adapters;
6. CLI and machine-readable interfaces;
7. provenance/run-record persistence.

Core models must not import concrete Slurm, SSH, rsync, or Apptainer
implementations.

Backend implementations may depend on core interfaces, never the reverse.

## Experiment and reproducibility requirements

- Experiment configurations must be serializable as YAML.
- Every stochastic experiment task must have an explicit integer seed.
- A run must preserve the effective configuration used for execution.
- Run directories must be immutable once execution begins, except for
  append/write locations explicitly intended for runtime state and outputs.
- Preserve enough provenance to identify:
  - experiment definition;
  - effective config;
  - seed or task set;
  - source revision when available;
  - dirty source diff when available;
  - container identity;
  - requested resources;
  - execution target;
  - scheduler job identifiers;
  - timestamps;
  - exit status;
  - produced artifacts.
- Raw experiment results must remain separate from derived analysis outputs.

## Agent-facing requirements

Assume that the CLI and APIs will be called by LLM agents as well as humans.

Therefore:

- Every important operation must have deterministic, structured output.
- Public machine-readable output must use documented JSON schemas.
- Do not require agents to parse human-oriented scheduler output.
- Errors must be explicit and actionable.
- Preserve stable identifiers for runs and tasks.
- Operations that may consume significant resources must be inspectable before
  submission.
- Design resource-policy enforcement as a first-class future capability.
- Never bypass scheduler/account/QOS restrictions.

Human-readable CLI output may change more freely than documented JSON output.

## Security and remote-execution rules

Treat remote clusters as security boundaries.

- Never interpolate untrusted strings into shell commands without safe quoting
  or argument-based execution.
- Prefer subprocess argument arrays over `shell=True`.
- Never log SSH private keys, tokens, passwords, or other credentials.
- Never store credentials in experiment specifications or run records.
- Do not weaken SSH host verification by default.
- Do not expose a network daemon or scheduler endpoint merely for convenience.
- Remote commands must run with the privileges of the configured user.
- Backend-specific native scheduler options must remain explicit and auditable.

## CLI conventions

The high-level interface includes operations such as:

- `run`
- `submit`
- `wait`
- `await`
- `plan`
- `status`
- `list`
- `logs`
- `fetch`
- `cancel`
- `inspect`
- `targets`

Do not implement commands only as thin aliases around scheduler commands.
Expose portable experiment/run semantics.

All commands that return programmatically useful information should support
`--json`.

## First implementation constraints

Unless `docs/project_specs.md` says otherwise, preserve the reference path:

local client
→ SSH
→ remote workspace
→ configured scheduler
→ Apptainer
→ shared filesystem / rsync
→ result retrieval

Do not add another scheduler, storage service, workflow engine, or persistent
service without a concrete requirement and an explicit execution plan. Preserve
the existing backend boundaries when extending supported adapters.

## Testing requirements

Unit tests must not require access to a real Slurm cluster.

Use fake/mock adapters for core orchestration tests.

Backend command-generation logic must be testable independently of executing
the generated commands.

Where practical, integration tests should cover:

- local execution;
- fake SSH transport;
- fake Slurm scheduler;
- staging round trips;
- run-record persistence;
- JSON CLI output;
- deterministic seed propagation;
- failure propagation.

Tests requiring an actual cluster must be marked as integration/system tests and
must not run by default.

## Documentation

When public behavior, schemas, CLI commands, configuration syntax, or backend
interfaces change:

- update `docs/project_specs.md` if the architectural contract changes;
- update user-facing examples where applicable;
- update schema examples and tests together.

Do not document features as implemented unless they actually exist.

## Validation

Before declaring a task complete:

1. Run unit tests.
2. Run the formatter and linter.
3. Run type checking.
4. Run the minimal local example when the change affects execution.
5. Confirm fixed-seed reproducibility when relevant.
6. Confirm machine-readable output remains valid when relevant.
7. Check that documentation matches any changed public behavior.

## Development commands

This repository uses `uv` for Python environment and dependency management.

- Install/synchronize dependencies: `uv sync`
- Run tests: `uv run pytest`
- Run linting: `uv run ruff check .`
- Check formatting: `uv run ruff format --check .`
- Run type checking: `uv run mypy src`

For repository searches:

- Prefer `rg` for searching file contents.
- Prefer `fd` for finding files.
- Fall back to `grep` and `find` when necessary.

Do not install dependencies globally.
Do not switch package managers without explicit authorization.

## Implementation discipline

For a substantial feature, cross-cutting architectural change, or significant
refactor, write or update an execution plan before implementation.

Prefer incremental milestones that leave the repository in a working state.

When choosing between:

- a quick implementation coupled to Slurm; and
- a slightly cleaner implementation preserving the documented backend boundary,

prefer the latter.

Do not generalize beyond an observed or clearly documented requirement merely
because a future backend might need it.

## SSH Access
For SSH access to the shoal cluster, use:

    ssh -F /var/local/codex/shoal/ssh/config HOST COMMAND

Allowed hosts are fishvision and shoal1 through shoal8.

Treat `fishvision` strictly as an SSH gateway and Slurm controller. Commands on
`fishvision` may perform capability probes, staging and retrieval, and Slurm
submission/status/cancellation. Never run analysis scripts, builds, containers,
tests, simulations, or other computation directly on `fishvision`. Run local
computation on `bigfish`; run cluster computation only inside a Slurm allocation
on `shoal1` through `shoal8`.

Do not alter SSH configuration, keys, known_hosts, or authorized_keys.
Do not use SSH port forwarding or agent forwarding.

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
- `rundr.yaml` is optional for conventional local projects. With adjacent
  `experiment.yaml` and `config.yaml`, Rundra uses its packaged local target,
  generates and records a seed, and reports every built-in launch source.
  Remote targets and preparation recipes remain explicit configuration.
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
