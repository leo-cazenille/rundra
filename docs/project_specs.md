# Rundra — Project Specification

> **Status:** Initial implementation specification
>
> - **Project/brand:** Rundra
> - **GitHub repository:** `rundra`
> - **Python package:** `rundra`
> - **PyPI distribution:** `rundra`
> - **CLI command:** `rundr`
>
> **Primary reference deployment:** the shoal cluster, accessed through `fishvision`
> **Initial implementation language:** Python 3.12

---

## 1. Purpose

Rundra is a portable experiment-execution layer for reproducible scientific computing.

Its initial purpose is to make the following workflow possible from a developer workstation or laptop:

1. develop or modify a scientific project locally;
2. define an experiment through a project entry point and YAML configuration;
3. stage the current source tree and inputs to a remote execution target;
4. submit one or more tasks through the target scheduler;
5. execute the tasks inside an Apptainer/Singularity-compatible container;
6. monitor the run;
7. collect logs, outputs, and provenance;
8. retrieve requested results to the initiating machine.

The first working implementation targets the **shoal** laboratory cluster:

```text
developer workstation
        |
        | SSH + rsync
        v
    fishvision
        |
        | shared /shoalhome workspace
        | Slurm
        v
 shoal compute nodes
        |
        | Apptainer
        v
 scientific executable
```

However, the internal architecture must not make shoal, Slurm, SSH, rsync,
shared-filesystem implementation details, or Apptainer inseparable from the
core experiment model.

The long-term goal is a more general execution framework usable across:

- local workstations;
- laboratory clusters;
- institutional HPC clusters;
- national or international supercomputers;
- multiple batch schedulers;
- multiple staging/storage mechanisms;
- Apptainer/Singularity-based scientific deployments;
- human users;
- coding agents;
- autonomous or semi-autonomous research agents.

The framework is intended to be useful as an **execution and provenance substrate for agentic research**, but it is not itself an autonomous scientist.

---

## 2. Core use case

A large fraction of target projects follow a simple scientific pattern:

```text
main executable
      +
YAML configuration
      +
explicit random seed
      =
one experimental task
```

Examples include:

```bash
python3 main.py --config configs/test.yaml --seed 17
```

or:

```bash
./simulation --config configs/test.yaml --seed 17
```

The framework should make this pattern first-class without requiring every supported project to follow it exactly.

A logical run may contain:

```text
one config × one seed
```

or:

```text
one config × many seeds
```

For example:

```text
configs/baseline.yaml × seeds 0..99
```

is one logical `Run` containing 100 `Task` objects.

A scheduler backend may optimize such a task set into a native job array, but the core model must not define the task set as a Slurm array.

---

## 3. Long-term vision

The long-term architecture should support a common experiment interface across heterogeneous execution environments.

Potential future scheduler backends include:

- Slurm;
- PBS Pro;
- LSF;
- SGE;
- HTCondor;
- other batch systems encountered on scientific clusters.

Potential future transports and staging mechanisms include:

- local filesystem;
- SSH;
- rsync;
- SFTP;
- shared POSIX filesystems;
- Globus;
- object storage where appropriate.

Potential future execution runtimes include:

- Apptainer;
- Singularity;
- possibly other container runtimes if a real use case requires them.

Potential future interfaces include:

- command-line interface;
- Python API;
- MCP server;
- REST API.

Potential future high-level capabilities include:

- parameter sweeps;
- adaptive experiment campaigns;
- structured result metrics;
- run lineage;
- resource-policy enforcement;
- approval thresholds for autonomous agents;
- integration with workflow engines;
- automated experimental loops.

These are **design goals**, not version 0.1 requirements.

---

## 4. Non-goals

The initial project is not intended to be:

- a replacement for Slurm, PBS, LSF, or other schedulers;
- a cluster provisioning or administration system;
- a general DAG workflow engine such as Snakemake or Nextflow;
- a distributed object store;
- a container image builder;
- an experiment-analysis library;
- a hyperparameter-optimization framework;
- an autonomous scientific agent;
- a general remote-shell wrapper;
- an abstraction that attempts to hide every scheduler-specific feature.

The framework executes and records scientific experiments.

Higher-level software may later use it to implement workflows, autonomous research, optimization, or adaptive experimentation.

---

## 5. Design principles

### 5.1 Portable scientific domain model

Scientific concepts must be modeled independently from infrastructure.

Primary domain concepts are:

- `ExperimentSpec`
- `Run`
- `Task`
- `ResourceRequest`
- `Artifact`
- `RunRecord`
- `Target`

Infrastructure-specific concepts belong behind adapters:

- `Scheduler`
- `Transport`
- `Stager`
- `ContainerRuntime`

Core domain models must not depend on concrete Slurm, SSH, rsync, or Apptainer implementations.

---

### 5.2 Reproducibility by construction

The framework must preserve enough information to identify what actually ran.

A run should record, when available:

- effective experiment specification;
- effective YAML config;
- explicit task seeds;
- source revision;
- dirty working-tree changes;
- container identity;
- target;
- requested resources;
- scheduler identifiers;
- timestamps;
- exit status;
- produced artifacts.

A later user should be able to reconstruct the execution as far as external dependencies and infrastructure allow.

---

### 5.3 Agents and humans use the same execution model

The same core operations should support:

- interactive human CLI use;
- shell-based coding agents;
- future structured agent tool interfaces.

Human-readable output and machine-readable output must originate from the same internal result objects.

Agents must not need to parse:

- `squeue`;
- `sbatch` prose;
- scheduler-specific log naming conventions;
- arbitrary human-formatted CLI output.

---

### 5.4 Minimal remote footprint

Version 0.1 must not require a persistent daemon on the remote cluster.

The reference architecture should work using mechanisms commonly permitted on scientific HPC systems:

- SSH;
- remote command execution;
- scheduler CLI;
- shared filesystems;
- rsync;
- Apptainer.

A future optional service/API must not become a prerequisite for normal cluster use unless there is a compelling reason.

---

### 5.5 Progressive generalization

Implement one complete vertical path before adding broad backend support.

The first vertical path is:

```text
local client
    ↓
SSH
    ↓
remote workspace
    ↓
Slurm
    ↓
Apptainer
    ↓
shared filesystem
    ↓
result collection
    ↓
rsync back to client
```

Do not implement PBS, LSF, Globus, MCP, REST services, or a distributed database before the first real shoal workflow works.

At the same time, avoid unnecessary coupling that would force a rewrite when a second concrete backend is introduced.

---

### 5.6 Generalize from real implementations

Do not create abstractions solely because they might become useful.

A preferred rule is:

> Implement the first backend cleanly. Add the second real backend when needed. Refactor shared abstractions based on evidence from both.

The framework should avoid both extremes:

- a Slurm-specific implementation disguised as a generic framework;
- a large speculative plugin architecture before one reliable execution path exists.

---

## 6. Conceptual architecture

```text
                    Human CLI
                        |
                        |
                 +------+------+
                 | Client/API  |
                 +------+------+
                        |
                        | normalized requests/results
                        |
                 +------+------+
                 | Orchestration|
                 +------+------+
                        |
          +-------------+-------------+
          |             |             |
          v             v             v
      Run model      Planner      Provenance
          |
          +--------------------------------------------+
          |                |              |            |
          v                v              v            v
      Transport        Scheduler       Stager    ContainerRuntime
          |                |              |            |
         SSH             Slurm          rsync      Apptainer
          |
          v
      remote target
```

Future interfaces such as a Python API or MCP server should sit above the orchestration layer rather than duplicating execution logic.

---

## 7. Core terminology

### 7.1 Experiment

An **Experiment** is the reusable scientific definition of how one task is executed.

It is described by an `ExperimentSpec`.

---

### 7.2 Task

A **Task** is one concrete execution of an experiment.

A task normally contains at least:

- experiment identity;
- effective config;
- explicit seed;
- task ID;
- resource request.

---

### 7.3 Run

A **Run** groups one or more Tasks created by one logical submission.

Examples:

```text
Run A
└── config=test.yaml, seed=17
```

```text
Run B
├── config=baseline.yaml, seed=0
├── config=baseline.yaml, seed=1
├── ...
└── config=baseline.yaml, seed=99
```

A Run is a framework concept.

A scheduler job ID is an execution detail.

---

### 7.4 Target

A **Target** describes where and how execution occurs.

Examples:

- local workstation;
- shoal;
- another Slurm cluster;
- a PBS Pro supercomputer.

A target combines site-specific backend configuration without embedding it in the experiment definition.

---

### 7.5 Artifact

An **Artifact** is a file or directory associated with execution.

Initial artifact categories include:

- source snapshot;
- effective configuration;
- stdout;
- stderr;
- raw results;
- scheduler metadata;
- provenance metadata.

Derived scientific analysis is outside the execution core.

---

## 8. Experiment specification

### 8.1 Goals

`ExperimentSpec` must describe **portable execution semantics**, not scheduler directives.

A representative project-level file may look like:

```yaml
version: 1

experiment:
  name: collective-departure

command:
  argv:
    - python3
    - main.py
    - --config
    - "{config}"
    - --seed
    - "{seed}"

container:
  image: containers/project.sif
  gpu: true

resources:
  nodes: 1
  tasks: 1
  cpus_per_task: 4
  gpus_per_task: 1
  memory: 16GiB
  walltime: "02:00:00"

outputs:
  include:
    - results/**
    - logs/**

sync:
  exclude:
    - .git/
    - .venv/
    - __pycache__/
    - .pytest_cache/
    - .mypy_cache/
    - .ruff_cache/
    - shoal-results/
```

The implemented version-1 schema is strict: unknown fields and incompatible
versions fail explicitly. Its portable/backend separation is stable through
v0.1; incompatible changes require a new schema version.

---

### 8.2 Command representation

Commands should be represented internally as argument vectors whenever practical.

Preferred representation:

```python
[
    "python3",
    "main.py",
    "--config",
    "/path/to/config.yaml",
    "--seed",
    "17",
]
```

Avoid storing the canonical command only as a shell string.

This provides:

- safer execution;
- easier quoting;
- easier testing;
- clearer provenance;
- better machine manipulation.

---

### 8.3 Configs are opaque scientific inputs

The framework does not need to understand the scientific contents of the project YAML config.

For example:

```yaml
population:
  size: 100

simulation:
  duration: 5000

noise:
  sigma: 0.1
```

is primarily an opaque input passed to the scientific executable.

The framework needs to know:

- which config file was used;
- its effective content;
- where it was staged;
- which tasks consumed it.

Version 0.1 does not require application-specific config schemas.

---

### 8.4 Explicit seeds

Every stochastic Task must have an explicit integer seed.

The integer may be supplied by a caller or generated by Rundra's launch layer.
If it is generated, generation must happen exactly once before pure planning
and execution. The resolved integer must be shown to the caller and passed to
the application exactly as if it had been supplied explicitly. The framework
must never rely on an application's implicit random seed.

A Run may specify:

```text
seed = 42
```

or:

```text
seeds = 0..99
```

The seed supplied to each task must be recorded in the run metadata.

When no seed is supplied by the CLI, project profile, or user defaults, Rundra
may generate a non-negative 63-bit seed from operating-system entropy. This
bounded representation is portable through signed 64-bit interfaces. Replaying
with the recorded integer, rather than merely omitting the seed again, is the
reproducible operation. Entropy resolution belongs outside the deterministic
planner.

---

## 9. Resource model

### 9.1 Portable resource fields

The initial `ResourceRequest` should support:

- `nodes`;
- `tasks`;
- `cpus_per_task`;
- `gpus_per_task`;
- memory;
- wall time.

Additional portable fields may be added only when supported by concrete use cases.

---

### 9.2 Backend-specific resources

Not every scheduler or site can be represented perfectly through one portable ontology.

Backend-specific options must therefore be possible, explicitly and separately.

Example:

```yaml
resources:
  nodes: 1
  tasks: 1
  cpus_per_task: 8
  gpus_per_task: 1
  memory: 32GiB
  walltime: "04:00:00"

  native:
    slurm:
      partition: gpu
      qos: normal
      constraint: a100
```

Portable resource semantics must not be silently changed by native options.

Native options must be visible in the execution plan and provenance.

---

### 9.3 Unsupported resources

If a target cannot satisfy a requested portable resource field, the framework must fail explicitly rather than silently ignore it.

Example structured error:

```json
{
  "ok": false,
  "error": {
    "code": "UNSUPPORTED_RESOURCE",
    "message": "Target 'example' does not support gpus_per_task."
  }
}
```

---

## 10. Target configuration

Site-specific configuration must live outside project experiment configuration.

A possible target configuration is:

```yaml
version: 1

targets:

  local:
    transport:
      type: local

    scheduler:
      type: local

    staging:
      type: local

    container:
      type: native

    workspace: "~/.local/share/rundra"

  shoal:
    transport:
      type: ssh
      host: fishvision

    scheduler:
      type: slurm

    staging:
      type: rsync

    container:
      type: apptainer

    workspace: "/shoalhome/{user}/.rundra"
```

Target configuration may later include:

- scheduler defaults;
- partitions/queues;
- scratch locations;
- persistent storage locations;
- module setup;
- scheduler account/project;
- site-specific environment setup;
- staging constraints.

Version-1 target files accept only executable backend stacks: all-local
transport/scheduler/staging with either native or Apptainer execution, or SSH
with Slurm or OpenPBS, rsync or shared staging, and Apptainer. SSH workspaces
must be absolute, non-root paths. This is static configuration validation;
executable discovery, connectivity, authentication, and site availability
remain execution preflight concerns and are never contacted by `plan`.

Target configuration version 2 adds preparation cache roots and explicit image
search paths. Version 3 adds a required per-target `execution` policy. Every
limit is explicit and site-owned: `hard_task_limit`, `confirmation_threshold`,
`max_active_tasks`, `max_concurrent_jobs`, `max_array_size`, `output_shard_tasks`,
`automatic_retrieval_threshold`, and a `worker_pool` mapping containing
`activation_threshold`, `max_workers`, `tasks_per_lease`,
`infrastructure_retry_limit`, and `requeue_limit`. Project configuration and
launch options may select behavior within these limits but cannot raise them.

Version 4 adds `task_slots_per_worker` to the worker-pool policy. Version 5 adds
an explicit shared-POSIX staging backend for clients that mount the target
filesystem directly:

```yaml
version: 5
targets:
  shoal:
    transport: {type: ssh, host: fishvision}
    scheduler: {type: slurm}
    staging: {type: shared, root: /shoalhome}
    container: {type: apptainer}
    workspace: /shoalhome/USER/.rundra
    execution: # same required bounded version-4 policy
      # ...
```

Version 6 separates conservative worker defaults from site-owned worker and
slot ceilings. Version 7 adds optional `max_memory_per_worker`, expressed with
the same `B`, `KiB`, `MiB`, `GiB`, or `TiB` syntax as experiment resources.
Planning rejects aggregate worker memory above this ceiling before staging or
scheduler contact. Older target versions retain their existing behavior.

The shared root must be absolute and non-root. Rundra resolves staged source,
workspace, and retrieval destinations beneath it and rejects symlink escapes.
Scheduling and container commands still use the configured SSH and Slurm
adapters; only file movement is performed directly through the shared mount.

Large parameter/seed products use an inclusive arithmetic seed range and a
constant-size TaskSpace. Ordinals are parameter-major and seed-minor; the Task
at ordinal `p * seed_count + s` has parameter-set ordinal `p`, seed ordinal `s`,
and the existing deterministic `task_NNNNNN` identity. Implementations must not
materialize the complete TaskSpace merely to plan or summarize a Run.

---

### 10.1 Configuration location

A default user-level location should be supported, for example:

```text
~/.config/rundra/targets.yaml
```

The exact location may be finalized during implementation.

Project repositories should not need to contain private cluster details.

---

### 10.2 Credentials

Secrets must never be stored in:

- `ExperimentSpec`;
- target YAML committed to repositories;
- `RunRecord`;
- logs.

Authentication should rely on external mechanisms such as:

- SSH agent;
- SSH config;
- Kerberos/session mechanisms where required;
- future credential stores appropriate to specific transports.

---

## 11. Backend interfaces

The initial architecture should keep four infrastructure concerns independent.

---

### 11.1 Transport

Responsibilities:

- execute a command on the target control host;
- test connectivity;
- retrieve basic target capability information.

Initial implementations:

- `LocalTransport`
- `SSHTransport`

Conceptual interface:

```python
class Transport(Protocol):
    def run(self, command: Command, ...) -> CommandResult:
        ...
```

The exact Python API is not mandated by this document.

---

### 11.2 Scheduler

Responsibilities:

- submit an execution unit;
- query scheduler state;
- wait or poll where required;
- cancel jobs;
- expose accounting information where available;
- map native scheduler states to portable states.

Initial implementations:

- `LocalScheduler`
- `SlurmScheduler`

Future implementations may include:

- PBS Pro;
- LSF;
- SGE;
- HTCondor.

A Scheduler consumes a nonempty normalized `SchedulerGroup`, not an
`ExperimentSpec` or planner model directly. Each `SchedulerUnit` contains only
the logical `TaskId`, executable `Command`, and portable `ResourceRequest`
needed by both local and Slurm adapters. A group must not repeat Task IDs.

Submission returns an opaque scheduler reference and a nonempty explicit
mapping from every submitted logical Task ID to its native scheduler identity.
Run IDs, Task IDs, scheduler references, and backend-native Task identities are
distinct values; adapters must not derive or conflate them.

A scheduler observation contains the opaque reference, portable execution
state, separately preserved nonblank native state, and optional exit code,
timezone-aware start/finish timestamps, scalar native metadata, and captured
command result. Scheduler observations cannot report pre-scheduler
`CREATED`/`STAGING` states. Nonterminal observations cannot contain an exit,
finish time, or command result; successful observations cannot contain a
nonzero exit status. When a synchronous result is present, its exit status and
timestamps must agree with the observation.

For synchronous schedulers, a scheduler observation may include the captured
`CommandResult` that produced it. This is optional at the portable boundary:
the local scheduler uses it to return stdout, stderr, timestamps, and exit
status, while remote schedulers may instead expose log paths and accounting
metadata.

The M3 Slurm adapter renders a deterministic single-Task sbatch script before
submission. Portable node/task/CPU/GPU/memory/walltime requests own their
directives; only `account`, `constraint`, `partition`, `qos`, and boolean
`exclusive` are accepted from `resources.native.slurm`. Output/error directives
and other framework-owned fields cannot be overridden. `sbatch --parsable`,
delimiter-stable `squeue`, and parsable `sacct` output are interpreted only in
the adapter. A job absent from both queue and accounting is reported as
portable `UNKNOWN` with explicit accounting-pending metadata rather than being
assumed complete or failed.

---

### 11.3 Stager

Responsibilities:

- create isolated run workspaces;
- stage source/input files;
- retrieve requested outputs;
- support filesystem semantics of the target.

Initial implementations:

- `LocalStager`
- `RsyncStager`

A `SharedFilesystemStager` may be introduced if it makes the shoal implementation clearer.

---

### 11.4 ContainerRuntime

Responsibilities:

- construct container execution;
- enable GPU passthrough;
- apply bind mounts;
- apply safe environment configuration;
- perform basic runtime validation.

Initial implementation:

- `ApptainerRuntime`

Example conceptual output:

```bash
apptainer exec --nv IMAGE \
    python3 main.py --config CONFIG --seed SEED
```

Apptainer-specific command construction must belong to the container backend, not the Slurm backend.

---

## 12. Filesystem and staging model

Do not assume every cluster resembles shoal.

Potential target storage concepts include:

```text
$HOME       shared, persistent, often quota-limited
$PROJECT    shared, persistent
$SCRATCH    shared, high-throughput, temporary
$TMPDIR     node-local
```

The architecture should eventually be able to distinguish semantic storage roles.

Version 0.1 only needs what is required for shoal and local execution.

---

### 12.1 Immutable run workspaces

Each submitted Run must receive its own isolated workspace.

Never repeatedly synchronize development files into one mutable directory used by already-running jobs.

Conceptually:

```text
<workspace>/
└── runs/
    ├── run_01.../
    ├── run_02.../
    └── run_03.../
```

A new local edit and submission must not alter the files being used by an existing Run.

---

### 12.2 Conceptual run-directory layout

A remote Run may use:

```text
runs/<run-id>/
├── source/
├── input/
├── runtime/
├── output/
├── logs/
└── metadata/
    ├── experiment.yaml
    ├── config.yaml
    ├── run.json
    ├── provenance.json
    └── scheduler.json
```

The physical directory layout is not a stable public API unless explicitly documented as such.

For replicated Runs, M5.1 defines backend-neutral Task paths without making
the physical layout a public API. `StagedWorkspace.for_task(task_id)` keeps the
sealed source snapshot and exact config shared across Tasks, and derives
isolated mutable locations equivalent to:

```text
runtime/<task-id>/
output/<task-id>/
logs/<task-id>.stdout
logs/<task-id>.stderr
metadata/<task-id>/
```

These paths are pure staging semantics at M5.1. Array construction and use of
the isolated locations begin in M5.2/M5.3; existing single-Task execution is
unchanged.

The M1 local implementation follows this layout through backend-neutral
`StagedWorkspace` paths. It copies the source tree and exact effective config,
then removes all write bits from `source/` and `input/` before returning the
workspace. `runtime/`, `output/`, `logs/`, and `metadata/` remain explicitly
writable. This permission sealing enforces the orchestration invariant during
normal execution; it is not a security boundary against the owning user, who
can deliberately change permissions.

Local source symlinks are dereferenced into the snapshot so later changes to a
link target cannot alter an existing Run. Common transient directories are
excluded by default, including version-control metadata, virtual environments,
Python caches, test/type/lint caches, and Rundra's own `.rundra` workspace.
Experiment `sync.exclude` patterns add to those defaults. Unsafe absolute or
parent-traversing patterns are rejected.

The concrete default patterns are `.git`, `.hg`, `.svn`, `.venv`, `venv`,
`__pycache__`, `.pytest_cache`, `.mypy_cache`, `.ruff_cache`, `.tox`, `.nox`,
`.rundra`, `.agents`, `retrieved`, `tmp`, `downloads`, `*.py[cod]`, `*.sif`,
and `*.simg`. Local staging, rsync staging, and mutable preparation snapshots
share this list so excluded content is neither transferred nor included in
source/build cache identities.

Local fetch treats patterns as relative to `output/`, rejects symlink results
and destinations inside the Run workspace, and atomically replaces each copied
destination file. Repeating the same fetch is therefore safe and updates an
existing retrieved file without partially overwriting it.

`rundr fetch` accepts `--mode auto|copy|reference|archive`. Local targets
resolve `auto` to `copy`. Version-5 shared targets resolve `auto` to
`reference`. Rsync targets first perform a private controller-to-client token
round trip through the exact Run metadata directory; a matching token and
symlink-safe locally visible workspace select `reference`, while a failed probe
falls back to ordinary rsync. Rundra writes an atomic read-only
`rundra-reference.json` in the destination instead of duplicating result data.
The manifest records the immutable terminal Run's output, metadata, and log
roots plus selected output patterns. Explicit `--mode copy` retains ordinary
file materialization. Reference retrieval is rejected until the Run is
terminal and remains dependent on the configured remote-workspace retention
policy.

Explicit `--mode reference` applies the same visibility proof and fails rather
than silently copying when the target workspace is not jointly visible. The
probe uses a random private file, removes it best-effort through the controller,
does not trust matching path names alone, and never scans unrelated filesystem
locations.

For bundled Slurm runs at or above the target's `output_shard_tasks` threshold,
each scheduled worker lane seals its completed Task output directories into one
deterministic uncompressed tar shard on the allocated compute node. The shard
contains a bounded TSV index with Task exit codes and every regular member's
size and SHA-256. Symlink outputs are rejected. The archive and checksum are
published atomically and read-only before loose Task directories are removed;
packaging failures preserve loose outputs and fail the worker. Run scheduler
metadata records the shard root. Fetches select the lane archives rather than
requesting files that were compacted, reducing a 20,000-Task Run with 320 lanes
to roughly 640 result files including checksums.

`rundr fetch --extract` verifies each archive checksum and indexed member hash,
rejects unsafe paths and symlink parents, and atomically publishes only selected
Task files. `--mode archive` is rejected for unsharded Runs. On a shared target,
`auto --extract` selects archive materialization instead of a reference. Without
`--extract`, archives remain compact. Python analysis may use the public
`rundra.artifacts.open_result_shard` reader, which verifies the whole-archive
sidecar, validates the bounded index, and checks a selected member's size and
SHA-256 before returning bytes.

Human rendering of large plans, submissions, status responses, and fetch Task
selections is bounded. Public lifecycle JSON for Runs with at least 1,000
materialized Tasks is also bounded and uses envelope format version 5: it emits
TaskSpace identity and aggregate counts rather than complete Task, seed, or
exit-code arrays. Individual state remains available through paginated `tasks`;
`inspect` remains the explicit complete durable-record operation. Existing
materialized RunRecord schemas are not silently relabeled or rewritten.

`rundr purge` applies retention only to one exact terminal Run derived from its
persisted target and Run ID. Outputs are the default scope; `--workspace`
selects the complete per-Run workspace. Mutation requires `--confirm RUN_ID`,
while `--dry-run` reports existence and resumable tombstones without mutation.
Local and shared targets delete locally; legacy rsync targets execute one
tightly scoped SSH controller command. This cleanup is an allowed control-plane
operation on fishvision, not application computation. A fixed tombstone rename
precedes recursive deletion, symlinked semantic roots and collisions are
rejected, and retries resume safely. Separate atomic receipt version 1 records
attempts without modifying RunRecord v1-v4 documents.

---

### 12.3 Staging current working trees

Version 0.1 must support launching a run from the developer's current working tree without requiring:

- Git commit;
- Git push;
- Git clone/pull on the target.

This is essential for fast development cycles.

`rsync` is the default reference mechanism for remote source staging.

Common transient files should be excluded by default or configurable exclusions should make this straightforward.

---

## 13. Source provenance

When the project is a Git working tree, record when practical:

- current commit;
- current branch, if useful;
- dirty/clean status;
- relevant dirty diff or patch.

Git is used for provenance, not required as the transport protocol.

An experiment must still be executable from a non-Git source directory.

Missing optional Git provenance must not prevent execution.

The M1 Git provider invokes Git only through bounded argument-array subprocesses
against the original source root before staging. It records the current commit
when one exists, the symbolic branch when attached, and porcelain dirty state
including untracked files. A tracked dirty patch is included only when it is
valid UTF-8, at most 1 MiB by default, and contains none of the provider's common
credential markers. Untracked file contents are never added to the patch.

Missing Git, a non-repository source, an unborn or detached reference, timeout,
oversized/non-UTF-8 output, a credential marker, or another capture failure
leaves only the affected optional values unavailable. It does not prevent the
Run. Marker screening is defense in depth, not semantic secret detection;
credentials remain prohibited in source/configuration values and Run data.

---

## 14. Container provenance

A run should record, when practical:

- runtime type;
- image path/reference;
- image checksum/digest;
- GPU mode;
- bind mounts;
- relevant container-specific options.

Example:

```json
{
  "runtime": "apptainer",
  "image": "/shoalhome/user/containers/project.sif",
  "sha256": "...",
  "gpu": true
}
```

If calculating an image digest is prohibitively expensive for every run, the implementation may use an explicit caching or optional strategy. It must not fabricate a digest.

The v0.1 CLI records the declared image reference and GPU intent in the
normalized experiment and the selected runtime in the target. It does not
calculate an image digest or persist a runtime version, so `container_digest`
remains `null` for ordinary Runs. This absence is explicit rather than inferred.

---

## 15. Run identity

Every Run receives a framework-generated immutable identifier independent of scheduler IDs.

Example:

```text
run_01K...
```

Requirements:

- globally unique enough for normal use;
- safe in filesystem paths;
- safe in JSON;
- not derived solely from the Slurm job ID.

Scheduler IDs are recorded separately.

Human-friendly aliases may be added later.

---

## 16. Task identity

Within a Run, every Task must have a stable task identifier.

For a replicated experiment the logical relation may be:

```text
run_ABC
├── task 0 → seed 0
├── task 1 → seed 1
├── ...
└── task 99 → seed 99
```

A backend may map task IDs to scheduler-array indices, but they remain distinct concepts.

---

## 17. Portable state model

Minimum Run states:

- `CREATED`
- `STAGING`
- `SUBMITTED`
- `QUEUED`
- `RUNNING`
- `SUCCEEDED`
- `FAILED`
- `CANCELLED`
- `UNKNOWN`

Tasks may have their own states when a Run contains multiple tasks.

Native scheduler states must be translated into this model while preserving the original scheduler state when useful.

For example:

```json
{
  "state": "FAILED",
  "backend_state": "OUT_OF_MEMORY"
}
```

---

## 18. Failure model

The framework must distinguish at least:

- local validation failure;
- target configuration failure;
- transport/connectivity failure;
- source-staging failure;
- scheduler-submission failure;
- queue/execution failure;
- scheduler timeout;
- cancellation;
- experiment non-zero exit;
- result-retrieval failure;
- provenance-recording failure where relevant.

These failures must not be collapsed into one generic message if they imply different corrective actions.

A result-retrieval failure after successful computation must not be reported as a computation failure.

Partial logs and outputs should remain retrievable when possible after failure.

---

## 19. Run record and provenance

A `RunRecord` should preserve, when available:

- run ID;
- experiment name;
- framework/schema version;
- run creation time;
- initiator;
- target;
- normalized `ExperimentSpec`;
- effective config;
- task definitions;
- task seeds;
- source path/context;
- Git commit;
- dirty-tree status;
- dirty diff;
- container reference;
- container digest/checksum;
- portable resource request;
- backend-native resource options;
- scheduler type;
- scheduler job IDs;
- allocated nodes where available;
- submission timestamp;
- start timestamp;
- completion timestamp;
- portable final state;
- native final state;
- task exit codes;
- artifact manifest;
- retrieval state.

Missing optional data must be represented as unavailable, not fabricated.

The version-1 persisted fields and their availability rules are documented in
[`docs/artifacts-and-provenance.md`](artifacts-and-provenance.md) and checked by
[`docs/schemas/run-record-v1.json`](schemas/run-record-v1.json). CLI-created
Runs currently leave `initiator` and `container_digest` unavailable. Git fields
are best-effort; exact effective config, seeds, normalized experiment/target,
portable resources/states, and stable identities are mandatory.

---

## 20. Artifacts and results

Raw execution outputs should remain conceptually distinct from later analysis.

Suggested categories:

```text
raw execution
├── stdout/stderr
├── raw result files
├── checkpoints
└── experiment-produced metadata

derived analysis
├── plots
├── statistics
└── reports
```

The framework primarily manages raw execution artifacts.

Projects may choose to include analysis artifacts in their requested outputs, but the framework should not impose an analysis model in v0.1.

RunRecord version 1 classifies `source_snapshot`, `effective_config`, `stdout`,
`stderr`, `raw_result`, `scheduler_metadata`, and `provenance_metadata`.
The current Git provider stores provenance directly in RunRecord fields and
does not emit a separate `provenance_metadata` file. Artifact paths are
locators, optional sizes are measurements, and v0.1 does not claim artifact
checksums or a content-addressed store. Computation and retrieval states remain
independent so transfer failure never rewrites scientific execution state.

---

## 21. CLI design

The CLI is the first public interface.

The top-level `rundr --version` option and the equivalent `rundr version`
command print `rundr version VERSION`, where `VERSION` is read from installed
distribution metadata. They exit successfully without loading project or Run
configuration.

The installed agent-guide section remains self-contained and links to the
canonical PyPI project overview for additional installation and workflow
documentation. Because that page describes the latest release, installed help
and version output remain authoritative for local behavior.
Agents can request only the relevant `setup`, `launch`, `large-runs`,
`lifecycle`, `results`, `preparation`, or `recovery` guidance through
`agent-guide --topic` or MCP `get_guidance`, avoiding repeated full-guide
transmission.

The version-2 doctor result is the first-run capability boundary for humans and
agents. Bare `rundr doctor` exercises the effective local Run store and
preparation cache with private temporary files. Experiment mode additionally
reports the exact source, config, destination, target, SSH, network, and socket
requirements. `--connect` performs a reversible staging round trip; the
separate `--scheduler-probe` opt-in submits one bounded no-op job and guarantees
best-effort cancellation and exact-path cleanup. `ready` means that no known
requirement failed, while `complete` means every requested check passed. The
audit may generate agent-specific configuration text but never applies it,
reads credential contents into output, fetches sources, pulls images, builds
applications, or creates a scientific RunRecord.

An SSH target is remote-only unless topology is declared otherwise. Shared
staging automatically audits client write access to the target workspace and
preparation cache, read access to explicit target image-search paths, and a
client-to-controller token round trip through the shared path. Rsync targets
perform the same local path audit only with `--local-target-access`, intended
for system tests and clients that separately mount target storage. This avoids
both silently losing shared-filesystem optimizations under an agent sandbox and
incorrectly requiring cluster paths on remote laptops.

M12.1 adds an opt-in Docker Compose system boundary with one Slurm controller,
two compute nodes, SSH/rsync staging, and restricted-capability nested
Apptainer. It validates 1,000 logical Tasks through bounded worker allocations,
sharded retrieval, and compute-host evidence. The harness is excluded from
default tests, runs manually and nightly, generates credentials at runtime, and
does not use privileged containers. It is a lifecycle and logical-scale proof,
not a performance model of a production cluster.

The command-line executable is:

```text
rundr
```

---

### 21.1 `validate`

```bash
rundr validate experiment.yaml
```

Responsibilities:

- parse experiment specification;
- validate portable schema;
- detect obvious invalid fields;
- not submit anything.

Optional:

```bash
rundr validate experiment.yaml --target shoal
```

may additionally validate target capabilities.

---

### 21.2 `plan`

```bash
rundr plan experiment.yaml \
    --config configs/test.yaml \
    --seeds 0:9 \
    --target shoal
```

`plan` must not submit jobs.

It should expose:

- normalized task set;
- target;
- requested resources;
- selected execution strategy where known;
- expected staging behavior;
- backend/native options;
- policy result when policy support exists.

For an agent, `plan` is a key safety boundary.

The version-1 plan result exposes normalized plan-level resources and
backend-native options, the selected execution strategy and Task grouping,
explicit array mapping when applicable, and expected staging transfers,
workspace root, input sealing, and result retrieval. A successful plan has
statically validated the configured backend stack, container/resource
compatibility, GPU allocation versus device passthrough, native scheduler
namespace, and Slurm's v0.1 native-option allowlist.

Planning reads and validates its experiment, effective config, target, and
launch inputs only. It does not contact the target, discover executables, run
preflight, allocate a Run ID or RunRecord, create a local or remote workspace,
stage data, or submit work. The JSON `validation` and `safety` objects state
these guarantees explicitly; the human renderer reports the same boundary.

#### Project-managed preparation (schema version 2)

An adjacent project `rundra.yaml` may use version 2 to add an immutable
preparation recipe while the portable experiment remains schema version 1.
Version-1 project documents retain their existing plan and RunRecord contracts.

A version-2 recipe identifies a source using a full Git commit, identifies one
prebuilt SIF using a logical filename, URI, and pinned SHA-256, and may define a
shell-free application build argv with declared outputs and explicit CPU,
memory, and walltime bounds. Relative image and output paths must not escape
their roots. Credentials are forbidden in fields and source URLs.

The experiment's relative `container.image` must equal the recipe's logical
image filename. Preparation resolves that name to a verified absolute SIF path;
the effective experiment and version-2 RunRecord preserve the absolute path and
set `container_digest` to the verified SIF digest.

`--source-root` selects mutable-working-tree preparation. Rundra snapshots and
hashes that tree after normal sync exclusions and never compiles in the
developer's original tree. Without that explicit option, the pinned Git source
is used. `--prepare-location auto|local|target`, `--rebuild`, and `--offline`
control location, compiled-output reuse, and network access respectively.
For an SSH/Slurm target, forced `local` preparation compiles only in the local
isolated cache, publishes the verified SIF atomically into the target content
cache with rsync, and stages the prepared source; no compilation runs on the
SSH login process.

Version-2 planning remains pure. It reports source and image identities, build
argv/outputs/resources, cache scope, requested location, possible actions, and
safety effects. It does not probe candidates or caches and never claims a cache
hit. Content and cache resolution occurs only during `run` or `submit`.

Local preparation uses content-addressed immutable source, image, and prepared
source caches. Candidate images are accepted only after SHA-256 verification;
cache publication uses per-key locks, temporary paths, and atomic rename.
Builds run inside the verified SIF against a writable copy of the source
snapshot. Declared outputs and executable bits are verified before the prepared
source is sealed and published.

Optional version-2 user and target configuration may select preparation cache
roots and explicit image search directories. Version-1 configuration shapes
remain unchanged. Local paths default to `~/.cache/rundra`; target paths default
to `<target.workspace>/cache`. Resolution checks only the requested logical
filename in configured directories and never recursively scans a home tree.

The target preparation design extends this lifecycle with one bounded Slurm
job on a remote cache miss. Compilation must not run in an SSH login process,
and experiment jobs use a framework-owned `afterok` dependency. Preparation
state and scheduler identity remain separate from scientific Task identities.
After a synchronous prepared run, the version-2 record finalizes whether the
target image was pulled, copied from a verified candidate, or reused and
whether compiled outputs were built or reused.

Project schema version 3 extends preparation without changing the portable
experiment schema. Its `source` selects either pinned `git` or an explicit empty
`working_tree` recipe. Its `image` selects either `prebuilt` with the existing
URI and SHA-256 identity or `definition` with a safe snapshot-relative `.def`
path and bounded CPU, memory, and walltime. Arbitrary definition files are
trusted project input; external base tags may be mutable, so Rundra records and
caches the measured SIF digest but cannot claim cold-build reproducibility
unless the definition itself pins its base identity.

Project schema version 4 requires each definition recipe to declare
`context.include`, including an explicit empty list when the definition has no
additional build inputs. Entries are unique, safe snapshot-relative files or
directories; the definition file is included automatically. The recipe key
uses the deterministic content of only this context. Version 3 continues to
use the complete source snapshot identity for compatibility. Resource limits
authorize execution but are not image content and therefore do not alter the
version-4 recipe identity.

Targets schema version 8 may authorize definition builds with
`preparation.definition_build`. The target owner selects allowed `local` and/or
`target` locations, `unprivileged` or `fakeroot` mode, and hard resource
ceilings. Project files cannot select privilege. In `auto` mode for an SSH
target, Rundra prefers a local content-addressed build followed by verified
atomic publication to the target image cache. Local builds execute
`apptainer build` as an argument array against the immutable source snapshot,
never the developer tree. The definition recipe key includes selected context
content, definition path, target, platform, builder version, and mode; the
published image cache is keyed by its measured SHA-256. `--rebuild-image`
bypasses only the recipe index. `--offline` permits only an existing verified
definition-image cache hit.

Forced `--prepare-location target` submits one scheduler job with the
definition recipe's bounded resources and the target-owned privilege mode; it
never builds on the SSH controller. Rundra waits for this job because the SIF
digest does not exist before the build, reads a framework-owned manifest,
updates `container_digest` to the measured SHA-256, and only then submits the
scientific jobs. If an application build is also configured, the same job runs
it sequentially inside the newly verified SIF. Its cache key is derived from
the measured image digest, declared outputs are verified before publication,
and the scheduler request uses the maximum CPU/memory requirement plus the sum
of both walltimes. A failed image or application build prevents scientific
submission and retains scheduler logs.
For pinned Git sources, successful target preparation also publishes an
immutable recipe index. A later warm `run` or `submit` validates the index,
platform fingerprint, image digest, build marker, and every declared output on
the target before copying the prepared source directly into the new Run
workspace. This path does not require a controller-side checkout or source
upload. Working-tree preparation never uses this recipe index because its
identity is local mutable content.
For asynchronous `submit`, the same finalization occurs during a later
successful `status` reconciliation; `inspect` remains network-free.
The implementation status and remaining adapter work are tracked in
`docs/m7-execution-plan.md`.

`run` and `submit` accept `--verbose` for meaningful lifecycle transitions and
`--progress` for a TQDM phase display. Live feedback is always written to
stderr, including when `--json` is selected, so stdout remains exactly one
machine-readable result document. Scheduler polling emits feedback only when
the preparation or scientific execution state changes. Neither option changes
the persisted RunRecord or the execution plan.

TQDM rendering deduplicates identical observations and coalesces changed
observations to a configurable `--progress-interval`, defaulting to ten
seconds. Phase transitions and terminal completion render immediately. A
captured `--json --progress` combination warns on stderr because carriage-return
redraws may be retained by an agent transcript. Agent automation should omit
`--progress` and consume the single final JSON document. `wait --notify` emits
one local terminal alert only after terminal reconciliation; it adds no daemon,
webhook, credentials, or scheduler-side callback.
`wait --notify-file PATH` is the noninteractive counterpart. It writes no
intermediate observations and atomically publishes one mode-0600 JSON document
only after terminal reconciliation. Existing symlinks and files identifying a
different Run are rejected.

For synchronous arrays, progress contains six lifecycle units plus one unit per
planned Task. Terminal Task observations advance the bar and its detail reports
terminal/total, running, queued, failed, and distinct allocated-node counts.
The resulting rate and ETA therefore adapt to array size and observed scheduler
throughput rather than treating a two-Task and thousand-Task Run identically.

---

### 21.3 `run`

```bash
rundr run experiment.yaml \
    --config configs/test.yaml \
    --seed 42 \
    --target shoal
```

Semantics:

```text
validate
  ↓
plan
  ↓
stage
  ↓
submit
  ↓
wait
  ↓
collect metadata
  ↓
fetch requested outputs
  ↓
return meaningful exit status
```

A failed task must still allow logs and partial outputs to be collected when possible.

---

### 21.4 `submit`

```bash
rundr submit experiment.yaml \
    --config configs/test.yaml \
    --seeds 0:99 \
    --target shoal
```

Semantics:

```text
validate
  ↓
plan
  ↓
stage
  ↓
submit
  ↓
record run ID
  ↓
return immediately
```

This is the preferred interface for long-running experiments.

---

### 21.5 `status`

```bash
rundr status <run-id>
```

For multi-task Runs, status should summarize task states.

Example human output:

```text
Run: run_ABC
State: RUNNING

Tasks:
  succeeded: 61
  running:    8
  queued:    31
  failed:     0
```

Version-1 JSON retains aggregate counts under `tasks` and adds ordered
`task_details`. Each detail contains the stable Task ID, seed, portable
execution and retrieval states, native scheduler identity/state when
available, and exit code when known.

Version-2 status additionally reports the preparation job's separate scheduler
identity, portable and native states, and builder location. A failed
preparation marks the Run failed before scientific work can execute.

---

### 21.6 `logs`

```bash
rundr logs <run-id>
```

Task selection should be possible:

```bash
rundr logs <run-id> --task task_000017
# zero-based ordinals are also accepted:
rundr logs <run-id> --task 17
rundr logs <run-id> --preparation
```

Useful options may later include:

```text
--stdout
--stderr
--tail N
```

Agents and users should not need to know native scheduler log filenames.
Preparation logs remain separate from scientific Task logs.

---

### 21.7 `fetch`

```bash
rundr fetch <run-id>
rundr fetch <run-id> --destination retrieved
rundr fetch --last
rundr fetch <run-id> --destination retrieved --mode reference
rundr fetch <run-id> --destination retrieved --task task_000017
rundr fetch <run-id> --destination retrieved --task 17 --task 18
```

Retrieves configured/requested outputs and relevant metadata.

The destination must be deterministic or explicitly reported.

Fetching should be idempotent where practical.

Omitting `--task` fetches every Task. Repeating `--task` selects a subset.
Per-Task retrieval state is durable, failed transfers may be retried, and a
Run reports retrieval `SUCCEEDED` only after every Task has succeeded.

---

### 21.8 `cancel`

```bash
rundr cancel <run-id>
```

Cancels active scheduler jobs associated with the Run.

For a prepared Run, cancellation covers both the framework-owned preparation
job and dependent scientific jobs.

Cancellation must be recorded in the RunRecord.

For an array, Rundra reconciles first, excludes already terminal elements, and
cancels only the remaining native Task identities. Elements that finish during
that race retain their observed terminal result.

---

### 21.9 `inspect`

```bash
rundr inspect <run-id>
```

Returns detailed run/provenance information.

---

### 21.10 `list`

```bash
rundr list
```

Lists known Runs.

The default human and JSON forms return compact Run summaries in deterministic
pages of at most 100 Runs. `--offset` and `--limit` select a page, with a hard
limit of 1,000, and the JSON page envelope reports the total and next offset.
Per-Task details are omitted unless `--include-tasks` is explicit; agents should
normally use the paginated `tasks RUN_ID` command instead.
This paginated list document is format version 2; readers may continue to
accept the retained unpaginated version-1 contract.

Filtering by state, target, experiment, or recency may be added later.

---

### 21.11 `targets`

```bash
rundr targets
```

Lists configured targets and basic capabilities.

Potential future operation:

```bash
rundr targets inspect shoal
```

---

## 22. Structured output

Every important command that returns programmatically useful information must support:

```text
--json
```

M6.1 makes `--json` a common option: it may appear immediately after `rundr`
or anywhere accepted by the selected subcommand. Both forms execute the same
operation and produce byte-identical deterministic JSON. A machine-readable
argument failure, unknown command, or missing command uses the standard
version-1 envelope with error code `CLI_USAGE_ERROR`; a failure tied to a known
command retains that operation name, while a missing/unknown command uses
operation `cli`. JSON is written to stdout with an empty stderr. Human failures
use the same error code/message on stderr.

Human output may evolve more freely.

Documented JSON structures are public interfaces and require greater stability.

Human and JSON output must be generated from the same internal result object, not through independent execution paths.

The v0.1 CLI envelope has `format_version: 1`, `operation`, and `ok` at its
root. Successful results carry an operation-specific value; failures carry an
`error` object with `code`, `message`, and structured `details`. Normative v0.1
examples for every operation are checked in under `docs/schemas/`; `inspect`
composition is contract-tested against the durable RunRecord example.
Backward-incompatible changes to
those documented fields require a new format version.

---

### 22.1 Submission result example

```json
{
  "ok": true,
  "run_id": "run_01K2...",
  "state": "SUBMITTED",
  "target": "shoal",
  "tasks": 100,
  "backend": {
    "scheduler": "slurm",
    "job_ids": ["18372"]
  }
}
```

---

### 22.2 Status result example

```json
{
  "ok": true,
  "run_id": "run_01K2...",
  "state": "RUNNING",
  "tasks": {
    "total": 100,
    "succeeded": 61,
    "running": 8,
    "queued": 31,
    "failed": 0,
    "cancelled": 0
  }
}
```

---

### 22.3 Structured error example

```json
{
  "ok": false,
  "error": {
    "code": "RESOURCE_VALIDATION_ERROR",
    "message": "Requested GPU resources are not valid for target 'shoal'.",
    "details": {
      "gpus_per_task": 4
    }
  }
}
```

Failures must not be communicated only through free-form prose.

CLI process exit codes should also reflect success or failure appropriately.

The audited v0.1 exit-code contract is:

- 0 for a successfully performed operation and ordinary human help;
- 1 for usage, configuration, capability, persistence, transport, scheduler,
  staging, retrieval, and other operation failures;
- 2 only when `run` successfully returns a durable Run result whose aggregate
  execution state is `FAILED` or `CANCELLED`.

Thus status/list/logs/fetch/inspect/cancel may successfully describe a failed
or cancelled Run with process exit zero; their own operation succeeded. Help
remains human-oriented. Plain `rundr` displays help with exit zero, whereas
`rundr --json` without a command is a structured usage failure with exit one.

The M1.5 local CLI adds synchronous `run` and persisted `status`, `list`,
`logs`, `fetch`, and `inspect`. Each useful command supports `--json` and both
renderers consume the same typed result. The default record directory is
`~/.local/share/rundra/runs`; `--data-dir` selects another store. `run` accepts
one seed or an inclusive multi-seed range, uses the current directory as its
default source root, and reports the output destination and stable Run ID. A
successfully reconciled
task failure returns a Run document with state `FAILED` and process exit code
2; operation/configuration/infrastructure failures return exit code 1; other
successful operations return 0.

`list` and `inspect` load strict persisted RunRecords. `status` additionally
refreshes active remote Runs through the selected scheduler adapter. `logs`
resolves framework-managed stdout/stderr artifacts by stable Task ID and reads
remote scheduler logs through the configured transport. Local `fetch` reconstructs the
per-Run workspace from its target and Run ID, is idempotent, and updates the
artifact manifest; refetching an already successful retrieval keeps its
successful state. Asynchronous
`submit` and idempotent `cancel` are available for supported SSH scheduler
targets using Slurm or OpenPBS; local `submit` remains the structured
`ASYNC_UNAVAILABLE` capability error because no detached local process owner
exists.

Synchronous `run` wires the Git provenance provider into the same orchestration
service. `inspect` exposes the persisted optional fields through the existing
RunRecord contract; no CLI-specific Git command path exists.

### 22.4 Launch defaults and profiles

The pre-M2 M1E milestone retains the fully explicit CLI and adds a strict,
versioned project launch file and user defaults. A configured project supports:

```bash
rundr run experiment.yaml
```

Launch values resolve in this order:

```text
explicit CLI argument
→ selected project profile
→ project default
→ user default
→ built-in default
```

An adjacent project file can define defaults and named profiles without
embedding backend definitions:

```yaml
version: 1
default_profile: local
profiles:
  local:
    config: config.yaml
    target: local
    source_root: .
    destination: retrieved
    # seed is optional
```

Optional user defaults live at `~/.config/rundra/config.yaml`:

```yaml
version: 1
defaults:
  target: local
  targets_file: targets.yaml
  data_dir: records
```

Project configuration may select a target name, but target backend definitions
remain in the separate target file. Paths are resolved relative to the file
that declares them. Resolution is inspectable in human and JSON plan/run output,
never prompts in an agent-facing workflow, and returns a structured error when
config or target remains unavailable. Automatic discovery is deliberately
adjacent-only; `--project-file` selects a non-adjacent project file.

An omitted seed requests framework generation, not application-level implicit
randomness. The launch resolver generates the integer before invoking the pure
planner; the Task and RunRecord contain that concrete value. A non-submitting
plan may generate and display a preview seed, but an independent later run will
generate another seed unless the preview is passed explicitly.

When no launch layer supplies a retrieval destination, Rundra derives
`<project-root>/retrieved/<config-stem>`, or
`<current-working-directory>/retrieved/<config-stem>` without a discovered
project file. Explicit CLI, profile, project, and user destinations retain
their exact meaning.

Successful plan/run JSON includes a sibling `launch` value containing the
selected profile, consumed effective values, and a source for each field. The
stable source forms are `cli`, `project_profile:NAME`, `project`, `user`,
`built_in`, and `generated`. This is additive to the existing version-1
operation payload.

---

## 23. Agent-facing design requirements

Version 0.1 does not implement an autonomous agent.

It must nevertheless expose a clean substrate for agents.

Required characteristics:

- stable Run IDs;
- stable Task IDs;
- deterministic JSON output;
- explicit seeds;
- `plan` before submission;
- inspectable resource requests;
- structured errors;
- asynchronous `submit`/`status` workflow;
- machine-readable provenance;
- no need to parse scheduler output;
- no need to understand remote directory layout;
- meaningful failure categories;
- safe cancellation;
- retrieval of logs and artifacts through framework operations.

---

### 23.1 Agent interface principle

Agents should interact conceptually with:

```text
validate
plan
submit
status
logs
fetch
cancel
inspect
```

rather than:

```text
ssh
rsync
sbatch
squeue
sacct
scancel
find slurm-*.out
```

This boundary is a central design goal.

---

### 23.2 Future MCP interface

A future MCP server may expose the same core operations.

MCP must be an interface adapter over the execution core, not the foundation of the architecture.

Version 0.1 should rely on the CLI plus `--json` as the first agent-compatible interface.

The implemented MCP adapter is optional and remains above the same typed
operation layer as the CLI. It fixes its project root, RunStore, and target
file at startup, constrains tool paths, and requires a fresh plan digest before
execution. Local stdio remains the default. An explicitly selected Streamable
HTTP transport requires an environment-sourced static bearer token and
DNS-rebinding host policy; TLS terminates at an operator-managed reverse proxy.
The process is launched by the user and is not a persistent Rundra daemon.

Long Runs use renewable waiting. `wait` reconciles durable state until terminal
or timeout and never fetches implicitly. MCP waits are bounded so an agent host
can renew them; no Rundra daemon is required.

MCP asynchronous submission uses the same durable submission receipts as the
CLI. Agents can call `resume_submission` after a disconnected or interrupted
submission to recover scheduler identifiers without creating a duplicate Run.
Run discovery is bounded: `list_runs` returns compact summaries by default and
accepts `offset`, `limit`, and explicit `include_tasks` arguments; task details
remain available through paginated `list_tasks` calls.

---

## 24. Resource policy — future design constraint

A future policy layer should be able to restrict autonomous execution.

Example conceptual policy:

```yaml
agents:
  development-agent:
    allowed_targets:
      - shoal

    maximum:
      nodes: 2
      gpus: 2
      walltime: "00:30:00"
      concurrent_tasks: 16
      gpu_hours_per_run: 2
```

Possible outcomes:

```text
allowed
rejected
approval_required
```

Policy evaluation belongs above scheduler submission.

Scheduler accounts, QOS, partitions, and site permissions remain authoritative.

No full policy engine is required for version 0.1.

However, v0.1 architecture must not make such a layer difficult to add.

---

## 25. Parameter sweeps

An application YAML passed through `--config` opts into deterministic expansion
with a strict top-level `_rundr` block:

```yaml
_rundr:
  version: 1
  seeds: "0:19"
parameters:
  density:
    batch_options: [0.1, 0.2, 0.3, 0.4]
  noise:
    batch_options_range: {start: 0.0, stop: 0.1, step: 0.05}
```

The supported marker subset is `batch_options`, `batch_options_range`, and
`batch_hierarchical_options`. Dimensions expand as a Cartesian product in YAML
traversal and value order. Hierarchical choices merge into their containing
mapping; `default` is excluded from choices and `name` labels provenance.
Rundra strips `_rundr` and expansion markers from every effective application
config.

Seed precedence is CLI `--seed`/`--seeds`/`--random-seed`, `_rundr.seeds`,
configured launch defaults, then generation. One Run contains parameter sets x
seeds. Repeated seeds are valid across parameter sets, while Task IDs remain
globally unique. Slurm represents the complete set as one array and maps each
array index to a Task ID, seed, and immutable Task-specific config.

Parameterized plans and RunRecords use format version 3. Each Task records a
deterministic parameter-set ID and chosen values; plans additionally expose the
effective-config SHA-256. Retrieval includes `metadata/tasks.json` with the
Task, seed, choices, config digest, and output mapping. Unswept v1/v2 documents
remain unchanged and readers support all three versions.

Rundra does not interpret application parameters beyond materializing YAML and
does not merge scientific results. Pogosim-specific conventions and richer
campaign behavior may live in a separate `pogorundr` layer built on these
portable Run semantics.

---

## 26. Structured scientific metrics — future design constraint

Experiment applications may later optionally publish a compact machine-readable result file.

Example:

```json
{
  "success": true,
  "metrics": {
    "accuracy": 0.843,
    "mean_departure_time": 42.7,
    "polarization": 0.68
  }
}
```

This would let agents inspect scientific results without parsing arbitrary logs or loading large result datasets.

Structured metrics are distinct from arbitrary artifacts.

Version 0.1 does not require scientific result interpretation.

---

## 27. Run lineage — future design constraint

Future adaptive or agent-driven research may create Runs based on earlier Runs.

A RunRecord may eventually include:

```json
{
  "parent_run": "run_ABC",
  "reason": "Increase replication near density=0.3",
  "initiator": {
    "type": "agent",
    "name": "research-agent"
  }
}
```

This should support reconstruction of experiment campaigns such as:

```text
initial hypothesis
      |
      v
initial sweep
   |      |
 run A   run B
           |
           v
     agent follow-up
       |        |
     run C    run D
```

No lineage system is required for v0.1.

---

## 28. Shell and command safety

Remote execution is a security boundary.

Requirements:

- invoke every local subprocess with an argument array and explicit
  `shell=False`;
- never concatenate untrusted YAML values directly into executable shell syntax;
- quote values safely when shell scripts are unavoidable;
- never log credentials;
- never disable SSH host verification by default;
- never expose a network daemon merely for convenience;
- never bypass scheduler permissions;
- run remote operations with the privileges of the configured user;
- make backend-native options explicit and auditable.

Generated scheduler scripts should be inspectable where practical.

The v0.1 implementation has exactly two shell serialization formats. OpenSSH
requires one remote login-shell command, whose arguments, environment, and
working directory are serialized by the shared POSIX quoting boundary described
in section 36. Slurm requires an sbatch script; framework-owned directives are
rendered from typed portable resources, allowed native values are restricted to
one unambiguous directive token, and experiment commands and array manifests use
the same quoting boundary without `eval`. A static test audits every production
`subprocess.run` call for the local argument-array invariant, while executable
tests pass shell metacharacters, substitutions, quotes, wildcards, and newlines
through both unavoidable formats as literal data.

Local native execution deliberately inherits the invoking environment and then
overlays the explicit experiment environment. Container execution instead uses
Apptainer `--cleanenv --no-eval` and its runtime-owned environment mapping. No
environment value, external-tool stderr, generated remote command, or
process-start exception text is included in framework failure diagnostics.

---

## 29. Local execution backend

A local backend is required early for:

- development;
- tests;
- CI;
- validating the orchestration model without a real cluster.

Local execution should follow the same high-level model as remote execution:

```text
ExperimentSpec
    ↓
Run
    ↓
Task
    ↓
ContainerRuntime / command
    ↓
RunRecord
    ↓
Artifacts
```

Local execution must not be implemented as an unrelated special-case CLI path.

The M1 local execution implementation consists of a shell-free
`LocalTransport` and a synchronous, one-Task `LocalScheduler`. The transport
overlays the command's explicit environment on the inherited process
environment, applies its working directory, captures stdout/stderr as UTF-8
text, and returns non-zero exits as normal `CommandResult` values. Process-start
failures remain distinct adapter errors. The local scheduler assigns an opaque
native reference, executes through `Transport`, retains the terminal
observation, and maps exit zero to `SUCCEEDED` and all other exits to `FAILED`.
It provides no asynchronous cancellation or local resource enforcement in M1.

`OrchestrationService` implements the first common vertical slice for exactly
one planned Task. It rejects a plan that does not reproduce from the recorded
experiment, config, target, and seed; creates the `RunRecord` before capability
checks; stages the immutable snapshot; constructs the runtime command; and
submits it through `Scheduler`. The staged source and input directories are
bound read-only at `/workspace/source` and `/workspace/input`; output and
runtime directories are bound read-write at `/workspace/output` and
`/workspace/runtime`. Relative experiment working directories are rooted below
the semantic source directory.

M1 also provides an explicit no-container `native` runtime for the checked
minimal example because the development environment has an Apptainer executable
but no usable, pinned local image. Native execution is valid only when
transport, scheduler, and staging are all local and the experiment declares no
container image or GPU passthrough. It maps the same staged semantic paths to
their host paths and still uses the common planner, orchestration service,
scheduler, transport, store, and artifact path; it is not a CLI shortcut.
Containerized experiments continue to require the Apptainer runtime.

After reconciliation, stdout and stderr are written as separate Task artifacts
and the terminal state, native state, timestamps, scheduler reference, and exit
code are persisted. Requested outputs are fetched even after a non-zero task
exit. The versioned `RunRecord.artifacts` collection is the M1 artifact
manifest. Computation and retrieval state are updated independently, so a
retrieval failure after successful execution leaves computation `SUCCEEDED`.
Local asynchronous submission remains unavailable until it has durable
post-process semantics.

---

## 30. Shoal reference deployment

The initial real-cluster integration target is shoal.

Known topology:

```text
remote laptop/workstation
        |
        | SSH
        v
    fishvision
        |
        | Slurm control/submission
        | shared /shoalhome
        v
   shoal1 ... shoal8
        |
        | Apptainer
        v
 experiment processes
```

`fishvision` is strictly a transport and scheduler-control boundary. Framework
operations there are limited to capability/filesystem probes, staging,
retrieval, and Slurm submission, observation, and cancellation. Rundra must
never launch application commands, analysis, compilation, tests, simulations,
or containers directly on `fishvision`. Such work runs locally on `bigfish` or
inside a Slurm allocation on `shoal1` through `shoal8`; direct SSH execution on
a compute node is not a replacement for scheduler allocation.

`/shoalhome` is the intended shared workspace root for fishvision and the
compute nodes. During the M4.2 login-side preflight on 2026-08-15, both `stat`
and `findmnt` identified it as `zfs`, not NFSv4. The bounded M4.3 CPU and M4.4
GPU Runs subsequently executed staged source/config directly from `/shoalhome`
on `shoal1`, proving compute-node visibility for the tested path. The framework
must not infer shared visibility from a filesystem-type label alone.

Shoal currently supports CPU and NVIDIA GPU workloads.

A direct site-specific GPU allocation may use:

```bash
--gres=gpu:1
```

Rundra's portable `gpus_per_task: 1` request rendered
`--gpus-per-task=1` during M4.4, and Slurm recorded `gres/gpu=1` in the actual
allocation. Direct `--gres` syntax remains a backend/native detail rather than
part of the portable experiment model. Apptainer `--nv` remains an independent
container-runtime control.

---

## 31. Shoal CPU acceptance scenario

A user on a remote workstation must be able to launch a CPU experiment such as:

```bash
rundr run experiment.yaml \
    --config configs/test.yaml \
    --seed 1 \
    --target shoal
```

without manually:

- committing or pushing source;
- SSHing interactively into fishvision;
- synchronizing project files by hand;
- writing an sbatch script;
- invoking `sbatch`;
- finding Slurm logs;
- rsyncing result directories back.

---

## 32. Shoal GPU acceptance scenario

The same must work for a GPU experiment whose `ExperimentSpec` requests a GPU.

The generated execution must correctly provide GPU access inside Apptainer, including the equivalent of:

```bash
apptainer exec --nv ...
```

and the scheduled task must receive the appropriate scheduler GPU allocation.

---

## 33. Multi-seed execution

Version 0.1 must support one logical Run containing multiple seeds.

Example:

```bash
rundr submit experiment.yaml \
    --config configs/test.yaml \
    --seeds 0:99 \
    --target shoal
```

The core expands this into 100 logical Tasks.

The Slurm backend may optimize this using a native job array.

The mapping between:

- framework task ID;
- seed;
- Slurm array index;

must be explicit and recorded.

M5.1 fixes the v0.1 seed-set semantics before scheduler grouping:

- CLI ranges use strict inclusive `START:STOP` syntax, so `0:2` means the
  ordered seeds `(0, 1, 2)` and `4:4` means one Task;
- negative integer endpoints are valid, whitespace, extra separators, decimal
  endpoints, and reversed ranges are invalid;
- caller-provided seed sequences retain their order and reject duplicates;
- Task IDs are contiguous zero-based ordinals in that requested order
  (`task_000000`, `task_000001`, ...), never seed-derived or scheduler-derived;
- every Task in the v0.1 replicated Run consumes the same exact effective
  config snapshot.

The version-1 RunRecord represents these facts without duplicating them: the
ordered `run.tasks` array contains each Task ID, seed, config, resources, and
state; `task_exit_codes` is keyed by Task ID; and every task-specific artifact
carries its Task ID. Shared source/config artifacts remain Run-level.

M5.2 adds a pure, inspectable execution grouping before any scheduler command
is constructed. A Slurm target with two or more homogeneous Tasks selects the
`slurm_array` strategy and one ordered group; a single Task or a non-Slurm
target retains `one_unit_per_task` with singleton groups. Groups must partition
the logical Tasks exactly once and in plan order. Array Tasks must share the
same exact config and resource request.

For an array plan, `array_mapping` records a contiguous zero-based mapping:

```json
[
  {"task_id": "task_000000", "seed": 7, "array_index": 0},
  {"task_id": "task_000001", "seed": 8, "array_index": 1}
]
```

The same mapping is preserved as immutable `task_array_mapping` definition
data in RunRecord v1. Pre-M5.2 version-1 records load with an empty mapping.
The mapping contains no scheduler job ID and does not make a Task an array
element; it only records how a selected backend strategy will relate the two.
M5.2 does not generate an array script, submit work, or interpret Slurm
accounting.

M5.3 adds a construction-only Slurm boundary. A `SlurmArrayRequest` requires:

- at least two ordered scheduler units;
- the exact M5.2 Task/seed/index mapping;
- uniform resources;
- an absolute NUL-free remote manifest path;
- an explicit positive site `MaxArraySize`, with the Task count no greater
  than that bound.

The Task manifest is a deterministic POSIX-shell `case` dispatcher. It accepts
exactly one validated zero-based index and executes the corresponding canonical
`Command` serialized through Rundra's existing safe remote-shell boundary.
Task IDs and integer seeds appear only as inert comments; command/config/seed
literals are shell-quoted, and neither `eval` nor a nested `sh -c` is used.
Missing or unknown indices exit 64.

The separate sbatch script contains portable and allowlisted native resource
directives plus the exact contiguous `--array=0-N` bound. Array stdout and
stderr paths must be absolute and contain both Slurm's `%A` array-job and `%a`
array-index placeholders. The script contains no Task command/config/seed
payload; it passes `SLURM_ARRAY_TASK_ID` to the safely quoted manifest path.

M5.3 deliberately keeps multi-Task `SlurmScheduler.submit` disabled before any
transport call. Persisting the manifest, submitting it, and reconciling
per-Task scheduler/accounting observations are completed with the M5.4
lifecycle work; the pure renderer cannot allocate resources.

M5.4 enables that lifecycle through an additive array-scheduler capability.
The Slurm adapter reads the controller's positive `MaxArraySize`, constructs
the bounded M5.3 request, atomically writes a mode-500 manifest under the
isolated Run metadata directory without overwriting an existing file, and
submits the separate sbatch script. The root array job ID remains in
`scheduler_job_ids`; `task_scheduler_ids` durably maps every logical Task to
its opaque native array-element identity before `submit` returns.

Status reconciliation queries every element in Task order. Each Task retains
its own portable state in `run.tasks`, native state in `task_native_states`,
exit in `task_exit_codes`, and stdout/stderr artifacts tagged with its Task ID.
These two new Task-keyed mappings are additive RunRecord-v1 fields; older
version-1 documents load them as empty mappings. Run-level allocated nodes and
timestamps summarize available observations without replacing the per-Task
facts. When native element states differ, Run-level `native_state` is `MIXED`.

Aggregate portable Run state is deterministic and does not become terminal
while any Task remains nonterminal. Running work takes precedence; a succeeded
or failed Task combined with queued/submitted/staging/created siblings also
keeps the aggregate `RUNNING`. Otherwise active precedence is `QUEUED`, then
`SUBMITTED`, `STAGING`, and `CREATED`; an otherwise unresolved mixture is
`UNKNOWN`. Once every Task is terminal, precedence is `FAILED`, then
`CANCELLED`, then `SUCCEEDED`. Array cancellation, per-Task log selection, and
partial/repeated fetch behavior are completed by M5.5.

M5.5 exposes replicated execution through `run --seeds START:STOP` and
`submit --seeds START:STOP`. Array cancellation first reconciles all elements,
then sends only active native Task identities to the scheduler and applies
race outcomes independently. `logs --task` accepts either a stable Task ID or
zero-based ordinal and terminal logs use durable artifacts without another
scheduler query.

`fetch --task` is repeatable and selects isolated Task output prefixes; without
selectors it fetches the whole Run. RunRecord v1 adds the backward-compatible
`task_retrieval_states` mapping. Older records inherit their Run-level
retrieval state. Partial success remains globally `PENDING`, any attempted
failure is `FAILED`, failed Tasks may transition through `PENDING` on retry,
and only all-Task success is globally `SUCCEEDED`. Public status JSON preserves
aggregate counts and adds ordered per-Task seed, execution/retrieval, native,
and exit details.

M5.6 completes the replicated-experiment acceptance gate. Default integration
coverage repeats the same seed set through the complete fake Slurm lifecycle
and proves identical Task IDs, configs, explicit mapping, generated dispatch
manifest, exits, and retrieval outcomes. The separately gated Shoal proof uses
three bounded CPU Tasks in one array, including one controlled non-zero exit,
and verifies every Task's mapping, state, exit, logs, raw result, and retrieval.

Slurm accounting is queried with the display `JobID`, not `JobIDRaw`. For array
elements the former preserves the stable `array-root_index` alias used by
submission, logs, and Rundra's Task mapping. Some Slurm installations assign a
different raw allocation ID to each element, so `JobIDRaw` cannot be used as a
portable array-element correlation key. This remains an adapter detail; core
Task identity and RunRecord schemas are unchanged.

---

## 34. Slurm backend

Version 0.1 Slurm support must include:

- submission;
- synchronous waiting for `run`;
- asynchronous submission for `submit`;
- scheduler job ID capture;
- state queries;
- portable state mapping;
- cancellation;
- stdout/stderr handling;
- exit/failure detection;
- Slurm arrays for multi-seed runs;
- CPU resources;
- GPU resources.

Where appropriate, the implementation may use standard Slurm commands such as:

```text
sbatch
squeue
sacct
scancel
srun
```

The rest of the application should not depend directly on their textual output formats.

Parsing belongs inside the Slurm adapter.

The M3 implementation supports the one-Task reference path. It stores the
native job reference before the first query, polls without a daemon, leaves an
active Run nonterminal when the client wait times out, and permits a later
client process to continue by Run ID. Active state comes from `squeue`; missing
jobs fall back to `sacct`, including native state, exit code, timestamps, and
allocated nodes. Slurm stdout/stderr use adapter-owned job-ID paths under the
target workspace and are exposed as portable Task artifacts. Slurm arrays and
multi-seed grouping remain the M5 deliverable.

Native timestamp text is retained in scheduler metadata. A timestamp without a
UTC offset is promoted to a portable timezone-aware value only when the
adapter has an explicit site timezone; Rundra does not silently assume UTC.

---

## 35. Apptainer backend

Version 0.1 assumes Apptainer is installed on the execution target.

Minimum supported features:

- image path;
- normal execution;
- NVIDIA GPU enablement;
- bind mounts;
- environment variables needed by the experiment.

Apptainer invocation must be generated independently from Slurm submission.

The architecture must remain compatible with sites still exposing the `singularity` command where practical, but implementing a separate Singularity backend is not a v0.1 requirement.

The M1 command-construction contract is deliberately pure: it validates the
request and returns an argument array without starting the runtime. Commands
use `exec`, `--cleanenv`, and `--no-eval`; add `--nv` only for requested GPU
access; encode every bind independently with an explicit `ro` or `rw` mode;
and use `--cwd` for an absolute container working directory. Experiment
environment values are passed through the runtime's inherited
`APPTAINERENV_` variables (or `SINGULARITYENV_` when the configured executable
is `singularity`) so values do not need to be encoded into a comma-delimited
argument. Bind paths that cannot be represented unambiguously are rejected.

The initial capability check establishes only that the configured executable
is discoverable; it does not execute a version probe. Container execution and
runtime-version provenance are introduced only when the orchestration path
actually runs commands.

---

## 36. SSH transport

The OpenSSH transport relies on normal user SSH configuration. Its constructor
accepts one host or host alias and, optionally, an alternate `ssh` executable
and explicit local OpenSSH config file. Target configuration represents these
as `transport.executable` and `transport.config_file`; the latter must be an
absolute non-root client path. The same selection is used by SSH transport,
rsync staging/retrieval, diagnostics, and reconstructed lifecycle operations.
Capability checking confirms that executable is discoverable without making a
network connection. Command execution invokes OpenSSH with a local subprocess
argument array and `shell=False`, disables pseudo-terminal allocation, and
returns the same typed `CommandResult` used by local transport.

The adapter accepts only an unambiguous host alias or `user@host` destination;
option-like, whitespace-bearing, shell-bearing, and colon-bearing destinations
are rejected before OpenSSH or rsync starts. It deliberately supplies no
authentication, jump-host, host-key, or user options of its own. OpenSSH
therefore continues to use:

- host aliases;
- SSH agent authentication;
- standard `~/.ssh/config`;
- existing jump hosts when supported transparently by SSH.

Rundra passes an explicit config as OpenSSH `-F` and never weakens host-key
verification. It records only the config path in target provenance, never key
or credential contents.

Rundra does not disable host-key verification or build a parallel SSH
configuration system. Authentication material is neither accepted by the
adapter nor included in its start-failure diagnostics. Nonzero remote exits are
returned as command results; inability to discover or start the local OpenSSH
client raises a transport-specific error.

OpenSSH necessarily passes its remote command through the login shell. Rundra
quotes each command argument, environment assignment, and working directory at
one focused, reusable serialization boundary. The boundary rejects values that
cannot cross a process or POSIX-shell interface, orders environment assignments
deterministically, and preserves spaces, quotes, metacharacters, backslashes,
and newlines as literal data.

The executable remote string contains the command's literal values and must not
be used as diagnostic text. Transport diagnostics use only a structural summary
containing redaction markers and value counts. Process-start exception text and
exception chaining are omitted because either could echo arguments or
environment data supplied to the subprocess boundary.

The transport should not require inbound connectivity from the cluster to the client.

### 36.1 Remote workspace allocation

Remote Run workspaces are allocated beneath an absolute, non-root POSIX
workspace path as `WORKSPACE/runs/RUN_ID`. The allocator accepts only validated
framework `RunId` values, rejects relative roots, traversal, NUL, and filesystem
root, and verifies every semantic child path remains contained beneath the
configured root before issuing a command.

Allocation creates a unique Run directory without overwriting an existing path,
then creates separate `source`, `input`, `runtime`, `output`, `logs`, and
`metadata` children. A collision is distinct from a connectivity, permission,
or directory-creation failure. This step does not upload or seal source data.

---

## 37. Rsync staging

For the shoal reference implementation, rsync is the preferred source/result transport.

Benefits include:

- efficient incremental uploads;
- no requirement to commit source before testing;
- support for large project trees;
- familiar HPC deployment semantics.

Each Run must still receive an immutable source snapshot after staging.

The M2 rsync stager first allocates the validated unique remote workspace, then
invokes the locally installed `rsync` with argument arrays, archive mode,
copy-link snapshot semantics, protected arguments, deletion within the new
empty source directory, and one explicit exclusion argument per default or
experiment pattern. It copies the current filesystem tree directly, including
uncommitted and non-Git files; Git is not involved in transfer.

The exact effective configuration bytes are flushed to a private temporary
local file and transferred separately to `input/config.yaml`. Only after both
transfers succeed does the stager recursively remove write permissions from the
remote `source` and `input` trees. A failed or interrupted transfer raises a
staging error and leaves the unique Run directory reserved, so partial content
cannot be reused or reported as a completed immutable snapshot. Transfer
diagnostics contain the Run ID and exit category, not rsync output or source and
configuration values.

Result retrieval remains possible separately from submission. A newly
constructed rsync stager with the target host can validate an existing semantic
workspace and fetch its `output`, `logs`, and `metadata` trees into distinct
subdirectories of an explicit local destination. Output include patterns remain
relative and are passed as separate rsync filter arguments.

Retrieval uses protected arguments, refuses symlinks, and enables rsync delayed
updates so completed files replace their destinations atomically. Repeating the
same request safely updates the same paths. Only after all three transfers
succeed are regular files scanned into raw-result, stdout/stderr, and scheduler
metadata artifacts with measured sizes. Nonzero or interrupted transfers do not
return a successful `FetchResult` and their diagnostics omit remote output.
Before local or rsync retrieval writes, every existing destination path
component and existing descendant is checked without following symlinks. A
symlink therefore cannot redirect a fetched artifact outside the selected local
destination.

Portable computation and retrieval states remain independent. A transfer
failure moves retrieval from `PENDING` to `FAILED` without changing a completed
Run or Task execution state, exit code, or scheduler identity; a later explicit
fetch may transition `FAILED` back through `PENDING` to `SUCCEEDED`.

Default integration tests use executable OpenSSH and rsync shims backed by
temporary local trees. They exercise the real subprocess and remote-shell
boundaries, live non-Git upload, exact config transfer, sealing, two isolated
snapshots across a source edit, result/log/metadata round trips, repeat fetch,
interruption, and retry without network or Slurm access. Tests against a real
SSH host or rsync installation are developer-owned opt-in system tests only and
must never run in ordinary CI without explicit configuration.

---

## 38. Persistence

The client must persist enough metadata to find and manage previously submitted Runs after the original process exits.

For asynchronous runs, this is mandatory.

A simple local persistence mechanism is preferred for v0.1.

Possible implementation:

```text
~/.local/share/rundra/runs/
```

with one structured record per Run.

A relational database is not required for v0.1 unless implementation evidence shows that it substantially simplifies correctness.

Persistence details are implementation choices, but public Run semantics must not depend on the initiating shell process remaining alive.

The version-1 local store uses one `<run-id>.json` document per Run. Creation is
collision-safe, updates replace a fully written temporary document atomically,
and incompatible versions or unknown fields fail explicitly. The persisted
format stores portable computation and result-retrieval states independently;
its checked example is `docs/schemas/run-record-v1.json`.

Every update is a compare-and-swap operation: the caller supplies the complete
record it observed along with the desired replacement. A standard-library
advisory lock serializes writers for that Run across processes, and the store
compares the observed record while holding the lock. A changed record produces
`RunStoreConflictError` rather than silently losing the newer state. If the
desired replacement is already present, the update succeeds idempotently.
Readers remain lock-free and see either complete old or complete new JSON.
The store also recursively rejects credential-bearing field names before a
programmatic record is written and after JSON is loaded. This preserves the
no-credential invariant even when a caller bypasses YAML loaders or a persisted
document is modified outside Rundra.

---

## 39. Concurrency and idempotency

The implementation should anticipate multiple Runs existing simultaneously.

Requirements:

- Run IDs must avoid collisions;
- staging one Run must not alter another;
- status queries must be safe concurrently;
- repeated `fetch` operations should not corrupt results;
- repeated `cancel` requests should fail gracefully or be idempotent where practical.

Distributed locking is not a v0.1 requirement unless tests demonstrate a real need.

M6.3 stress tests demonstrated a same-Run lost-update race, so the local store
uses only a per-Run advisory writer lock; it adds no global lock, daemon,
database, or distributed coordination. Simultaneous identical status, fetch,
and cancel operations converge idempotently. Incompatible concurrent lifecycle
updates preserve the winner and return structured `RUN_STORE_CONFLICT` with an
explicit retry action. Interrupted writes leave the previous record intact and
remove temporary files.

Asynchronous scheduler submission uses a separate atomic receipt per Run.
Readers support the historical version-1 pending/completed representation;
writers use version 2 with explicit `pending`, `accepted`, `rejected`,
`uncertain`, and `operator_resolved` outcomes plus safe backend, phase, exit
code, and failure-classification fields. Rundra writes a pending receipt before
scheduler contact and records every scheduler root and Task mapping as accepted
before transitioning the RunRecord to `SUBMITTED`. A definitive scheduler
rejection transitions the Run to `FAILED`; transport and response ambiguity
remain `STAGING` with an uncertain receipt.

`rundr resume RUN_ID` adopts an accepted receipt after an interrupted
client-side RunRecord update, reports an already durable submission as `found`,
and refuses to resubmit unknown outcomes. After independently inspecting the
scheduler, an operator who has verified that no job exists may run `rundr
resolve-submission RUN_ID --not-submitted --confirm RUN_ID`. The exact repeated
Run ID, absent scheduler identities, uncertain/pending receipt, and `STAGING`
state are all mandatory; the operator resolution is persisted before the Run
becomes `FAILED`. Rundra never creates a possible duplicate merely to make
forward progress. Run registration is printed to stderr as soon as the initial
RunRecord is durable, while structured stdout remains valid JSON when
requested.

---

## 40. Versioned schemas

Public structured representations use explicit versioning. Experiment, target,
project-launch, and user-launch YAML documents require `version: 1`. Persisted
RunRecords and public CLI JSON require `format_version: 1`.

Example:

```yaml
version: 1
```

Do not silently reinterpret an incompatible old schema.

Migration tooling is not required before there is more than one released schema version.

---

## 41. Logging

Framework logs must be distinguishable from experiment stdout/stderr.

Do not mix:

```text
rundr orchestration logs
```

with:

```text
scientific application stdout/stderr
```

Machine-readable result objects should expose paths or identifiers for relevant logs.

Framework diagnostics must describe command shape and failure category without
including argument values, environment values, generated remote command text,
or external-tool stderr. Experiment stdout/stderr remains explicit user-facing
run data and is not reclassified as framework diagnostic output.

---

## 42. Testing strategy

### 42.1 Unit tests

Unit tests should cover:

- experiment-schema validation;
- target-schema validation;
- seed expansion;
- task construction;
- resource normalization;
- command construction;
- state mapping;
- Slurm command/script generation;
- shell quoting where unavoidable;
- RunRecord serialization;
- JSON result serialization;
- error serialization;
- artifact manifests.

---

### 42.2 Fake adapters

Unit and integration tests must not require access to a real Slurm cluster.

Implement fake/mock infrastructure adapters early.

At minimum, tests should be able to simulate:

```text
SUBMITTED
    ↓
QUEUED
    ↓
RUNNING
    ↓
SUCCEEDED
```

and:

```text
SUBMITTED
    ↓
RUNNING
    ↓
FAILED
```

as well as cancellation.

Recommended testing helpers include:

- `FakeScheduler`;
- `FakeTransport`;
- `FakeStager`;
- fake or recording container runtime.

---

### 42.3 Integration tests

Default CI integration tests should cover complete orchestration without requiring shoal.

Examples:

- local Run lifecycle;
- fake remote staging;
- fake scheduler submission;
- multi-task state aggregation;
- run persistence;
- JSON CLI output;
- deterministic seed propagation;
- failure propagation;
- result retrieval.

---

### 42.4 System tests

Tests requiring a real cluster must be opt-in.

The M4.2 Shoal harness adds a second explicit command-line opt-in in addition
to its registered marker. It first constructs a bounded pure plan, then checks
local OpenSSH/rsync discovery, SSH connectivity, the remote workspace or its
nearest writable existing ancestor, mandatory Slurm commands (`sbatch`,
`squeue`, `scancel`, and `scontrol`), optional `sacct` discovery, Apptainer and
its configured image, requested resources through
`sbatch --test-only`, and the login-side `/shoalhome` filesystem identity. It
does not allocate a Run, stage source, execute a container, or submit a job.
Arbitrary remote stderr is excluded from its layer-specific diagnostics.

M4.3 validates the first bounded CPU execution with a second explicit opt-in.
The system test constructs a pure plan and runs preflight before submission,
then exercises one synchronous CLI Run from a temporary dirty Git repository.
It verifies tracked and untracked source execution, exact config/seed, sealed
remote source, Slurm identity/state/node/timestamps/exit, normalized logs, raw
result retrieval, artifact manifest, and available Git provenance. At the time
of the first CPU run, Shoal exposed the `sacct` command with accounting storage
disabled, so Slurm reconciliation used `scontrol show job -o` when the primary
accounting query failed. Native timestamp strings are retained without
fabricating a site timezone. Accounting was enabled before M4.4; the adapter
continues to support both configurations.

M4.4 adds an independently gated, bounded one-GPU execution. Its pure plan and
preflight require one Slurm GPU and Apptainer NVIDIA enablement before
submission. The live check verifies the scheduler's actual `AllocTRES` through
`scontrol`, independently executes `nvidia-smi` inside the `apptainer --nv`
container, and checks exact seed/config propagation, logs, retrieval, terminal
state, and the durable requested resources. GPU-related environment variables
inside an Apptainer `--cleanenv` process are diagnostic only and are not treated
as authoritative allocation evidence.

M4.5 uses a separate opt-in for two bounded real-backend failure scenarios. A
one-CPU experiment writes a partial raw result and both log streams before a
deliberate non-zero exit; Rundra must preserve failed computation and the exact
exit while independently completing retrieval. A safe staging scenario points
a temporary target workspace at an existing regular image file, verifies that
file before and after, and expects remote workspace allocation to fail before
submission. The durable failure must remain `STAGING_FAILED` with retrieval
`NOT_REQUESTED` and no invented scheduler reference, node, exit, or artifact.
The intentional staging failure performs a pure plan but not the normal remote
preflight, because preflight would prevent creation of the RunRecord whose
failure semantics the scenario validates.

M4.6 reconciles the complete real-cluster evidence without widening portable
schemas or ports. Preflight no longer rejects a Slurm installation merely
because `sacct` is absent: it reports `sacct_available` and relies on the
already-tested `scontrol` terminal-query fallback. The observed 2026-08-15
reference stack was local OpenSSH 9.6p1, local and remote rsync 3.2.7 with
protocol 31, Slurm 23.11.4, Apptainer 1.4.5, and a login-side ZFS identity for
`/shoalhome`. These are point-in-time observations, not minimum supported
versions or portable-model fields. CPU/GPU execution, dirty source, exact
config/seeds, logs/results, accounting-enabled and fallback reconciliation,
and failure-state separation all fit the existing adapter-owned metadata and
semantic port values.

M6.6 adds the final separately gated disconnected-client lifecycle scenario.
Two short CPU Runs are submitted before either is reconciled and are then
managed through new CLI processes by Run ID. A third Run is observed running
with positive started-log evidence before cancellation. The test covers
new-process status/log/fetch/cancel, repeat fetch/cancel, distinct concurrent
Run workspaces and scheduler identities, final cancellation reconciliation,
and partial output retrieval. Queue state, empty-log creation, and
scheduler-appended cancellation diagnostics are treated as legitimate races.

The resulting opt-in Shoal matrix covers:

- SSH connectivity;
- rsync staging;
- CPU Slurm job;
- GPU Slurm job;
- Apptainer execution;
- single-seed Run;
- multi-seed array Run;
- status querying;
- log retrieval;
- result retrieval;
- cancellation;
- experiment failure;
- scheduler failure where safely testable.

They must not run by default in ordinary CI.

### 42.5 Continuous integration policy

Every `main` commit and pull request runs two infrastructure-free status
checks. Feature-branch pushes are validated through their pull request rather
than duplicate push and pull-request events. The quality check executes
ordinary pytest, Ruff lint and formatting, and strict mypy validation. The
package check builds the wheel and source distribution, audits the public
distribution boundary and metadata, installs the wheel into a clean Python
3.12 environment, and smoke-tests the installed CLI. Release validation reuses
these repository commands and adds security, dependency, reproducibility, and
publication checks.

The quality gate also runs actionlint across every GitHub Actions definition so
workflow syntax and expression-context mistakes fail a still-valid commit
workflow. Monthly Dependabot proposals cover pinned GitHub Actions and the `uv`
dependency set, with one grouped pull request permitted per ecosystem. They are
never merged without passing normal checks and review. Security updates may be
proposed independently of the routine version-update schedule.

Containerized scheduler boundaries remain separate from commit gates. The
Docker Slurm lifecycle suite runs nightly and manually, while Docker OpenPBS
runs weekly and manually. Privileged Slurm cgroup validation remains
manual-only. Live Shoal tests always require explicit local authorization and
must never become an automatic GitHub Actions trigger. Scheduler-system
availability is not a merge prerequisite.

---

## 43. Version 0.1 scope

Version 0.1 is the first working vertical slice.

Required infrastructure:

- Python 3.12;
- local execution;
- SSH transport;
- Slurm scheduler;
- rsync staging;
- Apptainer runtime;
- explicit native runtime for all-local, no-container experiments;
- local run persistence;
- shoal target.

Required scientific execution:

- YAML config;
- explicit seed;
- one main executable/entry point;
- single-seed task;
- multi-seed task set.

Required CLI operations:

- `validate`;
- `plan`;
- `run`;
- `submit`;
- `status`;
- `list`;
- `logs`;
- `fetch`;
- `cancel`;
- `inspect`;
- `targets`.

Required structured interface:

- `--json` for programmatically useful commands.

---

## 44. Version 0.1 required scenarios

The following scenarios must work before version 0.1 is considered complete.

### Scenario 1 — local single-seed CPU task

```text
local source
  ↓
ExperimentSpec
  ↓
config + seed
  ↓
local/container execution
  ↓
RunRecord
  ↓
results
```

---

### Scenario 2 — shoal single-seed CPU task

```text
client
  ↓
rsync
  ↓
fishvision
  ↓
Slurm
  ↓
shoal compute node
  ↓
Apptainer
  ↓
CPU experiment
  ↓
results
  ↓
client
```

---

### Scenario 3 — shoal single-seed GPU task

Same as Scenario 2 with correct scheduler GPU allocation and Apptainer NVIDIA support.

---

### Scenario 4 — shoal multi-seed Run

One config and multiple seeds are represented as one Run and multiple Tasks.

The Slurm backend uses a Slurm job array where appropriate.

---

### Scenario 5 — experiment failure

A task exits non-zero.

Requirements:

- final state is correctly reported;
- exit code is recorded;
- stderr/stdout remain available;
- partial outputs can be fetched where possible.

---

### Scenario 6 — cancellation

An asynchronous Run can be cancelled through `rundr cancel`.

The framework records the cancellation and reconciles scheduler state.

---

### Scenario 7 — disconnected client

After:

```bash
rundr submit ...
```

the initiating shell process may exit.

A later invocation can still:

```bash
rundr status <run-id>
rundr logs <run-id>
rundr fetch <run-id>
```

---

## 45. Version 0.1 acceptance criterion

A user on a remote laptop must be able to go from modified local source code to a completed shoal experiment with one framework command, without manually performing the deployment/scheduler/retrieval workflow.

The primary synchronous experience should approximate:

```bash
rundr run experiment.yaml \
    --config configs/test.yaml \
    --seed 17 \
    --target shoal
```

The primary asynchronous experience should approximate:

```bash
rundr submit experiment.yaml \
    --config configs/test.yaml \
    --seeds 0:99 \
    --target shoal
```

followed later by:

```bash
rundr status <run-id>
rundr logs <run-id>
rundr fetch <run-id>
```

An LLM coding agent must be able to perform equivalent operations using documented CLI semantics and `--json`, without parsing scheduler-specific output.

---

## 46. Suggested initial repository structure

A possible structure is:

```text
src/
└── rundra/
    ├── __init__.py
    │
    ├── domain/
    │   ├── experiment.py
    │   ├── run.py
    │   ├── task.py
    │   ├── resources.py
    │   ├── artifacts.py
    │   └── states.py
    │
    ├── orchestration/
    │   ├── planner.py
    │   ├── runner.py
    │   └── provenance.py
    │
    ├── backends/
    │   ├── transport/
    │   │   ├── base.py
    │   │   ├── local.py
    │   │   └── ssh.py
    │   │
    │   ├── scheduler/
    │   │   ├── base.py
    │   │   ├── local.py
    │   │   └── slurm.py
    │   │
    │   ├── staging/
    │   │   ├── base.py
    │   │   ├── local.py
    │   │   └── rsync.py
    │   │
    │   └── container/
    │       ├── base.py
    │       └── apptainer.py
    │
    ├── config/
    │   ├── experiments.py
    │   └── targets.py
    │
    ├── persistence/
    │   └── runs.py
    │
    └── cli/
        ├── main.py
        └── output.py

tests/
├── unit/
├── integration/
└── system/

examples/
└── minimal/

docs/
└── project_specs.md
```

This structure is guidance, not an immutable API.

If implementation experience reveals a simpler decomposition with equally clear boundaries, prefer the simpler design.

---

## 47. Development milestones

### M0 — repository skeleton and core models

Deliver:

- Python package;
- `uv` project setup;
- CLI entry point;
- test/lint/type-check tooling;
- core domain models;
- schema loading/validation;
- target configuration;
- fake test adapters.

Repository must remain runnable and tested.

---

### M1 — local experiment execution

Deliver:

- local target;
- local staging;
- config + explicit seed;
- command construction;
- Apptainer runtime;
- explicit native runtime for all-local, no-container experiments;
- local Run lifecycle;
- isolated Run directory;
- RunRecord;
- artifact manifest;
- JSON CLI output.

Acceptance:

```bash
rundr run ... --target local
```

works for the minimal example.

The checked minimal example uses an explicit integer seed and emits canonical
JSON. Repeating it with the same source snapshot, effective configuration,
seed, Python 3.12 environment, and runtime must produce byte-identical raw
result bytes. This criterion does not assert byte identity across different
interpreter or runtime versions.

---

### M1E — launch defaults and ergonomic CLI

Deliver:

- versioned project launch configuration and named profiles;
- versioned user defaults kept separate from target definitions;
- deterministic CLI/profile/project/user/built-in precedence;
- optional config, target, and seed flags when defaults can resolve them;
- framework-generated concrete seeds when no fixed seed is configured;
- resolved-value and resolution-source visibility in human and JSON output.

Acceptance:

```bash
rundr run experiment.yaml
```

works in a configured project. The resulting Task always contains a concrete
integer seed, whether supplied or generated, and replaying with that recorded
seed reproduces the fixed-seed behavior.

This milestone changes launch resolution only. It must not introduce remote
transport, scheduler behavior, multi-Task Runs, or entropy inside the planner.

---

### M2 — remote transport and staging

Deliver:

- SSH transport;
- rsync staging;
- remote workspace creation;
- immutable remote source snapshot;
- remote command execution;
- result retrieval.

Do not require Slurm yet for the transport tests.

On 2026-08-16, one all-opt-in invocation passed all nine Shoal system tests in
121.24 seconds. The ordinary default invocation continued to skip all nine and
contacted no real infrastructure. Exact Run/job evidence is recorded in
`docs/shoal.md`; observed versions remain point-in-time deployment evidence.

---

### M3 — Slurm backend

Deliver:

- `sbatch` submission;
- scheduler job ID capture;
- state querying;
- waiting;
- state mapping;
- cancellation;
- logs;
- scheduler metadata;
- error handling.

Unit tests must cover command/script generation without using a real Slurm installation.

---

### M4 — shoal vertical slice

Deliver and validate on shoal:

- CPU job;
- GPU job;
- Apptainer execution;
- source staging;
- config and seed propagation;
- stdout/stderr;
- result collection;
- source provenance;
- container provenance where practical;
- remote failure handling.

At the end of M4, the framework should already be useful for real development.

---

### M5 — replicated experiments

Deliver:

- logical multi-seed Task sets;
- Slurm job-array optimization;
- task-to-array-index mapping;
- per-task states;
- per-task logs;
- aggregated Run status;
- partial failure handling.

---

### M6 — hardening and v0.1 release

Deliver:

- complete `plan`;
- structured errors;
- capability validation;
- robust persistence;
- documentation;
- examples;
- system-test documentation;
- public CLI consistency;
- JSON-interface tests;
- security/quoting review;
- cleanup of premature abstractions.

M6 implementation acceptance passed on 2026-08-16. The source remains
`0.1.0.dev0`; version 0.1 is not a published release until the checked release
procedure is explicitly authorized and completed.

---

## 48. Post-v0.1 roadmap

The order should follow concrete use cases rather than speculative completeness.

A plausible roadmap is:

1. parameter sweeps;
2. improved target capability discovery;
3. Python API;
4. resource-policy layer;
5. second real scheduler backend;
6. alternative staging mechanism;
7. run lineage;
8. structured metrics;
9. MCP interface;
10. adaptive/agent-driven experiment orchestration.

A second scheduler implementation should be completed before claiming that the scheduler abstraction is stable.

The second backend should preferably correspond to a real cluster used by project contributors rather than a synthetic compatibility exercise.

---

## 49. Public API stability

M6.7 freezes the documented v0.1 external surface: version-1 experiment,
target, and launch YAML; CLI command/argument semantics; portable Run and
retrieval state names; stable Run/Task identifiers; version-1 JSON operation
documents; and the persisted version-1 RunRecord. The checked fixtures under
`docs/schemas/` and `docs/cli-reference.md` are normative. Removing a field,
changing its type or meaning, or reinterpreting a version-1 YAML field requires
an explicit new version. Additive CLI JSON fields remain compatible within
version 1; persisted RunRecords reject unknown fields.

Rundra v0.1 exposes no supported Python API. Every `rundra.*` module, class,
function, protocol, and import path is internal and may evolve before a future
documented Python API is introduced above orchestration. Human-readable
formatting and physical local/remote storage layouts are also not stable APIs.
The complete compatibility policy is in `docs/stability.md`.

The v0.1 user-visible change inventory is maintained in `CHANGELOG.md`. The
final version, build, authorization, publication, and post-release procedure is
the checked `docs/release-checklist.md`; completing M6.7 does not itself publish
artifacts, push a tag, or claim unreserved domains.

---

## 50. Documentation policy

The authoritative high-level architecture and scope live in:

```text
docs/project_specs.md
```

When implementation changes:

- public CLI behavior;
- experiment schema;
- target schema;
- state model;
- backend contracts;
- provenance semantics;
- v0.1 scope;

the specification must be updated as part of the same change.

Do not document an unimplemented feature as if it already works.

Future-looking sections must remain clearly labeled as future constraints or roadmap items.

---

## 50.1 Scalable Run state and current implementation boundary

Target schema version 3 owns hard Task, confirmation, active-task, array,
worker-pool, output-shard, and automatic-retrieval limits. Version-4 plans use
a constant-size TaskSpace and no more than ten preview units. Version-4 durable
Run summaries use CompactRun plus a sparse per-Run SQLite sidecar; untouched
Tasks are represented implicitly, while transactional updates, grouped counts,
direct lookup, and pages of at most 1,000 preserve individual identity.

Slurm submissions larger than `MaxArraySize` are partitioned into multiple
bounded arrays. All root IDs are persisted, status queries are batched, root
cancellation is bounded, partial submission cancels known roots, and rsync
retrieval uses a filter file rather than one argument per output pattern.

The compute-node worker core assigns deterministic strided leases, records and
continues scientific failures without retrying them, signals requeue before an
unsealable lease, and atomically publishes read-only uncompressed tar shards
with indexed member hashes. Shard verification and selected extraction reject
links and traversal and explicitly refuse computation on `fishvision`.

`run` and `submit` operations enforce `max_concurrent_jobs`
(default 256 for target-v3 policy) by mapping excess logical Tasks onto a
bounded Slurm worker array. Workers execute deterministic assignments
sequentially, preserve per-Task timeouts and output directories, and atomically
publish exit journals used by lifecycle reconciliation. After scheduler
acceptance, worker-pool Runs with at least 1,000 Tasks are atomically converted
to a version-4 `CompactRun`; exact Task identity, scheduler reference, native
state, exit code, attempt, and retrieval state live in the SQLite sidecar.
Lifecycle reconciliation, cancellation, pages, and retrieval update the sidecar
without adding unbounded maps back to the JSON record.

Compact worker-pool submissions use a backend-neutral constant-size scheduler
request and an ordinal-driven Slurm manifest whose size depends on parameter-set
count and worker lanes, not logical Task count. They stage one immutable config
per parameter set and return bounded worker identities; the SQLite sidecar
expands the deterministic ordinal-to-worker assignment transactionally.
Compact worker-pool submissions also use a version-3 constant-size
receipt containing their TaskSpace, execution and retrieval policies, root
scheduler IDs, and SQLite sidecar filename. The sidecar transaction persists
all Task scheduler identities and root IDs before receipt acceptance. `resume`
can therefore finish an interrupted receipt transition or RunRecord compaction
without duplicate submission. Initial CLI sweep planning and pre-submission
RunRecord construction still materialize logical Tasks. Constant-memory launch
planning, requeue recovery, and remote shard ingestion remain future work.

Slurm worker lanes append versioned `START` and `FINISH` events before and
after every logical Task. Reconciliation treats started Tasks as running even
while their scheduler worker remains active, counts distinct active worker
references, and derives throughput and ETA only after a minimum observation
window and completed sample. Legacy two-column terminal journals remain
readable. These observations improve truthful status without changing durable
Task identity or retry semantics.

---

## 51. Architectural invariants

The following are intended as durable constraints.

1. **Runs are not scheduler jobs.**
2. **Tasks are not Slurm array indices.**
3. **Experiment definitions do not contain mandatory Slurm semantics.**
4. **Target/site configuration is separate from scientific project configuration.**
5. **Every stochastic Task has an explicit seed before planning and execution,
   whether caller-supplied or framework-generated and persisted.**
6. **Each Run executes from an isolated source snapshot.**
7. **Machine-readable interfaces do not depend on human scheduler output.**
8. **Infrastructure backends depend on core abstractions, not the reverse.**
9. **Agents and humans use the same execution semantics.**
10. **Large speculative abstractions are avoided until supported by real backends.**
11. **No persistent remote service is required for the v0.1 reference path.**
12. **Secrets are never embedded in experiment/run records.**
13. **A successful computation and a successful result transfer are distinct states/events.**
14. **Backend-native options remain explicit and auditable.**
15. **The scheduler remains authoritative for actual resource permissions and accounting.**

---

## 52. Decision rule for implementation trade-offs

When choosing between:

```text
a quick implementation tightly coupled to shoal/Slurm
```

and:

```text
a slightly cleaner implementation preserving an already-documented backend boundary
```

prefer the latter.

When choosing between:

```text
a simple concrete implementation sufficient for the current real backend
```

and:

```text
a generalized abstraction introduced only because an imagined future backend might need it
```

prefer the former.

This balance is central to the project.

---

## 53. Definition of done for version 0.1

Version 0.1 is done when:

- the minimal local example works;
- a configured project can use the concise one-argument `run` form;
- omitted seeds are generated before planning, exposed, persisted, and replayable;
- fixed-seed behavior is reproducible;
- a remote client can stage uncommitted source to shoal;
- a CPU experiment can run on shoal;
- a GPU experiment can run on shoal;
- multi-seed execution works;
- Slurm arrays are used correctly where appropriate;
- asynchronous Runs survive client-process termination;
- status/log/fetch/cancel work by Run ID;
- failed experiments remain inspectable;
- results can be retrieved without manual rsync;
- Run provenance is persisted;
- machine-readable JSON interfaces are tested;
- unit/integration tests run without a real cluster;
- real shoal system tests are opt-in;
- linting, formatting, type checking, and tests pass;
- documentation accurately reflects implemented behavior.

At that point, Rundra should already be useful as a practical deployment and experiment-execution tool, while retaining a credible path toward a broader portable research-execution framework.

## M12 PyPI artifact privacy

The public wheel and source distribution use explicit file allowlists and a
separate publication README. GitHub may retain agent setup, Shoal examples,
system tests, plans, and dated operational evidence, but those files are not
PyPI package content. Shipped runtime code contains no Shoal host, path, or
controller constants: remote preflight is backend-generic, and computation on
an SSH controller is rejected by comparing the local hostname with the
configured target host. A release contract audits both archive manifests and
text before Trusted Publishing can upload them.

## M10.1 intra-allocation concurrency

Targets version 4 extends the target-owned worker-pool policy with the required
`task_slots_per_worker` field. Target versions 1 through 3 retain their exact
behavior and document shapes; their materialized workers execute one logical
Task at a time. A target-v4 scalable `plan` emits schema version 5 and reports
the bounded worker count, slots per worker, total concurrent Task capacity,
maximum lane depth, and effective worker allocation resources.

For Slurm, one worker is one array element and one node allocation. Rundra uses
one `srun` step per worker with scheduler-enforced task and CPU counts. Each
step task is a deterministic sequential lane. This allows a site to expose
node-level process concurrency without increasing controller-visible job count
or oversubscribing CPUs. Multi-slot workers currently support homogeneous,
single-node, CPU-only logical Task resources.

Lane-local atomic journals distinguish scientific command failures from worker
infrastructure failures. Scientific failures do not stop a lane. If `srun`
fails, the worker fails without an implicit retry, and reconciliation preserves
outcomes from journals completed before the failure. Scheduling details are
stored in existing RunRecord `scheduler_metadata`; the RunRecord top-level
schema version does not change.

Example site policy for eight 40-core Shoal nodes:

```yaml
version: 4
targets:
  shoal:
    transport: {type: ssh, host: fishvision}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /cluster/work/rundra
    execution:
      hard_task_limit: 100000000
      confirmation_threshold: 10000
      max_active_tasks: 320
      max_concurrent_jobs: 8
      max_array_size: 1001
      output_shard_tasks: 1000
      automatic_retrieval_threshold: 20000
      worker_pool:
        activation_threshold: 10000
        max_workers: 8
        task_slots_per_worker: 40
        tasks_per_lease: 100
        infrastructure_retry_limit: 2
        requeue_limit: 8
```

## M14 explicit worker scaling

Targets version 6 separates conservative worker defaults from hard target
ceilings. `default_workers` and `default_task_slots_per_worker` select the
scale used when a Run has no explicit request; `max_workers`,
`max_task_slots_per_worker`, `max_active_tasks`, and `max_concurrent_jobs`
remain target-owned policy. CLI or project-profile requests above a ceiling
fail before staging and are never silently clamped.

`plan`, `run`, and `submit` use one scalable decision. A target-v6 plan emits
schema version 6 and records requested scale, effective worker count and slots,
concurrent capacity, exact aggregate worker resources, policy ceilings, and
scheduler-controlled placement. Materialized submission passes those exact
worker resources to the scheduler adapter. Slurm workers remain single-node,
CPU-only allocations; one `srun` lane is created for each reserved Task slot.
Rundra does not infer topology, request exclusive nodes implicitly, or
overcommit declared Task memory.

Targets versions 1 through 5 retain their parsing and default semantics. Their
launches nevertheless use the same scalable decision as `plan`, fixing the
previous loss of `task_slots_per_worker` between planning and submission.

## M16 durable submission outcomes and worker memory policy

Target version 7 adds the optional, site-owned `max_memory_per_worker` ceiling.
The scalable planner compares it to the exact worker allocation memory after
effective slot selection. Exceeding the ceiling returns
`WORKER_MEMORY_LIMIT_EXCEEDED` with logical Task memory, slots, aggregate
worker memory, and the configured ceiling. Rundra does not infer topology,
exclusive-node policy, or partition capacity.

## M13 OpenPBS scheduler backend

Targets may select `scheduler: {type: pbs}` with the existing SSH, staging,
container, and workspace abstractions. The OpenPBS adapter owns `qsub`, `qstat`,
and `qdel` command generation, PBS resource translation, job-array submission,
array Task-ID mapping, terminal-state reconciliation, scheduler log metadata,
dependencies, and cancellation. Portable experiment and Run models remain
scheduler-neutral; PBS-native queue, account, project, priority, and placement
options remain explicit target policy.

PBS memory is rendered on each `select` chunk in integral `mb` units. Array
parallelism uses OpenPBS's `max_run_subjobs` syntax, while large logical Task
sets continue to use Rundra's scheduler-neutral partitioning and worker
strategies. Readers accept persisted PBS scheduler identities without changing
version-1 Slurm document shapes.

The opt-in Docker OpenPBS system test builds a pinned official OpenPBS release,
starts one controller and two MOM services, and exercises real submission,
arrays, bounded subjob concurrency, successful retrieval, partial scientific
failure, and durable cancellation. It uses a shared workspace and OpenPBS
`$usecp` mappings, never disables SSH host verification, and does not run in
the default unit suite.
