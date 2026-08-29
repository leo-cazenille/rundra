# Rundra

[![CI](https://github.com/leo-cazenille/rundra/actions/workflows/ci.yml/badge.svg)](https://github.com/leo-cazenille/rundra/actions/workflows/ci.yml)
[![Local deployment](https://github.com/leo-cazenille/rundra/actions/workflows/local-deployment.yml/badge.svg)](https://github.com/leo-cazenille/rundra/actions/workflows/local-deployment.yml)
[![Docker Slurm system](https://github.com/leo-cazenille/rundra/actions/workflows/docker-slurm-system.yml/badge.svg)](https://github.com/leo-cazenille/rundra/actions/workflows/docker-slurm-system.yml)
[![Docker OpenPBS system](https://github.com/leo-cazenille/rundra/actions/workflows/docker-pbs-system.yml/badge.svg)](https://github.com/leo-cazenille/rundra/actions/workflows/docker-pbs-system.yml)
[![Docker HTCondor system](https://github.com/leo-cazenille/rundra/actions/workflows/docker-htcondor-system.yml/badge.svg)](https://github.com/leo-cazenille/rundra/actions/workflows/docker-htcondor-system.yml)

Rundra runs reproducible scientific experiments on a workstation or a shared
compute cluster. An experiment combines an executable command, a configuration,
an explicit random seed, requested resources, and declared outputs. Rundra turns
that description into durable Runs and Tasks that can be planned, submitted,
monitored, retrieved, inspected, cancelled, and purged through one interface.

Rundra is useful when you want to:

- run the same experiment locally and through a batch scheduler;
- launch many seeds or parameter combinations without writing scheduler scripts;
- build or acquire a pinned Apptainer image and cache prepared applications;
- retain exact source, configuration, image, resource, and scheduler provenance;
- retrieve large result sets safely, including compact archive retrieval;
- automate experiments through stable JSON output or MCP tools.

The Python package is `rundra`; the command is `rundr`.

## Quick start

### 1. Install Rundra

Install the command as an isolated tool:

```bash
uv tool install --python 3.12 rundra
rundr --version
rundr help
```

Upgrade an existing installation with `uv tool upgrade rundra`. Contributors
working from this repository should instead run:

```bash
uv sync --locked
uv run rundr --version
```

The commands below use the installed `rundr` executable. Prefix them with
`uv run` when running from a source checkout.

### 2. Create a first experiment

Create an empty project directory and add `experiment.yaml`:

```yaml
version: 1

experiment:
  name: random-samples

command:
  argv:
    - python3
    - main.py
    - --config
    - "{config}"
    - --seed
    - "{seed}"
    - --output
    - /workspace/output/results/result-{seed}.json

resources:
  nodes: 1          # nodes requested by each Rundra Task
  tasks: 1          # scheduler ranks/process slots inside each Rundra Task
  cpus_per_task: 1  # CPU cores assigned to each scheduler task
  gpus_per_task: 0  # GPUs assigned to each scheduler task
  memory: 512MiB
  walltime: "00:05:00"

outputs:
  include: [results/**]
```

Resource fields apply to **each logical Rundra Task**, not to the whole Run. A
logical Task is normally one seed and parameter-set combination. The inclusive
seed range `0:9`, for example, creates ten independent Rundra Tasks. With the
resource block above, each Task
requests one node, one scheduler task, one CPU, no GPU, 512 MiB of memory, and
up to five minutes. The ten Tasks are not pinned to one shared node: a remote
scheduler may distribute them across any compatible nodes and execute as many
concurrently as target policy and current capacity allow.

`resources.tasks` is scheduler terminology, such as Slurm's `--ntasks`; it is
not the number of Rundra Tasks or seeds. Rundra invokes the experiment command
once per logical Task and does not automatically create MPI ranks or child
processes merely because this value exceeds one. Single-process applications
should normally keep `nodes: 1` and `tasks: 1`. Use `cpus_per_task` for the
threads or bounded child processes used by that application, and request
`gpus_per_task` only when each scheduler task actually consumes those GPUs.

Add `config.yaml`:

```yaml
sample_count: 3
```

Add `main.py`:

```python
import argparse
import json
import random
from pathlib import Path
from time import sleep

import yaml

parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True, type=Path)
parser.add_argument("--seed", required=True, type=int)
parser.add_argument("--output", required=True, type=Path)
args = parser.parse_args()

config = yaml.safe_load(args.config.read_text(encoding="utf-8"))
sleep(2)  # Simulate 2000 ms of computation.
rng = random.Random(args.seed)
result = {
    "seed": args.seed,
    "samples": [rng.random() for _ in range(config["sample_count"])],
}

args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
```

No target file or `rundra.yaml` is needed for this local quick start.

### 3. Plan and launch

Planning is offline and does not create a Run or consume resources:

```bash
rundr validate experiment.yaml
rundr plan experiment.yaml
rundr run experiment.yaml
```

After the three files above exist, previewing a ten-seed Run is also valid:

```bash
rundr plan experiment.yaml --seeds 0:9
rundr run experiment.yaml --seeds 0:9 --progress
```

The built-in local target detects the CPUs available to the Rundra process
(including affinity restrictions) and runs up to one single-CPU Task per
available CPU. A Task requesting multiple CPUs reduces the number of concurrent
Tasks accordingly. With `--progress`, completed local Tasks advance the bar as
each process exits, including when a Run needs several concurrency waves. You
can impose a lower local cap explicitly after reviewing the plan:

```bash
rundr plan experiment.yaml --seeds 0:9 \
  --workers 1 --task-slots-per-worker 2
rundr run experiment.yaml --seeds 0:9 \
  --workers 1 --task-slots-per-worker 2 --progress
```

That request caps execution at two concurrent application processes.

Review the command, seed, resources, staging behavior, target, destination, and
the source snapshot size before launching. The preview applies the same
exclusions as staging and highlights the largest included top-level paths.
For this zero-configuration form, Rundra uses the adjacent `config.yaml`, a
generated and recorded seed, the experiment directory as source, the built-in
local target, and `retrieved/config` as destination. Human output identifies
the effective config path and whether it came from the CLI, project, user, or
adjacent default.

An adjacent `rundra.yaml` is optional. Add one when the project needs named
profiles, preparation, a remote default target, or nonstandard paths; its
values override the safe built-in conventions.

Project-specific exclusions belong in the portable experiment file, at the
top level alongside `resources` and `outputs`:

```yaml
sync:
  exclude:
    - results/
    - derived/
    - downloads/
```

These paths are relative to `--source-root` and are added to Rundra's built-in
transient-file exclusions. Rundra always excludes `results`, `outputs`, `tmp`,
`retrieved`, `downloads`, `*.sif`, and `*.simg`. Exclude other generated or
retrieved data rather than
placing a local workspace inside an included source tree.
Then execute the Run:

```bash
rundr run experiment.yaml --seed 17
cat retrieved/config/results/result-17.json
```

For a multi-seed Run, every logical Task writes a distinct file:

```bash
rundr run experiment.yaml --seeds 4:12 --progress
find retrieved/config -name 'result-*.json' -print
```

Single-task retrieval places the file directly under `results/`. Multi-task
retrieval preserves a `task_NNNNNN/` directory for each logical Task so files
from distinct Tasks remain isolated even when an application reuses names.

#### Combine the results as JSONL

Each result file contains one JSON object followed by a newline. Sort the Task
paths and concatenate them into one JSON Lines file:

```bash
find retrieved/config -type f -name 'result-*.json' -print0 | sort -z | xargs -0 cat > results.jsonl
```

For seeds `4:12`, the combined file contains nine records:

```bash
wc -l results.jsonl
head results.jsonl
```

Rundra snapshots source inputs, copies the effective configuration, executes in
an isolated workspace, retrieves declared outputs, and stores the RunRecord in
`~/.local/share/rundra/runs`. The repository contains a checked version of
this workflow in [`examples/minimal`](examples/minimal).

### 4. Select a named local target

Every configured target automatically acts as a same-named profile when no
project `rundra.yaml` exists. The packaged target therefore supports
`--profile local` immediately:

```bash
rundr plan experiment.yaml --profile local --seeds 0:9
rundr run experiment.yaml --profile local --seeds 0:9 --progress
```

An adjacent `rundra.yaml` is optional. Add one only when the named profile must
also select project-specific values such as a destination or configuration:

```yaml
version: 1
default_profile: local

defaults:
  config: config.yaml
  source_root: .

profiles:
  local:
    target: local
    destination: retrieved/local
```

If `~/.config/rundra/targets.yaml` does not exist, the name `local` selects
Rundra's packaged local target. It automatically uses the CPUs available to the
Rundra process and requires no target configuration. Explicit project profiles
take precedence over automatic target profiles with the same name.

Create `~/.config/rundra/targets.yaml` only when you need a custom local
workspace or container runtime. CPU policy remains optional: when `execution`
is omitted, Rundra uses all CPUs available through process affinity and adjusts
concurrency for each logical Task's `cpus_per_task` request:

```yaml
version: 6

targets:
  local:
    transport: {type: local}
    scheduler: {type: local}
    staging: {type: local}
    container: {type: native}
    workspace: ~/.local/share/rundra/workspaces
```

Add a version-6 `execution` section only to impose explicit local safety or
concurrency ceilings. Such an explicit local worker-pool policy must use
`requeue_limit: 0`: no external scheduler owns synchronous local processes, so
scheduler requeue recovery is unavailable. CLI options can always request a
lower capacity. This omission is also valid when the same target file uses a
newer schema version for other targets.

### 5. Run inside an Apptainer or Singularity container

To execute the same experiment in a prebuilt SIF image, add a container target
to `~/.config/rundra/targets.yaml`:

```yaml
version: 1
targets:
  local-container:
    transport: {type: local}
    scheduler: {type: local}
    staging: {type: local}
    container: {type: apptainer}
    workspace: ~/.local/share/rundra/workspaces
```

Create `python-with-pyyaml.def` beside the quick-start files:

```text
Bootstrap: docker
From: python:3.12-slim

%post
    python3 -m pip install --no-cache-dir PyYAML==6.0.3

%environment
    export PYTHONDONTWRITEBYTECODE=1

%runscript
    exec python3 "$@"
```

Build the exact SIF used below, then verify its Python dependency:

```bash
apptainer build --fakeroot /tmp/python-with-pyyaml.sif python-with-pyyaml.def
apptainer exec /tmp/python-with-pyyaml.sif \
  python3 -c 'import yaml; print(yaml.__version__)'
```

`--fakeroot` requires an Apptainer installation configured for unprivileged
builds. If the local administrator provides a different approved build mode,
use that mode without changing the output path. Building pulls the declared
base image and Python package, so it requires registry and package-index access.

Add the container block to `experiment.yaml`, using the same SIF path as the
build command:

```yaml
container:
  image: /tmp/python-with-pyyaml.sif
  gpu: false
```

Rundra excludes arbitrary `*.sif` files from source snapshots, so this direct
prebuilt-image example uses an explicit absolute image path. `/tmp` keeps the
walkthrough self-contained; use a durable user-owned absolute path for regular
work. The later preparation example instead lets Rundra build, verify, cache,
and resolve a logical SIF name automatically.

Plan and run against the container target:

```bash
rundr doctor experiment.yaml --target local-container
rundr plan experiment.yaml --target local-container --seed 17
rundr run experiment.yaml --target local-container --seed 17
```

Rundra invokes either a compatible `apptainer` or `singularity` executable,
runs the command inside the SIF, and records the runtime and image identity in
the Run provenance. On a remote scheduler, the same experiment-level
`container` block is portable; only the target definition changes.

Rundra can also build and cache a SIF from an Apptainer definition instead of
requiring a prebuilt image. See the checked
[`python-multiprocessing` self-building example](examples/python-multiprocessing/README.md)
for its `python.def`, version-4 project preparation recipe, target build policy,
and local and Slurm launch commands.

### Pinned and unpinned prebuilt images

Project-managed preparation normally pins the exact SIF bytes:

```yaml
version: 6
preparation:
  source:
    working_tree: {}
  image:
    name: application.sif
    prebuilt:
      uri: library://example/application:v1
      sha256: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

When a command verifies this image, a mismatch is fatal and reports the file,
expected digest, and observed digest. Rundra never silently accepts different
bytes or downgrades a mismatched pin.

Version 6 also permits omission of `sha256` when an existing SIF must be
trusted:

```yaml
version: 6
preparation:
  source:
    working_tree: {}
  image:
    name: application.sif
    prebuilt:
      uri: library://example/application:v1
```

This mode emits a warning. Rundra searches the project and configured
`preparation.image_search_paths`, trusts an existing regular file, measures its
SHA-256, and records that measured digest in the Run. It does not pull an
unpinned URI: if no existing file is found, preparation fails with the checked
paths. Prefer a pin whenever the intended digest is known.

## Advanced example: Apptainer on a Slurm cluster

This example reuses the quick-start `main.py` and `config.yaml`, but executes
four seeds through Slurm inside a prebuilt Apptainer image. It adds a tracked
project profile and a one-time, user-owned cluster target.

First, define `experiment.yaml` with an image path visible from every compute
node. The image must contain Python 3.12 and PyYAML:

```yaml
version: 1

experiment:
  name: clustered-random-samples

command:
  argv:
    - python3
    - main.py
    - --config
    - "{config}"
    - --seed
    - "{seed}"
    - --output
    - /workspace/output/results/result-{seed}.json

container:
  image: /shared/containers/python-3.12-pyyaml.sif
  gpu: false

resources:
  nodes: 1
  tasks: 1
  cpus_per_task: 1
  gpus_per_task: 0
  memory: 512MiB
  walltime: "00:05:00"

outputs:
  include: [results/**]
```

Add the adjacent tracked `rundra.yaml`. Unlike the local quick start, this file
records the project's cluster profile and retrieval convention:

```yaml
version: 1
default_profile: cluster

defaults:
  config: config.yaml
  source_root: .

profiles:
  cluster:
    target: cluster
    destination: retrieved/cluster
    workers: 2
    task_slots_per_worker: 4
```

`defaults` apply to every profile, while the selected profile overlays them.
`default_profile` selects `cluster` when `--profile` is omitted; an explicit
CLI option still has highest precedence. Relative paths are resolved from the
directory containing `rundra.yaml`. The file should contain reproducible
project choices, not SSH credentials or site secrets.

The two scaling fields request a bounded worker pool from the selected target:

- `workers: 2` requests two scheduler-owned worker allocations. Slurm normally
  represents them as elements of one worker array submission.
- `task_slots_per_worker: 4` reserves four concurrent logical Task slots inside
  each worker allocation.
- The resulting maximum scientific concurrency is `2 x 4 = 8` logical Tasks.
  If the Run contains more than eight Tasks, each slot executes further
  deterministic assignments sequentially.

These values are requests, not permission to exceed cluster policy. They are
validated against target-owned ceilings during `plan` and rejected rather than
silently reduced.

Register the machine-specific target once in
`~/.config/rundra/targets.yaml`, replacing the SSH alias, username path, and
any scheduler policy with values supplied by the cluster operator:

```yaml
version: 8  # enables bounded worker pools and worker-memory policy

targets:
  cluster:
    transport:
      type: ssh                 # contact the cluster through OpenSSH
      host: cluster-login       # alias from ~/.ssh/config, not a shell command
    scheduler:
      type: slurm               # submit and monitor scheduler-owned jobs
    staging:
      type: rsync               # copy sealed inputs and retrieve declared outputs
    container:
      type: apptainer           # execute scientific commands inside the SIF
    # Durable path visible from the login host and every compute node.
    workspace: /shared/users/YOUR_USERNAME/rundra-work
    execution:
      # Reject a Run containing more logical Rundra Tasks than this.
      hard_task_limit: 10000
      # At or above this Task count, require --confirm-tasks with the exact count.
      confirmation_threshold: 1000
      # Maximum logical scientific Tasks allowed to execute concurrently.
      max_active_tasks: 32
      # Maximum scheduler jobs or array elements Rundra may submit for one Run.
      max_concurrent_jobs: 4
      # Maximum elements in one scheduler array submission.
      max_array_size: 1000
      # Maximum logical Task outputs packed into one retrieval shard.
      output_shard_tasks: 1000
      # Prefer scalable compact retrieval at or above this logical Task count.
      automatic_retrieval_threshold: 1000
      # Hard aggregate memory ceiling for one worker allocation.
      max_memory_per_worker: 4GiB
      worker_pool:
        # Auto strategy may bundle Tasks into workers at or above this count.
        activation_threshold: 10
        # Conservative worker count when neither project nor CLI requests one.
        default_workers: 1
        # Hard site ceiling for scheduler-owned worker allocations.
        max_workers: 4
        # Fill all eight site-approved CPU slots in each selected worker.
        default_task_slots_per_worker: 8
        # Hard ceiling on concurrent Task slots inside one worker.
        max_task_slots_per_worker: 8
        # Maximum deterministic Task assignments claimed in one worker lease.
        tasks_per_lease: 10
        # Retries allowed for framework/infrastructure failures, not science errors.
        infrastructure_retry_limit: 1
        # Scheduler requeues allowed for an interrupted worker allocation.
        requeue_limit: 2
```

Keep SSH keys, proxy jumps, ports, and host verification in normal OpenSSH
configuration, never in Rundra YAML. The login host is used only for staging
and scheduler operations; the scientific command runs in a Slurm allocation.
The `execution` section is site policy and should be supplied or reviewed by
the cluster operator. Conservative defaults prevent a project from reserving
all allowed workers merely because the target permits them.

Diagnose and review the resolved profile before submitting:

```bash
rundr doctor experiment.yaml --profile cluster --connect
rundr plan experiment.yaml --profile cluster --seeds 0:19
rundr submit experiment.yaml --profile cluster --seeds 0:19
```

This plan contains twenty logical Rundra Tasks but requests only two workers.
Each logical Task needs one CPU and 512 MiB, so four slots make each worker ask
Slurm for four CPUs and 2 GiB. Up to eight Tasks run concurrently; the workers
then execute the remaining assignments in their lanes without creating one
scheduler element per seed. Rundra preserves separate configs, output
directories, timeouts, states, and provenance for all twenty Tasks.

Set `default_task_slots_per_worker` equal to
`max_task_slots_per_worker` when normal Runs should use all site-approved cores
inside each selected worker. `default_workers` independently limits how many
worker allocations or nodes are requested, so filling a worker does not imply
reserving the entire cluster. The target's `max_active_tasks`,
`max_concurrent_jobs`, `max_workers`, and
`max_task_slots_per_worker` are hard ceilings. `activation_threshold` controls
when automatic planning may choose workers for a large Run, while explicit
project or CLI worker requests still undergo the same checks. `rundr plan`
reports the chosen strategy, worker count, slot count, aggregate worker
resources, lane depth, and exact logical Task count before submission.

Retain the displayed Run ID, then wait and retrieve the declared outputs:

```bash
rundr wait RUN_ID --progress
rundr fetch RUN_ID
```

The RunRecord preserves all four seeds, the effective config, target, Slurm job
identifiers, requested resources, container identity, Task outcomes, and
retrieved artifacts. To build and cache the SIF from a definition instead of
using a prebuilt shared image, continue with the checked
[`python-multiprocessing` preparation example](examples/python-multiprocessing/README.md).

## Everyday workflow

Use `run` for short synchronous work. For long or remote experiments, submit
the Run, retain the displayed Run ID, and follow it interactively:

```bash
rundr submit experiment.yaml --seeds 0:9
rundr wait RUN_ID --progress
rundr fetch RUN_ID
rundr inspect RUN_ID
```

`wait --progress` displays Task completion and Run state until the Run finishes.
It is intended for a human watching an interactive terminal. The detached Run
continues on the scheduler if the waiting client is interrupted.

| Command | Purpose |
|---|---|
| `doctor` | Check configuration, permissions, connectivity, and optional scheduler access. |
| `validate` | Validate the portable experiment schema. |
| `plan` | Resolve Tasks, resources, preparation, storage, and target behavior without execution. |
| `run` | Submit, wait, and retrieve while the client remains attached. |
| `submit` | Register and durably submit a Run, then return. |
| `wait` | Follow one Run, optionally with an interactive progress display. |
| `await` | Block silently on one or several Runs for harness automation. |
| `status` | Reconcile and display Run state. |
| `tasks` | Page through individual Task state. |
| `logs` | Read framework-managed preparation or Task logs. |
| `fetch` | Retrieve or reference declared outputs. |
| `cancel` | Cancel active preparation and scientific work. |
| `purge` | Safely remove terminal Run data after exact-ID confirmation. |

Run `rundr help` or `rundr help COMMAND` for the installed command surface.

## Configuration model

Rundra separates portable experiments from machine- and user-specific launch
configuration.

| File | Contains | Default location |
|---|---|---|
| Experiment | Command, resources, outputs, optional container request | Any `experiment.yaml` |
| Project launch file | Project defaults and named profiles | `rundra.yaml` beside the experiment |
| User launch file | User-specific defaults and Run store | `~/.config/rundra/config.yaml` |
| Target file | Named backend stacks, workspaces, and site policy | `~/.config/rundra/targets.yaml` |

Launch precedence is command line, selected project profile, project defaults,
user defaults, then built-ins. `plan` reports resolved launch values and their
sources. Credentials never belong in these files.

## Target configuration

### Local targets

The quick-start target uses the complete local/native stack. Automatic profiles
and custom-target forms are shown in [Select a named local target](#4-select-a-named-local-target).
To run a local Apptainer image, change `container.type` to `apptainer` and
declare the image in the experiment. Local execution is synchronous; `submit`
is intentionally unavailable because Rundra does not create unmanaged
background processes.

### Remote scheduler targets

A minimal SSH, Slurm, rsync, and Apptainer target looks like this:

```yaml
version: 1
targets:
  cluster:
    transport:
      type: ssh
      host: cluster-login
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /shared/users/alice/rundra-work
```

The workspace must be an absolute path visible from the login/controller side
and compute nodes. Keep SSH identities, proxy jumps, ports, and host verification
in normal OpenSSH configuration.

Rundra supports Slurm, OpenPBS, and HTCondor. Their capabilities are not
identical. Inspect the configured target and plan instead of inferring behavior
from the scheduler name:

```bash
rundr targets
rundr doctor experiment.yaml --connect
rundr plan experiment.yaml --profile cluster
```

Target-owned policies can bound worker concurrency and retries. Newer target
schemas also support Slurm partition routing and scheduler-provided CPU/GPU
scratch storage. HTCondor requires an explicitly verified shared workspace.
See the [target setup guide](docs/getting-started.md),
[scheduler capability matrix](docs/scheduler-capabilities.md), and
[CLI reference](docs/cli-reference.md) before configuring a production target.

Do not use scheduler-native options to bypass account, partition, QOS, memory,
GPU, concurrency, or site-storage policy.

## Reproducibility and preparation

Every stochastic Task has an explicit integer seed. If no seed is configured,
Rundra generates and records one before execution; replay it with `--seed`.
Inclusive seed ranges create multiple Tasks: `--seeds 0:2` means seeds 0, 1,
and 2.

An explicit `--source-root` snapshots the current working tree after normal
exclusions and never builds in the developer's original tree. Prepared projects
can instead pin a full Git commit, immutable SIF digest, application build
recipe, or Apptainer definition recipe. Rundra caches verified preparation
inputs and outputs by content identity.

Inspect preparation before execution:

```bash
rundr plan experiment.yaml
rundr doctor experiment.yaml --offline
```

Do not add `--offline` to a cold first Run. Offline readiness means every
required immutable source and image input is already available in the selected
cache.

## Results and provenance

Raw outputs remain separate from derived analysis. `fetch` defaults to `auto`:
when client and target share storage, Rundra can publish a verified reference
manifest instead of copying a large result tree. Use `--mode copy` when an
analysis program requires ordinary local files. Compact Runs can retain indexed
archives; add `--extract` only when individual files are necessary.

RunRecords preserve the effective experiment and configuration, seeds, source
identity, image digest, requested resources, target, scheduler identities,
timestamps, Task outcomes, preparation state, and retrieved artifacts where
available.

## Using Rundra from an AI agent

Add `--json` to programmatically useful commands to receive deterministic JSON
instead of human-oriented output. This format is intended for AI agents, MCP
clients, scripts, and other automation. Agents should use `await` rather than
interactive `wait --progress` so terminal redraws do not consume transcript
tokens.

Rundra's JSON interfaces and bounded guidance are designed for agents. On a new
machine, repository, or sandbox session:

```bash
rundr doctor --agent codex --json
rundr agent-guide --write AGENTS.md
rundr agent-guide --list-topics
```

Grant only the filesystem and network access reported by `doctor`, restart the
agent sandbox if required, and rerun the diagnostic until `ready` is true. The
first Codex audit may return a `run_store_durability.verification_argv`; execute
that argv as a separate command so Rundra can prove the Run store survives
between sandboxed processes. If verification fails, grant persistent access to
the reported store or use `--data-dir` inside the agent's persistent workspace.
Then use:

```bash
rundr doctor experiment.yaml --connect --agent codex --json
rundr plan experiment.yaml --json
rundr submit experiment.yaml --json
rundr await RUN_ID --json
rundr fetch RUN_ID --json
```

Agent rules:

- retain the explicit Run ID and `--data-dir` returned at submission;
- use `await` rather than waking the model to poll every few minutes;
- avoid `--progress` in captured transcripts because redraws consume tokens;
- review Task count, seeds, resources, concurrency, target, and retrieval before
  submission;
- continue an interrupted submission with `rundr resume RUN_ID`, never by
  creating a duplicate Run;
- use Rundra lifecycle JSON instead of parsing scheduler-native output;
- use `agent-guide --topic TOPIC` for bounded instructions;
- refresh instructions after upgrades with `agent-guide --topic upgrade` and
  `agent-guide --write AGENTS.md`.

The optional MCP server exposes the same lifecycle to compatible clients.
Install it with `uv tool install --python 3.12 'rundra[mcp]'`. See the
[agent tutorial](docs/tutorials/03-agent-async.md),
[JSON schemas](docs/schemas/README.md), and
[portable agent instructions](docs/agent-instructions.md).

## Security model

Remote clusters are security boundaries. Rundra does not store credentials,
disable SSH host verification, execute scientific work on login hosts, or allow
user-native scheduler options to override framework-managed directives. Inspect
a plan before expensive operations. Above a configured safety threshold, Rundra
requires the exact `--confirm-tasks N` value.

## Development

The authoritative architecture is [`docs/project_specs.md`](docs/project_specs.md).
Public changes are recorded in [`CHANGELOG.md`](CHANGELOG.md), and development
uses Python 3.12 with `uv`:

```bash
uv sync --locked
tools/check.sh
```

Default tests require no real cluster. Dockerized Slurm, OpenPBS, and HTCondor
system suites and real-cluster acceptance tests are explicit opt-ins. See
[`docs/continuous-integration.md`](docs/continuous-integration.md) and the
[`release checklist`](docs/release-checklist.md).

Rundra is licensed under Apache-2.0.
