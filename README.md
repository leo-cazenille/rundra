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

### 2. Configure a local target

Targets describe where and how Rundra executes work. Create the standard target
file:

```bash
mkdir -p ~/.config/rundra
cat > ~/.config/rundra/targets.yaml <<'YAML'
version: 1
targets:
  local:
    transport: {type: local}
    scheduler: {type: local}
    staging: {type: local}
    container: {type: native}
    workspace: .rundra
YAML
```

This target executes directly on the current machine, without SSH, a batch
scheduler, or a container runtime. Add `.rundra/` to the project's ignore file
when the workspace is inside the repository.

### 3. Create a first experiment

Create an empty project directory and add `experiment.yaml`:

```yaml
version: 1

experiment:
  name: random-samples

command:
  argv: [python3, main.py, --config, "{config}", --seed, "{seed}"]

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

import yaml

parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True, type=Path)
parser.add_argument("--seed", required=True, type=int)
args = parser.parse_args()

config = yaml.safe_load(args.config.read_text(encoding="utf-8"))
rng = random.Random(args.seed)
result = {
    "seed": args.seed,
    "samples": [rng.random() for _ in range(config["sample_count"])],
}

output = Path("../output/results/result.json")
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
```

Finally, add the adjacent project file `rundra.yaml`:

```yaml
version: 1
default_profile: local
profiles:
  local:
    config: config.yaml
    target: local
    source_root: .
    destination: retrieved
```

### 4. Plan and launch

Planning is offline and does not create a Run or consume resources:

```bash
rundr validate experiment.yaml
rundr plan experiment.yaml --seed 17
```

Review the command, seed, resources, staging behavior, target, and destination.
Then execute the Run:

```bash
rundr run experiment.yaml --seed 17
cat retrieved/results/result.json
```

Rundra snapshots source inputs, copies the effective configuration, executes in
an isolated workspace, retrieves declared outputs, and stores the RunRecord in
`~/.local/share/rundra/runs`. The repository contains a checked version of
this workflow in [`examples/minimal`](examples/minimal).

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
    workspace: .rundra
```

Declare the immutable image in `experiment.yaml`. Use an absolute path and make
sure the image contains every runtime dependency, including Python and PyYAML
for this example:

```yaml
container:
  image: /absolute/path/to/python-with-pyyaml.sif
  gpu: false
```

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

The quick-start target uses the complete local/native stack. To run a local
Apptainer image, change `container.type` to `apptainer` and declare the image
in the experiment. Local execution is synchronous; `submit` is intentionally
unavailable because Rundra does not create unmanaged background processes.

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
