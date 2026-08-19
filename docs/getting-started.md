# Installation and target setup

This guide describes the released v0.1 interface and current development
extensions. Install the latest release from PyPI or use a source checkout when
developing Rundra itself.

## Requirements

The client requires Python 3.12. Install the released command as an isolated
tool:

```bash
uv tool install --python 3.12 rundra
rundr --version
rundr help
```

From a checkout, contributors should instead synchronize the locked environment
and run the source version:

```bash
uv sync --locked
uv run rundr --version
```

The examples below use `uv run rundr`; omit `uv run` when using the isolated
PyPI installation.

The all-local native path needs no external execution tool. Other paths need:

| Path | Client-side commands | Execution-side commands |
|---|---|---|
| local/native | none beyond Rundra | Python/application executable requested by the experiment |
| local/Apptainer | `apptainer` | same machine |
| SSH/Slurm/rsync/Apptainer | OpenSSH and rsync | rsync, Slurm client commands, Apptainer, and the experiment executable inside the image |
| SSH/OpenPBS/rsync/Apptainer | OpenSSH and rsync | rsync, OpenPBS client commands, Apptainer, and the experiment executable inside the image |

Slurm execution requires `sbatch`, `squeue`, `scancel`, and `scontrol`. Rundra
uses `sacct` when available and otherwise falls back to `scontrol` for retained
jobs. OpenPBS execution requires `qsub`, `qstat`, and `qdel`.

## Configuration locations

Rundra keeps three concerns separate:

| File | Purpose | Automatic location |
|---|---|---|
| target file | named backend stacks and workspaces | `~/.config/rundra/targets.yaml` |
| user launch file | optional per-user launch defaults | `~/.config/rundra/config.yaml` |
| project launch file | project defaults and named profiles | `rundra.yaml` beside the experiment |

Persisted RunRecords default to `~/.local/share/rundra/runs`. This is a client
path, not a directory that must exist on every remote login or controller node.
Use `--data-dir` or the user launch file's `data_dir` to select another store.

All configuration documents require `version: 1`, reject unknown fields, and
must not contain credentials. Relative paths in project and user launch files
are resolved against the file that declares them.

## Local target

Create the standard target file with an all-local native target:

```yaml
version: 1
targets:
  local:
    transport: {type: local}
    scheduler: {type: local}
    staging: {type: local}
    container: {type: native}
    workspace: .rundra
```

The native runtime is valid only with the all-local stack and an experiment
that does not request a container or GPUs. To test local container execution,
change only `container.type` to `apptainer` and declare the image in the
experiment. Local execution is synchronous; local `submit` returns the
structured `ASYNC_UNAVAILABLE` error because Rundra does not create an
unmanaged background process.

`workspace` is the staging root. Relative local workspaces are resolved by the
local filesystem path in use; exclude an in-project workspace such as
`.rundra/` from version control and source synchronization.

## SSH scheduler target

Implemented remote stacks combine SSH, Slurm or OpenPBS, rsync or shared
staging, and Apptainer:

```yaml
version: 1
targets:
  cluster:
    transport:
      type: ssh
      host: cluster-login
    scheduler: {type: slurm}  # use pbs for OpenPBS
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /shared/users/alice/rundra-work
```

The SSH `host` is a plain host alias or `user@host` destination. Configure
ports, proxy jumps, identities, agent authentication, and host-key policy in
normal OpenSSH configuration. Rundra neither accepts credentials nor adds
flags that weaken host verification.

The remote workspace is mandatory, absolute, and not `/`. It should be on a
filesystem visible from both the login/controller side and scheduler compute
nodes.
It does not need to exist before the first Run: preflight checks the nearest
existing writable ancestor, and staging creates `WORKSPACE/runs/RUN_ID` when
execution begins. Rundra never implicitly requires or creates `~/.rundra`; it
uses exactly the workspace configured for the selected target.

Before running an experiment, verify the external path directly:

```bash
ssh cluster-login true
rsync --version
ssh cluster-login 'command -v rsync && command -v apptainer'
ssh cluster-login 'command -v sbatch && command -v squeue && command -v scancel && command -v scontrol'  # Slurm
ssh cluster-login 'command -v qsub && command -v qstat && command -v qdel'  # OpenPBS
```

Then validate the target file and inspect an offline plan:

```bash
uv run rundr targets --targets-file /path/to/targets.yaml
uv run rundr plan experiment.yaml \
  --config config.yaml \
  --seed 17 \
  --target cluster \
  --targets-file /path/to/targets.yaml
```

`plan` validates the supported stack, container/GPU/resource compatibility,
the selected scheduler's native-option allowlist, and staging intent without
connecting, creating a workspace or RunRecord, or submitting work. Live
capability checks happen only on execution or through an explicit `doctor`
connection or scheduler probe.

## Launch defaults

A project launch file can reduce a complete command to one experiment path:

```yaml
version: 1
default_profile: local
defaults:
  source_root: .
profiles:
  local:
    config: config.yaml
    target: local
    destination: retrieved
  cluster:
    config: config.yaml
    target: cluster
    destination: retrieved-cluster
```

User-only paths belong in `~/.config/rundra/config.yaml`:

```yaml
version: 1
defaults:
  targets_file: targets.yaml
  data_dir: records
```

Launch precedence is explicit CLI, selected project profile, project defaults,
user defaults, then built-ins. A missing seed is generated once before planning
and persisted; use `--seed N` to replay it or `--random-seed` to override a
configured fixed seed. `plan --json` and `run --json` expose every resolved value
and its source under `launch`.

If no launch layer supplies `destination`, Rundra retrieves into
`PROJECT_ROOT/retrieved/<config-stem>`. Without a discovered project file, the
base is the current working directory. Configured destinations keep their exact
existing meaning.

## Next steps

- The root [README](../README.md) runs the checked minimal local example.
- [Shoal system testing](shoal.md) covers the concrete remote template,
  resource-bounded examples, and explicit test opt-ins.
- [Versioned JSON contracts](schemas/README.md) documents agent-facing output.
