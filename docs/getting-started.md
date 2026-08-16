# Installation and target setup

This guide describes the implemented v0.1-development interface. Rundra is not
yet published on PyPI, so install or run it from a source checkout rather than
assuming that `uv tool install rundra` resolves a released distribution.

## Requirements

The client requires Python 3.12. From a checkout, synchronize the locked
environment and verify the installed console script:

```bash
uv sync --locked
uv run rundr --help
```

All repository examples below use `uv run rundr`. A source installation outside
the repository is also possible:

```bash
uv tool install --python 3.12 /absolute/path/to/rundra
rundr --help
```

Reinstall that tool after changing the checkout. Contributors should use the
locked development environment and commands in the root README instead.

The all-local native path needs no external execution tool. Other paths need:

| Path | Client-side commands | Execution-side commands |
|---|---|---|
| local/native | none beyond Rundra | Python/application executable requested by the experiment |
| local/Apptainer | `apptainer` | same machine |
| SSH/Slurm/rsync/Apptainer | OpenSSH and rsync | rsync, Slurm client commands, Apptainer, and the experiment executable inside the image |

For the remote path, the Slurm commands required for execution are `sbatch`,
`squeue`, `scancel`, and `scontrol`. Rundra uses `sacct` when it is available and
usable, but works without it by falling back to `scontrol` for retained jobs.

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

## SSH/Slurm target

The only implemented remote stack is SSH/Slurm/rsync/Apptainer:

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

The SSH `host` is a plain host alias or `user@host` destination. Configure
ports, proxy jumps, identities, agent authentication, and host-key policy in
normal OpenSSH configuration. Rundra neither accepts credentials nor adds
flags that weaken host verification.

The remote workspace is mandatory, absolute, and not `/`. It should be on a
filesystem visible from both the login/controller side and Slurm compute nodes.
It does not need to exist before the first Run: preflight checks the nearest
existing writable ancestor, and staging creates `WORKSPACE/runs/RUN_ID` when
execution begins. Rundra never implicitly requires or creates `~/.rundra`; it
uses exactly the workspace configured for the selected target.

Before running an experiment, verify the external path directly:

```bash
ssh cluster-login true
rsync --version
ssh cluster-login 'command -v rsync && command -v sbatch && command -v squeue && command -v scancel && command -v scontrol && command -v apptainer'
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
Slurm native-option allowlist, and staging intent without connecting, creating a
workspace or RunRecord, or submitting work. Live capability checks happen only
on execution or through the explicitly opted-in Shoal preflight harness.

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

## Next steps

- The root [README](../README.md) runs the checked minimal local example.
- [Shoal system testing](shoal.md) covers the concrete remote template,
  resource-bounded examples, and explicit test opt-ins.
- [Versioned JSON contracts](schemas/README.md) documents agent-facing output.
