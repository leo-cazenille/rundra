# Shoal system testing

Shoal is Rundra's first real-cluster validation target. Its known path is a
local client through the `fishvision` SSH host, then Slurm and Apptainer on the
compute nodes, with `/shoalhome` shared between the login and compute nodes.

The checked target example is
[`examples/shoal/targets.yaml`](../examples/shoal/targets.yaml). Copy it to an
untracked location and replace `YOUR_USERNAME` with your Shoal username:

```bash
cp examples/shoal/targets.yaml /tmp/rundra-shoal-targets.yaml
sed -i "s/YOUR_USERNAME/$USER/" /tmp/rundra-shoal-targets.yaml
```

The `fishvision` name is an OpenSSH host alias, not embedded connection or
authentication configuration. Configure it in the normal user SSH files and
keep private keys, passwords, tokens, and other credentials out of Rundra
target, experiment, and RunRecord files. Host-key verification remains under
OpenSSH's normal policy and must not be disabled for testing.

The target deliberately does not prescribe a Slurm account, partition, QOS,
constraint, or GPU model. Those site/user-specific requests belong in the
experiment's explicit `resources.native.slurm` section and must follow Shoal
policy. The example also does not claim that the placeholder workspace exists,
is writable, or that any backend is reachable.

## Opt-in system-test harness

Shoal tests carry the registered `shoal_system` marker and are skipped unless
the explicit command-line switch is present. The harness additionally requires
the paths to the operator's edited target, experiment, and opaque configuration
files. The target name defaults to `shoal` and can be overridden when needed:

```bash
RUNDRA_SHOAL_TARGETS_FILE=/tmp/rundra-shoal-targets.yaml \
RUNDRA_SHOAL_EXPERIMENT=/absolute/path/to/experiment.yaml \
RUNDRA_SHOAL_CONFIG=/absolute/path/to/config.yaml \
  uv run pytest tests/system \
    -m shoal_system \
    --run-shoal-system-tests \
    -vv
```

```bash
RUNDRA_SHOAL_TARGETS_FILE=/tmp/rundra-shoal-targets.yaml \
RUNDRA_SHOAL_EXPERIMENT=/absolute/path/to/experiment.yaml \
RUNDRA_SHOAL_CONFIG=/absolute/path/to/config.yaml \
RUNDRA_SHOAL_TARGET=my-shoal \
  uv run pytest tests/system \
    -m shoal_system \
    --run-shoal-system-tests \
    -vv
```

M4.1 validates only the explicit target selection and conservative future CPU
defaults: one node, one task, one CPU, 1 GiB, and a five-minute walltime. It
makes no SSH connection and submits no work. Missing environment configuration,
an unknown target, a relative/root workspace, or the unchanged
`YOUR_USERNAME` placeholder fails with an actionable message after explicit
opt-in.

## M4.2 non-submitting preflight

The selected experiment must describe exactly one bounded CPU Task: one node,
one task, at most one CPU, no GPU, at most 1 GiB, and at most five minutes. Its
container image must be an absolute path visible from `fishvision`; relative
images cannot be inspected before source staging.

After the pure plan passes, preflight checks these layers independently:

- supported SSH/Slurm/rsync/Apptainer target selection;
- local OpenSSH and rsync clients;
- authenticated SSH connectivity under normal host-key policy;
- an existing writable/searchable remote workspace;
- remote `sbatch`, `squeue`, `sacct`, and `scancel` commands;
- remote Apptainer availability and image inspection;
- requested Slurm resources through `sbatch --test-only`;
- an NFS filesystem beneath the configured workspace.

`sbatch --test-only` writes a temporary script, asks Slurm to validate the
request, and removes the script. It does not submit a job. The M4.2 harness does
not allocate a Rundra workspace or Run ID, stage source, invoke `rundr run` or
`rundr submit`, or execute the container. Failures name the affected layer and
provide a corrective action without copying arbitrary remote stderr into test
output.

The NFS check observes the login-side mount only. Compute-node visibility is
not claimed until the bounded CPU execution in M4.3. Ordinary `uv run pytest`
continues to skip every marked Shoal test and never contacts the cluster. GPU
submission remains a later, separately authorized checkpoint.
