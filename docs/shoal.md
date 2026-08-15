# Shoal system testing

Shoal is Rundra's first real-cluster validation target. Its known path is a
local client through the `fishvision` SSH host, then Slurm and Apptainer on the
compute nodes, with `/shoalhome` intended as the shared workspace root.

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
- an existing writable/searchable remote workspace or nearest existing parent;
- remote `sbatch`, `squeue`, `sacct`, `scancel`, and `scontrol` commands;
- remote Apptainer availability and image inspection;
- requested Slurm resources through `sbatch --test-only`;
- a safely identified filesystem beneath the documented `/shoalhome` root.

`sbatch --test-only` writes a temporary script, asks Slurm to validate the
request, and removes the script. It does not submit a job. The M4.2 harness does
not allocate a Rundra workspace or Run ID, stage source, invoke `rundr run` or
`rundr submit`, or execute the container. Failures name the affected layer and
provide a corrective action without copying arbitrary remote stderr into test
output.

Preflight does not create the configured workspace. If it is absent, the check
walks upward to the nearest existing directory and verifies that Rundra can
create the workspace there later; normal staging performs the actual
`mkdir -p WORKSPACE/runs`. The filesystem check inspects that same existing
ancestor on the login side. Compute-node
visibility is not claimed until the bounded CPU execution in M4.3. Ordinary
`uv run pytest` continues to skip every marked Shoal test and never contacts
the cluster. GPU submission remains a later, separately authorized checkpoint.

### Recorded M4.2 observation

On 2026-08-15, the explicitly enabled harness passed against `fishvision` with
the owner-only `/shoalhome/shoal/.rundra` workspace and the readable
`/shoalhome/shoal/test_slurm_jobs/alpine.sif` image. The one-Task plan requested
one CPU, no GPU, 1 GiB, and five minutes. OpenSSH, local rsync, the required
remote Slurm commands, remote Apptainer, image inspection, and
`sbatch --test-only` all passed. No Run ID or scheduler job ID was created.

Both `stat` and `findmnt` reported the login-side `/shoalhome` filesystem as
`zfs`, contrary to the earlier NFS-specific assumption. Preflight therefore
records a safely bounded filesystem-type value while requiring the workspace
to remain below `/shoalhome`; it does not infer compute-node visibility from a
login-side filesystem label.

The follow-up regression used the deliberately absent
`/shoalhome/shoal/.rundra-preflight-absent-m42` target. All three opt-in checks
passed, and the path was still absent afterward, confirming that preflight does
not require or create a per-host `.rundra` directory.

## M4.3 bounded CPU vertical slice

CPU execution has an additional opt-in so that the preflight command cannot
accidentally submit work. The checked `examples/shoal/cpu` experiment uses only
`/bin/sh`, requests one node/task/CPU, no GPU, 1 GiB, and five minutes. Supply a
readable absolute CPU image path and run only its system-test module:

```bash
RUNDRA_SHOAL_TARGETS_FILE=/tmp/rundra-shoal-targets.yaml \
RUNDRA_SHOAL_CPU_IMAGE=/absolute/path/to/cpu-image.sif \
  uv run pytest tests/system/test_shoal_cpu.py \
    -m 'shoal_system and shoal_cpu' \
    --run-shoal-system-tests \
    --run-shoal-cpu-test \
    -vv
```

The test constructs and preflights the exact plan before submission. It then
creates a temporary Git repository, commits the checked baseline, modifies a
tracked payload and experiment, adds an untracked payload, and executes one
synchronous CLI Run. Assertions cover the concrete seed/config, execution of
both dirty source files, persisted Git commit/branch/diff, Slurm reference and
allocated node, terminal state and timestamps, sealed remote source, normalized
stdout/stderr, raw result retrieval, and the artifact manifest.

Ordinary system-test opt-in without `--run-shoal-cpu-test` still skips this
resource-consuming test.

### Recorded M4.3 observation

On 2026-08-15, the first bounded CPU workload completed on `shoal1`, but client
reconciliation exposed that Shoal has `sacct` installed while Slurm accounting
storage is disabled. Its scheduler output showed exit code zero, but Rundra
correctly stopped before claiming terminal state or retrieval. A default-CI
regression added `scontrol show job -o` as the fallback when `sacct` cannot be
queried; `sacct` remains primary where accounting is available.

The subsequent end-to-end Run passed in 18.39 seconds. Its durable record
contained a numeric scheduler reference, allocated node, native start/end
strings, exit code zero, `COMPLETED`, and computation/retrieval states
`SUCCEEDED`. The isolated read-only source snapshot executed both a modified
tracked payload and an untracked payload. The retrieved raw evidence contained
seed 17 and the exact `label: shoal-cpu` configuration. Git commit, branch,
dirty flag, and bounded tracked diff were preserved; container digest remained
explicitly unavailable. `rundr logs` returned the expected separate stdout and
stderr without manual scheduler inspection.

The test leaves immutable remote evidence under
`WORKSPACE/runs/RUN_ID` and scheduler logs under the target's managed log
directory. The exact Run and job identifiers remain in the operator's local
RunRecord rather than this repository. After evidence is no longer needed,
inspect that record, verify the Run is terminal, and remove only those exact
per-Run and per-job paths. Do not recursively clean the target workspace root.

## M4.4 bounded GPU vertical slice

GPU execution has its own opt-in and cannot be enabled by either the general
system-test flag or CPU flag alone. The checked `examples/shoal/gpu` experiment
requests one node/task/CPU/GPU, 1 GiB, and five minutes. It also sets
`container.gpu: true`; the Slurm GPU request and Apptainer NVIDIA enablement are
separate controls. Supply a readable absolute GPU-capable image path:

```bash
RUNDRA_SHOAL_TARGETS_FILE=/tmp/rundra-shoal-targets.yaml \
RUNDRA_SHOAL_GPU_IMAGE=/absolute/path/to/gpu-image.sif \
  uv run pytest tests/system/test_shoal_gpu.py \
    -m 'shoal_system and shoal_gpu' \
    --run-shoal-system-tests \
    --run-shoal-gpu-test \
    -vv
```

The exact plan and non-submitting preflight must pass before the Run is
allocated. After completion, the test uses `scontrol show job -o` to verify
that Slurm actually allocated `gres/gpu=1`; this check works independently of
Slurm accounting. It separately requires `nvidia-smi -L` to succeed inside the
Apptainer NVIDIA-enabled container. It also checks seed/config propagation,
terminal state and exit status, normalized logs, raw result retrieval, and the
artifact manifest. `CUDA_VISIBLE_DEVICES`, `SLURM_JOB_GPUS`, and
`SLURM_GPUS_ON_NODE` are retained as optional diagnostics, not used as proof of
allocation inside `--cleanenv`.

### Recorded M4.4 observation

On 2026-08-15, the first bounded GPU Run received one GPU on `shoal1`, but the
test's initial mandatory `CUDA_VISIBLE_DEVICES` assertion caused experiment
exit 69. Slurm's retained `AllocTRES` nevertheless showed `gres/gpu=1`. The
test was corrected to keep scheduler-allocation and container-runtime evidence
independent.

The retry passed in 14.15 seconds. Slurm again recorded one allocated GPU, and
`nvidia-smi -L` inside the Ubuntu 24.04 Apptainer image reported one NVIDIA GPU.
The retrieved evidence contained seed 23 and the exact `label: shoal-gpu`
configuration; normalized stdout/stderr matched the experiment, and the durable
record reported `COMPLETED`, exit zero, and computation/retrieval states
`SUCCEEDED`. With accounting enabled for this run, normal reconciliation used
`sacct`; the previously tested `scontrol` fallback remains available when
accounting is absent.

As with M4.3, retained evidence is isolated by Run ID. Inspect the terminal
record before removing only its exact remote Run directory and scheduler log
paths; never recursively clean the workspace root.
