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

For ordinary `plan`, `run`, `submit`, and Run-ID lifecycle examples, see
[Running and managing experiments](usage.md). The remainder of this document is
the developer-owned, resource-gated Shoal test procedure and its recorded
point-in-time evidence.

## Opt-in system-test harness

Shoal tests carry the registered `shoal_system` marker and are skipped unless
the explicit command-line switch is present. The harness additionally requires
the paths to the operator's edited target, experiment, and opaque configuration
files. The target name defaults to `shoal` and can be overridden when needed:

| Scope | Additional required switch | Submits work |
|---|---|---|
| target validation and preflight | none beyond `--run-shoal-system-tests` | no |
| bounded CPU | `--run-shoal-cpu-test` | yes |
| bounded GPU | `--run-shoal-gpu-test` | yes |
| controlled failure scenarios | `--run-shoal-failure-tests` | one experiment-failure case |
| three-element CPU array | `--run-shoal-array-test` | yes |
| disconnected lifecycle and cancellation | `--run-shoal-lifecycle-test` | three bounded CPU jobs |

Passing only a resource-specific switch is insufficient; the general switch is
always required. Run one bounded module at a time and inspect its plan/preflight
output before allowing submission.

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
- remote `sbatch`, `squeue`, `scancel`, and `scontrol` commands, plus whether
  optional `sacct` is available;
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
one CPU, no GPU, 1 GiB, and five minutes. OpenSSH, local rsync, the then-required
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

## M4.5 bounded failure scenarios

M4.5 has another independent opt-in. The checked
`examples/shoal/failure` experiment requests one node/task/CPU, no GPU, 1 GiB,
and five minutes. It writes a partial raw result and both log streams before
deliberately exiting 23. The same module also exercises a non-submitting remote
staging failure:

```bash
RUNDRA_SHOAL_TARGETS_FILE=/tmp/rundra-shoal-targets.yaml \
RUNDRA_SHOAL_CPU_IMAGE=/absolute/path/to/cpu-image.sif \
  uv run pytest tests/system/test_shoal_failures.py \
    -m 'shoal_system and shoal_failure' \
    --run-shoal-system-tests \
    --run-shoal-failure-tests \
    -vv
```

The experiment-failure test plans and preflights the exact bounded request
before submitting its single job. It expects CLI exit 2 with a successful
structured operation containing a failed Run, then verifies native/portable
state, exit 23, normalized stdout/stderr, and successful retrieval of the
partial output.

The infrastructure test copies the operator target to a temporary file and
uses the existing CPU image regular file as an intentionally impossible
workspace root. It performs a pure plan but intentionally does not preflight
that invalid target: the purpose is to validate the durable orchestration
failure that preflight would normally prevent. Remote `stat` output must be
identical before and after. The structured operation must fail with CLI exit 1
and `STAGING_FAILED`; retrieval remains `NOT_REQUESTED`, and no Slurm job,
allocated node, task exit, or artifact may appear.

### Recorded M4.5 observation

On 2026-08-15, the deliberate experiment failure passed all assertions in
15.86 seconds. Its durable scheduler and portable states were `FAILED`, exit 23
was exact, and retrieval was `SUCCEEDED`. The retrieved partial file contained
seed 29, the exact `label: shoal-failure` configuration, and its pre-exit marker;
both normalized log streams remained available by Run ID.

The safe staging scenario passed in 2.47 seconds without submitting a job. The
remote allocator could not create `runs/` below the regular image file, and
Rundra recorded `STAGING_FAILED` without fabricating downstream state. The
remote image's reported file type and size were unchanged. No production
adapter correction was required by either scenario.

Retain the failed experiment's exact Run directory and scheduler logs only as
long as the evidence is useful, then remove those precise paths after checking
the terminal record. The staging-failure scenario creates no remote Run
directory or scheduler logs.

## M4.6 reconciled reference path

M4.6 audited the complete CPU, GPU, and failure evidence against the portable
ports, target schema/defaults, adapter parsers, fake fixtures, and public
documentation. The only implementation mismatch was preflight requiring
`sacct` even though scheduler reconciliation already works without it.
Preflight now requires `sbatch`, `squeue`, `scancel`, and `scontrol`, reports
`sacct_available` as structured detail, and accepts either value. When usable,
`sacct` remains the primary terminal query; otherwise Rundra uses
`scontrol show job -o`.

### Observed versions and capabilities

These values were sampled on 2026-08-15 from the actual M4 path. They are
deployment evidence, not minimum supported versions, target defaults, or
portable schema fields.

| Layer | Observed evidence |
|---|---|
| SSH transport client | OpenSSH 9.6p1, using normal host verification and external authentication |
| Local rsync client | rsync 3.2.7, protocol 31 |
| Remote rsync peer | rsync 3.2.7, protocol 31 |
| Slurm | 23.11.4; CPU/GPU submission, `squeue`, accounting-enabled `sacct`, and `scontrol` fallback validated |
| Apptainer | 1.4.5; `exec --cleanenv --no-eval`, read-only/read-write binds, and NVIDIA `--nv` validated |
| Shared workspace | `/shoalhome` reported as ZFS on `fishvision`; staged CPU and GPU Runs executed from it on `shoal1` |

No NFS version was observed because the tested deployment reports ZFS. The
framework relies on configured shared-path behavior and verified execution,
not an NFS-specific core model or filesystem label. Login-side inspection alone
still does not prove sharing on an arbitrary deployment.

The M4 evidence did not justify adding account, partition, QOS, constraint, GPU
model, filesystem type, or runtime versions to the checked target defaults.
Those site/user-specific scheduler choices remain explicit native experiment
options when needed. Existing semantic ports represented every live value;
adapter-owned scheduler metadata retained native states, paths, nodes, and
available timestamps without fabricating a timezone or container digest.

All seven authorized Shoal checks passed across bounded M4.6 invocations: the
target/resource harness, pure plan, non-submitting preflight, CPU Run, GPU Run,
deliberate experiment failure, and safe non-submitting staging failure. Ordinary
`uv run pytest` continues to skip all seven checks and requires no cluster.

## M5.6 bounded replicated-array proof

The checked `examples/shoal/array` experiment requests one node, Task, and CPU
per element, no GPU, 1 GiB, and five minutes. Seeds 40 through 42 form one
three-element Slurm array. Every element writes seed/config evidence; seed 41
then deliberately exits 23 so one Run contains two successes and one failure.
The submission has a separate opt-in:

```bash
RUNDRA_SHOAL_TARGETS_FILE=/tmp/rundra-shoal-targets.yaml \
RUNDRA_SHOAL_CPU_IMAGE=/absolute/path/to/cpu-image.sif \
  uv run pytest tests/system/test_shoal_array.py \
    -m 'shoal_system and shoal_array' \
    --run-shoal-system-tests \
    --run-shoal-array-test \
    -vv
```

The harness performs a pure plan and complete remote preflight before it
submits anything. It requires the exact stable mapping `task_000000/40/0`,
`task_000001/41/1`, and `task_000002/42/2`, one numeric array root, isolated
results, Task-tagged artifacts, terminal exits `0/23/0`, and per-Task logs. It
also invokes `status` and `logs` through fresh CLI processes using only the
durable local record and Run ID. Ordinary tests and the general Shoal opt-in do
not submit this array.

### Recorded M5.6 observation

On 2026-08-16, the bounded proof passed on `fishvision` in 22.35 seconds as
array job 54. The durable Run was `FAILED` with native state `MIXED`, while
retrieval independently succeeded for all three Tasks. Seeds 40 and 42 were
`COMPLETED` with exit zero; seed 41 was `FAILED` with exit 23. All three exact
config/seed files and both log streams matched their logical Task IDs.

The first observation exposed a Slurm compatibility issue: this controller's
`sacct` `JobIDRaw` values were distinct allocation IDs, while its `JobID`
values retained the submitted `array-root_index` aliases. Rundra now correlates
on `JobID`; default adapter and lifecycle tests cover that alias form without
adding a site-specific field to core models.

The system test retains evidence under its exact Run ID. If cleanup is wanted,
inspect the record first and remove only that Run's configured workspace path
and its exact array-element scheduler log files; do not recursively clean the
workspace root.

## M6.6 disconnected lifecycle and final matrix

The checked `examples/shoal/lifecycle` experiment adds one final independent
opt-in. Seeds 71 and 72 each run for twelve seconds; they are submitted before
either is reconciled so two distinct asynchronous Runs coexist. Seed 73 writes
started evidence and then sleeps for up to four minutes, allowing the harness to
observe `RUNNING` and readable started stdout before cancellation. Every Task
requests one CPU, no GPU, 1 GiB, and a five-minute walltime.

```bash
RUNDRA_SHOAL_TARGETS_FILE=/tmp/rundra-shoal-targets.yaml \
RUNDRA_SHOAL_CPU_IMAGE=/absolute/path/to/cpu-image.sif \
  uv run pytest tests/system/test_shoal_lifecycle.py \
    -m 'shoal_system and shoal_lifecycle' \
    --run-shoal-system-tests \
    --run-shoal-lifecycle-test \
    -vv
```

Each `submit`, `status`, `logs`, `fetch`, and `cancel` call is a fresh CLI
process sharing only the explicit client RunRecord directory. The test requires
distinct Run IDs, scheduler IDs, and workspaces; successful isolated seed/config
results; repeat fetch without duplicate manifest keys; terminal cancel
idempotency; active cancellation followed by final status; stable started
stdout even when Slurm appends cancellation diagnostics to stderr; and partial
result retrieval from the cancelled Run.

### Recorded M6.6 observation

On 2026-08-16, the standalone lifecycle proof passed in 55.43 seconds. The final
all-opt-in system invocation then passed all nine tests in 121.24 seconds using
the configured `/shoalhome/shoal/rundra-m66` workspace. Its lifecycle Runs were:

| Seed | Run ID | Slurm job | Final computation | Retrieval |
|---|---|---:|---|---|
| 71 | `run_a99c74ada5a744e28a50573eb701838e` | 125 | `SUCCEEDED` | `SUCCEEDED` |
| 72 | `run_37dcadc467cf4cddabb4a6b126109a3a` | 126 | `SUCCEEDED` | `SUCCEEDED` |
| 73 | `run_471089bee93041138691bd12efdf3d68` | 127 | `CANCELLED` | `SUCCEEDED` |

The same final invocation passed array job 115 (`FAILED` only because its
middle seed deliberately exited 23, retrieval `SUCCEEDED`), dirty-source CPU
job 119, deliberate-failure job 121, GPU job 123, target validation, remote
preflight, and the safe non-submitting staging failure. The latter retained no
scheduler ID and retrieval `NOT_REQUESTED`.

Three preliminary lifecycle assertions were corrected before the green matrix:
the cancel payload is named `cancel`; a queued cancellation need not have log
files, so the partial-evidence case now waits for positive started output; and
Slurm may create an empty log before the first write and append legitimate
cancellation diagnostics afterward. These were harness expectation races, not
production lifecycle failures. Every submitted preliminary job reached a
terminal scheduler state.

Ordinary `uv run pytest` skips this lifecycle test along with every other Shoal
test. The final default run reported nine explicit skips and made no network
connection. Retained live evidence and logs are Run-specific; inspect terminal
records and remove only exact Run/job paths if site-approved cleanup is wanted.
