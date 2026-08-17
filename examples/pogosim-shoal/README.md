# Pogosim on Shoal

This example runs Pogosim's `run_and_tumble` simulation as a Slurm array on
Shoal. Rundra creates one task per seed; every task runs one simulator process
and produces its own raw Feather file. `pogobatch` is intentionally not used:
Rundra and Slurm already own the fan-out, resources, lifecycle, and provenance.

The checked workload is a short 50-robot, ten-simulated-second run. Start with
three seeds. The 100-seed command is an operator-controlled showcase, not a
test-suite action.

## 1. Prepare a pinned Pogosim checkout

Use the tested Pogosim 0.10.10 commit rather than a moving branch:

```bash
export POGOSIM_COMMIT=fe012bb58ef17eae2155b9904bc3eedb650a86bc
export POGOSIM_CHECKOUT=/shoalhome/$USER/src/pogosim-$POGOSIM_COMMIT

git clone https://github.com/Adacoma/pogosim.git "$POGOSIM_CHECKOUT"
git -C "$POGOSIM_CHECKOUT" checkout --detach "$POGOSIM_COMMIT"
git -C "$POGOSIM_CHECKOUT" status --short
```

Keep this checkout unchanged after compiling it. Rundra records its revision
and dirty diff as source provenance.

## 2. Pull and identify the stable upstream image

Pull Pogosim's published v0.10.10 image instead of rebuilding its Apptainer
definition. The resulting SIF must live on storage visible from Shoal compute
nodes.

```bash
export POGOSIM_IMAGE=/shoalhome/$USER/images/pogosim-full-v0.10.10.sif

mkdir -p "$(dirname "$POGOSIM_IMAGE")"
apptainer pull "$POGOSIM_IMAGE" \
    library://leo.cazenille/pogosim/pogosim-full:v0.10.10
sha256sum "$POGOSIM_IMAGE" | tee "$POGOSIM_IMAGE.sha256"
```

The prebuilt image provides Pogosim's libraries, arenas, and build support. The
pinned external checkout—not the Rundra repository—is still the launch
`source_root`, because it supplies the selected `run_and_tumble` source and
executable and lets Rundra preserve its exact Git provenance.

Edit `experiment.yaml` and replace its example image path with the absolute
value of `POGOSIM_IMAGE`.

## 3. Compile the example with that image

Compile the selected example once in the pinned checkout. The prebuilt image
supplies the same headers and libraries that will be present during each
scheduled run; this compiles the experiment executable, not the SIF:

```bash
apptainer exec "$POGOSIM_IMAGE" \
    make -C "$POGOSIM_CHECKOUT/examples/run_and_tumble" clean sim

test -x "$POGOSIM_CHECKOUT/examples/run_and_tumble/run_and_tumble"
```

Do not rebuild or edit the checkout between planning and submitting a run.

## 4. Configure reusable Rundra defaults

The example requires a target named `shoal`. First list the targets in Rundra's
default file:

```bash
uv run rundr targets --json | python3 -m json.tool
```

If that output does not contain `shoal`, make a non-secret, untracked target
file from the checked template without replacing your existing user config:

```bash
cp examples/shoal/targets.yaml /tmp/rundra-shoal-targets.yaml
sed -i "s/YOUR_USERNAME/$USER/" /tmp/rundra-shoal-targets.yaml
export RUNDRA_SHOAL_TARGETS_FILE=/tmp/rundra-shoal-targets.yaml
uv run rundr targets --targets-file "$RUNDRA_SHOAL_TARGETS_FILE" --json \
    | python3 -m json.tool
```

Otherwise, select the default file explicitly for the remaining commands:

```bash
export RUNDRA_SHOAL_TARGETS_FILE="$HOME/.config/rundra/targets.yaml"
```

`$HOME/.config/rundra/targets.yaml` is Rundra's default targets-file path. The
commands below keep it explicit so each one shows exactly which cluster
configuration it will use.

That target should name fishvision as its SSH host and use an explicit shared
workspace accessible to compute nodes. Scheduler account, partition, and QOS
choices belong in the target configuration and are never bypassed here.

Copy the launch template to the Rundra repository root and replace its
`source_root` placeholder:

```bash
cp examples/pogosim-shoal/rundra.yaml.example rundra.yaml
sed -i "s|/ABSOLUTE/PATH/TO/PINNED/POGOSIM|$POGOSIM_CHECKOUT|" rundra.yaml
```

If this project already has a `rundra.yaml`, merge the `shoal` profile instead
of replacing it. You can also omit the project file and pass its values as CLI
options. The commands below pass `--project-file rundra.yaml` explicitly:
automatic project-file discovery looks next to the experiment definition, not
in the shell's current working directory.

## 5. Validate and inspect a three-seed plan

Run these from the Rundra repository root:

```bash
uv run rundr validate examples/pogosim-shoal/experiment.yaml --json \
    | python3 -m json.tool

uv run rundr plan examples/pogosim-shoal/experiment.yaml \
    --project-file rundra.yaml \
    --targets-file "$RUNDRA_SHOAL_TARGETS_FILE" \
    --seeds 0:2 \
    --json | python3 -m json.tool
```

Inspect the resolved image, source revision, command arguments, task-to-seed
mapping, requested resources, target, and remote workspace before submitting.

## 6. Submit the three-seed smoke run

```bash
uv run rundr submit examples/pogosim-shoal/experiment.yaml \
    --project-file rundra.yaml \
    --targets-file "$RUNDRA_SHOAL_TARGETS_FILE" \
    --seeds 0:2 \
    --json | tee /tmp/rundra-pogosim-submit.json | python3 -m json.tool

export POGOSIM_RUN_ID=run_REPLACE_WITH_THE_ID_FROM_SUBMIT_JSON
```

Set `POGOSIM_RUN_ID` from the displayed JSON. Rundra's recorded run ID is the
stable handle; do not substitute a native Slurm job ID.

Follow the run from separate CLI invocations:

```bash
uv run rundr status "$POGOSIM_RUN_ID" --json | python3 -m json.tool
uv run rundr logs "$POGOSIM_RUN_ID" --task 0 --json | python3 -m json.tool
uv run rundr inspect "$POGOSIM_RUN_ID" --json | python3 -m json.tool
```

Fetch all completed task outputs:

```bash
uv run rundr fetch "$POGOSIM_RUN_ID" \
    --destination examples/pogosim-shoal/retrieved \
    --json | python3 -m json.tool
find examples/pogosim-shoal/retrieved -name data.feather -type f -size +0c
```

Task-specific `logs` and `fetch` options can be used while other array elements
are still running. Failed elements remain visible in status and are not retried
automatically. Partial raw results are therefore safe to inspect without
hiding failures.

Cancel a run that should not continue:

```bash
uv run rundr cancel "$POGOSIM_RUN_ID" --json | python3 -m json.tool
```

## 7. Launch the 100-seed showcase

Only do this after the three-seed smoke succeeds and cluster policy permits the
resource request:

```bash
uv run rundr plan examples/pogosim-shoal/experiment.yaml \
    --project-file rundra.yaml \
    --targets-file "$RUNDRA_SHOAL_TARGETS_FILE" \
    --seeds 0:99 \
    --json | python3 -m json.tool

uv run rundr submit examples/pogosim-shoal/experiment.yaml \
    --project-file rundra.yaml \
    --targets-file "$RUNDRA_SHOAL_TARGETS_FILE" \
    --seeds 0:99 \
    --json | tee /tmp/rundra-pogosim-100-submit.json | python3 -m json.tool
```

There is no implicit concurrency throttle in this example: Slurm decides when
array elements run under the configured account, partition, and QOS. If the
site requires an array concurrency limit, make that policy explicit in the
Shoal target's auditable native scheduler options before submission.

## Output contract

Each successful task writes only raw files below its Rundra output directory:

```text
console.txt
data.feather
```

The files are placed directly in `/workspace/output`. Pogosim opens configured
file paths but does not create their parent directories, while Rundra already
creates and binds this output root for every Task.

The seed, effective YAML configuration, source revision/dirty diff, container
identity, resources, scheduler IDs, timestamps, and exit state remain in the
Rundra run record. Put merged dataframes, plots, and other derived products in a
separate analysis directory; do not mutate completed run directories.

## Maintainer-only live system test

The repository includes an independently gated three-seed system test. It is
skipped during ordinary pytest runs and submits work only when the authorization
variable and all prerequisites are explicit:

```bash
RUNDRA_POGOSIM_SOURCE_ROOT="$POGOSIM_CHECKOUT" \
RUNDRA_POGOSIM_IMAGE="$POGOSIM_IMAGE" \
RUNDRA_SHOAL_TARGET=shoal \
RUNDRA_SHOAL_TARGETS_FILE="$HOME/.config/rundra/targets.yaml" \
uv run pytest tests/system/pogosim \
    -m shoal_pogosim \
    --run-shoal-system-tests \
    --run-shoal-pogosim-test \
    -v
```

The test plans before submission, polls through fresh CLI processes, checks
logs and scheduler identifiers, fetches all three outputs, and verifies the
Feather/Arrow file signature without adding PyArrow as a Rundra dependency.
