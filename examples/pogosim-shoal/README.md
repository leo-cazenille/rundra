# Pogosim on Shoal

This example runs Pogosim's `run_and_tumble` simulation as a three-element
Slurm array. Rundra acquires the pinned source, verifies the published SIF,
compiles the application in a bounded preparation job, executes all seeds, and
retrieves each Task's raw outputs.

## Prerequisite: define the Shoal target once

Rundra does not infer SSH, workspace, account, partition, or QOS policy. Define
a target named `shoal` in `~/.config/rundra/targets.yaml`. The checked
`examples/shoal/targets.yaml` is a non-secret structural template; replace its
username placeholder and add only site-authorized scheduler policy.

```bash
uv run rundr targets --json | python3 -m json.tool
```

The target workspace must be visible from Shoal compute nodes. Preparation
caches default to `<target.workspace>/cache`; a version-2 target file may select
a different shared cache and explicit image search directories.

## One-command three-seed run

Run from the repository root:

```bash
uv run rundr run examples/pogosim-shoal/experiment.yaml --seeds 0:2
```

The adjacent `rundra.yaml` supplies the config, target, full Git
commit, immutable image URI/SHA-256, build command, declared executable, and
bounded build resources. `run` waits for preparation and scientific work, then
retrieves results under `examples/pogosim-shoal/retrieved/default`.

Use `plan` to inspect the network-free execution description without probing
the target or claiming cache hits:

```bash
uv run rundr plan examples/pogosim-shoal/experiment.yaml --seeds 0:2 --json \
  | python3 -m json.tool
```

Use `submit` instead when the client should return after both preparation and
dependent scientific jobs are durably submitted:

```bash
uv run rundr submit examples/pogosim-shoal/experiment.yaml --seeds 0:2 --json
```

Preparation state and logs are separate from scientific Task state and logs:

```bash
uv run rundr status RUN_ID --json | python3 -m json.tool
uv run rundr logs RUN_ID --preparation
uv run rundr logs RUN_ID --task 0
uv run rundr inspect RUN_ID --json | python3 -m json.tool
```

Each successful Task produces `data.feather` and `console.txt`. Raw experiment
results remain separate from derived analysis outputs.

## Twenty-seed MSD sweep

The checked sweep compares mostly ballistic motion with long tumbles using the
same 20 seeds for both parameter sets. The config's `_rundr.seeds` metadata
means the complete 40-Task Slurm array needs one Rundra command:

```bash
uv run rundr run examples/pogosim-shoal/experiment.yaml \
  --config examples/pogosim-shoal/conf/msd-120s.yaml --progress
```

Rundra retrieves raw results to `examples/pogosim-shoal/retrieved/msd-120s`.
Its `metadata/tasks.json` maps every Task to its seed, parameter choices,
effective-config digest, and output directory. Analyze the sweep with a
self-contained PEP 723 script; `uv` resolves PyArrow in the script environment:

```bash
uv run examples/pogosim-shoal/analysis/analyze_msd.py \
  --input examples/pogosim-shoal/retrieved/msd-120s \
  --output examples/pogosim-shoal/derived/msd-120s
```

Derived `summary.json` and `curves.csv` files stay outside the immutable raw
retrieval tree.

## Troubleshooting only

Ordinary use does not require a manual checkout, image pull, or compilation.
To diagnose registry or compiler failures independently, use the exact recipe
identities and command:

```bash
git clone https://github.com/Adacoma/pogosim.git /tmp/pogosim
git -C /tmp/pogosim checkout --detach \
  fe012bb58ef17eae2155b9904bc3eedb650a86bc

apptainer pull /tmp/pogosim-full-v0.10.10.sif \
  library://leo.cazenille/pogosim/pogosim-full:v0.10.10
echo '4005aa26696ca542f1bb462d46085b13ab56f2b51eb4c27f3483c6761995dfd8  /tmp/pogosim-full-v0.10.10.sif' \
  | sha256sum --check

apptainer exec /tmp/pogosim-full-v0.10.10.sif \
  make -C /tmp/pogosim/examples/run_and_tumble clean sim
```

Do not substitute a moving source revision or an unverified image. Building a
SIF from an Apptainer definition remains outside this milestone.

## Maintainer-only live cold/warm test

The gated Shoal test invokes the checked one-command path twice. Start it only
after explicitly clearing or selecting an empty target preparation cache; it
requires a cold pull/build followed by a warm no-pull/no-build run.

```bash
RUNDRA_SHOAL_TARGET=shoal \
RUNDRA_SHOAL_TARGETS_FILE="$HOME/.config/rundra/targets.yaml" \
uv run pytest tests/system/pogosim \
  -m shoal_pogosim \
  --run-shoal-system-tests \
  --run-shoal-pogosim-test \
  -v
```

The test verifies all six retrieved Feather files across the two Runs and the
recorded cold/warm preparation actions. It never runs by default.
