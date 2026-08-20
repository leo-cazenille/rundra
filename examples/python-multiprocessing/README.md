# Python multiprocessing

This example uses four explicitly bounded Python child processes inside each
Rundra Task. The children evaluate disjoint intervals of the midpoint rule for
`4 / (1 + x^2)` on `[0, 1]`; the combined integral approximates pi.

## Local run

The checked target permits bounded fakeroot definition builds. Rundra builds
`python.def` once and reuses the content-addressed SIF on later runs:

```bash
uv run rundr plan examples/python-multiprocessing/experiment-local.yaml \
  --targets-file examples/python-multiprocessing/targets-local.yaml --seed 17
uv run rundr run examples/python-multiprocessing/experiment-local.yaml \
  --targets-file examples/python-multiprocessing/targets-local.yaml \
  --seed 17 --progress
uv run examples/python-multiprocessing/analyze.py \
  --input examples/python-multiprocessing/retrieved/local \
  --output examples/python-multiprocessing/derived/local-summary.json
```

## Shoal run

The configured `shoal` target must use targets schema version 8 and permit local
definition builds in `preparation.definition_build`. In `auto` mode Rundra
builds the SIF on the client, publishes it by measured SHA-256 to the target
cache, and reuses it on later runs. The target must also permit two workers,
ten Task slots per worker, 40 active Tasks, and at least 256 MiB per logical
Task. Then run:

```bash
uv run rundr plan examples/python-multiprocessing/experiment-shoal.yaml \
  --profile shoal --seeds 0:19
uv run rundr run examples/python-multiprocessing/experiment-shoal.yaml \
  --profile shoal --seeds 0:19 --confirm-tasks 20 --progress
uv run examples/python-multiprocessing/analyze.py \
  --input examples/python-multiprocessing/retrieved/shoal \
  --output examples/python-multiprocessing/derived/shoal-summary.json
```

Each logical Task requests four CPUs and starts four processes. Rundra's Shoal
profile independently creates two Slurm workers with ten Task slots each. Each
worker therefore requests `10 x 4 = 40` CPUs and occupies one 40-core node;
Slurm chooses which two compute nodes. Do not increase either level without
checking the target policy and aggregate memory.

The result records CPU affinity, child PIDs, hostname, seed, interval
partitions, and numerical error. PIDs and hostnames are execution evidence and
are not reproducible values. The integration result is deterministic for a
fixed Python runtime and configuration.

`fishvision` is only the SSH gateway and Slurm controller. Python computation
must run locally on bigfish or inside Slurm allocations on `shoal1` through
`shoal8`, never directly on fishvision.
