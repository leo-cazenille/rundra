#!/bin/sh
set -eu

seed=$4
mkdir -p /workspace/output/results
printf '{"seed":%s,"scratch":"%s"}\n' "$seed" "${SLURM_TMPDIR:-}" \
    > /workspace/output/results/result.json
