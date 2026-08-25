#!/bin/sh
set -eu

seed=$4
mkdir -p /workspace/output/results
printf '{"seed":%s}\n' "$seed" \
    > /workspace/output/results/result.json
