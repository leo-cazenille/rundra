#!/bin/sh
set -eu

config=$2
seed=$4
failure_seed=$(awk '$1 == "failure_seed:" {print $2}' "$config")
sleep_seconds=$(awk '$1 == "sleep_seconds:" {print $2}' "$config")
test "$sleep_seconds" = 0 || sleep "$sleep_seconds"
mkdir -p /workspace/output/results
printf '{"host":"%s","seed":%s}\n' "$(hostname)" "$seed" \
    > /workspace/output/results/result.json
test "$seed" -ne "$failure_seed" || exit 23
