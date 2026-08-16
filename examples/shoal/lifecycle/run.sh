#!/bin/sh
set -eu

if [ "$#" -ne 4 ] || [ "$1" != "--config" ] || [ "$3" != "--seed" ]; then
    echo "usage: run.sh --config CONFIG --seed SEED" >&2
    exit 64
fi

config=$2
seed=$4

mkdir -p /workspace/output/results
evidence=/workspace/output/results/evidence.txt
{
    printf 'seed=%s\n' "$seed"
    printf '%s\n' 'phase=started'
    printf '%s\n' 'config:'
    cat "$config"
} > "$evidence"
printf 'RUNDRA_LIFECYCLE_STDOUT started seed=%s\n' "$seed"

if [ "$seed" -eq 73 ]; then
    sleep 240
else
    sleep 12
fi

printf '%s\n' 'phase=completed' >> "$evidence"
printf 'RUNDRA_LIFECYCLE_STDERR completed seed=%s\n' "$seed" >&2
