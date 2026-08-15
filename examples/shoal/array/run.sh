#!/bin/sh
set -eu

if [ "$#" -ne 4 ] || [ "$1" != "--config" ] || [ "$3" != "--seed" ]; then
    echo "usage: run.sh --config CONFIG --seed SEED" >&2
    exit 64
fi

config=$2
seed=$4
mkdir -p /workspace/output/results
{
    printf 'seed=%s\n' "$seed"
    printf '%s\n' 'config:'
    cat "$config"
} > /workspace/output/results/evidence.txt

printf 'RUNDRA_ARRAY_STDOUT seed=%s\n' "$seed"
if [ "$seed" -eq 41 ]; then
    printf 'RUNDRA_ARRAY_STDERR deliberate-exit=23 seed=%s\n' "$seed" >&2
    exit 23
fi
printf 'RUNDRA_ARRAY_STDERR success seed=%s\n' "$seed" >&2
