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
    printf '%s\n' 'status=partial-before-exit'
} > /workspace/output/results/partial.txt

printf 'RUNDRA_FAILURE_STDOUT seed=%s\n' "$seed"
printf 'RUNDRA_FAILURE_STDERR deliberate-exit=23\n' >&2
exit 23
