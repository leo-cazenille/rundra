#!/bin/sh
set -eu

if [ "$#" -ne 4 ] || [ "$1" != "--config" ] || [ "$3" != "--seed" ]; then
    echo "usage: run.sh --config CONFIG --seed SEED" >&2
    exit 64
fi

config=$2
seed=$4
tracked=$(cat payload.txt)
untracked=$(cat untracked.txt)

mkdir -p /workspace/output/results
{
    printf 'seed=%s\n' "$seed"
    printf 'tracked=%s\n' "$tracked"
    printf 'untracked=%s\n' "$untracked"
    printf '%s\n' 'config:'
    cat "$config"
} > /workspace/output/results/evidence.txt

printf 'RUNDRA_CPU_STDOUT seed=%s\n' "$seed"
printf 'RUNDRA_CPU_STDERR source-snapshot-ok\n' >&2
