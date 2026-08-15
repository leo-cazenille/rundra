#!/bin/sh
set -eu

if [ "$#" -ne 4 ] || [ "$1" != "--config" ] || [ "$3" != "--seed" ]; then
    echo "usage: run.sh --config CONFIG --seed SEED" >&2
    exit 64
fi

config=$2
seed=$4
if ! command -v nvidia-smi >/dev/null 2>&1; then
    echo "Apptainer NVIDIA enablement did not expose nvidia-smi" >&2
    exit 69
fi

mkdir -p /workspace/output/results
nvidia-smi -L > /workspace/output/results/nvidia-smi.txt
{
    printf 'seed=%s\n' "$seed"
    printf 'cuda_visible_devices=%s\n' "${CUDA_VISIBLE_DEVICES-}"
    printf 'slurm_job_gpus=%s\n' "${SLURM_JOB_GPUS-}"
    printf 'slurm_gpus_on_node=%s\n' "${SLURM_GPUS_ON_NODE-}"
    printf '%s\n' 'config:'
    cat "$config"
} > /workspace/output/results/evidence.txt

printf 'RUNDRA_GPU_STDOUT seed=%s\n' "$seed"
printf 'RUNDRA_GPU_STDERR nvidia-container-view-ok\n' >&2
