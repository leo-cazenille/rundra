#!/bin/sh
set -eu

# Slurm parses TaskProlog stdout and adds these variables to the Task
# environment. The paths themselves are private compute-service tmpfs mounts.
printf '%s\n' 'export SLURM_TMPDIR=/scratch'
printf '%s\n' 'export SLURM_GPUTMPDIR=/gpu-scratch'
