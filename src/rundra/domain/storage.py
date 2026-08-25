from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SlurmScratchPolicy:
    """Site-owned allocation-local execution storage for Slurm jobs."""

    cpu_environment: str = "SLURM_TMPDIR"
    gpu_environment: str = "SLURM_GPUTMPDIR"
    stage_image: bool = True
    copy_back: str = "task"

    def __post_init__(self) -> None:
        if self.cpu_environment != "SLURM_TMPDIR":
            raise ValueError("CPU scratch environment must be SLURM_TMPDIR")
        if self.gpu_environment != "SLURM_GPUTMPDIR":
            raise ValueError("GPU scratch environment must be SLURM_GPUTMPDIR")
        if type(self.stage_image) is not bool or not self.stage_image:
            raise ValueError("Slurm scratch execution requires image staging")
        if self.copy_back != "task":
            raise ValueError("Slurm scratch execution requires per-Task copy-back")
