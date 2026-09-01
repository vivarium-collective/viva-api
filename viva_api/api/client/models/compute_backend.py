from enum import Enum


class ComputeBackend(str, Enum):
    BATCH = "batch"
    RAY = "ray"
    SLURM = "slurm"

    def __str__(self) -> str:
        return str(self.value)
