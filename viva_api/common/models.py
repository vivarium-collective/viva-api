from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from pydantic import BaseModel


class StrEnumBase(StrEnum):
    @classmethod
    def keys(cls) -> list[str]:
        return cast(list[str], vars(cls)["_member_names_"])

    @classmethod
    def member_keys(cls) -> list[str]:
        return cast(list[str], vars(cls)["_member_names_"])

    @classmethod
    def values(cls) -> list[str]:
        vals: list[str] = []
        for key in cls.member_keys():
            val = getattr(cls, key, None)
            if val is not None:
                vals.append(val)
        return vals

    @classmethod
    def to_dict(cls) -> dict[str, str]:
        return dict(zip(cls.keys(), cls.values(), strict=False))

    @classmethod
    def to_list(cls, sort: bool = False) -> list[str]:
        vals = cls.values()
        return sorted(vals) if sort else vals


class SSHTarget(StrEnumBase):
    """Named SSH connection targets."""

    SLURM = "slurm"  # SLURM login node (slurm_submit_*) — job submission, status, logs, data
    BUILD = "build"  # ARM64 build machine (build_node_*) — Docker image builds + ECR push


class JobBackend(StrEnumBase):
    """Backend system used to execute HPC jobs."""

    SLURM = "slurm"
    K8S = "k8s"
    LOCAL = "local"  # In-process async task (e.g. SSH build on submit node)
    RAY = "ray"  # AWS Batch multi-node-parallel job running a transient Ray cluster


@dataclass(frozen=True)
class JobId:
    """Backend-tagged job identifier.

    Wraps a backend-specific job identifier string with its backend type.
    Use the factory methods to construct instances.
    """

    value: str
    backend: JobBackend

    @classmethod
    def slurm(cls, slurm_id: int) -> "JobId":
        return cls(value=str(slurm_id), backend=JobBackend.SLURM)

    @classmethod
    def k8s(cls, job_name: str) -> "JobId":
        return cls(value=job_name, backend=JobBackend.K8S)

    @classmethod
    def local(cls, task_id: str) -> "JobId":
        return cls(value=task_id, backend=JobBackend.LOCAL)

    @classmethod
    def ray(cls, batch_job_id: str) -> "JobId":
        """Tag an AWS Batch (multi-node-parallel Ray) job id."""
        return cls(value=batch_job_id, backend=JobBackend.RAY)

    @property
    def as_slurm_int(self) -> int:
        """Get SLURM job ID as int. Raises TypeError if not a SLURM job."""
        if self.backend != JobBackend.SLURM:
            raise TypeError(f"Not a SLURM job ID: {self}")
        return int(self.value)

    def __str__(self) -> str:
        return self.value


class JobStatus(StrEnumBase):
    """Shared job status enum for simulations, analyses, and other HPC jobs."""

    UNKNOWN = "unknown"
    WAITING = "waiting"
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"

    @classmethod
    def from_slurm_state(cls, slurm_state: str) -> "JobStatus":
        """Parse SLURM job state string to JobStatus enum.

        SLURM states can include additional info after the base state,
        e.g., "CANCELLED by 17163" or "FAILED" or "RUNNING".
        This method extracts the base state and maps it to JobStatus.

        Args:
            slurm_state: Raw SLURM job state string (e.g., "RUNNING", "CANCELLED by 17163")

        Returns:
            Corresponding JobStatus enum value, or UNKNOWN if not recognized
        """
        # Extract base state (first word, uppercase)
        base_state = slurm_state.split()[0].upper() if slurm_state else ""
        return _SLURM_STATE_MAP.get(base_state, cls.UNKNOWN)

    @classmethod
    def from_batch_state(cls, batch_state: str) -> "JobStatus":
        """Parse an AWS Batch job status string to JobStatus enum.

        AWS Batch job states: SUBMITTED, PENDING, RUNNABLE, STARTING, RUNNING,
        SUCCEEDED, FAILED. See:
        https://docs.aws.amazon.com/batch/latest/userguide/job_states.html
        """
        return _BATCH_STATE_MAP.get(batch_state.strip().upper() if batch_state else "", cls.UNKNOWN)


# Map SLURM job states to JobStatus (defined after enum class)
# See: https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES
_SLURM_STATE_MAP: dict[str, JobStatus] = {
    # Pre-run states
    "PENDING": JobStatus.PENDING,
    "CONFIGURING": JobStatus.PENDING,  # Job allocated resources, waiting for them to become ready
    "REQUEUED": JobStatus.PENDING,  # Job was requeued
    "RESIZING": JobStatus.PENDING,  # Job is about to change size
    "SUSPENDED": JobStatus.PENDING,  # Job suspended
    # Running states
    "RUNNING": JobStatus.RUNNING,
    "COMPLETING": JobStatus.RUNNING,  # Job is finishing up
    # Completed states
    "COMPLETED": JobStatus.COMPLETED,
    # Failed states
    "FAILED": JobStatus.FAILED,
    "CANCELLED": JobStatus.CANCELLED,
    "TIMEOUT": JobStatus.FAILED,
    "NODE_FAIL": JobStatus.FAILED,
    "OUT_OF_MEMORY": JobStatus.FAILED,
    "PREEMPTED": JobStatus.FAILED,
    "BOOT_FAIL": JobStatus.FAILED,
    "DEADLINE": JobStatus.FAILED,
    "REVOKED": JobStatus.FAILED,
}


# Map AWS Batch job states to JobStatus.
# See: https://docs.aws.amazon.com/batch/latest/userguide/job_states.html
_BATCH_STATE_MAP: dict[str, JobStatus] = {
    "SUBMITTED": JobStatus.QUEUED,
    "PENDING": JobStatus.QUEUED,  # waiting on a dependency
    "RUNNABLE": JobStatus.QUEUED,
    "STARTING": JobStatus.PENDING,  # container being placed/started
    "RUNNING": JobStatus.RUNNING,
    "SUCCEEDED": JobStatus.COMPLETED,
    "FAILED": JobStatus.FAILED,
}


class DataId(BaseModel):
    scope: str
    label: str
    timestamp: str

    def str(self) -> str:
        return f"{self.scope}-{self.label}-{self.timestamp}"
