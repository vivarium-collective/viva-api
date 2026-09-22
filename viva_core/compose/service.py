"""What a compose backend is: submit a composite run, build its container when the backend needs one,
and say what became of a job. One implementation per way of running; the SLURM one stays in the
application until a SLURM site can test it (``docs/plan-core.md`` P3d-4), the Batch one follows in 3d-4c.
"""

from abc import ABC, abstractmethod

from viva_core.compose.database_service import ComposeDatabaseService
from viva_core.compose.models import ComposeHpcRun, ComposeJobStatus, ComposeSimulation, ComposeSimulatorVersion
from viva_core.models import JobBackend


class ComposeSimulationService(ABC):
    # Which JobBackend this service submits to — tags the ComposeHpcRun so status
    # polling knows whether to query SLURM (via SSH) or AWS Batch (describe_jobs).
    backend: "JobBackend"
    # SLURM builds a per-def Singularity container before the run; prebuilt-image
    # backends (Ray/Batch) skip that build-and-wait step in _dispatch_compose_job.
    requires_container_build: bool = True

    @abstractmethod
    async def submit_simulation_job(
        self, simulation: ComposeSimulation, experiment_id: str, override_command: str | None = None
    ) -> str:
        """Submit the run; return the backend job id as a string (SLURM int-as-str or Batch UUID)."""

    @abstractmethod
    async def build_container(
        self, simulator_version: ComposeSimulatorVersion, random_str: str, db_service: ComposeDatabaseService
    ) -> ComposeHpcRun:
        pass

    async def get_job_status(self, job_id_ext: str) -> ComposeJobStatus | None:
        """Poll this backend for a run's status. Default None = 'use the SLURM monitor path'."""
        return None
