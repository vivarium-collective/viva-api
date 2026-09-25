"""What a compose backend is: submit a composite run, build its container when the backend needs one,
and say what became of a job. One implementation per way of running: ``simulation_service_ray``
(AWS Batch, P3d-4c) and ``simulation_service_hpc`` (SLURM over SSH, §4b U2b-2 -- once a SLURM cluster
could be run in Docker for its tests).
"""

from abc import ABC, abstractmethod
from pathlib import Path

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

    async def results_archive(self, experiment_id: str) -> Path | None:
        """A results archive this backend BUILT ITSELF, fetched to a local path -- or ``None`` when
        the backend's runs write their outputs straight to object storage, where the results route
        streams them from the run's prefix. The SLURM backend zips on the HPC side and downloads
        that zip over SSH; the Batch backend returns None. (P3d-4d-2: out of the route, which knew
        one backend's transport.)"""
        return None
