from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, override

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.models import JobId
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import (
    ParcaDataset,
    ParcaDatasetRequest,
    ParcaOptions,
    Simulation,
    SimulatorVersion,
)
from viva_api.simulation.simulation_service import SimulationService


class MockSSHSession:
    """Mock SSH session for tests that don't need real SSH."""

    async def run_command(self, command: str) -> tuple[int, str, str]:
        """Mock run_command that returns success."""
        return 0, "", ""


class MockSSHSessionService:
    """Mock SSH session service for tests that use mock simulation services."""

    @asynccontextmanager
    async def session(self, wait_closed: bool = True) -> AsyncIterator[MockSSHSession]:
        """Yield a mock SSH session."""
        yield MockSSHSession()


class ConcreteSimulationService(SimulationService):
    @override
    async def get_latest_commit_hash(
        self,
        git_repo_url: str = "https://github.com/vivarium-collective/vEcoli",
        git_branch: str = "api-support",
    ) -> str:
        raise NotImplementedError()

    @override
    async def submit_build_image_job(self, simulator_version: SimulatorVersion) -> JobId:
        raise NotImplementedError()

    @override
    async def submit_parca_job(self, parca_dataset: ParcaDataset) -> JobId:
        raise NotImplementedError()

    @override
    async def submit_ecoli_simulation_job(
        self, ecoli_simulation: Simulation, database_service: DatabaseService, correlation_id: str
    ) -> JobId:
        raise NotImplementedError

    @override
    async def read_config_template(
        self, simulator_version: SimulatorVersion, config_filename: str, allow_default_fallback: bool = False
    ) -> str:
        raise NotImplementedError

    @override
    async def get_job_status(self, job_id: JobId) -> JobStatusInfo | None:
        raise NotImplementedError

    @override
    async def cancel_job(self, job_id: JobId) -> None:
        raise NotImplementedError

    @override
    async def close(self) -> None:
        raise NotImplementedError


class SimulationServiceMockCloneAndBuild(ConcreteSimulationService):
    submit_build_args: tuple[Any, ...] = ()
    expected_build_job_id: JobId = JobId.slurm(-1)

    def __init__(self, expected_build_job_id: JobId = JobId.slurm(-1)) -> None:
        self.expected_build_job_id = expected_build_job_id

    @override
    async def submit_build_image_job(self, simulator_version: SimulatorVersion) -> JobId:
        self.submit_build_args = (simulator_version,)
        return self.expected_build_job_id


class SimulationServiceMockParca(ConcreteSimulationService):
    submit_parca_args: tuple[ParcaDataset] = (
        ParcaDataset(
            database_id=0,
            parca_dataset_request=ParcaDatasetRequest(
                simulator_version=SimulatorVersion(database_id=0, git_branch="", git_repo_url="", git_commit_hash=""),
                parca_config=ParcaOptions(),
            ),
        ),
    )
    expected_job_id: JobId

    def __init__(self, expected_job_id: JobId = JobId.slurm(-1)) -> None:
        self.expected_job_id = expected_job_id

    @override
    async def submit_parca_job(self, parca_dataset: ParcaDataset) -> JobId:
        return self.expected_job_id
