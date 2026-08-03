from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from tests.fixtures.simulation_service_mocks import (
    MockSSHSessionService,
    SimulationServiceMockCloneAndBuild,
    SimulationServiceMockParca,
)
from viva_api.common.models import JobId, SSHTarget
from viva_api.dependencies import (
    get_simulation_service,
    get_ssh_session_service_or_none,
    set_simulation_service,
    set_ssh_session_service,
)
from viva_api.simulation.simulation_service import SimulationServiceHpc

if TYPE_CHECKING:
    from viva_api.common.ssh.ssh_service import SSHSessionService


@pytest_asyncio.fixture(scope="function")
async def simulation_service_slurm(
    ssh_session_service: "SSHSessionService",  # Ensures SSH singleton is initialized first
) -> AsyncGenerator[SimulationServiceHpc]:
    simulation_service = SimulationServiceHpc()
    saved_simulation_service = get_simulation_service()
    set_simulation_service(simulation_service)

    yield simulation_service

    await simulation_service.close()
    set_simulation_service(saved_simulation_service)


@pytest.fixture
def expected_build_job_id() -> JobId:
    """Fixture to provide the expected job ID for build jobs."""
    return JobId.slurm(999)


@pytest.fixture(scope="function")
def mock_ssh_session_service() -> Generator[MockSSHSessionService]:
    """Fixture to provide a mock SSH session service for tests that don't need real SSH."""
    saved_ssh_service = get_ssh_session_service_or_none(SSHTarget.SLURM)
    mock_service = MockSSHSessionService()
    set_ssh_session_service(mock_service, name=SSHTarget.SLURM)  # type: ignore[arg-type]

    yield mock_service

    set_ssh_session_service(saved_ssh_service, name=SSHTarget.SLURM)


@pytest.fixture(scope="function")
def simulation_service_mock_clone_and_build(
    expected_build_job_id: JobId,
    mock_ssh_session_service: MockSSHSessionService,
) -> Generator[SimulationServiceMockCloneAndBuild]:
    """Fixture to provide a mock simulation service that clones a repository and submits a build job."""
    saved_simulation_service = get_simulation_service()
    simulation_service_mock_clone_and_build = SimulationServiceMockCloneAndBuild(
        expected_build_job_id=expected_build_job_id
    )
    set_simulation_service(simulation_service_mock_clone_and_build)

    yield simulation_service_mock_clone_and_build

    set_simulation_service(saved_simulation_service)


@pytest.fixture
def expected_parca_database_id() -> int:
    """Fixture to provide the expected database ID for parca datasets."""
    return 12345


@pytest.fixture(scope="function")
def simulation_service_mock_parca(
    expected_build_job_id: JobId,
    mock_ssh_session_service: MockSSHSessionService,
) -> Generator[SimulationServiceMockParca]:
    """Fixture to provide a mock simulation service that submits a parca job."""
    saved_simulation_service = get_simulation_service()
    simulation_service_mock_parca = SimulationServiceMockParca(expected_job_id=expected_build_job_id)
    set_simulation_service(simulation_service_mock_parca)

    yield simulation_service_mock_parca

    set_simulation_service(saved_simulation_service)
