"""Fixtures for K8s backend tests — mock K8sJobService, SimulationServiceK8s, FileService."""

from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.fixtures.simulation_service_mocks import MockSSHSessionService
from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.hpc.k8s_job_service import K8sJobService
from viva_api.common.hpc.local_task_service import LocalTaskService
from viva_api.common.models import JobId, JobStatus, SSHTarget
from viva_api.common.storage.file_service import FileService, ListingItem
from viva_api.dependencies import (
    get_file_service,
    get_simulation_service,
    get_ssh_session_service_or_none,
    set_file_service,
    set_simulation_service,
    set_ssh_session_service,
)
from viva_api.simulation.simulation_service_k8s import SimulationServiceK8s


@pytest.fixture(scope="function")
def mock_k8s_job_service() -> MagicMock:
    """Mock K8sJobService that tracks calls and returns configurable responses."""
    service = MagicMock(spec=K8sJobService)
    service._namespace = "test-ns"

    # Default: create_job and create_configmap succeed silently
    service.create_job.return_value = MagicMock()
    service.create_configmap.return_value = MagicMock()

    # Default: get_job_status returns COMPLETED
    service.get_job_status.return_value = JobStatusInfo(
        job_id=JobId.k8s("test-job"),
        status=JobStatus.COMPLETED,
    )

    # Default: get_job_logs returns sample log
    service.get_job_logs.return_value = "N E X T F L O W\nWorkflow completed successfully"

    return service


@pytest.fixture(scope="function")
def simulation_service_k8s_mock(
    mock_k8s_job_service: MagicMock,
) -> Generator[SimulationServiceK8s]:
    """SimulationServiceK8s with mocked K8sJobService and SSH, injected as global singleton."""
    saved_simulation_service = get_simulation_service()
    saved_ssh_service = get_ssh_session_service_or_none(SSHTarget.BUILD)

    # Mock SSH for build phase (build machine)
    set_ssh_session_service(MockSSHSessionService(), name=SSHTarget.BUILD)  # type: ignore[arg-type]

    service = SimulationServiceK8s(
        k8s_job_service=mock_k8s_job_service,
        local_task_service=LocalTaskService(),
    )
    set_simulation_service(service)

    yield service

    set_simulation_service(saved_simulation_service)
    set_ssh_session_service(saved_ssh_service, name=SSHTarget.BUILD)


@pytest.fixture(scope="function")
def mock_file_service() -> Generator[MagicMock]:
    """Mock FileService for S3 output retrieval tests, injected as global singleton."""
    saved_file_service = get_file_service()

    mock_service = MagicMock(spec=FileService)

    # Default: empty listing
    mock_service.get_listing = AsyncMock(return_value=[])

    # Default: download_file succeeds
    mock_service.download_file = AsyncMock()

    # Default: get_file_contents returns empty bytes
    mock_service.get_file_contents = AsyncMock(return_value=b"")

    set_file_service(mock_service)

    yield mock_service

    set_file_service(saved_file_service)


def make_listing_item(key: str, size: int = 100) -> ListingItem:
    """Helper to create ListingItem objects for mock S3 listings."""
    from datetime import datetime

    return ListingItem(Key=key, LastModified=datetime.now(), ETag='"abc123"', Size=size)
