import datetime
from typing import cast

import pytest

import viva_api
import viva_api.api
from tests.fixtures.simulation_service_mocks import SimulationServiceMockCloneAndBuild
from viva_api.api.client import Client
from viva_api.api.client.api.ecoli_sim.get_simulator_status import asyncio as get_simulator_status_async
from viva_api.api.client.api.ecoli_sim.get_simulator_versions import asyncio as get_simulator_versions_async
from viva_api.api.client.api.ecoli_sim.insert_simulator_version import asyncio as insert_simulator_version_async
from viva_api.api.client.models import HpcRun, HTTPValidationError, RegisteredSimulators
from viva_api.api.client.models.simulator import Simulator as SimulatorDto
from viva_api.api.client.models.simulator_version import SimulatorVersion as SimulatorVersionDto
from viva_api.api.client.types import UNSET
from viva_api.common.handlers.simulators import RepoUrl, verify_simulator_payload
from viva_api.common.models import JobStatus
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import JobType, Simulator, SimulatorVersion


@pytest.mark.asyncio
async def test_insert_simulator_version(
    monkeypatch: pytest.MonkeyPatch,
    database_service: DatabaseServiceSQL,
    simulation_service_mock_clone_and_build: SimulationServiceMockCloneAndBuild,
    in_memory_api_client: Client,
) -> None:
    expected_commit_hash = "abc1234"
    expected_git_repo_url = "https://github.com/vivarium-collective/vEcoli"
    expected_git_branch = "messages"
    simulator_dto = SimulatorDto(
        git_commit_hash=expected_commit_hash, git_repo_url=expected_git_repo_url, git_branch=expected_git_branch
    )
    response: HTTPValidationError | SimulatorVersionDto | None = await insert_simulator_version_async(
        client=in_memory_api_client, body=simulator_dto
    )
    assert type(response) is SimulatorVersionDto
    returned_simulator_version_dto: SimulatorVersionDto = response
    assert type(returned_simulator_version_dto) is SimulatorVersionDto

    registered_simulators = await get_simulator_versions_async(client=in_memory_api_client)
    assert type(registered_simulators) is RegisteredSimulators
    assert len(registered_simulators.versions) == 1

    simulator_status = await get_simulator_status_async(
        client=in_memory_api_client, simulator_id=returned_simulator_version_dto.database_id
    )
    assert type(simulator_status) is HpcRun
    assert simulator_status.status == viva_api.api.client.models.job_status.JobStatus.RUNNING
    assert simulator_status.job_type == viva_api.api.client.models.job_type.JobType.BUILD_IMAGE
    assert simulator_status.ref_id == returned_simulator_version_dto.database_id

    # Verify that the build job was submitted (cloning now happens in the SBATCH script)
    created_at: datetime.datetime | None = None
    if returned_simulator_version_dto.created_at is not UNSET:
        created_at = cast(datetime.datetime, returned_simulator_version_dto.created_at)
    returned_simulator_version = SimulatorVersion(
        database_id=returned_simulator_version_dto.database_id,
        git_commit_hash=returned_simulator_version_dto.git_commit_hash,
        git_repo_url=returned_simulator_version_dto.git_repo_url,
        git_branch=returned_simulator_version_dto.git_branch,
        created_at=created_at,
    )
    assert simulation_service_mock_clone_and_build.submit_build_args == (returned_simulator_version,)

    # ensure the returned simulator version matches the expected values
    expected_job_id = simulation_service_mock_clone_and_build.expected_build_job_id
    image_build_hpcrun = await database_service.get_hpcrun_by_job_id(job_id=expected_job_id)
    assert image_build_hpcrun is not None
    assert image_build_hpcrun.job_id == expected_job_id
    assert image_build_hpcrun.ref_id == returned_simulator_version.database_id
    assert image_build_hpcrun.job_type == JobType.BUILD_IMAGE
    assert image_build_hpcrun.status == JobStatus.RUNNING
    assert image_build_hpcrun.start_time is not None
    assert image_build_hpcrun.end_time is None

    assert returned_simulator_version.git_commit_hash == expected_commit_hash
    assert returned_simulator_version.git_repo_url == expected_git_repo_url
    assert returned_simulator_version.git_branch == expected_git_branch
    assert returned_simulator_version.database_id == image_build_hpcrun.ref_id
    assert returned_simulator_version.created_at is not None

    # cleanup database entries
    await database_service.delete_hpcrun(hpcrun_id=image_build_hpcrun.database_id)
    await database_service.delete_simulator(simulator_id=returned_simulator_version.database_id)


def make_simulator(repo_url: str, branch: str) -> Simulator:
    return Simulator(
        git_commit_hash="1111223",
        git_repo_url=repo_url,
        git_branch=branch,
    )


@pytest.mark.parametrize(
    "repo_url,branch",
    [
        (RepoUrl.VECOLI_FORK_REPO_URL, "messages"),
        (RepoUrl.VECOLI_FORK_REPO_URL, "master"),
        (RepoUrl.VECOLI_FORK_REPO_URL, "composite"),
        (RepoUrl.VECOLI_FORK_REPO_URL, "any-feature-branch"),
        (RepoUrl.VECOLI_PUBLIC_REPO_URL, "master"),
        (RepoUrl.VECOLI_PUBLIC_REPO_URL, "multi-parca-aws"),
        (RepoUrl.VECOLI_PUBLIC_REPO_URL, "feature-x"),
        (RepoUrl.VECOLI_PRIVATE_REPO_URL, "master"),
        (RepoUrl.VECOLI_PRIVATE_REPO_URL, "any-branch"),
    ],
)
def test_verify_simulator_payload_valid(repo_url: str, branch: str) -> None:
    simulator = make_simulator(repo_url, branch)

    # Should not raise — any branch is accepted for recognized repos
    verify_simulator_payload(simulator)


def test_verify_simulator_payload_unrecognized_repo_url() -> None:
    """Unrecognized repo URLs should be rejected."""
    simulator = make_simulator(
        "https://github.com/random-org/random-repo",
        "main",
    )

    with pytest.raises(ValueError, match="Unrecognized repo URL"):
        verify_simulator_payload(simulator)
