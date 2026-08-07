import pytest

from tests.fixtures.simulation_service_mocks import SimulationServiceMockCloneAndBuild
from viva_api.common.handlers.simulators import upload_simulator
from viva_api.common.models import JobStatus
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import JobType


@pytest.mark.asyncio
async def test_upload_simulator_handler(
    database_service: DatabaseServiceSQL, simulation_service_mock_clone_and_build: SimulationServiceMockCloneAndBuild
) -> None:
    """
    Test the upload_simulator handler to ensure it submits a build job.
    The build job now includes repository cloning as part of the SBATCH script.
    The simulation_service_slurm fixture is used to mock the SimulationService.
    The database_service is not mocked, but it is assumed to be a real instance connected to a test database.
    """
    expected_commit_hash = "abc1234"
    expected_git_repo_url = "https://github.com/vivarium-collective/vEcoli"
    expected_git_branch = "api-support"

    returned_simulator_version = await upload_simulator(
        commit_hash=expected_commit_hash,
        git_repo_url=expected_git_repo_url,
        git_branch=expected_git_branch,
        simulation_service_slurm=simulation_service_mock_clone_and_build,
        database_service=database_service,
    )

    # Verify that the build job was submitted
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
