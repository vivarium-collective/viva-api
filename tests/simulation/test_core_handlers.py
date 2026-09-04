import asyncio
from typing import override

import pytest

from tests.fixtures.simulation_service_mocks import SimulationServiceMockCloneAndBuild
from viva_api.common.handlers.simulators import upload_simulator
from viva_api.common.hpc.local_task_service import LocalTaskService
from viva_api.common.models import JobId, JobStatus
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import JobType, SimulatorVersion


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


class _LocalBuildService(SimulationServiceMockCloneAndBuild):
    """A backend whose build is a LOCAL task that submits "to Batch" and
    records the handle -- the real shape of SimulationServiceK8s/Ray builds."""

    def __init__(self, fail: bool = False) -> None:
        super().__init__()
        self._local = LocalTaskService()
        self._fail = fail

    @override
    async def submit_build_image_job(self, simulator_version: SimulatorVersion) -> JobId:
        self.submit_build_args = (simulator_version,)

        async def _build() -> None:
            await self._local.record_external_job_ids(["batch-build-1"])
            await asyncio.sleep(0.01)
            if self._fail:
                raise RuntimeError("docker push exploded")

        return self._local.submit(_build(), name="build")


@pytest.mark.asyncio
@pytest.mark.parametrize("fail", [False, True])
async def test_upload_simulator_binds_a_local_build_to_its_row(
    database_service: DatabaseServiceSQL, fail: bool
) -> None:
    """viva-api#414: for a LOCAL build the handler binds task and row, so the
    Batch handle lands on the row and the row is finalized from the task."""
    service = _LocalBuildService(fail=fail)
    simulator = await upload_simulator(
        commit_hash=f"bind{int(fail)}234",
        git_repo_url="https://github.com/vivarium-collective/v2ecoli",
        git_branch="main",
        simulation_service_slurm=service,
        database_service=database_service,
    )
    (job_id,) = list(service._local._tasks)
    assert service._local.bound_hpcrun_id(job_id) is not None
    await service._local.wait_finalized(job_id)

    build = await database_service.get_hpcrun_by_ref(ref_id=simulator.database_id, job_type=JobType.BUILD_IMAGE)
    assert build is not None
    assert build.job_id == JobId.local(job_id)
    assert build.external_job_ids == ["batch-build-1"]
    assert build.end_time is not None
    if fail:
        assert build.status == JobStatus.FAILED
        assert "docker push exploded" in (build.error_message or "")
    else:
        assert build.status == JobStatus.COMPLETED

    await database_service.delete_hpcrun(hpcrun_id=build.database_id)
    await database_service.delete_simulator(simulator_id=simulator.database_id)
