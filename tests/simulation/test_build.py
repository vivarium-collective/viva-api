import asyncio
import time
from pathlib import Path

import pytest

from tests.fixtures.api_fixtures import SimulatorRepoInfo
from viva_api.common.models import JobStatus
from viva_api.common.ssh.ssh_service import SSHSessionService
from viva_api.config import get_settings
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.simulation_service import SimulationServiceHpc


@pytest.mark.integration
@pytest.mark.skip(reason="doesn't matter if this is the latest commit")
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_latest_repo_installed(
    ssh_session_service: SSHSessionService, simulator_repo_info: SimulatorRepoInfo
) -> None:
    repo_url, main_branch, commit_hash = simulator_repo_info
    async with ssh_session_service.session() as ssh:
        return_code, stdout, stderr = await ssh.run_command(f"git ls-remote -h {repo_url} {main_branch}")
    assert return_code == 0
    assert stdout.strip("\n").split()[0][:7] == commit_hash


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_build(
    simulation_service_slurm: SimulationServiceHpc,
    database_service: DatabaseServiceSQL,
    simulator_repo_info: SimulatorRepoInfo,
) -> None:
    repo_url, main_branch, commit_hash = simulator_repo_info

    # insert the latest commit into the database
    simulator = await database_service.insert_simulator(
        git_commit_hash=commit_hash, git_repo_url=repo_url, git_branch=main_branch
    )

    # Submit build job (which now includes cloning the repository)
    job_id = await simulation_service_slurm.submit_build_image_job(simulator_version=simulator)
    assert job_id is not None

    start_time = time.time()
    while start_time + 60 > time.time():
        job_info = await simulation_service_slurm.get_job_status(job_id=job_id)
        if job_info is not None and job_info.status in (
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        ):
            break
        await asyncio.sleep(5)

    assert job_info is not None
    assert job_info.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)
    assert job_info.job_id == job_id
