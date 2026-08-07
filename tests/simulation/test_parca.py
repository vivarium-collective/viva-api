import asyncio
import time
from pathlib import Path

import pytest

from tests.fixtures.api_fixtures import SimulatorRepoInfo
from viva_api.common.models import JobStatus
from viva_api.config import get_settings
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import ParcaDatasetRequest, ParcaOptions, SimulatorVersion
from viva_api.simulation.simulation_service import SimulationServiceHpc


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_parca(
    simulation_service_slurm: SimulationServiceHpc,
    database_service: DatabaseServiceSQL,
    simulator_repo_info: SimulatorRepoInfo,
) -> None:
    repo_url, main_branch, commit_hash = simulator_repo_info

    # check if the latest commit is already installed
    simulator: SimulatorVersion | None = None
    for _simulator in await database_service.list_simulators():
        if (
            _simulator.git_commit_hash == commit_hash
            and _simulator.git_repo_url == repo_url
            and _simulator.git_branch == main_branch
        ):
            simulator = _simulator
            break

    # insert the latest commit into the database
    if simulator is None:
        simulator = await database_service.insert_simulator(
            git_commit_hash=commit_hash, git_repo_url=repo_url, git_branch=main_branch
        )

    # Submit build job (which now includes cloning the repository)
    job_id = await simulation_service_slurm.submit_build_image_job(simulator_version=simulator)
    assert job_id is not None

    start_time = time.time()
    while start_time + 60 > time.time():
        job_info_build = await simulation_service_slurm.get_job_status(job_id=job_id)
        if job_info_build is not None and job_info_build.status in (
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        ):
            break
        await asyncio.sleep(5)

    assert job_info_build is not None
    assert job_info_build.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)
    assert job_info_build.job_id == job_id

    parca_dataset_request = ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    parca_dataset = await database_service.insert_parca_dataset(parca_dataset_request=parca_dataset_request)

    # run parca
    job_id = await simulation_service_slurm.submit_parca_job(parca_dataset=parca_dataset)
    assert job_id is not None

    start_time = time.time()
    while start_time + 60 > time.time():
        job_info_parca = await simulation_service_slurm.get_job_status(job_id=job_id)
        if job_info_parca is not None and job_info_parca.status in (
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        ):
            break
        await asyncio.sleep(7)

    assert job_info_parca is not None
    assert job_info_parca.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)
    assert job_info_parca.job_id == job_id
