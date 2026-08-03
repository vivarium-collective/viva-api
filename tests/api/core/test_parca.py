import pytest

import viva_api
import viva_api.api
from tests.fixtures.simulation_service_mocks import SimulationServiceMockParca
from viva_api.api.client import Client
from viva_api.api.client.api.ecoli_sim.get_parca_status import asyncio as get_parca_status_async
from viva_api.api.client.api.ecoli_sim.get_parca_versions import asyncio as get_parca_versions_async
from viva_api.api.client.api.ecoli_sim.get_simulator_versions import asyncio as get_simulator_versions_async
from viva_api.api.client.api.ecoli_sim.run_parca import asyncio as run_parca_async
from viva_api.api.client.models import (
    HpcRun,
    ParcaDataset,
    ParcaDatasetRequest,
    ParcaOptions,
    RegisteredSimulators,
)
from viva_api.simulation.database_service import DatabaseServiceSQL


@pytest.mark.asyncio
async def test_insert_parca(
    database_service: DatabaseServiceSQL,
    simulation_service_mock_parca: SimulationServiceMockParca,
    in_memory_api_client: Client,
) -> None:
    expected_commit_hash = "abc1234"
    expected_git_repo_url = "https://github.com/vivarium-collective/vEcoli"
    expected_git_branch = "api-support"
    _simulator = await database_service.insert_simulator(
        git_commit_hash=expected_commit_hash, git_repo_url=expected_git_repo_url, git_branch=expected_git_branch
    )
    simulation_versions = await get_simulator_versions_async(client=in_memory_api_client)
    assert simulation_versions is not None
    assert type(simulation_versions) is RegisteredSimulators
    assert len(simulation_versions.versions) == 1
    simulator_dto = simulation_versions.versions[0]

    parca_dataset_request = ParcaDatasetRequest(simulator_version=simulator_dto, parca_config=ParcaOptions())
    parca_dataset_response = await run_parca_async(client=in_memory_api_client, body=parca_dataset_request)
    assert type(parca_dataset_response) is ParcaDataset
    returned_parca_dataset_dto: ParcaDataset = parca_dataset_response

    parca_datasets = await get_parca_versions_async(client=in_memory_api_client)
    assert type(parca_datasets) is list
    assert len(parca_datasets) > 0

    parca_status = await get_parca_status_async(
        client=in_memory_api_client, parca_id=returned_parca_dataset_dto.database_id
    )
    assert type(parca_status) is HpcRun
    assert parca_status.status == viva_api.api.client.models.job_status.JobStatus.RUNNING
    assert parca_status.job_type == viva_api.api.client.models.job_type.JobType.PARCA
    assert parca_status.ref_id == returned_parca_dataset_dto.database_id
