import asyncio
import datetime
import logging
import time
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from tests.api.ecoli.test_simulations import CORE_ROUTER
from tests.fixtures.api_fixtures import (
    SimulatorRepoInfo,
)
from viva_api.analysis.models import AnalysisConfig, AnalysisConfigOptions, ExperimentAnalysisRequest
from viva_api.api import request_examples
from viva_api.api.main import app
from viva_api.common import handlers
from viva_api.common.ssh.ssh_service import SSHSessionService
from viva_api.common.utils import get_uuid, timestamp, unique_id
from viva_api.config import get_settings
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.hpc_utils import get_slurmjob_name
from viva_api.simulation.models import (
    ParcaDatasetRequest,
    ParcaOptions,
    SimulationConfig,
    SimulationRequest,
    SimulatorVersion,
)
from viva_api.simulation.simulation_service import SimulationServiceHpc

ENV = get_settings()


async def prepare_simulator(
    client: AsyncClient, repo_info: SimulatorRepoInfo, logger: logging.Logger
) -> SimulatorVersion:
    """
    Ensure simulator is registered and built using REST endpoints.

    1. GET /simulator/versions to find existing simulator
    2. POST /simulator/upload if not found (triggers build job)
    3. Poll GET /simulator/status until build completes

    Returns the simulator database ID.
    """
    # Check if simulator already exists
    versions_response = await client.get(f"{CORE_ROUTER}/simulator/versions")
    versions_response.raise_for_status()
    versions_data = versions_response.json()

    simulator_data: dict[str, str] = {}
    simulator_id: int | None = None

    for sim in versions_data.get("versions", []):
        if (
            sim.get("git_commit_hash") == repo_info.commit_hash
            and sim.get("git_repo_url") == repo_info.url
            and sim.get("git_branch") == repo_info.branch
        ):
            simulator_data = sim
            simulator_id = int(sim["database_id"])
            logger.info(f"  Found existing simulator: ID={simulator_id}")
            break

    # If not found, upload/create it

    if simulator_id is None:
        logger.info(f"  Creating new simulator for {repo_info.commit_hash}...")
        upload_response = await client.post(
            f"{CORE_ROUTER}/simulator/upload",
            json={
                "git_commit_hash": repo_info.commit_hash,
                "git_repo_url": repo_info.url,
                "git_branch": repo_info.branch,
            },
        )
        assert upload_response.status_code == 200, f"Upload failed: {upload_response.text}"
        simulator_data = upload_response.json()
        simulator_id = int(simulator_data["database_id"])
        logger.info(f"  Created simulator: ID={simulator_id}")

    # Poll build status until complete (build includes repo clone + image build)
    start_time = time.time()
    max_wait_seconds = 1800  # 30 minute timeout for build

    while time.time() - start_time < max_wait_seconds:
        try:
            status_response = await client.get(
                f"{CORE_ROUTER}/simulator/status",
                params={"simulator_id": simulator_id},
            )
            if status_response.status_code == 404:
                # No build job found - simulator was already built previously
                logger.info("  Simulator already built (no pending build job)")
                break

            status_response.raise_for_status()
            status_data = status_response.json()
            build_status = status_data.get("status")

            elapsed = int(time.time() - start_time)
            if build_status in ["COMPLETED", "completed"]:
                logger.info(f"  Build completed after {elapsed}s")
                break
            elif build_status in ["FAILED", "failed"]:
                pytest.fail(f"Simulator build failed: {status_data}")
            elif elapsed % 60 == 0 and elapsed > 0:
                logger.info(f"  Build status: {build_status} ({elapsed}s elapsed)")

            logger.info(f"Still running...the status is: {build_status}")
            await asyncio.sleep(10)
        except Exception as e:
            # If status check fails, assume simulator is ready (no active build)
            logger.info(f"  Build status check: {e}, assuming ready")
            break
    else:
        pytest.fail(f"Simulator build did not complete within {max_wait_seconds}s")

    assert simulator_id is not None, "Simulator ID should be set"
    assert len(simulator_data.keys()) > 0, "No data found."
    return SimulatorVersion(**simulator_data)  # type: ignore[arg-type]


@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_run_analysis(
    base_router: str,
    database_service: DatabaseService,
    ssh_session_service: SSHSessionService,
    simulation_service_slurm: SimulationServiceHpc,
    logger: logging.Logger,
) -> None:
    # Use the public simulator commit that is known to be cloned on HPC (same as test_handle_run_analysis_slurm).
    # The private vEcoli-private repo (SIMULATOR_COMMIT) may not be cloned on the test HPC node.
    HPC_SIMULATOR_COMMIT = "203ab2a"
    HPC_SIMULATOR_URL = "https://github.com/vivarium-collective/vEcoli"
    HPC_SIMULATOR_BRANCH = "api-support"
    HPC_EXPERIMENT_ID = "sim3-baseline-ca00"
    N_TP = 22

    from viva_api.analysis.models import PtoolsAnalysisConfig

    analysis_request = ExperimentAnalysisRequest(
        experiment_id=HPC_EXPERIMENT_ID,
        multiseed=[
            PtoolsAnalysisConfig(name="ptools_rna", n_tp=N_TP, variant=0),
            PtoolsAnalysisConfig(name="ptools_rxns", n_tp=N_TP, variant=0),
        ],
    )

    simulator = await handlers.simulators.upload_simulator(
        commit_hash=HPC_SIMULATOR_COMMIT,
        git_repo_url=HPC_SIMULATOR_URL,
        git_branch=HPC_SIMULATOR_BRANCH,
        simulation_service_slurm=simulation_service_slurm,
        database_service=database_service,
    )

    # Insert parca dataset referencing the simulator
    parca_request = ParcaDatasetRequest(
        simulator_version=simulator,
        parca_config=ParcaOptions(),
    )
    parca_dataset = await database_service.insert_parca_dataset(parca_dataset_request=parca_request)

    # Insert simulation with matching experiment_id (required by /analyses endpoint)
    sim_config = SimulationConfig(experiment_id=HPC_EXPERIMENT_ID)
    sim_request = SimulationRequest(
        experiment_id=HPC_EXPERIMENT_ID,
        simulation_config_filename="api_simulation_default_with_profile.json",
        simulator_id=simulator.database_id,
        parca_dataset_id=parca_dataset.database_id,
        config=sim_config,
    )
    await database_service.insert_simulation(sim_request=sim_request)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        # Submit analysis request via REST endpoint
        analyses_url = f"{base_router}/analyses"
        response = await client.post(analyses_url, json=analysis_request.model_dump())
        response.raise_for_status()
        analysis_response = response.json()
        assert isinstance(analysis_response, list), f"Unexpected analysis response type. Got: {type(analysis_response)}"
        assert len(analysis_response), (
            "No analyses were run during this test and thus no outputs are available. Something is wrong."
        )
        for obj in analysis_response:
            assert isinstance(obj, dict), (
                f"Unexpected datatype in analysis_response array. Expected: dict; Got: {type(obj)}"
            )
            assert sorted(list(obj.keys())) == ["content", "filename", "variant"]


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_get_analysis(
    base_router: str,
    analysis_request: ExperimentAnalysisRequest,
    database_service: DatabaseService,
    logger: logging.Logger,
) -> None:
    transport = ASGITransport(app=app)
    slurmjob_name = get_slurmjob_name(experiment_id=analysis_request.experiment_id)
    name = get_uuid(scope="test_get_analysis")
    analysis_record = await database_service.insert_analysis(
        name=name,
        config=analysis_request.to_config(analysis_name=name, env=ENV),
        last_updated=timestamp(),
        job_name=slurmjob_name,
        job_id=111122,
    )
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        analyses_url = f"{base_router}/analyses/{analysis_record.database_id}"
        response = await client.get(analyses_url)
        response.raise_for_status()
        analysis_response = response.json()
        experiment_ids = analysis_response["config"]["analysis_options"]["experiment_id"]
        assert isinstance(experiment_ids, list)
        assert experiment_ids == [analysis_request.experiment_id]


@pytest.mark.asyncio
async def test_get_outputs(base_router: str, database_service: DatabaseService) -> None:
    exp_name = unique_id(scope="pytest_analysis")
    analysis = await database_service.insert_analysis(
        name="analysis_multigen",
        last_updated=str(datetime.datetime.now()),
        job_name=exp_name,
        job_id=1234,
        config=AnalysisConfig(analysis_options=AnalysisConfigOptions(experiment_id=["sms_multigeneration"])),
    )
    analysis_data = await database_service.get_analysis(database_id=analysis.database_id)
    assert analysis_data.name == "analysis_multigen"


@pytest.mark.skipif(len(str(get_settings().simulation_outdir)) == 0, reason="simulation outdir not supplied")
@pytest.mark.asyncio
async def test_generate_analysis_request() -> None:
    request = request_examples.analysis_test_ptools

    analysis_name = get_uuid(scope="analysis")
    config = request.to_config(analysis_name=analysis_name, env=ENV)

    # env = get_settings()
    # expected_variant_dir = str(env.simulation_outdir / request.experiment_id / "variant_sim_data")

    actual_options = config.analysis_options
    # assert actual_options.variant_data_dir == [expected_variant_dir]
    assert actual_options.experiment_id == [request.experiment_id]
