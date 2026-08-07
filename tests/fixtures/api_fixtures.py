import datetime
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path
from random import randint
from typing import NamedTuple

import httpx
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport

from viva_api.analysis.models import (
    AnalysisConfig,
    AnalysisDomain,
    ExperimentAnalysisRequest,
)
from viva_api.api import request_examples
from viva_api.api import request_examples as examples
from viva_api.api.client import Client
from viva_api.api.main import app
from viva_api.common.gateway.utils import generate_analysis_request
from viva_api.common.hpc.slurm_service import SlurmService
from viva_api.common.messaging.messaging_service_redis import MessagingServiceRedis
from viva_api.common.simulator_defaults import DEFAULT_SIMULATOR
from viva_api.common.utils import get_uuid
from viva_api.config import REPO_ROOT, get_settings
from viva_api.dependencies import get_job_scheduler, set_job_scheduler

# from viva_api.data.biocyc_service import BiocycService
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.job_scheduler import JobScheduler
from viva_api.simulation.models import (
    ParcaDatasetRequest,
    ParcaOptions,
    Simulation,
    SimulationConfig,
    SimulationRequest,
    Simulator,
)
from viva_api.simulation.simulation_service import SimulationServiceHpc

ENV = get_settings()

# Default simulator repository configuration for tests
SIMULATOR_URL = DEFAULT_SIMULATOR.git_repo_url
SIMULATOR_BRANCH = DEFAULT_SIMULATOR.git_branch
SIMULATOR_COMMIT = DEFAULT_SIMULATOR.git_commit_hash


class SimulatorRepoInfo(NamedTuple):
    """Container for simulator repository information.

    Can be unpacked as tuple: url, branch, hash = repo_info
    """

    url: str
    branch: str
    commit_hash: str


@pytest_asyncio.fixture(scope="session")
async def simulator_repo_info() -> SimulatorRepoInfo:
    """Fixture providing the default simulator repository info for integration tests."""
    return SimulatorRepoInfo(
        url=SIMULATOR_URL,
        branch=SIMULATOR_BRANCH,
        commit_hash=SIMULATOR_COMMIT,
    )


@pytest_asyncio.fixture(scope="function")
async def local_base_url() -> str:
    return "http://testserver"


@pytest_asyncio.fixture(scope="function")
async def fastapi_app() -> FastAPI:
    return app


@pytest_asyncio.fixture(scope="session")
async def latest_commit_hash(simulator_repo_info: SimulatorRepoInfo) -> str:
    """Returns the commit hash from simulator_repo_info fixture."""
    return simulator_repo_info.commit_hash


@pytest_asyncio.fixture(scope="function")
async def in_memory_api_client() -> AsyncGenerator[Client]:
    transport = ASGITransport(app=app)
    async_client = httpx.AsyncClient(transport=transport, base_url="http://testserver")
    client = Client(base_url="http://testserver", raise_on_unexpected_status=True)
    client.set_async_httpx_client(async_client)
    yield client
    await async_client.aclose()


@pytest_asyncio.fixture(scope="session")
async def workspace_image_hash() -> str:
    return "079c43c"


@pytest_asyncio.fixture(scope="session")
async def analysis_config_path() -> Path:
    return Path(REPO_ROOT) / "tests" / "fixtures" / "configs" / "sms_multigen_analysis.json"


@pytest_asyncio.fixture(scope="session")
async def analysis_request() -> ExperimentAnalysisRequest:
    # return ptools_analysis
    return examples.analysis_multiseed_multigen


@pytest_asyncio.fixture(scope="function")
async def experiment_request(database_service: DatabaseServiceSQL) -> SimulationRequest:
    """Create a SimulationRequest with valid simulator_id and parca_dataset_id in the database."""
    import uuid

    # Use a unique commit hash for each test to avoid conflicts
    unique_commit_hash = f"test_{uuid.uuid4().hex[:7]}"

    # First insert the simulator
    simulator = await database_service.insert_simulator(
        git_commit_hash=unique_commit_hash,
        git_repo_url=DEFAULT_SIMULATOR.git_repo_url,
        git_branch=DEFAULT_SIMULATOR.git_branch,
    )

    # Then insert a parca dataset for this simulator
    parca_request = ParcaDatasetRequest(
        simulator_version=simulator,
        parca_config=ParcaOptions(),
    )
    parca_dataset = await database_service.insert_parca_dataset(
        parca_dataset_request=parca_request,
    )

    # Return a SimulationRequest with the valid IDs
    exp_id = f"test-{uuid.uuid4()!s}"
    return SimulationRequest(
        simulation_config_filename="api_simulation_default_with_profile.json",
        simulator_id=simulator.database_id,
        parca_dataset_id=parca_dataset.database_id,
        experiment_id=f"{exp_id}",
        config=SimulationConfig(
            experiment_id=f"{exp_id}",
            analysis_options=examples.analysis_options_omics(n_tp=7),
        ),
    )


@pytest_asyncio.fixture(scope="session")
async def parca_options() -> ParcaOptions:
    return ParcaOptions()


@pytest_asyncio.fixture(scope="session")
async def simulation_config(parca_options: ParcaOptions) -> SimulationConfig:
    return SimulationConfig(
        experiment_id="pytest_fixture_config",
        #     sim_data_path="/pytest/kb/simData.cPickle",
        #     suffix_time=False,
        #     parca_options=parca_options,
        #     generations=randint(1, 1000),
        #     max_duration=10800,
        #     initial_global_time=0,
        #     time_step=1,
        #     single_daughters=True,
        #     emitter="parquet",
        #     emitter_arg={"outdir": "/pytest/api_outputs"},
    )


@pytest_asyncio.fixture(scope="session")
async def ecoli_simulation(parca_options: ParcaOptions) -> Simulation:
    pytest_fixture = "pytest_fixture"
    return Simulation(
        database_id=-1,
        simulator_id=1,
        parca_dataset_id=1,
        experiment_id=pytest_fixture,
        simulation_config_filename="api_simulation_default_with_profile.json",
        config=SimulationConfig(
            experiment_id=pytest_fixture,
            # sim_data_path="/pytest/kb/simData.cPickle",
            # suffix_time=False,
            # parca_options=parca_options,
            # generations=randint(1, 1000),
            # max_duration=10800,
            # initial_global_time=0,
            # time_step=1,
            # single_daughters=True,
            # emitter="parquet",
            # emitter_arg={"outdir": "/pytest/api_outputs"},
        ),
        last_updated=str(datetime.datetime.now()),
        job_id=str(randint(10000, 1000000)),
    )


@pytest_asyncio.fixture(scope="session")
async def base_router() -> str:
    return "/api/v1"


@pytest_asyncio.fixture(scope="session")
async def ptools_analysis_request() -> ExperimentAnalysisRequest:
    return examples.analysis_ptools


@pytest_asyncio.fixture(scope="session")
async def analysis_request_config(ptools_analysis_request: ExperimentAnalysisRequest) -> AnalysisConfig:
    uid: str = get_uuid(scope="test_analysis")
    return ptools_analysis_request.to_config(analysis_name=uid, env=ENV)


@pytest_asyncio.fixture(scope="function")
async def analysis_request_ptools() -> ExperimentAnalysisRequest:
    return request_examples.analysis_ptools


@pytest_asyncio.fixture(scope="function")
async def analysis_request_base() -> ExperimentAnalysisRequest:
    return generate_analysis_request(
        experiment_id="publication_multiseed_multigen-a7ae0b4e093e20e6_1762830572273",
        requested_configs=[AnalysisDomain.MULTIGENERATION, AnalysisDomain.MULTISEED],
    )


@pytest_asyncio.fixture(scope="function")
async def workflow_config() -> SimulationConfig:
    return SimulationConfig(
        experiment_id="pytest_fixture",
        generations=randint(1, 5),
        # n_init_sims=randint(1, 5)
    )


@pytest_asyncio.fixture
async def workflow_request_payload(
    simulation_config: SimulationConfig, simulation_service_slurm: SimulationServiceHpc
) -> SimulationRequest:
    """Minimal simulation request payload for testing."""
    latest_hash = await simulation_service_slurm.get_latest_commit_hash(
        git_repo_url=SIMULATOR_URL, git_branch=SIMULATOR_BRANCH
    )
    return SimulationRequest(
        simulator=Simulator(git_commit_hash=latest_hash, git_repo_url=SIMULATOR_URL, git_branch=SIMULATOR_BRANCH),
        config=simulation_config,
        experiment_id=f"test-{uuid.uuid4()!s}",
        simulation_config_filename="api_simulation_default_with_profile.json",
    )


@pytest_asyncio.fixture(scope="function")
async def job_scheduler(database_service: DatabaseServiceSQL) -> AsyncGenerator[JobScheduler]:
    """Fixture that starts the JobScheduler for integration tests.

    The JobScheduler polls SLURM for job status updates and updates the database.
    This fixture starts the polling loop and stops it when the test completes.
    """
    # Save existing job scheduler if any
    saved_scheduler = get_job_scheduler()

    # Create messaging service (mock - we don't need Redis for status polling)
    messaging_service = MessagingServiceRedis()

    # Create and configure the JobScheduler
    slurm_service = SlurmService()
    scheduler = JobScheduler(
        messaging_service=messaging_service,
        database_service=database_service,
        slurm_service=slurm_service,
    )
    set_job_scheduler(scheduler)

    # Start polling with a short interval for tests
    await scheduler.start_polling(interval_seconds=5)

    yield scheduler

    # Cleanup
    await scheduler.stop_polling()
    set_job_scheduler(saved_scheduler)


@pytest_asyncio.fixture(scope="function")
async def expected_analysis_output_files_incorrect() -> set[str]:
    """Define expected files in the test simulation."""
    return {
        "config.json",
        "results/output.csv",
        "results/summary.txt",
    }


@pytest_asyncio.fixture(scope="function")
async def expected_analysis_output_files() -> set[str]:
    """Define expected files in the test simulation.

    NOTE: Only files with extensions in ["tsv", "html", "csv", "txt"] are included,
    matching the handler's `get_available_omics_output_paths` filter.
    """
    return {
        "ptools_rxns.txt",
        "ptools_rna.txt",
        "ptools_proteins.txt",
        "protein_counts_validation.html",
        "mass_fraction_summary.html",
        "doubling_time_histogram.html",
        "multivariant_cell_mass_report.html",
        "doubling_time.html",
        "wcm_monomers_MIX0-57.tsv",
        "wcm_rnas_MIX0-57.tsv",
        "wcm_metabolic_reactions_MIX0-57.tsv",
        "wcm_complexes_MIX0-57.tsv",
        "subgen.tsv",
    }


@pytest_asyncio.fixture
async def empty_simulation_id() -> int:
    return 1


@pytest_asyncio.fixture
async def simulation_mock(database_service: DatabaseServiceSQL) -> Simulation:
    experiment_id = "sms_multigeneration"

    # Check if a simulation with this experiment_id already exists
    existing_sim = await database_service.get_simulation_by_experiment_id(experiment_id)
    if existing_sim is not None:
        return existing_sim

    # Create a unique commit hash for the simulator
    unique_commit_hash = f"test_{uuid.uuid4().hex[:7]}"

    # Insert the simulator into the database
    simulator = await database_service.insert_simulator(
        git_commit_hash=unique_commit_hash,
        git_repo_url=DEFAULT_SIMULATOR.git_repo_url,
        git_branch=DEFAULT_SIMULATOR.git_branch,
    )

    # Insert a parca dataset for this simulator
    parca_request = ParcaDatasetRequest(
        simulator_version=simulator,
        parca_config=ParcaOptions(),
    )
    parca_dataset = await database_service.insert_parca_dataset(
        parca_dataset_request=parca_request,
    )

    # Create a SimulationConfig pointing to the existing sms_multigeneration output
    sim_config = SimulationConfig(  # type: ignore[call-arg]
        experiment_id=experiment_id,
        emitter="parquet",
        emitter_arg={"out_dir": "/projects/SMS/viva_api/alex/sims/sms_multigeneration"},
    )

    # Create the simulation request
    sim_request = SimulationRequest(
        experiment_id=experiment_id,
        simulation_config_filename="api_simulation_default_ccam.json",
        simulator_id=simulator.database_id,
        parca_dataset_id=parca_dataset.database_id,
        config=sim_config,
    )

    # Insert the simulation into the database
    inserted_sim = await database_service.insert_simulation(sim_request=sim_request)
    return inserted_sim


@pytest_asyncio.fixture
async def large_simulation_mock(database_service: DatabaseServiceSQL) -> Simulation:
    # Use a different experiment_id for large simulation mock to avoid conflicts
    experiment_id = "sms_multigeneration_large"

    # Check if a simulation with this experiment_id already exists
    existing_sim = await database_service.get_simulation_by_experiment_id(experiment_id)
    if existing_sim is not None:
        return existing_sim

    # Create a unique commit hash for the simulator
    unique_commit_hash = f"test_{uuid.uuid4().hex[:7]}"

    # Insert the simulator into the database
    simulator = await database_service.insert_simulator(
        git_commit_hash=unique_commit_hash,
        git_repo_url=DEFAULT_SIMULATOR.git_repo_url,
        git_branch=DEFAULT_SIMULATOR.git_branch,
    )

    # Insert a parca dataset for this simulator
    parca_request = ParcaDatasetRequest(
        simulator_version=simulator,
        parca_config=ParcaOptions(),
    )
    parca_dataset = await database_service.insert_parca_dataset(
        parca_dataset_request=parca_request,
    )

    # Create a SimulationConfig pointing to the existing sms_multigeneration output
    sim_config = SimulationConfig(  # type: ignore[call-arg]
        experiment_id=experiment_id,
        emitter="parquet",
        emitter_arg={"out_dir": "/projects/SMS/viva_api/alex/sims/sms_multigeneration"},
    )

    # Create the simulation request
    sim_request = SimulationRequest(
        experiment_id=experiment_id,
        simulation_config_filename="api_simulation_default_ccam.json",
        simulator_id=simulator.database_id,
        parca_dataset_id=parca_dataset.database_id,
        config=sim_config,
    )

    # Insert the simulation into the database
    inserted_sim = await database_service.insert_simulation(sim_request=sim_request)
    return inserted_sim
