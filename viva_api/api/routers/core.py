"""
This router relates to image builds (simulators) and parca
"""

import logging

from fastapi import Depends, HTTPException, Query

from viva_api.api import request_examples
from viva_api.common import handlers
from viva_api.common.gateway.models import ServerMode
from viva_api.common.gateway.utils import get_router_config
from viva_api.common.hpc.job_service import JobStatusUpdate
from viva_api.common.models import JobBackend, JobStatus
from viva_api.common.simulator_defaults import DEFAULT_BRANCH, DEFAULT_REPO
from viva_api.config import ComputeBackend, get_job_backend
from viva_api.dependencies import (
    get_database_service,
    get_postgres_engine,
    get_simulation_service,
    get_simulation_service_for_repo,
)
from viva_api.simulation.models import (
    HpcRun,
    JobType,
    ParcaDataset,
    ParcaDatasetRequest,
    RegisteredSimulators,
    Simulator,
    SimulatorVersion,
)

logger = logging.getLogger(__name__)


def get_server_url(dev: bool = True) -> ServerMode:
    return ServerMode.DEV if dev else ServerMode.PROD


# -- app components -- #

# TODO: mount nfs driver


config = get_router_config(prefix="core", version_major=False)


@config.router.get(
    path="/simulator/latest",
    response_model=Simulator,
    operation_id="get-latest-simulator",
    tags=["EcoliSim"],
    dependencies=[Depends(get_database_service), Depends(get_postgres_engine)],
    summary="Get the latest simulator version",
)
async def get_latest_simulator(
    git_repo_url: str = Query(default=DEFAULT_REPO),
    git_branch: str = Query(default=DEFAULT_BRANCH),
) -> Simulator:
    hpc_service = get_simulation_service()
    if hpc_service is None:
        logger.error("HPC service is not initialized")
        raise HTTPException(status_code=500, detail="HPC service is not initialized")

    try:
        latest_commit = await hpc_service.get_latest_commit_hash(git_branch=git_branch, git_repo_url=git_repo_url)
        return Simulator(git_commit_hash=latest_commit, git_repo_url=git_repo_url, git_branch=git_branch)
    except Exception as e:
        logger.exception("Error getting the latest simulator commit.")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulator/versions",
    response_model=RegisteredSimulators,
    operation_id="get-simulator-versions",
    tags=["EcoliSim"],
    dependencies=[Depends(get_database_service), Depends(get_postgres_engine)],
    summary="get the list of available simulator versions",
)
async def get_simulator_versions() -> RegisteredSimulators:
    sim_db_service = get_database_service()
    if sim_db_service is None:
        logger.error("Simulation database service is not initialized")
        raise HTTPException(status_code=500, detail="Simulation database service is not initialized")
    try:
        simulators = await sim_db_service.list_simulators()
        return RegisteredSimulators(versions=simulators)
    except Exception as e:
        logger.exception("Error getting list of simulation versions")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulator/status",
    response_model=HpcRun,
    operation_id="get-simulator-status",
    tags=["EcoliSim"],
    summary="Get simulator container build status by its ID",
)
async def get_simulator_status(simulator_id: int) -> HpcRun:
    db_service = get_database_service()
    if db_service is None:
        logger.error("Simulation database service is not initialized")
        raise HTTPException(status_code=500, detail="Simulation database service is not initialized")

    try:
        simulation_hpcrun: HpcRun | None = await db_service.get_hpcrun_by_ref(
            ref_id=simulator_id, job_type=JobType.BUILD_IMAGE
        )
    except Exception as e:
        logger.exception(f"Error fetching simulation results for simulator container build with id: {simulator_id}.")
        raise HTTPException(status_code=500, detail=str(e)) from e

    if simulation_hpcrun is None:
        raise HTTPException(status_code=404, detail=f"Simulator container build with id {simulator_id} not found.")

    # Live status check for LOCAL builds still showing RUNNING
    if simulation_hpcrun.job_id.backend == JobBackend.LOCAL and simulation_hpcrun.status in (
        JobStatus.RUNNING,
        JobStatus.PENDING,
    ):
        sim_service = get_simulation_service()
        if sim_service is not None:
            live = await sim_service.get_job_status(simulation_hpcrun.job_id)
            if live is not None and live.status != simulation_hpcrun.status:
                update = JobStatusUpdate(
                    job_id=simulation_hpcrun.job_id,
                    status=live.status,
                    error_message=live.error_message,
                )
                await db_service.update_hpcrun_status(hpcrun_id=simulation_hpcrun.database_id, update=update)
                simulation_hpcrun.status = live.status
                if live.error_message:
                    simulation_hpcrun.error_message = live.error_message

    return simulation_hpcrun


@config.router.post(
    path="/simulator/upload",
    response_model=SimulatorVersion,
    operation_id="insert-simulator-version",
    tags=["EcoliSim"],
    dependencies=[Depends(get_database_service), Depends(get_postgres_engine)],
    summary="Upload a new simulator (vEcoli) version.",
)
async def insert_simulator_version(
    simulator: Simulator,
    force: bool = False,
) -> SimulatorVersion:
    # verify simulator request
    handlers.simulators.verify_simulator_payload(simulator)

    # check parameterized service availabilities
    db_service = get_database_service()
    if db_service is None:
        logger.error("Simulation database service is not initialized")
        raise HTTPException(status_code=500, detail="Simulation database service is not initialized")
    # Route the build to the simulator's backend (v2ecoli→Ray builds v2ecoli:<sha> via its
    # docker/ recipe; vEcoli→Batch builds vecoli:{commit}). Passing the deployment DEFAULT
    # service here would send every repo's build to the default backend.
    sim_service = get_simulation_service_for_repo(simulator.git_repo_url)
    if sim_service is None:
        logger.error("Simulation service is not initialized")
        raise HTTPException(status_code=500, detail="Simulation service is not initialized")

    existing_version = await db_service.get_simulator_by_commit(simulator.git_commit_hash)
    if existing_version is not None and not force:
        # Check if previous build failed — if so, fall through to retry
        existing_build = await db_service.get_hpcrun_by_ref(
            ref_id=existing_version.database_id, job_type=JobType.BUILD_IMAGE
        )
        if existing_build is None or existing_build.status != JobStatus.FAILED:
            return existing_version

    try:
        return await handlers.simulators.upload_simulator(
            commit_hash=simulator.git_commit_hash,
            git_repo_url=simulator.git_repo_url,
            git_branch=simulator.git_branch,
            simulation_service_slurm=sim_service,
            database_service=db_service,
            force=force,
        )
    except Exception as e:
        logger.exception("Error inserting simulator version.")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.post(
    path="/simulation/parca",
    response_model=ParcaDataset,
    operation_id="run-parca",
    tags=["EcoliSim"],
    summary="Run a parameter calculation",
)
async def run_parameter_calculator(parca_request: ParcaDatasetRequest = request_examples.base_parca) -> ParcaDataset:
    if get_job_backend() != ComputeBackend.SLURM:
        raise HTTPException(
            status_code=501,
            detail="Standalone parca not supported for K8s backend (runs as part of the Nextflow workflow)",
        )
    db_service = get_database_service()
    if db_service is None:
        logger.error("Simulation database service is not initialized")
        raise HTTPException(status_code=500, detail="Simulation database service is not initialized")

    sim_service = get_simulation_service()
    if sim_service is None:
        logger.error("Simulation service is not initialized")
        raise HTTPException(status_code=500, detail="Simulation service is not initialized")

    try:
        return await handlers.simulations.run_parca(
            simulator=parca_request.simulator_version,
            simulation_service_slurm=sim_service,
            database_service=db_service,
            parca_config=parca_request.parca_config,
        )
    except Exception as e:
        logger.exception("Error running PARCA")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulation/parca/versions",
    response_model=list[ParcaDataset],
    operation_id="get-parca-versions",
    tags=["EcoliSim"],
    summary="Get list of parca calculations",
)
async def get_parcas() -> list[ParcaDataset]:
    db_service = get_database_service()
    if db_service is None:
        logger.error("Simulation database service is not initialized")
        raise HTTPException(status_code=500, detail="Simulation database service is not initialized")

    sim_service = get_simulation_service()
    if sim_service is None:
        logger.error("Simulation service is not initialized")
        raise HTTPException(status_code=500, detail="Simulation service is not initialized")

    try:
        return await handlers.simulations.get_parca_datasets(
            simulation_service_slurm=sim_service,
            database_service=db_service,
        )
    except Exception as e:
        logger.exception("Error running PARCA")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulation/parca/status",
    response_model=HpcRun,
    operation_id="get-parca-status",
    tags=["EcoliSim"],
    summary="Get parca calculation status by its ID",
)
async def get_parca_status(parca_id: int) -> HpcRun:
    db_service = get_database_service()
    if db_service is None:
        logger.error("Simulation database service is not initialized")
        raise HTTPException(status_code=500, detail="Simulation database service is not initialized")

    try:
        simulation_hpcrun: HpcRun | None = await db_service.get_hpcrun_by_ref(ref_id=parca_id, job_type=JobType.PARCA)
    except Exception as e:
        logger.exception(f"Error fetching simulation results for parca id: {parca_id}.")
        raise HTTPException(status_code=500, detail=str(e)) from e

    if simulation_hpcrun is None:
        raise HTTPException(status_code=404, detail=f"Parca with id {parca_id} not found.")
    return simulation_hpcrun
