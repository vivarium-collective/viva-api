import asyncio
import logging

from fastapi import HTTPException

from viva_api.common.hpc.job_service import JobStatusUpdate
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.common.simulator_defaults import DEFAULT_BRANCH, DEFAULT_REPO, RepoUrl
from viva_api.dependencies import get_database_service, get_simulation_service_for_repo
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import (
    JobType,
    RegisteredSimulators,
    Simulator,
    SimulatorVersion,
)
from viva_api.simulation.simulation_service import SimulationService, SimulationServiceHpc

logger = logging.getLogger(__name__)

# Re-export for backwards compatibility
__all__ = ["DEFAULT_BRANCH", "DEFAULT_REPO", "RepoUrl"]


def verify_simulator_payload(simulator: Simulator) -> None:
    url = simulator.git_repo_url
    if url not in RepoUrl.values():
        raise ValueError(f"Unrecognized repo URL: {url}. Accepted repos: {RepoUrl.values()}")
    return None


async def get_latest_simulator(
    git_repo_url: str,
    git_branch: str,
) -> Simulator:
    hpc_service = get_simulation_service_for_repo(git_repo_url)
    if hpc_service is None:
        logger.error("HPC service is not initialized")
        raise HTTPException(status_code=500, detail="HPC service is not initialized")

    try:
        latest_commit = await hpc_service.get_latest_commit_hash(git_branch=git_branch, git_repo_url=git_repo_url)
        return Simulator(git_commit_hash=latest_commit, git_repo_url=git_repo_url, git_branch=git_branch)
    except Exception as e:
        logger.exception("Error getting the latest simulator commit.")
        raise HTTPException(status_code=500, detail=str(e)) from e


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


def _register_build_done_callback(
    local_tasks: dict[str, asyncio.Task],  # type: ignore[type-arg]
    task_id: str,
    database_service: DatabaseService,
    hpcrun_id: int,
) -> None:
    """Register asyncio done-callback to propagate build result to DB."""
    task = local_tasks.get(task_id)
    if task is None:
        return
    callback = lambda t: asyncio.ensure_future(  # noqa: E731
        _update_build_status(t, database_service, hpcrun_id, task_id)
    )
    task.add_done_callback(callback)


async def _update_build_status(
    task: asyncio.Task,  # type: ignore[type-arg]
    db: DatabaseService,
    hpcrun_id: int,
    task_id: str,
) -> None:
    """Write final build status to the HpcRun record."""
    if task.cancelled():
        status, err = JobStatus.CANCELLED, None
    elif task.exception():
        status, err = JobStatus.FAILED, str(task.exception())
    else:
        status, err = JobStatus.COMPLETED, None
    try:
        await db.update_hpcrun_status(
            hpcrun_id=hpcrun_id,
            update=JobStatusUpdate(job_id=JobId.local(task_id), status=status, error_message=err),
        )
    except Exception:
        logger.exception(f"Failed to update HpcRun {hpcrun_id} status to {status}")


async def upload_simulator(  # noqa: C901
    commit_hash: str,
    git_repo_url: str,
    git_branch: str,
    simulation_service_slurm: SimulationService | SimulationServiceHpc | None = None,
    database_service: DatabaseService | None = None,
    force: bool = False,
) -> SimulatorVersion:
    if not simulation_service_slurm:
        # Route the build to the simulator's backend (v2ecoli→Ray builds v2ecoli:<sha>,
        # vEcoli→Batch builds vecoli:{commit}); default otherwise.
        simulation_service_slurm = get_simulation_service_for_repo(git_repo_url)
    if simulation_service_slurm is None:
        logger.exception("Simulation service is not initialized")
        raise RuntimeError("Simulation service is not initialized")
    if not database_service:
        database_service = get_database_service()
    if database_service is None:
        logger.exception("Simulation database service is not initialized")
        raise RuntimeError("Simulation database service is not initialized")

    # check if the simulator version is already installed
    simulator: SimulatorVersion | None = None
    for _simulator in await database_service.list_simulators():
        if (
            _simulator.git_commit_hash == commit_hash
            and _simulator.git_repo_url == git_repo_url
            and _simulator.git_branch == git_branch
        ):
            simulator = _simulator
            break

    # Check if we need to (re-)submit a build
    needs_build = simulator is None or force
    if simulator is not None and not force:
        # Re-trigger build if the previous one failed
        existing_build = await database_service.get_hpcrun_by_ref(
            ref_id=simulator.database_id, job_type=JobType.BUILD_IMAGE
        )
        if existing_build is not None and existing_build.status == JobStatus.FAILED:
            logger.info(f"Previous build for simulator {simulator.database_id} failed, retrying")
            needs_build = True

    # insert the latest commit into the database and submit build job
    if simulator is None:
        simulator = await database_service.insert_simulator(
            git_commit_hash=commit_hash, git_repo_url=git_repo_url, git_branch=git_branch
        )
        verify_simulator_payload(simulator)

    if needs_build:
        build_job_id = await simulation_service_slurm.submit_build_image_job(simulator_version=simulator)
        hpc_run = await database_service.insert_hpcrun(
            job_id=build_job_id,
            job_type=JobType.BUILD_IMAGE,
            ref_id=simulator.database_id,
            correlation_id="N/A",
        )

        # For LOCAL builds (K8s AND Ray both submit the DooD build as a LOCAL task),
        # register a done-callback to propagate the final status to the DB. Both services
        # expose the LocalTaskService as `_local`, so resolve it generically.
        if build_job_id.backend == JobBackend.LOCAL:
            local_svc = getattr(simulation_service_slurm, "_local", None)
            if local_svc is not None:
                _register_build_done_callback(
                    local_svc._tasks,
                    build_job_id.value,
                    database_service,
                    hpc_run.database_id,
                )

    return simulator
