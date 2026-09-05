import inspect
import logging

from fastapi import HTTPException

from viva_api.common.hpc.local_task_service import LocalTaskService
from viva_api.common.models import JobBackend, JobStatus
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


async def upload_simulator(  # noqa: C901
    commit_hash: str,
    git_repo_url: str,
    git_branch: str,
    simulation_service_slurm: SimulationService | SimulationServiceHpc | None = None,
    database_service: DatabaseService | None = None,
    force: bool = False,
    include_submit_image: bool = False,
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
        # ``include_submit_image``: also build the NEXTFLOW HEAD image
        # (base + JRE + the nextflow binary) beside the task image. Only the
        # process that runs ``nextflow run`` needs a JVM -- Batch TASKS run the
        # plain science image -- so this is a thin derived layer, off by default.
        # Supported by the Ray build path (viva-api#423); other services ignore
        # a flag they do not accept, so ask by keyword only where it exists.
        build_kwargs: dict[str, object] = {"simulator_version": simulator}
        if include_submit_image:
            if (
                "include_submit_image"
                not in inspect.signature(simulation_service_slurm.submit_build_image_job).parameters
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "include_submit_image is not supported by the build path for "
                        f"{simulator.git_repo_url!r}. The Nextflow head image is built by the "
                        "Ray/v2ecoli path; vEcoli builds its own -submit image unconditionally."
                    ),
                )
            build_kwargs["include_submit_image"] = True
        build_job_id = await simulation_service_slurm.submit_build_image_job(**build_kwargs)  # type: ignore[arg-type]
        hpc_run = await database_service.insert_hpcrun(
            job_id=build_job_id,
            job_type=JobType.BUILD_IMAGE,
            ref_id=simulator.database_id,
            correlation_id="N/A",
        )

        # For LOCAL builds (K8s AND Ray both submit the DooD build as a LOCAL task),
        # bind the task to its row: the LocalTaskService then finalizes the row
        # from the task's own outcome and persists the Batch job ids the task
        # records, so the build is recoverable if this pod dies mid-poll
        # (viva-api#414). Both services expose the LocalTaskService as `_local`.
        if build_job_id.backend == JobBackend.LOCAL:
            local_svc = getattr(simulation_service_slurm, "_local", None)
            if isinstance(local_svc, LocalTaskService):
                await local_svc.bind_hpcrun(build_job_id.value, hpc_run.database_id, database_service)

    return simulator
