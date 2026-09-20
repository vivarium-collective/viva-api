import inspect
import logging
import secrets

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
from viva_api.simulation.simulation_service_ray import SimulationServiceRay

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


def _builds_head_image_unconditionally(service: object) -> bool:
    """Does this backend build the Nextflow head image whether or not asked?

    Only vEcoli's K8s path does: `_run_build` calls `_build_command(...,
    submit_image=True)` as a literal on its amd64 branch, with no flag reaching
    it. Asserted by NAME rather than by signature, because the absence of an
    `include_submit_image` parameter is exactly what these two cases have in
    common -- a signature check cannot tell "already does it" from "cannot".

    Deliberately not a blanket True for every service lacking the parameter: on
    a backend that neither accepts the flag nor builds the image, silently
    accepting would produce a dispatch that fails minutes later at the image
    pull -- the silent-success shape the caller asked us to avoid.
    """
    return type(service).__name__ == "SimulationServiceK8s"


class SimulatorIsWriteOnce(ValueError):
    """A request would replace an authoritative simulator's image. Refused (D11)."""


def temporary_image_tag(commit_hash: str) -> str:
    """``tmp-<commit>-<nonce>``: visibly not a commit, and unique, so a temporary simulator can
    never claim or overwrite the tag an authoritative build of that commit owns -- nor another
    temporary simulator's."""
    return f"tmp-{commit_hash}-{secrets.token_hex(3)}"


async def upload_simulator(  # noqa: C901
    commit_hash: str,
    git_repo_url: str,
    git_branch: str,
    simulation_service_slurm: SimulationService | SimulationServiceHpc | None = None,
    database_service: DatabaseService | None = None,
    force: bool = False,
    include_submit_image: bool | None = None,
    stage_private_fork: bool = False,
    vecoli_private_commit: str | None = None,
    temporary: bool = False,
    label: str | None = None,
) -> SimulatorVersion:
    """Find the simulator at this commit, or register and build it.

    **Simulators are write-once** (``docs/plan-core.md`` D11): a record, its image and its tag
    are the provenance of every simulation that ran on them. So an authoritative simulator is
    built once. A build that FAILED is retried -- there is no image to replace -- but ``force``
    against one that is built, or building, is refused with ``SimulatorIsWriteOnce``.

    ``temporary`` is the exception, and it is a different thing rather than a flag on the same
    thing: every temporary request makes a NEW record with its own marked image tag
    (``tmp-<commit>-<nonce>``) and builds it. It never reuses, and never touches, anything else.
    """
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

    if temporary and not isinstance(simulation_service_slurm, SimulationServiceRay):
        # Only the Ray build path can push an image under a tag that is not the commit.
        raise ValueError("temporary simulators are supported for repositories built on the Ray / AWS Batch path only")

    # check if the simulator version is already installed. A temporary simulator is never
    # "already installed": it is a test artifact, and an authoritative request must not get one.
    simulator: SimulatorVersion | None = None
    for _simulator in [] if temporary else await database_service.list_simulators():
        if (
            not _simulator.temporary
            and _simulator.git_commit_hash == commit_hash
            and _simulator.git_repo_url == git_repo_url
            and _simulator.git_branch == git_branch
        ):
            simulator = _simulator
            break

    # Check if we need to (re-)submit a build. The only reason to build an EXISTING
    # authoritative simulator again is that its build failed.
    needs_build = simulator is None
    if simulator is not None:
        existing_build = await database_service.get_hpcrun_by_ref(
            ref_id=simulator.database_id, job_type=JobType.BUILD_IMAGE
        )
        if existing_build is not None and existing_build.status == JobStatus.FAILED:
            logger.info(f"Previous build for simulator {simulator.database_id} failed, retrying")
            needs_build = True
        elif force:
            status = existing_build.status if existing_build is not None else None
            state = status.value if status is not None else "of unknown status"
            raise SimulatorIsWriteOnce(
                f"simulator {simulator.database_id} ({commit_hash}) has a build that is {state}. Simulators are "
                f"write-once: its image is the provenance of the simulations that ran on it and is never "
                f"rebuilt. Register a different commit, or a temporary simulator (temporary=true with a label)."
            )

    # insert the latest commit into the database and submit build job
    if simulator is None:
        verify_simulator_payload(
            Simulator(
                git_commit_hash=commit_hash,
                git_repo_url=git_repo_url,
                git_branch=git_branch,
                temporary=temporary,
                label=label,
            )
        )
        simulator = await database_service.insert_simulator(
            git_commit_hash=commit_hash,
            git_repo_url=git_repo_url,
            git_branch=git_branch,
            temporary=temporary,
            label=label,
            image_tag=temporary_image_tag(commit_hash) if temporary else None,
        )

    if needs_build:
        # ``include_submit_image``: also build the NEXTFLOW HEAD image
        # (base + JRE + the nextflow binary) beside the task image. Only the
        # process that runs ``nextflow run`` needs a JVM -- Batch TASKS run the
        # plain science image -- so this is a thin derived layer, off by default.
        # Supported by the Ray build path (viva-api#423); other services ignore
        # a flag they do not accept, so ask by keyword only where it exists.
        build_kwargs: dict[str, object] = {"simulator_version": simulator}
        if include_submit_image is not False:
            # THREE-VALUED on purpose. True is a demand ("I am about to dispatch
            # Nextflow at this commit"), None is the default preference ("build it
            # where that is possible"). They must differ on a backend that cannot:
            # a demand has to fail loudly here rather than at the Batch image pull
            # minutes later, while the default must not turn every upload on the
            # SLURM path into a 400.
            demanded = include_submit_image is True
            accepts_flag = (
                "include_submit_image" in inspect.signature(simulation_service_slurm.submit_build_image_job).parameters
            )
            if accepts_flag:
                build_kwargs["include_submit_image"] = True
            elif _builds_head_image_unconditionally(simulation_service_slurm):
                # Not an error: the request is ALREADY SATISFIED. vEcoli's build
                # hardcodes `submit_image=True` on its amd64 branch
                # (simulation_service_k8s.py) and its own docstring says it submits
                # "ARM64 task + AMD64 submit". Refusing here rejected a request
                # that the backend fulfils by construction.
                logger.info(
                    "include_submit_image: %s builds the head image unconditionally; nothing to add",
                    type(simulation_service_slurm).__name__,
                )
            elif demanded:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "include_submit_image is not supported by the build path for "
                        f"{simulator.git_repo_url!r}, and that path does not build a Nextflow "
                        "head image on its own. A Nextflow dispatch against this commit would "
                        "fail at the container image pull."
                    ),
                )
            else:
                logger.info(
                    "include_submit_image: %s has no Nextflow head image; skipping (not requested explicitly)",
                    type(simulation_service_slurm).__name__,
                )
        # ``stage_private_fork``: stage vEcoli-private (not the public vEcoli mirror) as
        # the image's own wrapped /app/vEcoli fork, so a config's !ParameterSerializer[...]
        # tag whose value only lives in the private fork's param_store can actually
        # resolve. Plain boolean, always explicit by request -- there is no legitimate
        # "silently skip" case the way there was for include_submit_image (nobody should
        # get a differently-sourced fork than the one they asked for without knowing).
        # Only the Ray build path wraps a separate vEcoli fork inside its own image at all.
        if stage_private_fork:
            if not vecoli_private_commit:
                raise HTTPException(
                    status_code=400,
                    detail="vecoli_private_commit is required when stage_private_fork is True",
                )
            accepts_flag = (
                "stage_private_fork" in inspect.signature(simulation_service_slurm.submit_build_image_job).parameters
            )
            if not accepts_flag:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "stage_private_fork is not supported by the build path for "
                        f"{simulator.git_repo_url!r} -- only the v2ecoli/sms-ecoli Ray build "
                        "path wraps a separate vEcoli fork inside its own image."
                    ),
                )
            build_kwargs["stage_private_fork"] = True
            build_kwargs["vecoli_private_commit"] = vecoli_private_commit
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
