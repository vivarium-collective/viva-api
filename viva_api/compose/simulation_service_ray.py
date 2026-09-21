"""AWS Batch (Ray-MNP) implementation of the compose simulation service.

Runs the GENERIC ``run_pbg.py`` runner on the same Batch MNP machinery the
v2ecoli ensemble path already uses (``simulation.simulation_service_ray``): the
prebuilt workspace image (``<ray_ecr_repository>:<compose_ray_image_tag>``, which
already carries process-bigraph + pbg-emitters), the CDK-provisioned MNP
job-def/queue, and per-job env overrides. Only the job *command* differs — a
generic ``run_pbg.py <doc> -n <steps>`` instead of the vEcoli ensemble driver.

Nothing new in sms-cdk: this rides the existing ``ray_mnp_queue``/
``ray_mnp_job_definition``. The uploaded process-bigraph document is staged to S3
and downloaded on the head; ``run_pbg.py`` is embedded in the command via a
heredoc (same trick ``container_def.build_pbg_def`` uses for the SLURM path), so
the runner source travels with the job without living in the image.
"""

import importlib.resources as _res
import logging
import random
import string
import tempfile
from pathlib import Path
from typing import Protocol, override

from viva_api.common.models import JobBackend, JobStatus
from viva_api.common.site_environments import environment_image, job_definition_key, named_environment_image
from viva_api.common.storage import data_layout
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.compose.database_service import ComposeDatabaseService
from viva_api.compose.models import ComposeHpcRun, ComposeJobStatus, ComposeSimulation, ComposeSimulatorVersion
from viva_api.compose.simulation_service import ComposeSimulationService
from viva_api.config import get_settings

logger = logging.getLogger(__name__)

# The embedded generic runner (read once at import). Same source SLURM embeds.
_RUNNER_SRC = (_res.files("viva_api.compose") / "run_pbg.py").read_text()

# Where the runner writes inside the container; the Ray-on-Batch entrypoint syncs
# RAY_OUT_DIR → RAY_OUT_S3 (the compose results uri).
COMPOSE_OUT_DIR = "/tmp/pbg_out"  # noqa: S108
COMPOSE_DOC_PATH = "/tmp/pbg_doc.pbg"  # noqa: S108
COMPOSE_RUNNER_PATH = "/tmp/run_pbg.py"  # noqa: S108

# AWS Batch job state → ComposeJobStatus (via the shared JobStatus mapping).
_JOBSTATUS_TO_COMPOSE: dict[JobStatus, ComposeJobStatus] = {
    JobStatus.QUEUED: ComposeJobStatus.QUEUED,
    JobStatus.PENDING: ComposeJobStatus.PENDING,
    JobStatus.WAITING: ComposeJobStatus.WAITING,
    JobStatus.RUNNING: ComposeJobStatus.RUNNING,
    JobStatus.COMPLETED: ComposeJobStatus.COMPLETED,
    JobStatus.FAILED: ComposeJobStatus.FAILED,
    JobStatus.CANCELLED: ComposeJobStatus.CANCELLED,
    JobStatus.UNKNOWN: ComposeJobStatus.UNKNOWN,
}


def _rand_suffix() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


class SubmitContainer(Protocol):
    """Run ONE container job in an image -- the keyword arguments compose itself passes."""

    def __call__(
        self,
        *,
        job_name: str,
        job_definition: str,
        job_cmd: str,
        out_s3: str,
        out_dir: str,
        depends_on: list[str] | None = None,
        depends_type: str | None = "SEQUENTIAL",
        tags: dict[str, str] | None = None,
    ) -> str: ...


class AfterSubmit(Protocol):
    """A hook: a composite run was submitted -- here is its job, and the image and key it runs under.

    Compose runs composites; what an APPLICATION wants to follow one with is the application's. SMS
    chains its simulator's analysis here (``viva_api.simulation.compose_analysis``); a core with no
    application registers nothing. Called best-effort: the run is already submitted, so a failure in
    here is logged and never fails the submission.
    """

    async def __call__(
        self, *, simulation: ComposeSimulation, experiment_id: str, sim_job_id: str, commit: str | None, image: str
    ) -> object: ...


class StageInputs(Protocol):
    """A hook: ``(stage_s3, stage_dir)`` to sync into the job before the composite runs, or
    ``(None, None)``. ``commit`` is the resolved simulator commit, or ``None`` on the site-pinned image.

    Staging is the container contract's (``stage_s3`` -> ``stage_dir``); WHAT to stage is the
    application's. SMS stages its simulator's ParCa cache (``viva_api.simulation.compose_staging``).
    """

    def __call__(self, commit: str | None = None) -> tuple[str | None, str | None]: ...


class ComposeBatch(Protocol):
    """What compose asks of whatever runs its Batch jobs: one multi-node job for the run,
    one container job for the analysis that follows it, and what became of a job.

    Declared HERE, in compose's own terms, and satisfied today by the SMS Batch layer
    (``viva_api.simulation.dispatch.batch_layer.BatchLayer``) -- which compose does not import.
    The composition root hands one in. That is the direction compose has to face to move into
    core (``docs/plan-core.md`` P3): it names what it needs; the application provides it.
    Only the keyword arguments compose actually passes are declared.
    """

    def image_uri(self, commit: str) -> str: ...

    def ensure_mnp_job_def(self, image: str, commit: str) -> str: ...

    def submit_mnp(
        self,
        *,
        job_name: str,
        job_definition: str,
        num_nodes: int,
        ray_job_cmd: str,
        out_s3: str,
        out_dir: str,
        stage_s3: str | None = ...,
        stage_dir: str | None = ...,
    ) -> str: ...

    def ensure_container_job_def(self, image: str, commit: str) -> str: ...

    @property
    def submit_container(self) -> "SubmitContainer": ...

    def get_batch_job_statuses(self, job_ids: list[str]) -> dict[str, JobStatus]: ...


class ComposeSimulationServiceRay(ComposeSimulationService):
    """Submit generic compose documents to the existing Ray-on-Batch MNP queue."""

    backend = JobBackend.RAY
    requires_container_build = False  # prebuilt workspace image; no per-run singularity build

    def __init__(
        self,
        batch: ComposeBatch,
        *,
        after_submit: AfterSubmit | None = None,
        stage_inputs: StageInputs | None = None,
    ) -> None:
        # The Batch/job-def/submit plumbing, HANDED IN. Until P2.1 PR 6 this built a whole
        # ``SimulationServiceRay()`` -- an E. coli simulation service, with its scheduler-facing
        # surface and its two other backends -- to call five Batch methods on it.
        self._batch = batch
        # What the application wants to follow a run with (SMS: its science analysis). None: nothing.
        self._after_submit = after_submit
        self._stage_inputs = stage_inputs

    def _image_uri(self, commit: str | None = None) -> str:
        # A resolved per-commit build (item 98: ComposeSimulationRequest.simulator_id)
        # takes the exact same TRUE-commit-image shape the vEcoli ensemble path uses —
        # delegate to the shared primitive rather than re-deriving it.
        if commit is not None:
            return self._batch.image_uri(commit)
        settings = get_settings()
        if not settings.compose_ray_image_tag:
            # Fail here, at submit, with the setting name — not 10 minutes later as an
            # opaque Batch image-pull failure. The tag is the workspace commit and has
            # no safe default (see config.compose_ray_image_tag).
            raise RuntimeError(
                "compose_ray_image_tag is unset; set COMPOSE_RAY_IMAGE_TAG to the workspace "
                f"commit to run compose jobs on {settings.ray_ecr_repository}."
            )
        return environment_image(settings, settings.compose_ray_image_tag)

    def _compose_command(self, doc_s3_uri: str, runner_s3_uri: str, steps: int, *, workspace_core: bool = True) -> str:
        """Download the doc AND the runner from S3, run it → RAY_OUT_DIR.

        The runner is fetched from S3 (staged by ``submit_simulation_job``) rather
        than embedded in the command via a heredoc: AWS Batch caps a container
        override command at 8192 bytes, and inlining the full ``run_pbg.py`` source
        overflowed that once the runner grew (the emitter-redirect + workspace-core
        additions tipped it to 8199). ``aws s3 cp`` keeps the command a few hundred
        bytes regardless of runner size — the same mechanism already used for the doc.
        """
        # Name the workspace's own core builder when the deploy configures one, so a
        # document referencing workspace-registered TYPES (not just addresses) resolves.
        # ...but NOT in a registered environment: the builder is a module of the workspace's image,
        # and the runtime image has no workspace. (The runner would warn and fall back; not asking is cleaner.)
        core_builder = get_settings().compose_pbg_core_builder if workspace_core else None
        # PBG_REQUIRE_OUTPUT=1: a compose run that produced no emitted store is a
        # failure, not a success on the final_state.json fallback (audit §2.4 / P0-3).
        env = f"PBG_RESULTS_DIR={COMPOSE_OUT_DIR} PBG_REQUIRE_OUTPUT=1"
        if core_builder:
            env += f" PBG_CORE_BUILDER={core_builder}"
        return (
            f"mkdir -p {COMPOSE_OUT_DIR}"
            f" && aws s3 cp {doc_s3_uri} {COMPOSE_DOC_PATH}"
            f" && aws s3 cp {runner_s3_uri} {COMPOSE_RUNNER_PATH}"
            f" && {env} python {COMPOSE_RUNNER_PATH}"
            f" {COMPOSE_DOC_PATH} -o {COMPOSE_OUT_DIR} -n {steps}"
        )

    async def _resolve_commit(self, simulator_id: int | None) -> str | None:
        """Resolve a per-run ``simulator_id`` to its git commit, or None when unset.

        None preserves today's exact behavior (the deploy-wide static image).
        ``simulator_id`` resolves against the LEGACY simulator registry (same one
        ``POST /api/v1/simulations`` already uses), not this module's own
        ``ComposeSimulatorVersion`` -- see ``ComposeSimulationRequest.simulator_id``.
        """
        if simulator_id is None:
            return None
        from viva_api.dependencies import get_database_service

        database_service = get_database_service()
        if database_service is None:
            raise RuntimeError("Database service not initialized; cannot resolve simulator_id.")
        simulator = await database_service.get_simulator(simulator_id=simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {simulator_id} not found")
        return simulator.environment_key

    @override
    async def submit_simulation_job(
        self, simulation: ComposeSimulation, experiment_id: str, override_command: str | None = None
    ) -> str:
        from viva_api.dependencies import get_file_service

        file_service = get_file_service()
        if file_service is None:
            raise RuntimeError("FileService not initialized; cannot stage compose document to S3.")
        doc_path = simulation.sim_request.request_file_path
        if doc_path is None:
            raise RuntimeError("Compose simulation has no request_file_path to stage.")

        # Stage the uploaded document AND the generic runner to S3 (bucket-relative,
        # under the experiment prefix). The runner goes to S3 rather than into the
        # command because AWS Batch caps the container override at 8192 bytes.
        exp_prefix = data_layout.RayLayout.experiment_prefix(experiment_id)
        doc_key = f"{exp_prefix}/input.pbg"
        await file_service.upload_file(Path(doc_path), S3FilePath(s3_path=Path(doc_key)))
        doc_s3_uri = data_layout.s3_uri(doc_key)

        runner_key = f"{exp_prefix}/run_pbg.py"
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
            tmp.write(_RUNNER_SRC)
            runner_local = tmp.name
        try:
            await file_service.upload_file(Path(runner_local), S3FilePath(s3_path=Path(runner_key)))
        finally:
            Path(runner_local).unlink(missing_ok=True)
        runner_s3_uri = data_layout.s3_uri(runner_key)

        steps = int(simulation.sim_request.end_time_point)
        if simulation.sim_request.environment is not None:
            return self._submit_in_environment(
                simulation.sim_request.environment, experiment_id, doc_s3_uri, runner_s3_uri, steps
            )
        commit = await self._resolve_commit(simulation.sim_request.simulator_id)
        image = self._image_uri(commit)
        # `_ensure_mnp_job_def` keys the derived revision by a tag string — reuse the
        # resolved commit (or, absent one, the deploy-wide image tag) as that key so
        # resubmits against the same image reuse the revision.
        job_def = self._batch.ensure_mnp_job_def(image, commit or get_settings().compose_ray_image_tag)
        # What the APPLICATION wants staged into the job before the composite runs (SMS: the
        # simulator's ParCa cache). Compose knows nothing of it; with no hook, nothing is staged.
        stage_s3, stage_dir = self._stage_inputs(commit) if self._stage_inputs is not None else (None, None)
        # Per-request override (item 102) -- None preserves today's exact
        # behavior (the deploy-wide default). See ComposeSimulationRequest's
        # own num_nodes field docstring for why this is safe to read directly
        # off simulation.sim_request with no DB/call-chain threading needed.
        num_nodes = simulation.sim_request.num_nodes or get_settings().ray_num_nodes
        batch_job_id = self._batch.submit_mnp(
            job_name=f"compose-{experiment_id}"[:128],
            job_definition=job_def,
            num_nodes=num_nodes,
            ray_job_cmd=self._compose_command(doc_s3_uri, runner_s3_uri, steps),
            out_s3=data_layout.RayLayout.results_uri(experiment_id),
            out_dir=COMPOSE_OUT_DIR,
            stage_s3=stage_s3,
            stage_dir=stage_dir,
        )
        logger.info("Submitted compose Ray job %s (experiment=%s)", batch_job_id, experiment_id)

        if self._after_submit is not None and simulation.sim_request.analysis_options:
            try:
                await self._after_submit(
                    simulation=simulation,
                    experiment_id=experiment_id,
                    sim_job_id=batch_job_id,
                    commit=commit,
                    image=image,
                )
            except Exception:
                # Best-effort by design -- the compose sim job above is ALREADY submitted (and
                # possibly running), so a failure in what FOLLOWS it (for SMS: resolving the
                # analysis leg -- a bad job-def setting, e.g.) must not fail this whole submission
                # and orphan a real, expensive job. A submission failure INSIDE SMS's
                # submit_analysis_dag_node is already caught there and recorded as a FAILED
                # analyses row; this outer guard only catches failures BEFORE that point.
                logger.exception(
                    "Failed to chain the analysis DAG node onto compose sim %s (job %s)",
                    experiment_id,
                    batch_job_id,
                )
        return batch_job_id

    def _submit_in_environment(
        self, environment: str, experiment_id: str, doc_s3_uri: str, runner_s3_uri: str, steps: int
    ) -> str:
        """Run the composite in a REGISTERED environment (the core runtime image), as ONE container.

        Not a smaller copy of the path above -- a different shape, for a different case. A composite
        that needs only what process-bigraph ships needs no simulator: so no commit is resolved, no
        ParCa cache is staged, no Ray cluster is formed, and no analysis is chained (that analysis is
        the simulator's). What stays the same is everything a CLIENT sees: the same runner, the same
        command, the same results prefix, so status and results are read exactly as for any compose
        run. The router has already refused what cannot be combined with this, and a site that
        registers no such environment.
        """
        image = named_environment_image(get_settings(), environment)
        job_def = self._batch.ensure_container_job_def(image, job_definition_key(image))
        batch_job_id = self._batch.submit_container(
            job_name=f"compose-{experiment_id}"[:128],
            job_definition=job_def,
            job_cmd=self._compose_command(doc_s3_uri, runner_s3_uri, steps, workspace_core=False),
            out_s3=data_layout.RayLayout.results_uri(experiment_id),
            out_dir=COMPOSE_OUT_DIR,
            tags={"Project": "compose", "ExperimentId": experiment_id[:255], "Environment": environment},
        )
        logger.info(
            "Submitted compose container job %s (experiment=%s, environment=%s)",
            batch_job_id,
            experiment_id,
            environment,
        )
        return batch_job_id

    @override
    async def build_container(
        self, simulator_version: ComposeSimulatorVersion, random_str: str, db_service: ComposeDatabaseService
    ) -> ComposeHpcRun:
        # Ray uses a prebuilt image (requires_container_build=False), so the dispatch
        # never calls this. Present only to satisfy the ABC.
        raise NotImplementedError("ComposeSimulationServiceRay uses a prebuilt image; no container build.")

    @override
    async def get_job_status(self, job_id_ext: str) -> ComposeJobStatus | None:
        # A compose job id is always a Batch job id, so the only branch of the simulation
        # service's ``get_job_status`` compose ever took was "describe the job, map its state".
        status = self._batch.get_batch_job_statuses([job_id_ext]).get(job_id_ext)
        if status is None:
            logger.warning("No Batch job found with id %s", job_id_ext)
            return None
        return _JOBSTATUS_TO_COMPOSE.get(status, ComposeJobStatus.UNKNOWN)
