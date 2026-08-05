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
import tempfile
from pathlib import Path
from typing import override

from sms_api.common.models import JobBackend, JobStatus
from sms_api.common.storage import data_layout
from sms_api.common.storage.file_paths import S3FilePath
from sms_api.compose.database_service import ComposeDatabaseService
from sms_api.compose.models import ComposeHpcRun, ComposeJobStatus, ComposeSimulation, ComposeSimulatorVersion
from sms_api.compose.simulation_service import ComposeSimulationService
from sms_api.config import get_settings

logger = logging.getLogger(__name__)

# The embedded generic runner (read once at import). Same source SLURM embeds.
_RUNNER_SRC = (_res.files("sms_api.compose") / "run_pbg.py").read_text()

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


class ComposeSimulationServiceRay(ComposeSimulationService):
    """Submit generic compose documents to the existing Ray-on-Batch MNP queue."""

    backend = JobBackend.RAY
    requires_container_build = False  # prebuilt workspace image; no per-run singularity build

    def __init__(self) -> None:
        # Reuse the vEcoli Ray service's Batch/job-def/submit plumbing verbatim.
        from sms_api.simulation.simulation_service_ray import SimulationServiceRay

        self._ray = SimulationServiceRay()

    def _image_uri(self) -> str:
        settings = get_settings()
        if not settings.compose_ray_image_tag:
            # Fail here, at submit, with the setting name — not 10 minutes later as an
            # opaque Batch image-pull failure. The tag is the workspace commit and has
            # no safe default (see config.compose_ray_image_tag).
            raise RuntimeError(
                "compose_ray_image_tag is unset; set COMPOSE_RAY_IMAGE_TAG to the workspace "
                f"commit to run compose jobs on {settings.ray_ecr_repository}."
            )
        registry = f"{settings.ecr_account_id}.dkr.ecr.{settings.batch_region}.amazonaws.com"
        return f"{registry}/{settings.ray_ecr_repository}:{settings.compose_ray_image_tag}"

    def _compose_command(self, doc_s3_uri: str, runner_s3_uri: str, steps: int) -> str:
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
        core_builder = get_settings().compose_pbg_core_builder
        env = f"PBG_RESULTS_DIR={COMPOSE_OUT_DIR}"
        if core_builder:
            env += f" PBG_CORE_BUILDER={core_builder}"
        return (
            f"mkdir -p {COMPOSE_OUT_DIR}"
            f" && aws s3 cp {doc_s3_uri} {COMPOSE_DOC_PATH}"
            f" && aws s3 cp {runner_s3_uri} {COMPOSE_RUNNER_PATH}"
            f" && {env} python {COMPOSE_RUNNER_PATH}"
            f" {COMPOSE_DOC_PATH} -o {COMPOSE_OUT_DIR} -n {steps}"
        )

    def _parca_staging(self) -> tuple[str | None, str | None]:
        """(stage_s3, stage_dir) for the commit-keyed ParCa cache, or (None, None).

        The ensemble path stages this cache by passing these same two args to
        ``_submit_mnp`` (``simulation_service_ray.py``, sim submit) — the entrypoint
        turns them into RAY_STAGE_S3/RAY_STAGE_DIR and syncs S3 → local on every node
        before the job command runs. The compose driver-swap replaced the command but
        must keep the staging, or a composite whose ``cache_dir`` expects a populated
        ParCa bundle (v2ecoli's ``baseline``) starts against an empty directory.

        Keyed by the image tag because that IS the workspace commit, and the ParCa
        cache is commit-addressed. Disabled when no cache dir is configured.
        """
        settings = get_settings()
        if not settings.compose_parca_cache_dir:
            return None, None
        return (
            data_layout.RayLayout.parca_cache_uri(settings.compose_ray_image_tag),
            settings.compose_parca_cache_dir,
        )

    @override
    async def submit_simulation_job(
        self, simulation: ComposeSimulation, experiment_id: str, override_command: str | None = None
    ) -> str:
        from sms_api.dependencies import get_file_service

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
        image = self._image_uri()
        # `_ensure_mnp_job_def` keys the derived revision by a tag string — reuse the
        # image tag as that key so resubmits with the same image reuse the revision.
        job_def = self._ray._ensure_mnp_job_def(image, get_settings().compose_ray_image_tag)
        stage_s3, stage_dir = self._parca_staging()
        batch_job_id = self._ray._submit_mnp(
            job_name=f"compose-{experiment_id}"[:128],
            job_definition=job_def,
            num_nodes=get_settings().ray_num_nodes,
            ray_job_cmd=self._compose_command(doc_s3_uri, runner_s3_uri, steps),
            out_s3=data_layout.RayLayout.results_uri(experiment_id),
            out_dir=COMPOSE_OUT_DIR,
            stage_s3=stage_s3,
            stage_dir=stage_dir,
        )
        logger.info("Submitted compose Ray job %s (experiment=%s)", batch_job_id, experiment_id)
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
        from sms_api.common.models import JobId

        info = await self._ray.get_job_status(JobId.ray(job_id_ext))
        if info is None:
            return None
        return _JOBSTATUS_TO_COMPOSE.get(info.status, ComposeJobStatus.UNKNOWN)
