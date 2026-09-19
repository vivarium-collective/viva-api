"""The Ray service's Batch layer: how THIS service reaches the engine in viva_core.

Carved out of simulation_service_ray.py (docs/plan-core.md P2.1, cut 3) as a pure
move -- every method below is byte-for-byte what it was in SimulationServiceRay. It is
the base class the service's mixins share: each of them (tasks, build, ParCa, analysis, the
four dispatch mechanisms) submits through _submit_container / _submit_mnp and
resolves an image through _image_uri, so those have to live somewhere a mixin can
inherit from rather than in the class that inherits the mixins.

What is here is SMS's half of the Batch seam, not the engine: read settings through
_seams, pick the queue, append this application's entries to the env, then hand over to
viva_core.backends.batch.BatchJobClient.
"""

import logging
import random
import string
from typing import Any

from viva_api.common.models import JobStatus
from viva_api.common.storage import data_layout
from viva_api.simulation.ray import _seams
from viva_api.simulation.ray.image_paths import REPORT_PATH
from viva_api.simulation.simulation_service import SimulationService
from viva_core.backends.batch import BatchJobClient, BatchJobDetail, ecr_image_uri, stage_out_env

logger = logging.getLogger(__name__)


def _rand_suffix() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


class RayBatchLayer(SimulationService):
    """Abstract: it implements none of SimulationService's interface, only what the
    implementations of it are built from."""

    def _batch(self) -> Any:
        return _seams.boto3.client("batch", region_name=_seams.get_settings().batch_region)

    def _batch_jobs(self) -> BatchJobClient:
        """The Batch engine (``viva_core.backends.batch``), composed, not inherited.

        Built per call and handed ``self._batch`` LATE (the lambda), so a test that swaps
        ``service._batch`` -- or patches the seam under it -- is what the engine gets. The
        engine takes no settings; every method below reads them here, through the seam,
        and passes values in.
        """
        return BatchJobClient(lambda: self._batch())

    def _results_s3_uri(self, experiment_id: str) -> str:
        return data_layout.RayLayout.results_uri(experiment_id)

    def _image_uri(self, commit: str) -> str:
        """The TRUE commit image for a run: <account>.dkr.ecr.<region>/v2ecoli:<commit>."""
        settings = _seams.get_settings()
        return ecr_image_uri(
            account_id=settings.ecr_account_id,
            region=settings.batch_region,
            repository=settings.ray_ecr_repository,
            tag=commit,
        )

    def _ensure_mnp_job_def(self, image: str, commit: str) -> str:
        """Return an MNP job definition (name:revision) whose image is the commit's image.

        Batch MNP can't override the image per-submission, so — symmetric with how K8s
        sets the image per-Job — we derive a per-commit job-def revision: describe the
        CDK base job def (``ray_mnp_job_definition``: roles, resources, shm, log config,
        node count), swap ONLY every node range's container image to ``image``, and
        register it as ``<base>-<commit>``. An existing active revision already pointing
        at this image is reused, so resubmits don't churn revisions.
        """
        return self._batch_jobs().ensure_mnp_job_definition(
            base_definition=_seams.get_settings().ray_mnp_job_definition, image=image, suffix=commit
        )

    def _submit_mnp(
        self,
        *,
        job_name: str,
        job_definition: str,
        num_nodes: int,
        ray_job_cmd: str,
        out_s3: str,
        out_dir: str,
        stage_s3: str | None = None,
        stage_dir: str | None = None,
        depends_on: list[str] | None = None,
        depends_type: str | None = "SEQUENTIAL",
        tags: dict[str, str] | None = None,
        retry_strategy: dict[str, Any] | None = None,
        batch_client: Any = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | list[str] | None = None,
        require_clean_chain: bool = False,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
    ) -> str:
        """Submit a Ray MNP job via boto3, mirroring sms-cdk scripts/ray_batch_submit.sh.

        Env targeting matters: the entrypoint runs ``stage_inputs`` and the periodic
        output sync on EVERY node, so the staging/output/log knobs must reach all
        nodes — the workers need the ParCa cache to run seeds and must ship their own
        zarr to S3. Only ``RAY_JOB_CMD`` (the driver) and ``RAY_REPORT_PATH`` are
        head-only. So the shared env goes on node 0 (``0:0``) and, when there are
        workers, also on the worker range (``1:``). Returns the AWS Batch job id.

        ``depends_type`` selects the ``dependsOn`` shape. The default keeps the
        long-standing ParCa→sim edge byte-identical (``{"jobId": …, "type":
        "SEQUENTIAL"}``, live-verified). Pass ``None`` for a plain ``{"jobId": …}``
        wait — required when the DEPENDENCY is an Array job, whose parent id AWS
        Batch will not accept under a SEQUENTIAL type (real API rejection, hit live
        2026-08-06; see ``_submit_array``).

        ``retry_strategy``, passed through verbatim as ``SubmitJob.retryStrategy``,
        overrides whatever the job definition itself declares (per the real AWS
        Batch API — confirmed this session) — used by the per-seed chain-dispatch
        path (backlog item 33) to restore per-job retry on the MNP job definition,
        which (unlike the Array job definition) declares none of its own; omitted
        (``None``) everywhere else, unchanged from existing behavior.

        ``batch_client``, when given, is used INSTEAD of ``self._batch()`` for this
        one call — lets a caller submitting many jobs in a tight loop (chain
        dispatch) supply its own retry-configured client without changing what
        every other existing call site in this class gets from the shared
        ``self._batch()`` factory.
        """
        settings = _seams.get_settings()
        # Per-node knobs every node acts on (stage cache in, sync results out, ship logs).
        shared_env = self._stage_out_env(
            prefix="RAY",
            out_dir=out_dir,
            out_s3=out_s3,
            stage_s3=stage_s3,
            stage_dir=stage_dir,
            log_s3_prefix=settings.ray_log_s3_prefix,
            expect_new_genes=expect_new_genes,
            expect_bundle_overrides=expect_bundle_overrides,
            require_clean_chain=require_clean_chain,
            lineage_debug_division=lineage_debug_division,
        )
        # Backlog item 65: a standalone (numNodes=1) submission has no inter-node
        # traffic to protect, so it gains nothing from ray_mnp_queue's cluster-
        # placement-group compute environment and pays its full concurrency cost
        # for nothing -- route it to the dedicated no-placement-group queue
        # instead, when one is configured. Automatic and transparent to every
        # caller (chain-dispatch, ParCa, compose): both already pass their real
        # num_nodes here, no call-site changes needed. Falls back to
        # ray_mnp_queue unchanged for a genuine multi-node request (num_nodes >
        # 1, e.g. colony sims) or when ray_mnp_standalone_queue isn't set yet.
        job_queue = (
            settings.ray_mnp_standalone_queue
            if num_nodes == 1 and settings.ray_mnp_standalone_queue
            else settings.ray_mnp_queue
        )
        # The engine adds RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE and task_env
        # (sms-ecoli#166: the request's own env, validated at the boundary by
        # dispatch_validation.validate_task_env, reaching EVERY node -- e.g.
        # V2ECOLI_SKIP_CACHE_VERIFY=1 after a cache-re-keying v2ecoli commit),
        # composes the head env and the single "0:" node override, and submits.
        return self._batch_jobs().submit_mnp(
            job_name=job_name,
            job_queue=job_queue,
            job_definition=job_definition,
            num_nodes=num_nodes,
            job_cmd=ray_job_cmd,
            report_path=REPORT_PATH,
            shared_env=shared_env,
            task_env=task_env,
            depends_on=depends_on,
            depends_type=depends_type,
            tags=tags,
            retry_strategy=retry_strategy,
            client=batch_client,
        )

    def _stage_out_env(
        self,
        *,
        prefix: str,
        out_dir: str,
        out_s3: str,
        stage_s3: str | None = None,
        stage_dir: str | None = None,
        log_s3_prefix: str | None = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | list[str] | None = None,
        require_clean_chain: bool = False,
        lineage_debug_division: bool = False,
    ) -> list[dict[str, str]]:
        """Shared stage/output/log env-var construction for both the MNP (``RAY_*``)
        and container (``CONTAINER_*``) submission paths (backlog item 71) -- same
        conditional logic (only emit STAGE_*/LOG_S3_PREFIX when configured), a
        different env-var prefix per job shape, since each entrypoint script only
        reads its own prefix -- the values can't literally share one env list.

        ``expect_new_genes``/``expect_bundle_overrides`` (sms-ecoli#210 / #215): the
        STRAIN this run requested. The entrypoint's ``stage_inputs`` already runs
        ``verify_cache_version`` (schema + source-hash) on the staged cache; these
        let it ALSO reject a WRONG-STRAIN cache (P1-6). Emitted as
        ``{prefix}_EXPECT_NEW_GENES`` / ``{prefix}_EXPECT_BUNDLE_OVERRIDES`` only for
        a real strain -- ``off``/empty is wild-type and emits nothing, so a
        wild-type run is byte-identical to before and the entrypoint check stays
        inert until a real strain is requested.

        ``expect_bundle_overrides`` accepts a list (backlog items 93/104/106,
        ``ParcaOptions.bundle_overrides`` accepts a list as of #486, for a strain
        recipe that stacks multiple ``--bundle-overrides`` files) -- joined with
        ``","`` for the single env var, same normalization ``strain_from_config``'s
        own ``_norm`` helper already applies for the job-scheduler verification
        path. Real, confirmed gap this closes: a caller reaching this helper
        DIRECTLY with a list (e.g. via ``getattr(config.parca_options,
        "bundle_overrides", None)``, not through ``strain_from_config``) crashed
        with ``AttributeError: 'list' object has no attribute 'strip'`` -- caught
        live firing a real K4/J3 chassis rebuild whose recipe genuinely needs two
        stacked override files.

        ``require_clean_chain`` (item 106/#166 chassis-provenance thread, v2ecoli#735):
        emitted verbatim as ``V2E_REQUIRE_CLEAN_CHAIN`` -- UNPREFIXED, unlike every
        other var this helper emits -- because it is read directly by v2ecoli's own
        ``os.environ.get("V2E_REQUIRE_CLEAN_CHAIN")`` (``save_sim_input``/
        ``save_cache``/``verify_cache_version``), not by the ``RAY_*``/``CONTAINER_*``
        entrypoint scripts this helper otherwise targets. Default ``False`` emits
        nothing -- byte-identical to before this param existed -- because most
        existing callers (``new_gene_cache``, ``variant_cache``,
        ``build_condition_cache``, ``run_comparison_ensemble``) don't pass
        ``sources=`` yet (that wiring is v2ecoli's own PR 3); setting this
        unconditionally would hard-fail every one of them the moment v2ecoli#735
        lands, including Run 4's own already-built new-gene caches.

        ``lineage_debug_division`` (item 106/#210, v2ecoli#733): emitted verbatim as
        ``LINEAGE_DEBUG_DIVISION`` -- UNPREFIXED, same reasoning as
        ``require_clean_chain`` above -- v2ecoli's own
        ``LineageProcess._run_until_division`` reads it directly via
        ``os.environ.get``. Opt-in diagnostic only; default ``False`` emits nothing.
        """
        # The generic half of the contract (OUT_*, STAGE_*, LOG_S3_PREFIX) is core's;
        # everything appended below is this application telling ITS entrypoint more.
        env = stage_out_env(
            prefix=prefix,
            out_dir=out_dir,
            out_s3=out_s3,
            stage_s3=stage_s3,
            stage_dir=stage_dir,
            log_s3_prefix=log_s3_prefix,
        )
        # off/empty is wild-type -> no expectation to assert (matches the parca-side
        # normalization in build_cache.py and _parca_command's own flag guard).
        ng = (expect_new_genes or "").strip()
        if ng and ng != "off":
            env.append({"name": f"{prefix}_EXPECT_NEW_GENES", "value": ng})
        bo_raw = (
            ",".join(expect_bundle_overrides) if isinstance(expect_bundle_overrides, list) else expect_bundle_overrides
        )
        bo = (bo_raw or "").strip()
        if bo and bo != "off":
            env.append({"name": f"{prefix}_EXPECT_BUNDLE_OVERRIDES", "value": bo})
        if require_clean_chain:
            env.append({"name": "V2E_REQUIRE_CLEAN_CHAIN", "value": "1"})
        if lineage_debug_division:
            env.append({"name": "LINEAGE_DEBUG_DIVISION", "value": "1"})
        return env

    def _ensure_container_job_def(self, image: str, commit: str) -> str:
        """Return a container job definition (name:revision) whose image is the commit's image.

        Mirrors ``_ensure_mnp_job_def`` exactly, for the plain (non-MNP, non-array)
        standalone container job shape (backlog item 71 -- ParCa, the analysis DAG
        node, and eventually chain-dispatch's per-seed-per-generation jobs, none of
        which have any real inter-node traffic to protect). Plain container jobs
        can't override the image at submission time either -- same limitation as
        MNP -- so a per-commit job-def revision is derived the same way: describe
        the CDK base container job def (``ray_container_job_definition``: roles,
        resources, retry strategy, log config -- provisioned by sms-cdk's
        RayContainerJobDef), swap ONLY its image, and register it as
        ``<base>-<commit>``. An existing active revision already pointing at this
        image is reused, so resubmits don't churn revisions.
        """
        settings = _seams.get_settings()
        if not settings.ray_container_job_definition:
            # Matches this file's own compose_ray_image_tag precedent: fail loud with
            # the setting name rather than submit a doomed job with a blank job-def.
            raise RuntimeError("ray_container_job_definition is not set; cannot submit a container-type Batch job.")
        return self._batch_jobs().ensure_container_job_definition(
            base_definition=settings.ray_container_job_definition, image=image, suffix=commit
        )

    def _submit_container(
        self,
        *,
        job_name: str,
        job_definition: str,
        job_cmd: str,
        out_s3: str,
        out_dir: str,
        stage_s3: str | None = None,
        stage_dir: str | None = None,
        depends_on: list[str] | None = None,
        depends_type: str | None = "SEQUENTIAL",
        tags: dict[str, str] | None = None,
        retry_strategy: dict[str, Any] | None = None,
        batch_client: Any = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | list[str] | None = None,
        require_clean_chain: bool = False,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
        memory_class: str = "standard",
    ) -> str:
        """Submit a plain, standalone AWS Batch container-type job (backlog item 71).

        Sibling of ``_submit_mnp`` for the non-MNP, non-array job shape -- currently
        ParCa (``submit_parca_job``) and the analysis DAG node
        (``_submit_analysis_job``), both already ``num_nodes=1`` MNP jobs with no
        real inter-node traffic; chain-dispatch's per-seed-per-generation jobs
        migrate here too in a later phase. One task, one container: no node
        overrides, no head/worker split -- every env var goes in a single
        ``containerOverrides.environment`` list, matching
        ``docker/batch-container-entrypoint.sh``'s ``CONTAINER_*`` contract exactly
        (sms-ecoli). Returns the AWS Batch job id.

        Do NOT modify ``_submit_mnp`` -- this is a parallel path, not a
        replacement; genuinely multi-node Ray paths keep submitting through
        ``_submit_mnp`` unchanged.
        """
        settings = _seams.get_settings()
        if not settings.ray_container_queue:
            raise RuntimeError("ray_container_queue is not set; cannot submit a container-type Batch job.")

        # Memory-class routing (viva-api#625): a "large" job goes to the
        # large-memory (200 GB r7i) queue when one is provisioned; otherwise it
        # falls back to the standard queue (same convention as
        # ray_mnp_standalone_queue), so behaviour is unchanged until sms-cdk sets
        # ray_container_large_queue. Require a real non-empty string so a settings
        # double's auto-attribute can't accidentally route.
        job_queue = settings.ray_container_queue
        large_queue = getattr(settings, "ray_container_large_queue", "")
        if memory_class == "large" and isinstance(large_queue, str) and large_queue.strip():
            job_queue = large_queue
            logger.info("Container job %s: memory_class=large -> large-memory queue %s", job_name, job_queue)
        elif memory_class == "large":
            logger.info(
                "Container job %s: memory_class=large but no ray_container_large_queue set; using standard queue %s",
                job_name,
                job_queue,
            )

        # task_env (sms-ecoli#166): see _submit_mnp -- same passthrough, one container.
        return self._batch_jobs().submit_container(
            job_name=job_name,
            job_queue=job_queue,
            job_definition=job_definition,
            job_cmd=job_cmd,
            report_path=REPORT_PATH,
            stage_env=self._stage_out_env(
                prefix="CONTAINER",
                out_dir=out_dir,
                out_s3=out_s3,
                stage_s3=stage_s3,
                stage_dir=stage_dir,
                log_s3_prefix=settings.ray_log_s3_prefix,
                expect_new_genes=expect_new_genes,
                expect_bundle_overrides=expect_bundle_overrides,
                require_clean_chain=require_clean_chain,
                lineage_debug_division=lineage_debug_division,
            ),
            task_env=task_env,
            depends_on=depends_on,
            depends_type=depends_type,
            tags=tags,
            retry_strategy=retry_strategy,
            client=batch_client,
        )

    def _resolve_log_group(self, job_definition: str | None) -> str | None:
        """The CloudWatch log group a container job writes to: the configured
        ``ray_batch_log_group`` if set, else the awslogs-group from the job
        definition's logConfiguration. None when neither is available."""
        configured = _seams.get_settings().ray_batch_log_group
        if configured:
            return configured
        if not job_definition:
            return None
        return self._batch_jobs().job_definition_log_group(job_definition)

    def get_batch_job_statuses(self, job_ids: list[str]) -> dict[str, JobStatus]:
        """Batched ``describe_jobs`` status lookup for arbitrary AWS Batch job
        ids, chunked by ``DESCRIBE_JOBS_MAX_BATCH`` (100/call, the real API
        limit). An id absent from the response (not yet visible — brief
        eventual-consistency lag right after submission, or simply unknown) is
        simply absent from the returned mapping rather than raising; callers
        should treat a missing id as not-yet-terminal, the same discipline
        ``get_chain_campaign_result`` already established (and now reuses this
        exact helper for). Shared by that method and
        ``JobScheduler._advance_chain_campaign``'s per-seed poll (backlog item
        71 Phase 4), which needs the same batching for a campaign's
        ``chain_current_job_ids`` on every tick.
        """
        return self._batch_jobs().job_statuses(job_ids)

    def get_batch_job_details(self, job_ids: list[str]) -> dict[str, BatchJobDetail]:
        """``get_batch_job_statuses`` plus what a failed job SAID: Batch's
        ``statusReason``, the container exit code and the attempt count. Used
        where a bare job id is not an answer -- a chain campaign's failed seeds
        (observability plan D4c). Same chunking, same missing-id semantics."""
        return self._batch_jobs().job_details(job_ids)
