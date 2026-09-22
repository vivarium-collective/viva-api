"""AWS Batch multi-node-parallel (MNP) Ray implementation of SimulationService.

The v2ecoli whole-cell sim runs distributed on a *transient* Ray cluster: one
Batch MNP job gang-schedules N nodes, the Ray-on-Batch entrypoint
(``ray-batch-entrypoint.sh``, bundled in the v2ecoli image) forms the Ray
cluster, stages a ParCa cache from S3, exports ``RAY_ADDRESS``, and runs
``RAY_JOB_CMD`` (the v2ecoli ensemble) on the head — no Nextflow. This service
submits those MNP jobs.

Data flow (no shared filesystem):
  - ParCa runs first as its own 1-node MNP job; its cache is captured to a
    deterministic S3 URI (``RAY_OUT_S3``).
  - The simulation MNP job ``dependsOn`` the ParCa job (Batch gates it until
    ParCa SUCCEEDED), stages that cache (``RAY_STAGE_S3``), runs the ensemble,
    and captures the zarr/summary outputs to S3 (``RAY_OUT_S3``).
  - For the multi-generation batch_baseline sweep, an ANALYSIS job ``dependsOn``
    the simulation job and runs the ported cd1_*/ptools_* analyses over the
    landed S3 sweep. The whole pipeline is therefore one Batch dependency DAG
    (parca -> sim -> analysis); nothing external has to notice a completion and
    react to it. See ``_analysis_command`` for why this is a third DAG node and
    not the composite's own inline flush.

The image is the **workload-owned**, self-contained ``v2ecoli:<sha>`` (bundles
the AWS CLI + the Ray entrypoint), built by ``submit_build_image_job`` via a DooD
Batch job — symmetric with how ``SimulationServiceK8s`` builds ``vecoli:{commit}``.
Each run uses its simulator's TRUE commit image: since Batch MNP can't override the
image per submission, we derive a per-commit MNP job-def revision from the sms-cdk
base (cloning its node properties, swapping the image to ``v2ecoli:<commit>``).
"""

import logging
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, override

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.hpc.k8s_job_service import K8sJobService
from viva_api.common.hpc.local_task_service import LocalTaskService
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.common.simulator_defaults import DEFAULT_BRANCH, DEFAULT_REPO
from viva_api.common.storage import data_layout
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.compose.handlers import hooks_source
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.dispatch import _seams, parca_spec
from viva_api.simulation.dispatch.batch_layer import BatchLayer
from viva_api.simulation.dispatch.build import ImageBuilder
from viva_api.simulation.dispatch.chain import ChainStrategy, chain_base_tags
from viva_api.simulation.dispatch.ensemble import EnsembleStrategy
from viva_api.simulation.dispatch.mbp_tracked import MbpTrackedStrategy
from viva_api.simulation.dispatch.multi_node import MultiNodeCompositeStrategy
from viva_api.simulation.dispatch.nextflow import NextflowStrategy
from viva_api.simulation.dispatch.parca import ParcaService
from viva_api.simulation.dispatch.tasks import TaskService
from viva_api.simulation.github_repo import (
    fetch_config_template,
    fetch_latest_commit_hash,
    fetch_repo_discovery,
)
from viva_api.simulation.models import (
    HpcRun,
    ParcaDataset,
    RepoDiscovery,
    Simulation,
    SimulatorVersion,
)
from viva_api.simulation.simulation_service import SimulationService
from viva_core.backends.batch import (
    BatchJobDetail,
    batch_exit_code,
)
from viva_core.compose.runner_files import HOOKS_FILENAME, RUNNER_FILENAME, runner_source

if TYPE_CHECKING:
    # ``types-boto3`` is a dev dependency (annotations only): never imported at runtime.
    pass

logger = logging.getLogger(__name__)

# The generic runner every Ray-Batch job (ensemble or compose) executes. Read once as a
# resource (same source viva_api.compose.simulation_service_ray stages for compose jobs) so
# the multi-generation batch path below dispatches through the identical mechanism instead
# of a v2ecoli-specific CLI script — see backlog items 26/27.

# The SubmitJob pacer, its 50-TPS rationale and the DescribeJobs chunk size moved to
# ``viva_core.backends.batch`` with the rest of the Batch engine (docs/plan-core.md P2.1,
# cut 2). A canonical 1000-seed x 10-generation chain campaign submits N*G=10,000
# individual per-seed-per-generation jobs upfront (see ``submit_chain_dispatch_job``),
# which is why that loop is the pacer's main customer.


@dataclass
class ChainCampaignPollResult:
    """A chain-dispatch campaign's analysis-fan-in poll outcome — backlog item 33.

    ``terminal`` means every one of the campaign's tracked final-generation job
    ids (``HpcRun.chain_final_job_ids`` — one per seed, each seed's own LAST
    successfully-submitted generation job) has reached a Batch-terminal state
    (SUCCEEDED or FAILED); a campaign with any tracked job still
    SUBMITTED/PENDING/RUNNABLE/STARTING/RUNNING is NOT terminal and must be
    polled again next interval. A job id that hasn't shown up in ``describe_jobs``
    yet (e.g. brief eventual-consistency lag right after submission) is treated
    as not-yet-terminal, not an error — the caller just polls again.

    Unlike the per-generation-array design this superseded, there is no local
    array position to remap: each tracked id is already a real, independent AWS
    Batch job id (one seed's final chain link), so ``succeeded_job_ids`` /
    ``failed_job_ids`` are real job ids directly, usable as-is.
    """

    terminal: bool
    succeeded_job_ids: list[str] = field(default_factory=list)
    failed_job_ids: list[str] = field(default_factory=list)


class SimulationServiceRay(SimulationService):
    """Ray-on-Batch (MNP) implementation of SimulationService."""

    def __init__(
        self,
        local_task_service: LocalTaskService | None = None,
        k8s_job_service: "K8sJobService | None" = None,
        batch: BatchLayer | None = None,
    ) -> None:
        self._local = local_task_service or LocalTaskService()
        # Only the Nextflow dispatch uses this: its HEAD runs as a K8s Job so it
        # inherits the `batch-submit` ServiceAccount's IRSA identity. Every other
        # path here submits to Batch directly and needs no cluster access.
        self._k8s = k8s_job_service
        # The Batch layer, COMPOSED (it was this class's base until P2.1 PR 5). One instance
        # for the service's lifetime: everything that submits -- this class, the ParCa and
        # task services, soon the dispatch strategies -- is handed THIS object, so a test
        # that patches ``service.batch.submit_container`` is what all of them get.
        self.batch = batch or BatchLayer()

    def get_batch_job_statuses(self, job_ids: list[str]) -> dict[str, JobStatus]:
        """``self.batch.get_batch_job_statuses``, kept on the service because the scheduler
        asks the SERVICE for it (that ends in P6, with the scheduler's split)."""
        return self.batch.get_batch_job_statuses(job_ids)

    def get_batch_job_details(self, job_ids: list[str]) -> dict[str, BatchJobDetail]:
        """``self.batch.get_batch_job_details`` -- the scheduler and the chain-progress handler
        ask the SERVICE for it; see ``get_batch_job_statuses``."""
        return self.batch.get_batch_job_details(job_ids)

    async def stage_runner(self, experiment_id: str) -> str:
        """Upload the generic run_pbg.py runner (and SMS's hooks beside it) to S3 for this experiment; return its URI.

        Mirrors ``viva_api.compose.simulation_service_ray.ComposeSimulationServiceRay``'s
        own runner staging exactly (same source, same per-experiment S3 layout) -- the
        multi-generation batch path below downloads and runs it the identical way a
        compose job does. Staged (not embedded via heredoc) because AWS Batch caps a
        container override command at 8192 bytes.

        The returned URI is fully DETERMINISTIC from ``experiment_id`` alone (the S3
        key has no random component) — safe, cheap, and idempotent to call again
        rather than cache: ``JobScheduler``'s per-tick chain-dispatch advance
        (backlog item 71 Phase 4) calls this fresh whenever it's about to submit a
        generation, rather than persisting the URI anywhere.
        """
        from viva_api.dependencies import get_file_service

        file_service = get_file_service()
        if file_service is None:
            raise RuntimeError("FileService not initialized; cannot stage run_pbg.py to S3.")
        exp_prefix = data_layout.RayLayout.experiment_prefix(experiment_id)
        runner_key = f"{exp_prefix}/{RUNNER_FILENAME}"
        # Core's generic runner, and beside it SMS's hooks -- what its model needs of the runner
        # (P3d-4c-2). ``stage_runner_commands`` copies both into the job; ``PBG_RUNNER_ENV`` names them.
        for key, text in ((runner_key, runner_source()), (f"{exp_prefix}/{HOOKS_FILENAME}", hooks_source())):
            with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
                tmp.write(text)
                local = tmp.name
            try:
                await file_service.upload_file(Path(local), S3FilePath(s3_path=Path(key)))
            finally:
                Path(local).unlink(missing_ok=True)
        return data_layout.s3_uri(runner_key)

    @property
    def parca(self) -> ParcaService:
        """The ParCa cache jobs (a commit's cache, a new-gene cache, a variant cache), composed:
        handed this service as the thing that dispatches container jobs for them. Built per
        access; it holds no state of its own."""
        return ParcaService(self.batch)

    @override
    async def submit_parca_job(self, parca_dataset: ParcaDataset) -> JobId:
        """``SimulationService``'s contract; the work is ``ParcaService``'s."""
        return await self.parca.submit_parca_job(parca_dataset)

    def cache_s3_uri(self, commit: str, *, variant: str | None = None) -> str:
        """Where a commit's ParCa cache lives -- ``parca_spec.cache_s3_uri``, kept on the
        service because the scheduler and the handlers ask the SERVICE for it."""
        return parca_spec.cache_s3_uri(commit, variant=variant)

    @property
    def tasks(self) -> TaskService:
        """The task service (``POST /api/v1/tasks`` and friends), composed: it is handed this
        service as the thing that dispatches container jobs for it. Built per access; it
        holds no state of its own."""
        return TaskService(
            self.batch,
            latest_commit=lambda: self.get_latest_commit_hash(),
            results_uri=data_layout.RayLayout.results_uri,
        )

    def _nextflow(self) -> NextflowStrategy:
        """The Nextflow dispatch mechanism, as a strategy. Built per call around ``self._k8s`` and a
        late-bound ``stage_runner``, so a test (or anything else) that swaps either is what it gets."""
        return NextflowStrategy(
            self.batch, self._k8s, stage_runner=lambda experiment_id: self.stage_runner(experiment_id)
        )

    async def reap_cancelled_campaign(self, head_job_name: str) -> int | None:
        """``NextflowStrategy.reap_cancelled_campaign``, kept on the service because the scheduler
        asks the SERVICE for it (that ends in P6, with the scheduler's split)."""
        return await self._nextflow().reap_cancelled_campaign(head_job_name)

    def _chain(self) -> ChainStrategy:
        """The chain dispatch mechanism, as a strategy handed this service's Batch layer and its in-process
        task service. Built per call around ``self._local``, so a swap of either is what it gets."""
        return ChainStrategy(self.batch, self._local)

    # -- What the scheduler, the capability probe and direct callers still ask the SERVICE for. Each is
    #    one call, with the strategy's own signature spelled out so the scheduler's calls stay
    #    type-checked; each goes when the scheduler is split (P6). ``test_ray_seams`` pins the set.

    def chain_base_tags(self, *, simulation: Simulation, commit: str) -> dict[str, str]:
        return chain_base_tags(simulation=simulation, commit=commit)

    async def submit_chain_dispatch_job(
        self, ecoli_simulation: Simulation, database_service: DatabaseService, correlation_id: str | None = None
    ) -> JobId:
        return await self._chain().submit_chain_dispatch_job(ecoli_simulation, database_service, correlation_id)

    async def submit_chain_lineage_batch(
        self,
        *,
        seeds: list[int],
        n_generations: int,
        experiment_id: str,
        commit: str,
        cache_s3: str,
        runner_s3_uri: str,
        tags: dict[str, str],
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
        exchange_flux_basis: str | None = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | None = None,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
    ) -> dict[int, str]:
        return await self._chain().submit_chain_lineage_batch(
            seeds=seeds,
            n_generations=n_generations,
            experiment_id=experiment_id,
            commit=commit,
            cache_s3=cache_s3,
            runner_s3_uri=runner_s3_uri,
            tags=tags,
            injected_processes=injected_processes,
            variants=variants,
            composite_id=composite_id,
            exchange_fluxes=exchange_fluxes,
            exchange_flux_basis=exchange_flux_basis,
            expect_new_genes=expect_new_genes,
            expect_bundle_overrides=expect_bundle_overrides,
            lineage_debug_division=lineage_debug_division,
            task_env=task_env,
        )

    async def submit_campaign_analysis(
        self,
        *,
        simulation: Simulation,
        database_service: DatabaseService,
        commit: str,
        total_n_seeds: int,
        n_generations: int,
        correlation_id: str | None = None,
    ) -> str | None:
        return await self._chain().submit_campaign_analysis(
            simulation=simulation,
            database_service=database_service,
            commit=commit,
            total_n_seeds=total_n_seeds,
            n_generations=n_generations,
            correlation_id=correlation_id,
        )

    def _ensemble(self) -> EnsembleStrategy:
        """The ensemble dispatch mechanism (ParCa MNP job, then the simulation MNP job), as a strategy
        handed this service's Batch layer and a late-bound ``stage_runner``. Built per call."""
        return EnsembleStrategy(self.batch, stage_runner=lambda experiment_id: self.stage_runner(experiment_id))

    def _multi_node(self) -> MultiNodeCompositeStrategy:
        """The multi-node composite dispatch mechanism, as a strategy handed this service's Batch layer
        and a late-bound ``stage_runner``. Built per call; it holds no state of its own."""
        return MultiNodeCompositeStrategy(
            self.batch, stage_runner=lambda experiment_id: self.stage_runner(experiment_id)
        )

    async def submit_multi_node_analysis(
        self,
        *,
        simulation: Simulation,
        database_service: DatabaseService,
        commit: str,
        composite_id: str,
    ) -> str | None:
        """``MultiNodeCompositeStrategy.submit_analysis``, kept on the service because the scheduler
        asks the SERVICE for it (that ends in P6, with the scheduler's split)."""
        return await self._multi_node().submit_analysis(
            simulation=simulation, database_service=database_service, commit=commit, composite_id=composite_id
        )

    def _mbp_tracked(self) -> MbpTrackedStrategy:
        """The mbp-tracked dispatch mechanism, as a strategy handed this service's Batch layer.
        Built per call; it holds no state of its own."""
        return MbpTrackedStrategy(self.batch)

    def _image_builder(self) -> ImageBuilder:
        """The build service, composed. Built per call around ``self._local`` so that a test
        (or anything else) that swaps the local task service is what the builder gets --
        the same late binding as ``_batch_jobs``."""
        return ImageBuilder(self._local)

    @override
    async def submit_build_image_job(
        self,
        simulator_version: SimulatorVersion,
        *,
        include_new_gene_data: bool = False,
        include_submit_image: bool = False,
        stage_private_fork: bool = False,
        vecoli_private_commit: str | None = None,
    ) -> JobId:
        """Build the simulator image (``ImageBuilder.submit``). The keyword arguments are
        spelled out, not ``**kwargs``: ``handlers.simulators`` inspects this signature to
        decide which of them this backend supports."""
        return await self._image_builder().submit(
            simulator_version,
            include_new_gene_data=include_new_gene_data,
            include_submit_image=include_submit_image,
            stage_private_fork=stage_private_fork,
            vecoli_private_commit=vecoli_private_commit,
        )

    @override
    async def get_latest_commit_hash(
        self,
        git_repo_url: str = DEFAULT_REPO,
        git_branch: str = DEFAULT_BRANCH,
    ) -> str:
        return await fetch_latest_commit_hash(git_repo_url, git_branch, _seams.get_settings().github_token)

    @override
    async def submit_ecoli_simulation_job(
        self, ecoli_simulation: Simulation, database_service: DatabaseService, correlation_id: str
    ) -> JobId:
        """Submit ParCa (1 node) + the simulation ensemble (N nodes), gated by a Batch dependency.

        The tracked job id is the *simulation* job. Batch will not start it until
        the ParCa job SUCCEEDED, so the cache is in S3 before the sim stages it.

        ROUTING (backlog item 33 rework): the canonical batch_baseline sweep
        (``composite`` is None, more than one generation) is delegated ENTIRELY
        to ``submit_chain_dispatch_job`` — individual per-seed AWS Batch job
        chains, a true v2 analogy of vEcoli-private's own fully-asynchronous
        per-seed Nextflow execution (Alex's explicit decision). This check runs
        BEFORE any of the MNP/single-shot setup below, so a canonical request
        never touches that setup, and never needs Array-job machinery at all —
        ``_submit_array``/``_array_sim_command``/``_ensure_array_job_def`` were
        REMOVED as part of this rework once this routing landed made them dead
        code (their only caller was the array branch this replaced; confirmed
        via a fresh repo-wide grep before deleting them, not assumed).

        That delegation runs in the BACKGROUND (via
        ``_submit_chain_dispatch_background``), so this method returns in
        seconds no matter how large the campaign is, and the returned id is a
        ``JobId.local(...)`` rather than the ParCa job's ``JobId.ray(...)``.
        Submitting a campaign's N*G jobs takes minutes of real wall time
        (~15 for the canonical 1000x10 shape) and used to happen inline, inside
        the ``POST /api/v1/simulations`` request — see that method's docstring
        for the real production failure that caused. Progress stays trackable
        through the unchanged ``GET /api/v1/simulations/{id}/status``.
        ``submit_chain_dispatch_job`` itself is untouched and still runs
        synchronously to completion for its direct callers.

        Every OTHER shape still reaches the MNP path below exactly as before:
        the composite-driven two-engine comparison ensemble (genuinely fans out
        via Ray actors, at ANY generation count — chain-dispatch is v2ecoli-only
        and does not apply), and the single-generation phase0 ensemble.
        """
        if database_service is None:
            raise RuntimeError("DatabaseService is not available. Cannot submit Ray simulation job.")

        config = ecoli_simulation.config

        # Backlog item 88: a generic multi-node process-bigraph composite dispatch
        # (e.g. a colony composite distributed across N Ray-cluster nodes) arrives
        # as an extra key (SimulationConfig's extra="allow") rather than a declared
        # field -- checked FIRST, before the chain-dispatch routing below, since a
        # multi-node request may otherwise also satisfy that check's own
        # composite-is-None/generations>1 condition and would silently misroute.
        # A Nextflow dispatch is checked BEFORE multi_node_dispatch for the same
        # reason that one is checked before chain-dispatch: it carries a
        # composite_id too, so a later check would silently claim it first and the
        # request would run on the wrong mechanism while looking like it worked.
        #
        # Deliberately a per-request extra rather than a new ComputeBackend:
        # compute_backend_for_repo maps repo -> backend, so a NEXTFLOW member would
        # either reroute every v2ecoli request or be dead configuration. This is the
        # same axis multi_node_dispatch already uses to pick pbg-native over
        # chain-dispatch for the same repo, same image, one config field apart.
        nf_dispatch = getattr(config, "nextflow_dispatch", None)
        if nf_dispatch is not None:
            return await self._nextflow().submit(
                ecoli_simulation, database_service, nf_dispatch, correlation_id=correlation_id
            )

        # Run 1's real missing-output fix (Alex's Option 1 decision, 2026-09-06):
        # a run_mbp_tracked.py variant dispatch, e.g. reactor_bird_coupled. Checked
        # here for the same reason nextflow_dispatch/multi_node_dispatch are --
        # it carries no composite_id/generations shape that would otherwise
        # satisfy a different branch's own routing condition, but checking early
        # keeps every extra-field dispatch shape's own precedence explicit rather
        # than incidental.
        mbp_dispatch = getattr(config, "mbp_dispatch", None)
        if mbp_dispatch is not None:
            return await self._mbp_tracked().submit(
                ecoli_simulation, database_service, mbp_dispatch, correlation_id=correlation_id
            )

        mnp_dispatch = getattr(config, "multi_node_dispatch", None)
        if mnp_dispatch is not None:
            return await self._multi_node().submit(
                ecoli_simulation, database_service, mnp_dispatch, correlation_id=correlation_id
            )

        composite = getattr(config, "composite", None)
        # config.generations is a real (non-"extra") SimulationConfig field, unlike
        # the comparison knobs read further below -- read directly, not via getattr.
        n_generations = int(config.generations or 1)
        if composite is None and n_generations > 1:
            return await self._chain().submit(ecoli_simulation, database_service, correlation_id=correlation_id)

        return await self._ensemble().submit(ecoli_simulation, database_service, correlation_id=correlation_id)

    @override
    async def read_config_template(
        self,
        simulator_version: SimulatorVersion,
        config_filename: str,
        allow_default_fallback: bool = False,
    ) -> str:
        return await fetch_config_template(
            simulator_version, config_filename, _seams.get_settings().github_token, allow_default_fallback
        )

    @override
    async def discover_repo_contents(self, simulator_version: SimulatorVersion) -> RepoDiscovery:
        return await fetch_repo_discovery(simulator_version, _seams.get_settings().github_token)

    @override
    async def get_job_status(self, job_id: JobId) -> JobStatusInfo | None:
        """Status — LOCAL, a K8s-hosted Nextflow head, or AWS Batch describe_jobs."""
        if job_id.backend == JobBackend.LOCAL:
            return self._local.get_status(job_id.value)
        if job_id.backend == JobBackend.K8S_NEXTFLOW:
            if self._k8s is None:
                raise RuntimeError(
                    f"job {job_id.value} is a K8s-hosted Nextflow head, but this service has no "
                    f"K8sJobService (k8s_job_namespace unset)"
                )
            # The value is a Job NAME, not a Batch job id -- describe_jobs would
            # return an empty list and this would report None rather than fail.
            return self._k8s.get_job_status(job_id.value)

        job = self.batch.engine().describe_job(job_id.value)
        if job is None:
            logger.warning("No Batch job found with id %s", job_id.value)
            return None
        status = JobStatus.from_batch_state(job.get("status", ""))
        started = job.get("startedAt")
        stopped = job.get("stoppedAt")
        return JobStatusInfo(
            job_id=job_id,
            status=status,
            start_time=str(started) if started else None,
            end_time=str(stopped) if stopped else None,
            exit_code=batch_exit_code(job),
            error_message=job.get("statusReason") if status == JobStatus.FAILED else None,
        )

    def get_chain_campaign_result(self, job_ids: list[str]) -> ChainCampaignPollResult:
        """Poll a chain-dispatch campaign's tracked final-generation job ids —
        one per seed, each seed's own last successfully-submitted generation
        job (``HpcRun.chain_final_job_ids``) — for the analysis-fan-in
        condition (backlog item 33 rework, replacing the per-generation-array
        "wave" design's own single-array-job ``get_wave_result``).

        "Terminal" means EVERY tracked job has reached a Batch-terminal state
        (SUCCEEDED or FAILED, via the same ``JobStatus.from_batch_state``
        mapping ``get_job_status`` already uses) — a seed's chain ending in
        FAILED (that job's own retries exhausted, or an earlier generation in
        its chain having failed and auto-propagated via Batch's own dependsOn)
        is expected economics, not an orchestrator error; the caller
        (``JobScheduler._advance_chain_campaign``) decides whether the
        campaign as a whole produced anything worth analyzing. A tracked id
        that hasn't appeared in a ``describe_jobs`` response yet (brief
        eventual-consistency lag right after submission) is treated as
        not-yet-terminal, not a hard failure — this poller runs on an
        interval, it just checks again next time.

        ``describe_jobs`` accepts at most 100 job ids per call (verified
        against the real API model this session) — a campaign's up to 1000
        tracked ids are chunked accordingly, unlike the array-job design this
        superseded (which polled ONE array parent's own
        ``arrayProperties.statusSummary`` plus paginated ``list_jobs`` calls).
        """
        if not job_ids:
            # Nothing tracked at all -- every seed failed even generation 0's
            # submission. Trivially "terminal" (nothing left to wait for); the
            # caller's own zero-succeeded handling covers marking the campaign
            # FAILED without submitting an analysis over an empty sweep.
            return ChainCampaignPollResult(terminal=True)

        statuses = self.get_batch_job_statuses(job_ids)
        succeeded = [jid for jid in job_ids if statuses.get(jid) == JobStatus.COMPLETED]
        failed = [jid for jid in job_ids if statuses.get(jid) == JobStatus.FAILED]
        # A missing id (not in `statuses` at all) or one still queued/running is
        # simply neither succeeded nor failed above -- either way, not yet terminal.

        if len(succeeded) + len(failed) < len(job_ids):
            return ChainCampaignPollResult(terminal=False)
        return ChainCampaignPollResult(terminal=True, succeeded_job_ids=succeeded, failed_job_ids=failed)

    @override
    async def cancel_job(self, job_id: JobId) -> None:
        """Cancel — LOCAL task or AWS Batch terminate_job (also kills child MNP nodes)."""
        if job_id.backend == JobBackend.LOCAL:
            self._local.cancel(job_id.value)
            logger.info("Cancelled local task %s", job_id.value)
            return
        if job_id.backend == JobBackend.K8S_NEXTFLOW:
            if self._k8s is None:
                raise RuntimeError(f"cannot cancel {job_id.value}: no K8sJobService configured")
            # Deleting the Job SIGTERMs the head, which asks Nextflow to shut
            # down; its hook then terminates the tasks it submitted. That is the
            # correct mechanism -- Nextflow is the only party that knows exactly
            # which tasks belong to this run -- and the pod now gets
            # NF_HEAD_TERMINATION_GRACE_SECONDS to do it.
            #
            # But it was OBSERVED not to happen (viva-api#472): on simulation 441,
            # 8 of 10 lineage tasks outlived the cancel by ~100 minutes and filled
            # host disk until a later campaign started failing. A cancel that
            # leaves work running is worse than one that refuses, because the
            # operator believes the resources are free. So we verify rather than
            # assume, and say what was actually stopped.
            #
            # The reap that guarantees it is NOT here any more. Done inline it was
            # synchronous inside the cancel request (a pod restart mid-cancel lost
            # the intent), scanned one queue, did not paginate, and -- worst --
            # ran while the head was still in its grace period, so Nextflow saw
            # each termination as a task failure and RESUBMITTED it (measured:
            # terminating 10 tasks produced 9 fresh jobs). The handler writes
            # CANCELLED after this returns; ``JobScheduler``'s
            # ``reconcile_cancelled_nextflow_campaigns`` then reaps from that
            # row every tick, only once the head is actually gone.
            self._k8s.delete_job(job_id.value)
            logger.info("Deleted Nextflow head Job %s", job_id.value)
            return
        self.batch.engine().terminate(job_id.value, reason="cancelled via sms-api")
        logger.info("Terminated Ray Batch job %s", job_id.value)

    async def cancel_companion_jobs(self, hpc_run: HpcRun) -> int:
        """Terminate the Batch jobs a run owns besides its tracked one; returns how many.

        Called BEFORE the tracked job is cancelled: that job depends on these, and Batch keeps
        a terminated job PENDING until its dependency ends -- so stopping the dependency
        first is also what lets the tracked job leave the queue.

        Only a Batch-backed run has companions of this kind. A LOCAL row's
        ``external_job_ids`` are its build's jobs, which ``LocalTaskService`` owns.
        """
        if hpc_run.job_id.backend != JobBackend.RAY:
            return 0
        companions = [j for j in hpc_run.external_job_ids or [] if j and j != hpc_run.job_id.value]
        for companion in companions:
            await self.cancel_job(JobId.ray(companion))
        return len(companions)

    async def cancel_chain_campaign(self, campaign: HpcRun) -> None:
        """Cancel every seed's current in-flight job for a chain-dispatch
        campaign (backlog item 71 Phase 4, folding in backlog item 53's
        cancellation design). Simpler than item 53's original walk-back-through-
        dependsOn proposal: under the per-seed app-level-gated model there is at
        most ONE in-flight job per seed at any time, directly readable from
        ``chain_current_job_ids`` — no dependsOn chain to walk. Reuses
        ``cancel_job``'s existing ``terminate_job`` call unchanged, which item
        53's own empirical testing already validated works correctly across
        every non-terminal Batch state (RUNNING, RUNNABLE, PENDING) — no
        state-dependent branching needed. Idempotent: a seed whose chain
        already resolved (its ``chain_current_job_ids`` entry already ``None``)
        is simply skipped.

        This only terminates the AWS-side jobs — writing the campaign's own
        CANCELLED status is the caller's responsibility (mirrors the existing
        single-job ``cancel_job``/``cancel_simulation`` split: this service
        talks to AWS, the handler owns the DB write).
        """
        # The campaign's OWN job id is its ParCa job, and until ParCa succeeds no seed has a
        # current job at all -- so a campaign cancelled in that phase terminated nothing and
        # left ParCa running under a CANCELLED row (viva-api#709). Terminating a job that has
        # already finished is accepted by Batch, so there is no state to check first.
        if not campaign.chain_parca_done and campaign.job_id.backend == JobBackend.RAY:
            await self.cancel_job(campaign.job_id)
        for job_id in campaign.chain_current_job_ids or []:
            if job_id is None:
                continue
            await self.cancel_job(JobId.ray(job_id))

    @override
    async def close(self) -> None:
        pass
