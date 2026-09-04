import asyncio
import datetime
import logging

from viva_api.common.hpc.job_service import JobStatusUpdate
from viva_api.common.hpc.local_task_service import LocalTaskService
from viva_api.common.hpc.slurm_service import SlurmService
from viva_api.common.messaging.messaging_service import MessagingService
from viva_api.common.models import JobBackend, JobStatus, SSHTarget
from viva_api.config import ComputeBackend, compute_backend_for_repo, get_settings
from viva_api.dependencies import get_ssh_session_service
from viva_api.simulation import batch_build
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import (
    ChainCampaignUpdate,
    HpcRun,
    JobType,
    Simulation,
    WorkerEvent,
    WorkerEventMessagePayload,
)
from viva_api.simulation.simulation_service_ray import (
    SimulationServiceRay,
    injected_processes_from_config,
    strain_from_config,
)

logger = logging.getLogger(__name__)

# viva-api#414: how old an active LOCAL row must be before an UNOWNED one with
# nothing external to check is declared dead. Two pods overlap briefly during a
# rolling restart, and the new pod's very first tick sees rows the old pod
# still legitimately owns; a young row with no external handle yet is exactly
# what an in-flight submission looks like from outside. A row whose external
# Batch job IS known needs no grace -- its status is derived from Batch truth
# and writing that truth is idempotent whoever else is watching.
LOCAL_ORPHAN_GRACE_SECONDS = 600


def _hpcrun_age_seconds(hpc_run: HpcRun, now: datetime.datetime | None = None) -> float:
    """Seconds since the row's ``start_time`` (which insert_hpcrun always sets,
    naive, in the pod's own clock -- compared against the same clock here).
    A row with no start_time at all is treated as infinitely old."""
    if not hpc_run.start_time:
        return float("inf")
    try:
        started = datetime.datetime.fromisoformat(hpc_run.start_time)
    except ValueError:
        return float("inf")
    if started.tzinfo is not None:
        started = started.astimezone().replace(tzinfo=None)
    return ((now or datetime.datetime.now()) - started).total_seconds()


class JobScheduler:
    database_service: DatabaseService
    slurm_service: SlurmService | None
    simulation_service_ray: SimulationServiceRay | None
    local_task_service: LocalTaskService | None
    messaging_service: MessagingService
    _polling_task: asyncio.Task[None] | None = None
    _stop_event: asyncio.Event

    def __init__(
        self,
        messaging_service: MessagingService,
        database_service: DatabaseService,
        slurm_service: SlurmService | None = None,
        simulation_service_ray: SimulationServiceRay | None = None,
        local_task_service: LocalTaskService | None = None,
    ):
        self.messaging_service = messaging_service
        self.database_service = database_service
        self.slurm_service = slurm_service
        self.simulation_service_ray = simulation_service_ray
        self.local_task_service = local_task_service
        self._stop_event = asyncio.Event()
        # Orphaned LOCAL rows already announced at WARNING (one line per row,
        # not one per 5-second tick).
        self._orphans_announced: set[int] = set()
        # correlation_id -> hpcrun_id, HITS only (see get_hpcrun_by_correlation_id)
        self._hpcrun_ids_by_correlation: dict[str, int] = {}

    async def get_hpcrun_by_correlation_id(self, correlation_id: str) -> int | None:
        """Resolve a worker event's correlation id to its HpcRun id, caching
        only HITS (viva-api#416). This was ``@alru_cache``, which keeps a
        successful ``None`` forever -- and every dispatch path submits to the
        backend BEFORE inserting the row, so a worker event arriving first
        made "no row" the permanent answer and every later event for that
        run was silently dropped. An hpcrun_id is immutable, so a hit is safe
        to cache; a miss must be re-asked."""
        hpcrun_id = self._hpcrun_ids_by_correlation.get(correlation_id)
        if hpcrun_id is None:
            hpcrun_id = await self.database_service.get_hpcrun_id_by_correlation_id(correlation_id=correlation_id)
            if hpcrun_id is not None:
                self._hpcrun_ids_by_correlation[correlation_id] = hpcrun_id
        return hpcrun_id

    async def subscribe(self) -> None:
        channel = get_settings().redis_channel
        logger.info(f"Subscribing to messaging service for channel '{channel}'")

        async def message_handler(data: bytes) -> None:
            try:
                data_str = data.decode("utf-8")
                logger.debug(f"Received message on channel '{channel}': {data_str}")
                worker_event_message_payload = WorkerEventMessagePayload.model_validate_json(data_str)
                worker_event = WorkerEvent.from_message_payload(
                    worker_event_message_payload=worker_event_message_payload
                )
                hpcrun_id = await self.get_hpcrun_by_correlation_id(correlation_id=worker_event.correlation_id)
                if hpcrun_id is None:
                    logger.error(f"No HpcRun found for correlation ID {worker_event.correlation_id}. Skipping event.")
                    return
                _updated_worker_event = await self.database_service.insert_worker_event(
                    worker_event, hpcrun_id=hpcrun_id
                )
            except Exception:
                logger.exception(f"Exception while handling message: {data!r}")

        await self.messaging_service.subscribe(subject=channel, callback=message_handler)
        if self.messaging_service.is_connected():
            logger.info("Messaging service is connected and subscription is set up.")
        else:
            logger.error("Messaging service is not connected.")

    async def start_polling(self, interval_seconds: int = 30) -> None:
        if self._polling_task is not None and not self._polling_task.done():
            logger.warning("Polling task already running.")
            return
        self._stop_event.clear()
        self._polling_task = asyncio.create_task(self._polling_loop(interval_seconds))
        logger.info("Started job status polling task.")

    async def stop_polling(self) -> None:
        if self._stop_event:
            self._stop_event.set()
        if self._polling_task:
            await self._polling_task
            logger.info("Stopped job status polling task.")

    async def _polling_loop(self, interval_seconds: int) -> None:
        while not self._stop_event.is_set():
            # First, so the very first tick after startup reconciles whatever
            # the previous pod left behind (viva-api#414).
            try:
                await self.reconcile_local_tasks()
            except Exception:
                logger.exception("Error during orphaned local-task reconciliation")
            try:
                await self.update_running_jobs()
            except Exception:
                logger.exception("Error during job polling")
            try:
                await self.update_chain_campaigns()
            except Exception:
                logger.exception("Error during chain-dispatch campaign polling")
            try:
                await self.update_multi_node_jobs()
            except Exception:
                logger.exception("Error during multi-node composite job polling")
            await asyncio.sleep(interval_seconds)

    async def update_running_jobs(self) -> None:
        if self.slurm_service is None:
            return  # No SLURM polling when using K8s backend

        # Fetch all active (PENDING or RUNNING) HpcRun jobs
        running_jobs = await self.database_service.list_active_hpcruns()
        if not running_jobs:
            logger.debug("No active jobs found for polling.")
            return
        # Filter to SLURM-backend jobs (K8s jobs will be polled separately)
        slurm_runs = [job for job in running_jobs if job.job_id.backend == JobBackend.SLURM]
        if not slurm_runs:
            logger.debug("No active SLURM jobs found for polling.")
            return
        slurm_job_ids = [job.job_id.as_slurm_int for job in slurm_runs]
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            slurm_jobs_from_squeue = await self.slurm_service.get_job_status_squeue(ssh, slurm_job_ids)
            slurm_jobs_from_sacct = await self.slurm_service.get_job_status_scontrol(ssh, slurm_job_ids)
        slurm_job_map = {job.job_id: job for job in slurm_jobs_from_squeue}
        slurm_job_map.update({job.job_id: job for job in slurm_jobs_from_sacct})
        for hpc_run in slurm_runs:
            slurm_job = slurm_job_map.get(hpc_run.job_id.as_slurm_int)
            if not slurm_job or not slurm_job.job_state:
                continue
            new_status = JobStatus.from_slurm_state(slurm_job.job_state)
            if new_status == hpc_run.status:
                logger.debug(f"HpcRun {hpc_run.database_id} is still running with status {new_status}")
                continue

            # Build error message for failed/cancelled jobs
            error_message = None
            if new_status in (JobStatus.FAILED, JobStatus.CANCELLED):
                error_parts = [f"SLURM state: {slurm_job.job_state}"]
                if slurm_job.reason:
                    error_parts.append(f"reason: {slurm_job.reason}")
                if slurm_job.exit_code:
                    error_parts.append(f"exit_code: {slurm_job.exit_code}")
                error_message = ", ".join(error_parts)

            update = JobStatusUpdate(
                job_id=hpc_run.job_id,
                status=new_status,
                start_time=slurm_job.start_time,
                end_time=slurm_job.end_time,
                error_message=error_message,
            )
            await self.database_service.update_hpcrun_status(hpcrun_id=hpc_run.database_id, update=update)
            logger.info(f"Updated HpcRun {hpc_run.database_id} status to {new_status}")

    async def reconcile_local_tasks(self) -> None:
        """Finish every active LOCAL HpcRun row this process does not own
        (viva-api#414).

        A LOCAL row points at an in-process ``asyncio.Task`` that was polling
        real work submitted elsewhere -- a DooD image build on AWS Batch, or
        the chain-dispatch placeholder whose task submits ParCa and writes the
        real campaign row. The task dies with its pod; nothing else ever
        finished the row, so it stayed ``running`` forever while the work it
        watched completed normally (measured live: hpcrun 506, 2026-09-04,
        recovery was a redundant 10-minute rebuild).

        Runs on every tick, first, so it is also the startup reconciliation.
        Stateless by construction: nothing is re-attached, each tick derives
        the row's state from external truth and writes it once terminal --
        the same shape as ``update_chain_campaigns``. A row THIS process owns
        is skipped entirely (its own done-callback finalizes it).
        """
        if self.local_task_service is None:
            return
        rows = await self.database_service.list_active_local_hpcruns()
        for hpc_run in rows:
            if self.local_task_service.owns(hpc_run.job_id.value):
                continue
            try:
                await self._reconcile_orphaned_local_run(hpc_run)
            except Exception:
                logger.exception("Error reconciling orphaned LOCAL HpcRun %s", hpc_run.database_id)

    async def _reconcile_orphaned_local_run(self, hpc_run: HpcRun) -> None:
        age = _hpcrun_age_seconds(hpc_run)
        if hpc_run.database_id not in self._orphans_announced:
            self._orphans_announced.add(hpc_run.database_id)
            logger.warning(
                "Orphaned LOCAL HpcRun %s (%s, local task %s, %.0fs old): no live owner in this process; "
                "reconciling from external state (external_job_ids=%s)",
                hpc_run.database_id,
                hpc_run.job_type.value,
                hpc_run.job_id.value,
                age,
                hpc_run.external_job_ids,
            )
        if hpc_run.job_type == JobType.BUILD_IMAGE:
            await self._reconcile_orphaned_build(hpc_run, age)
        elif hpc_run.job_type == JobType.SIMULATION:
            await self._reconcile_orphaned_simulation_placeholder(hpc_run, age)
        elif age > LOCAL_ORPHAN_GRACE_SECONDS:
            await self._finish_orphan(
                hpc_run,
                JobStatus.FAILED,
                "orphaned: the api process that owned this local task restarted; its outcome is unknown",
            )

    async def _reconcile_orphaned_build(self, hpc_run: HpcRun, age: float) -> None:
        """A build row: resolve it from its Batch build job(s). The ids come
        from ``external_job_ids`` (written at submit time since #414) or, for
        a row that predates that column, from the build's deterministic job
        name. Once found by name they are persisted so the next tick is a
        plain describe."""
        job_ids = list(hpc_run.external_job_ids or [])
        if not job_ids:
            job_ids = await self._derive_build_job_ids(hpc_run)
            if job_ids:
                await self.database_service.set_hpcrun_external_job_ids(hpc_run.database_id, job_ids)
                logger.info(
                    "Orphaned build HpcRun %s: found its Batch job(s) by name: %s", hpc_run.database_id, job_ids
                )
        if not job_ids:
            if age > LOCAL_ORPHAN_GRACE_SECONDS:
                await self._finish_orphan(
                    hpc_run,
                    JobStatus.FAILED,
                    "orphaned: the api process that owned this build restarted and no Batch build job "
                    "could be found for it; re-upload the simulator to rebuild",
                )
            return

        states = await batch_build.describe_batch_jobs(job_ids)
        missing = [jid for jid in job_ids if jid not in states]
        if missing:
            if age > LOCAL_ORPHAN_GRACE_SECONDS:
                await self._finish_orphan(
                    hpc_run,
                    JobStatus.FAILED,
                    f"orphaned: AWS Batch no longer reports build job(s) {missing}; the outcome cannot be "
                    "recovered -- re-upload the simulator to rebuild",
                )
            return  # not yet visible -- check again next tick
        statuses = {jid: JobStatus.from_batch_state(states[jid].status) for jid in job_ids}
        failed = [jid for jid, st in statuses.items() if st in (JobStatus.FAILED, JobStatus.CANCELLED)]
        if failed:
            reasons = "; ".join(
                f"{states[jid].job_name}: {states[jid].status_reason or states[jid].status}" for jid in failed
            )
            await self._finish_orphan(
                hpc_run,
                JobStatus.FAILED,
                f"Batch build job(s) failed: {reasons}",
                end_time_ms=max((states[jid].stopped_at_ms or 0) for jid in job_ids) or None,
            )
            return
        if all(st == JobStatus.COMPLETED for st in statuses.values()):
            await self._finish_orphan(
                hpc_run,
                JobStatus.COMPLETED,
                None,
                end_time_ms=max((states[jid].stopped_at_ms or 0) for jid in job_ids) or None,
            )
            return
        logger.debug(
            "Orphaned build HpcRun %s: Batch job(s) still in flight (%s); leaving RUNNING",
            hpc_run.database_id,
            {jid: states[jid].status for jid in job_ids},
        )

    async def _derive_build_job_ids(self, hpc_run: HpcRun) -> list[str]:
        """Legacy fallback: find a build's Batch job(s) by deterministic name."""
        simulator = await self.database_service.get_simulator(simulator_id=hpc_run.ref_id)
        if simulator is None:
            logger.error("Orphaned build HpcRun %s: simulator %s not found", hpc_run.database_id, hpc_run.ref_id)
            return []
        settings = get_settings()
        commit = simulator.git_commit_hash
        backend = compute_backend_for_repo(simulator.git_repo_url)
        if backend == ComputeBackend.RAY:
            lookups = [(settings.build_amd64_queue, batch_build.ray_build_job_name(commit))]
        elif backend == ComputeBackend.BATCH:
            names = batch_build.k8s_build_job_names(commit)
            lookups = [(settings.build_arm64_queue, names["arm64"]), (settings.build_amd64_queue, names["amd64"])]
        else:
            return []
        created_after_ms: int | None = None
        if hpc_run.start_time:
            try:
                started = datetime.datetime.fromisoformat(hpc_run.start_time)
                # a minute of slack: the row is inserted AFTER the task is spawned
                created_after_ms = int((started.timestamp() - 60) * 1000)
            except ValueError:
                created_after_ms = None
        found: list[str] = []
        for queue, name in lookups:
            if not queue:
                return []
            ids = await batch_build.find_batch_job_ids_by_name(queue, name, created_after_ms=created_after_ms)
            if not ids:
                return []  # every job of the build must be findable, or we know nothing
            found.append(ids[0])  # newest submission of that name
        return found

    async def _reconcile_orphaned_simulation_placeholder(self, hpc_run: HpcRun, age: float) -> None:
        """The chain-dispatch placeholder (``SimulationServiceRay.
        _submit_chain_dispatch_background``): its task's whole job is to
        submit ParCa and insert the REAL campaign row for the same simulation.
        If that row exists the task did its job -- the placeholder is
        superseded and simply COMPLETED. If it does not, and the row is past
        the grace window, the submission died with its pod and nothing is
        tracking this simulation: say so, as FAILED, so the caller re-submits
        instead of waiting on a row nobody will ever update."""
        latest = await self.database_service.get_hpcrun_by_ref(ref_id=hpc_run.ref_id, job_type=JobType.SIMULATION)
        if latest is not None and latest.database_id != hpc_run.database_id:
            await self._finish_orphan(hpc_run, JobStatus.COMPLETED, None)
            return
        if age > LOCAL_ORPHAN_GRACE_SECONDS:
            await self._finish_orphan(
                hpc_run,
                JobStatus.FAILED,
                "orphaned: the api process restarted while this campaign was being submitted, before its "
                "campaign row was written; nothing is tracking it (a ParCa job it may have started is "
                "untracked) -- re-submit the simulation",
            )

    async def _finish_orphan(
        self, hpc_run: HpcRun, status: JobStatus, error_message: str | None, *, end_time_ms: int | None = None
    ) -> None:
        end_time = (
            datetime.datetime.fromtimestamp(end_time_ms / 1000).isoformat()
            if end_time_ms
            else datetime.datetime.now().isoformat()
        )
        await self.database_service.update_hpcrun_status(
            hpcrun_id=hpc_run.database_id,
            update=JobStatusUpdate(
                job_id=hpc_run.job_id, status=status, end_time=end_time, error_message=error_message
            ),
        )
        logger.warning(
            "Orphaned LOCAL HpcRun %s (%s) finished from external state: %s%s",
            hpc_run.database_id,
            hpc_run.job_type.value,
            status.value,
            f" -- {error_message}" if error_message else "",
        )

    async def update_chain_campaigns(self) -> None:
        """Advance every active chain-dispatch campaign by one tick each
        (backlog item 71 Phase 4 — app-level per-seed gating, replacing native
        Batch ``dependsOn`` chains, which never triggered AWS Batch's own
        compute-environment scaling reconciliation at real campaign scale —
        item 68's root cause). Unlike the design this superseded, there IS a
        real "advance to the next generation" step to do here now: this
        scheduler submits exactly one generation per seed at a time, only once
        the previous one is confirmed SUCCEEDED — see
        ``_advance_chain_campaign`` for the full per-tick state machine. No-op
        when no chain campaign is active, or on a deployment with no Ray/Batch
        backend wired (SLURM-only deployments pass
        ``simulation_service_ray=None``).
        """
        if self.simulation_service_ray is None:
            return
        # Narrow simulation_service_ray to non-None ONCE here (rather than a
        # runtime `assert` inside `_advance_chain_campaign`, which -O would
        # silently strip) and thread it through explicitly.
        simulation_service_ray = self.simulation_service_ray

        active_campaigns = await self.database_service.list_active_chain_campaigns()
        if not active_campaigns:
            logger.debug("No active chain-dispatch campaigns found for polling.")
            return
        for campaign in active_campaigns:
            try:
                await self._advance_chain_campaign(campaign, simulation_service_ray)
            except Exception:
                logger.exception("Error advancing chain-dispatch campaign HpcRun %s", campaign.database_id)

    async def _advance_chain_campaign(self, campaign: HpcRun, simulation_service_ray: SimulationServiceRay) -> None:
        """Advance one active chain-dispatch campaign by exactly one tick
        (backlog item 71 Phase 4). ``campaign`` only identifies WHICH campaign
        — the real read-decide-write below always operates on a FRESH copy,
        re-read inside ``DatabaseService.advance_chain_campaign``'s per-campaign
        advisory lock, so a concurrent tick against the same campaign (e.g. two
        pods briefly overlapping during a rolling restart) can never act on
        stale state — the explicit no-double-submit guarantee this rework
        exists to provide.

        One phase transition per tick, deliberately simple/safe:

        1. ParCa not yet confirmed done: poll it. SUCCEEDED fans out
           generation 0 for every seed at once (the one remaining genuine
           submission burst, TPS-paced — see
           ``SimulationServiceRay.submit_chain_generation_batch``). FAILED
           ends the whole campaign (no seed can start). Anything else is a
           no-op this tick.
        2. ParCa already done: batch-poll every seed's current in-flight job.
           A seed whose job reached a terminal state either advances to its
           next generation (submits ONE new job, no ``depends_on`` — app-level
           gating replaces native Batch dependency chains) or, if it just
           finished its last generation, resolves — its ``chain_current_job_ids``
           entry goes to ``None`` and its final job id is appended to
           ``chain_final_job_ids``. A still-running seed is left alone this
           tick. This never marks an individual seed's chain FAILED as an
           orchestrator decision — a seed's job reaching Batch's own FAILED
           state is what resolves it as failed; nothing here second-guesses
           that.
        3. Once every seed has resolved (every ``chain_current_job_ids`` entry
           is ``None``): the campaign itself is terminal. Classify succeeded
           vs failed by reusing ``get_chain_campaign_result`` on the now-fully-
           populated ``chain_final_job_ids`` — every entry of which is already
           known-terminal by construction, so this call is a fast formality,
           not a real wait — and submit the analysis DAG node if anything
           succeeded, exactly as before this rework.
        """

        async def _tick(fresh: HpcRun) -> ChainCampaignUpdate | None:
            if fresh.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
                return None  # already resolved by a concurrent tick or a cancel request

            current_job_ids = list(fresh.chain_current_job_ids or [])
            current_generation = list(fresh.chain_current_generation or [])
            final_job_ids = list(fresh.chain_final_job_ids or [])
            n_seeds = len(current_job_ids)
            n_generations = int(fresh.chain_n_generations or 1)

            simulation = await self.database_service.get_simulation(simulation_id=fresh.ref_id)
            if simulation is None:
                logger.error("Chain dispatch: Simulation %s not found for HpcRun %s", fresh.ref_id, fresh.database_id)
                return None
            simulator = await self.database_service.get_simulator(simulator_id=simulation.simulator_id)
            if simulator is None:
                logger.error(
                    "Chain dispatch: Simulator %s not found for simulation %s", simulation.simulator_id, fresh.ref_id
                )
                return None
            commit = simulator.git_commit_hash
            experiment_id = str(simulation.config.experiment_id)

            if not fresh.chain_parca_done:
                return await self._advance_parca_gate(
                    simulation_service_ray,
                    fresh=fresh,
                    simulation=simulation,
                    commit=commit,
                    experiment_id=experiment_id,
                    current_job_ids=current_job_ids,
                    current_generation=current_generation,
                    final_job_ids=final_job_ids,
                    n_seeds=n_seeds,
                )

            # ParCa already done -- batch-poll every seed's current in-flight job,
            # advancing or resolving each one (mutates both lists in place).
            await self._advance_seed_generations(
                simulation_service_ray,
                simulation=simulation,
                commit=commit,
                experiment_id=experiment_id,
                current_job_ids=current_job_ids,
                current_generation=current_generation,
                final_job_ids=final_job_ids,
                n_generations=n_generations,
            )

            if any(jid is not None for jid in current_job_ids):
                # some seeds still in flight (or nothing changed this tick) --
                # persist whatever progress was made, not yet terminal.
                return ChainCampaignUpdate(
                    chain_current_job_ids=current_job_ids,
                    chain_current_generation=current_generation,
                    chain_parca_done=True,
                    chain_final_job_ids=final_job_ids,
                )

            return await self._finalize_campaign(
                simulation_service_ray,
                fresh=fresh,
                simulation=simulation,
                commit=commit,
                experiment_id=experiment_id,
                current_job_ids=current_job_ids,
                current_generation=current_generation,
                final_job_ids=final_job_ids,
                n_seeds=n_seeds,
                n_generations=n_generations,
            )

        await self.database_service.advance_chain_campaign(campaign.database_id, _tick)

    async def _advance_parca_gate(
        self,
        simulation_service_ray: SimulationServiceRay,
        *,
        fresh: HpcRun,
        simulation: Simulation,
        commit: str,
        experiment_id: str,
        current_job_ids: list[str | None],
        current_generation: list[int | None],
        final_job_ids: list[str],
        n_seeds: int,
    ) -> ChainCampaignUpdate | None:
        """Phase 1 of ``_advance_chain_campaign``: gate generation-0 submission
        on ParCa. Extracted purely to keep ``_tick`` under the project's
        cyclomatic-complexity limit — see that method's own docstring for the
        full 3-phase state machine this is one third of.

        Backlog items 93, 105: ``injected_processes``/``variants``/
        ``composite_id``/``cache_variant`` are re-derived from
        ``simulation.config`` here, every tick, rather than persisted anywhere
        new — ``simulation`` is already re-read fresh from the DB by ``_tick``
        for every campaign, so this is restart-safe for free, matching how
        every other piece of per-tick state already works here.

        ``cache_variant`` (item 105): selects a ``variant``-labeled ParCa
        cache (see ``SimulationServiceRay.cache_s3_uri`` /
        ``submit_new_gene_cache_job``) instead of the plain commit-only one —
        e.g. a strain-specific induced-expression cache built on top of a
        prior ParCa run. ``None`` (every existing caller) preserves today's
        behavior byte-for-byte.
        """
        parca_info = await simulation_service_ray.get_job_status(fresh.job_id)
        if parca_info is None or parca_info.status not in (JobStatus.COMPLETED, JobStatus.FAILED):
            return None  # ParCa still running, or not yet visible -- nothing to do this tick
        if parca_info.status == JobStatus.FAILED:
            logger.warning("Chain dispatch %s: ParCa failed; campaign ends here, no seed can start", experiment_id)
            return ChainCampaignUpdate(
                chain_current_job_ids=current_job_ids,
                chain_current_generation=current_generation,
                chain_parca_done=False,
                chain_final_job_ids=final_job_ids,
                terminal_status=JobStatus.FAILED,
                error_message="chain dispatch: ParCa failed",
            )
        # ParCa SUCCEEDED -- fan out generation 0 for every seed at once.
        runner_s3_uri = await simulation_service_ray.stage_runner(experiment_id)
        cache_variant = getattr(simulation.config, "cache_variant", None) or None
        expect_new_genes, expect_bundle_overrides = strain_from_config(simulation.config)
        submitted = await simulation_service_ray.submit_chain_generation_batch(
            seeds=list(range(n_seeds)),
            generation_index=0,
            experiment_id=experiment_id,
            commit=commit,
            cache_s3=simulation_service_ray.cache_s3_uri(commit, variant=cache_variant),
            runner_s3_uri=runner_s3_uri,
            tags=simulation_service_ray.chain_base_tags(simulation=simulation, commit=commit),
            injected_processes=injected_processes_from_config(simulation.config),
            variants=getattr(simulation.config, "variants", None) or None,
            composite_id=getattr(simulation.config, "composite_id", None) or None,
            expect_new_genes=expect_new_genes,
            expect_bundle_overrides=expect_bundle_overrides,
        )
        for seed in range(n_seeds):
            if seed in submitted:
                current_job_ids[seed] = submitted[seed]
                current_generation[seed] = 0
            # else: generation-0 submission itself failed for this seed (even
            # after retry-on-throttle) -- current_job_ids[seed] stays None, so
            # this seed is already "resolved" (with no final id of its own)
            # the moment every OTHER seed resolves.
        logger.info(
            "Chain dispatch %s: ParCa succeeded -> %d/%d seed generation-0 jobs submitted",
            experiment_id,
            len(submitted),
            n_seeds,
        )
        return ChainCampaignUpdate(
            chain_current_job_ids=current_job_ids,
            chain_current_generation=current_generation,
            chain_parca_done=True,
            chain_final_job_ids=final_job_ids,
        )

    async def _advance_seed_generations(
        self,
        simulation_service_ray: SimulationServiceRay,
        *,
        simulation: Simulation,
        commit: str,
        experiment_id: str,
        current_job_ids: list[str | None],
        current_generation: list[int | None],
        final_job_ids: list[str],
        n_generations: int,
    ) -> None:
        """Phase 2 of ``_advance_chain_campaign``: batch-poll every seed's
        current in-flight job and advance or resolve each one — mutates
        ``current_job_ids``/``current_generation``/``final_job_ids`` in place
        (all three are freshly-copied lists owned by this one tick, never
        shared, so in-place mutation is safe). Extracted purely to keep
        ``_tick`` under the project's cyclomatic-complexity limit.

        Backlog items 93, 105: ``injected_processes``/``variants``/
        ``composite_id``/``cache_variant`` are re-derived from
        ``simulation.config`` once per tick (same campaign, same config, for
        every seed advanced this tick) rather than persisted anywhere new —
        see ``_advance_parca_gate``'s docstring for why re-deriving from the
        already-fresh ``simulation`` row is restart-safe for free, and for
        what ``cache_variant`` selects.
        """
        in_flight = [jid for jid in current_job_ids if jid is not None]
        if not in_flight:
            return
        statuses = simulation_service_ray.get_batch_job_statuses(in_flight)
        injected_processes = injected_processes_from_config(simulation.config)
        variants = getattr(simulation.config, "variants", None) or None
        composite_id = getattr(simulation.config, "composite_id", None) or None
        cache_variant = getattr(simulation.config, "cache_variant", None) or None
        expect_new_genes, expect_bundle_overrides = strain_from_config(simulation.config)
        next_gen_runner_s3_uri: str | None = None
        for seed, job_id in enumerate(current_job_ids):
            if job_id is None:
                continue
            status = statuses.get(job_id)
            if status not in (JobStatus.COMPLETED, JobStatus.FAILED):
                continue  # still running, or not yet visible -- leave alone this tick
            gen = current_generation[seed] or 0
            if status == JobStatus.FAILED or gen + 1 >= n_generations:
                # this seed's chain is done -- success (reached the last
                # generation) or permanent failure, either way it contributes
                # its final job id.
                current_job_ids[seed] = None
                final_job_ids.append(job_id)
                continue
            if next_gen_runner_s3_uri is None:
                next_gen_runner_s3_uri = await simulation_service_ray.stage_runner(experiment_id)
            current_job_ids[seed] = simulation_service_ray.submit_chain_generation(
                seed=seed,
                generation_index=gen + 1,
                experiment_id=experiment_id,
                commit=commit,
                cache_s3=simulation_service_ray.cache_s3_uri(commit, variant=cache_variant),
                runner_s3_uri=next_gen_runner_s3_uri,
                tags=simulation_service_ray.chain_base_tags(simulation=simulation, commit=commit),
                injected_processes=injected_processes,
                variants=variants,
                composite_id=composite_id,
                expect_new_genes=expect_new_genes,
                expect_bundle_overrides=expect_bundle_overrides,
            )
            current_generation[seed] = gen + 1

    async def _finalize_campaign(
        self,
        simulation_service_ray: SimulationServiceRay,
        *,
        fresh: HpcRun,
        simulation: Simulation,
        commit: str,
        experiment_id: str,
        current_job_ids: list[str | None],
        current_generation: list[int | None],
        final_job_ids: list[str],
        n_seeds: int,
        n_generations: int,
    ) -> ChainCampaignUpdate:
        """Phase 3 of ``_advance_chain_campaign``: every seed has resolved
        (``current_job_ids`` all ``None``), so the campaign itself is now
        terminal — classify succeeded vs failed and submit the analysis DAG
        node if anything succeeded. Extracted purely to keep ``_tick`` under
        the project's cyclomatic-complexity limit.

        Every id in ``final_job_ids`` is already known-terminal by
        construction (``_advance_seed_generations`` only ever appends one once
        its own poll observed it SUCCEEDED/FAILED), so the
        ``get_chain_campaign_result`` call below is a fast formality that
        reuses the existing classification logic, not a real wait.
        """
        result = simulation_service_ray.get_chain_campaign_result(final_job_ids)
        succeeded = result.succeeded_job_ids
        failed = result.failed_job_ids
        # COMPLETED only when EVERY expected seed chain succeeded. Previously the
        # campaign was marked COMPLETED whenever the succeeded list was merely
        # non-empty, so a single surviving lineage out of thousands reported the
        # whole sweep complete and the multivariant analysis then ran over a store
        # full of undetectable holes (CD2 audit §2.11 / P0-7). A partial result is
        # terminal but NOT a success: mark it FAILED (there is no PARTIAL job
        # status without a DB enum migration; see the PR note) and name the
        # missing/failed seed chains, and do not submit an analysis over an
        # incomplete store.
        all_succeeded = len(succeeded) == n_seeds
        if all_succeeded:
            error_message: str | None = None
        elif not succeeded:
            error_message = "chain dispatch: zero seed chains succeeded"
        else:
            missing = failed or ["(seeds that never submitted generation 0)"]
            error_message = (
                f"chain dispatch: partial completion — {len(succeeded)}/{n_seeds} seed chains "
                f"succeeded; missing/failed final job ids: {missing}"
            )
        update = ChainCampaignUpdate(
            chain_current_job_ids=current_job_ids,
            chain_current_generation=current_generation,
            chain_parca_done=True,
            chain_final_job_ids=final_job_ids,
            terminal_status=JobStatus.COMPLETED if all_succeeded else JobStatus.FAILED,
            error_message=error_message,
        )
        if not all_succeeded:
            logger.warning(
                "Chain dispatch %s: HpcRun %s NOT all seed chains succeeded "
                "(%d/%d succeeded); campaign ends here, no analysis submitted. %s",
                experiment_id,
                fresh.database_id,
                len(succeeded),
                n_seeds,
                error_message,
            )
            return update

        analysis_job_id = await simulation_service_ray.submit_campaign_analysis(
            simulation=simulation,
            database_service=self.database_service,
            commit=commit,
            total_n_seeds=n_seeds,
            n_generations=n_generations,
        )
        logger.info(
            "Chain dispatch %s: campaign HpcRun %s all-terminal (%d/%d chains succeeded) -> analysis job %s",
            experiment_id,
            fresh.database_id,
            len(succeeded),
            n_seeds,
            analysis_job_id,
        )
        return update

    async def update_multi_node_jobs(self) -> None:
        """Advance every active multi-node process-bigraph composite dispatch
        by one tick each (backlog item 88 — e.g. a colony composite spread
        across N Ray-cluster nodes). Gives this dispatch shape the same
        auto-triggered "Analysis flush" chain-dispatch campaigns already get
        (``update_chain_campaigns``/``_finalize_campaign``), via a completely
        separate, additive code path — deliberately NOT sharing logic with
        that method, since a multi-node composite is ONE job (a status
        transition), not an N-seed per-generation state machine. No-op when
        no Ray/Batch backend is wired (SLURM-only deployments pass
        ``simulation_service_ray=None``), or no such job is active.
        """
        if self.simulation_service_ray is None:
            return
        simulation_service_ray = self.simulation_service_ray

        active_jobs = await self.database_service.list_active_multi_node_composites()
        if not active_jobs:
            logger.debug("No active multi-node composite jobs found for polling.")
            return
        for hpc_run in active_jobs:
            try:
                await self._advance_multi_node_job(hpc_run, simulation_service_ray)
            except Exception:
                logger.exception("Error advancing multi-node composite HpcRun %s", hpc_run.database_id)

    async def _advance_multi_node_job(self, hpc_run: HpcRun, simulation_service_ray: SimulationServiceRay) -> None:
        """Poll one multi-node composite HpcRun's underlying AWS Batch job; once
        it's terminal, atomically finalize the row and — only for the ONE tick
        that actually performs that transition — submit its Analysis-flush
        node. ``get_job_status`` already generically handles an arbitrary
        ``JobId`` (LOCAL vs. AWS Batch ``describe_jobs``), so no new AWS client
        code is needed here, only the new polling loop + finalize + submit.
        """
        job_info = await simulation_service_ray.get_job_status(hpc_run.job_id)
        if job_info is None or job_info.status not in (JobStatus.COMPLETED, JobStatus.FAILED):
            return  # still running, or not yet visible -- nothing to do this tick

        # Atomic conditional transition: only the tick whose UPDATE actually
        # flips PENDING/RUNNING to terminal proceeds. A concurrent tick racing
        # against the same row (e.g. two pods briefly overlapping during a
        # rolling restart) sees `won=False` and does nothing further — the
        # concrete guarantee against double-submitting the analysis job for a
        # single completed dispatch.
        won = await self.database_service.finalize_multi_node_job(
            hpc_run.database_id, job_info.status, job_info.error_message
        )
        if not won or job_info.status != JobStatus.COMPLETED:
            return

        composite_id = hpc_run.multi_node_composite_id
        if composite_id is None:
            # Unreachable in practice -- list_active_multi_node_composites only
            # ever returns rows with this field set -- but narrow explicitly
            # rather than silently passing an empty string downstream.
            logger.error(
                "Multi-node composite: HpcRun %s finalized with no multi_node_composite_id set", hpc_run.database_id
            )
            return

        simulation = await self.database_service.get_simulation(simulation_id=hpc_run.ref_id)
        if simulation is None:
            logger.error(
                "Multi-node composite: Simulation %s not found for HpcRun %s", hpc_run.ref_id, hpc_run.database_id
            )
            return
        simulator = await self.database_service.get_simulator(simulator_id=simulation.simulator_id)
        if simulator is None:
            logger.error(
                "Multi-node composite: Simulator %s not found for simulation %s",
                simulation.simulator_id,
                hpc_run.ref_id,
            )
            return

        analysis_job_id = await simulation_service_ray.submit_multi_node_analysis(
            simulation=simulation,
            database_service=self.database_service,
            commit=simulator.git_commit_hash,
            composite_id=composite_id,
        )
        logger.info(
            "Multi-node composite HpcRun %s (%s) COMPLETED -> analysis job %s",
            hpc_run.database_id,
            composite_id,
            analysis_job_id,
        )

    async def close(self) -> None:
        await self.stop_polling()
        logger.debug("Closing messaging service connection")
        await self.messaging_service.disconnect()
