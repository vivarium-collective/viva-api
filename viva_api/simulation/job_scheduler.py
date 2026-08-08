import asyncio
import datetime
import logging

from async_lru import alru_cache

from viva_api.common.hpc.job_service import JobStatusUpdate
from viva_api.common.hpc.slurm_service import SlurmService
from viva_api.common.messaging.messaging_service import MessagingService
from viva_api.common.models import JobBackend, JobStatus, SSHTarget
from viva_api.config import get_settings
from viva_api.dependencies import get_ssh_session_service
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import HpcRun, WorkerEvent, WorkerEventMessagePayload
from viva_api.simulation.simulation_service_ray import SimulationServiceRay

logger = logging.getLogger(__name__)


class JobScheduler:
    database_service: DatabaseService
    slurm_service: SlurmService | None
    simulation_service_ray: SimulationServiceRay | None
    messaging_service: MessagingService
    _polling_task: asyncio.Task[None] | None = None
    _stop_event: asyncio.Event

    def __init__(
        self,
        messaging_service: MessagingService,
        database_service: DatabaseService,
        slurm_service: SlurmService | None = None,
        simulation_service_ray: SimulationServiceRay | None = None,
    ):
        self.messaging_service = messaging_service
        self.database_service = database_service
        self.slurm_service = slurm_service
        self.simulation_service_ray = simulation_service_ray
        self._stop_event = asyncio.Event()

    @alru_cache
    async def get_hpcrun_by_correlation_id(self, correlation_id: str) -> int | None:
        return await self.database_service.get_hpcrun_id_by_correlation_id(correlation_id=correlation_id)

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
            try:
                await self.update_running_jobs()
            except Exception:
                logger.exception("Error during job polling")
            try:
                await self.update_wave_jobs()
            except Exception:
                logger.exception("Error during wave job polling")
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

    async def update_wave_jobs(self) -> None:
        """Poll every active wave-dispatch HpcRun to terminal, then either
        submit the next generation's wave (remapping survivors, backlog item
        33) or leave it be — the analysis DAG node for a campaign's FINAL wave
        is submitted alongside that wave itself (see ``SimulationServiceRay.
        _submit_wave_and_maybe_analysis``), not from here. No-op when no wave
        campaign is active, or on a deployment with no Ray/Batch backend wired
        (SLURM-only deployments pass ``simulation_service_ray=None``).
        """
        if self.simulation_service_ray is None:
            return
        # Narrow simulation_service_ray to non-None ONCE here (rather than a
        # runtime `assert` inside `_advance_wave`, which -O would silently
        # strip) and thread it through explicitly.
        simulation_service_ray = self.simulation_service_ray

        active_waves = await self.database_service.list_active_wave_hpcruns()
        if not active_waves:
            logger.debug("No active wave-dispatch jobs found for polling.")
            return
        for wave in active_waves:
            try:
                await self._advance_wave(wave, simulation_service_ray)
            except Exception:
                logger.exception("Error advancing wave HpcRun %s", wave.database_id)

    async def _advance_wave(self, wave: HpcRun, simulation_service_ray: SimulationServiceRay) -> None:
        """Handle one active wave HpcRun: no-op if still running; otherwise mark
        it terminal and, if survivors remain and generations remain, submit the
        next wave (see ``update_wave_jobs`` for what happens on the final wave).
        """
        result = simulation_service_ray.get_wave_result(wave.job_id.value)
        if not result.terminal:
            return

        seed_indices = wave.wave_seed_indices or []
        survivors = [seed_indices[i] for i in result.succeeded_local_indices if i < len(seed_indices)]

        # Terminal is either every child SUCCEEDED, or the array parent went
        # FAILED-due-to-partial-loss -- both are "wave finished", not an
        # orchestrator error (Spot/OOM attrition is expected economics). Only a
        # wave with ZERO survivors is a genuine campaign-ending failure.
        await self.database_service.update_hpcrun_status(
            hpcrun_id=wave.database_id,
            update=JobStatusUpdate(
                job_id=wave.job_id,
                status=JobStatus.COMPLETED if survivors else JobStatus.FAILED,
                end_time=datetime.datetime.now().isoformat(),
                error_message=None if survivors else "wave dispatch: zero seeds survived this generation",
            ),
        )
        if not survivors:
            logger.warning(
                "Wave dispatch: HpcRun %s (generation %s) had zero survivors; campaign ends here.",
                wave.database_id,
                wave.wave_index,
            )
            return

        simulation = await self.database_service.get_simulation(simulation_id=wave.ref_id)
        if simulation is None:
            logger.error("Wave dispatch: Simulation %s not found for HpcRun %s", wave.ref_id, wave.database_id)
            return
        n_generations = int(simulation.config.generations or 1)
        next_generation = int(wave.wave_index or 0) + 1
        if next_generation >= n_generations:
            # This WAS already the campaign's final wave -- its analysis node was
            # submitted alongside it at submission time, not here.
            return

        simulator = await self.database_service.get_simulator(simulator_id=simulation.simulator_id)
        if simulator is None:
            logger.error(
                "Wave dispatch: Simulator %s not found for simulation %s", simulation.simulator_id, wave.ref_id
            )
            return
        total_n_seeds = int(simulation.num_seeds or len(seed_indices))

        next_job_id = await simulation_service_ray.submit_next_wave(
            simulation=simulation,
            database_service=self.database_service,
            commit=simulator.git_commit_hash,
            generation_index=next_generation,
            seed_indices=survivors,
            total_n_seeds=total_n_seeds,
            n_generations=n_generations,
        )
        logger.info(
            "Wave dispatch %s: generation %d (%d survivors) -> generation %d job %s",
            simulation.config.experiment_id,
            wave.wave_index,
            len(survivors),
            next_generation,
            next_job_id,
        )

    async def close(self) -> None:
        await self.stop_polling()
        logger.debug("Closing messaging service connection")
        await self.messaging_service.disconnect()
