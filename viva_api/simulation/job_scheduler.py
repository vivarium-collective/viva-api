import asyncio
import logging

from async_lru import alru_cache

from viva_api.common.hpc.job_service import JobStatusUpdate
from viva_api.common.hpc.slurm_service import SlurmService
from viva_api.common.messaging.messaging_service import MessagingService
from viva_api.common.models import JobBackend, JobStatus, SSHTarget
from viva_api.config import get_settings
from viva_api.dependencies import get_ssh_session_service
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import WorkerEvent, WorkerEventMessagePayload

logger = logging.getLogger(__name__)


class JobScheduler:
    database_service: DatabaseService
    slurm_service: SlurmService | None
    messaging_service: MessagingService
    _polling_task: asyncio.Task[None] | None = None
    _stop_event: asyncio.Event

    def __init__(
        self,
        messaging_service: MessagingService,
        database_service: DatabaseService,
        slurm_service: SlurmService | None = None,
    ):
        self.messaging_service = messaging_service
        self.database_service = database_service
        self.slurm_service = slurm_service
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

    async def close(self) -> None:
        await self.stop_polling()
        logger.debug("Closing messaging service connection")
        await self.messaging_service.disconnect()
