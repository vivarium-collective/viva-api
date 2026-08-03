"""Async job monitor for compose simulation jobs — SLURM polling + optional NATS events."""

import asyncio
import logging
from asyncio import Queue
from typing import Any

from async_lru import alru_cache

from sms_api.common.hpc.slurm_service import SlurmService
from sms_api.common.models import JobBackend, SSHTarget
from sms_api.compose.database_service import ComposeDatabaseService
from sms_api.compose.models import ComposeHpcRun, ComposeJobStatus, ComposeWorkerEvent, ComposeWorkerEventMessagePayload
from sms_api.config import ComputeBackend, get_settings
from sms_api.dependencies import get_ssh_session_service

logger = logging.getLogger(__name__)


class ComposeJobMonitor:
    database_service: ComposeDatabaseService
    nats_client: Any | None  # nats.aio.client.Client or None
    internal_listeners: dict[int, Queue[ComposeHpcRun]]
    _polling_task: asyncio.Task[None] | None = None
    _stop_event: asyncio.Event

    def __init__(
        self,
        nats_client: Any | None,
        database_service: ComposeDatabaseService,
        sim_registry: "dict[ComputeBackend, Any] | None" = None,
    ) -> None:
        self.nats_client = nats_client
        self.database_service = database_service
        # Per-backend compose services so non-SLURM (Ray/Batch) running jobs can be
        # polled via their own get_job_status (describe_jobs) instead of squeue.
        self.sim_registry = sim_registry or {}
        self.internal_listeners = {}
        self._stop_event = asyncio.Event()

    @alru_cache
    async def get_hpcrun_by_correlation_id(self, correlation_id: str) -> int | None:
        return await self.database_service.get_hpc_db().get_hpcrun_id_by_correlation_id(correlation_id=correlation_id)

    async def subscribe_nats(self) -> None:
        if self.nats_client is None:
            raise RuntimeError("NATS client is not set")
        subject = get_settings().compose_nats_worker_event_subject
        logger.info(f"Subscribing to NATS messages for subject '{subject}'")

        async def message_handler(msg: Any) -> Any:
            data = msg.data.decode("utf-8")
            logger.info(f"Received NATS message on '{msg.subject}': {data}")
            payload = ComposeWorkerEventMessagePayload.model_validate_json(data)
            worker_event = ComposeWorkerEvent.from_message_payload(payload)
            hpcrun_id = await self.get_hpcrun_by_correlation_id(correlation_id=worker_event.correlation_id)
            if hpcrun_id is None:
                logger.error(f"No ComposeHpcRun found for correlation ID {worker_event.correlation_id}")
                return
            await self.database_service.get_hpc_db().insert_worker_event(worker_event, hpcrun_id=hpcrun_id)

        await self.nats_client.subscribe(subject=subject, cb=message_handler)

    async def start_polling(self, interval_seconds: int = 30) -> None:
        if self._polling_task is not None and not self._polling_task.done():
            return
        self._stop_event.clear()
        self._polling_task = asyncio.create_task(self._polling_loop(interval_seconds))
        logger.info("Started compose job status polling task.")

    async def stop_polling(self) -> None:
        self._stop_event.set()
        if self._polling_task:
            await self._polling_task

    async def _polling_loop(self, interval_seconds: int) -> None:
        while not self._stop_event.is_set():
            try:
                await self.update_running_jobs()
            except Exception:
                logger.exception("Error during compose job polling")
            await asyncio.sleep(interval_seconds)

    async def update_running_jobs(self) -> None:
        running_jobs = await self.database_service.get_hpc_db().list_running_hpcruns()
        if not running_jobs:
            return
        # Backend split: SLURM runs are polled via squeue over SSH; Ray/Batch runs are
        # polled via their own service's get_job_status (describe_jobs) — no SSH needed
        # (and none available on Stanford).
        slurm_runs = [j for j in running_jobs if j.job_backend == JobBackend.SLURM.value]
        backend_runs = [j for j in running_jobs if j.job_backend != JobBackend.SLURM.value]
        await self._update_backend_jobs(backend_runs)
        await self._update_slurm_jobs(slurm_runs)

    async def _update_backend_jobs(self, running_jobs: list[ComposeHpcRun]) -> None:
        for hpc_run in running_jobs:
            service = self.sim_registry.get(ComputeBackend(hpc_run.job_backend)) if hpc_run.job_backend else None
            if service is None or not hpc_run.job_id_ext:
                continue
            try:
                new_status = await service.get_job_status(hpc_run.job_id_ext)
            except Exception:
                logger.exception("Error polling backend status for ComposeHpcRun %s", hpc_run.database_id)
                continue
            if new_status is not None and new_status != hpc_run.status:
                if new_status == ComposeJobStatus.FAILED:
                    await self.database_service.get_hpc_db().mark_hpcrun_failed(
                        hpc_run.database_id, "backend job reported FAILED"
                    )
                else:
                    await self.database_service.get_hpc_db().update_hpcrun_dispatch(
                        hpc_run.database_id,
                        job_id_ext=hpc_run.job_id_ext,
                        backend=JobBackend(hpc_run.job_backend),
                        status=new_status,
                    )

    async def _update_slurm_jobs(self, running_jobs: list[ComposeHpcRun]) -> None:
        if not running_jobs:
            return
        job_ids = [job.slurmjobid for job in running_jobs if job.slurmjobid]
        if not job_ids:
            return

        slurm_service = SlurmService()
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            slurm_jobs_squeue = await slurm_service.get_job_status_squeue(ssh, job_ids)
            slurm_jobs_sacct = await slurm_service.get_job_status_scontrol(ssh, job_ids)

        slurm_job_map = {job.job_id: job for job in slurm_jobs_squeue}
        slurm_job_map.update({job.job_id: job for job in slurm_jobs_sacct})

        for hpc_run in running_jobs:
            slurm_job = slurm_job_map.get(hpc_run.slurmjobid)
            if not slurm_job or not slurm_job.job_state:
                continue
            try:
                new_status = ComposeJobStatus(slurm_job.job_state.lower())
                if new_status != hpc_run.status:
                    await self.database_service.get_hpc_db().update_hpcrun_status(
                        hpcrun_id=hpc_run.database_id, new_slurm_job=slurm_job
                    )
            except ValueError:
                logger.exception(f"Error updating ComposeHpcRun {hpc_run.database_id}")

            if slurm_job.job_id in self.internal_listeners:
                self.internal_listeners[slurm_job.job_id].put_nowait(hpc_run)

    def internal_subscribe(self, queue: Queue[ComposeHpcRun], job_id: int) -> None:
        self.internal_listeners[job_id] = queue

    def internal_unsubscribe(self, job_id: int) -> None:
        self.internal_listeners.pop(job_id, None)

    async def close(self) -> None:
        await self.stop_polling()
        if self.nats_client:
            await self.nats_client.close()
