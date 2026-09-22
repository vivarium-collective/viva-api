"""Async job monitor for compose simulation jobs — SLURM polling + optional NATS events."""

import asyncio
import contextlib
import logging
from asyncio import Queue
from collections.abc import Awaitable, Callable, Mapping
from typing import Protocol

from viva_core.backends.slurm_service import SlurmService
from viva_core.compose.database_service import ComposeDatabaseService
from viva_core.compose.models import (
    ComposeHpcRun,
    ComposeJobStatus,
    ComposeWorkerEvent,
    ComposeWorkerEventMessagePayload,
)
from viva_core.compose.service import ComposeSimulationService
from viva_core.infra.ssh.ssh_service import SSHSessionService
from viva_core.models import ComputeBackend, JobBackend
from viva_core.settings import get_core_settings as get_settings

logger = logging.getLogger(__name__)


class WorkerEventMessage(Protocol):
    """What the monitor reads of a message a worker published."""

    @property
    def data(self) -> bytes: ...
    @property
    def subject(self) -> str: ...


class WorkerEventBus(Protocol):
    """What the monitor asks of a message bus -- a ``nats.aio.client.Client`` today."""

    async def subscribe(self, subject: str, *, cb: Callable[[WorkerEventMessage], Awaitable[None]]) -> object: ...

    async def close(self) -> None: ...


class ComposeJobMonitor:
    database_service: ComposeDatabaseService
    nats_client: WorkerEventBus | None
    internal_listeners: dict[int, Queue[ComposeHpcRun]]
    _polling_task: asyncio.Task[None] | None = None
    _stop_event: asyncio.Event

    def __init__(
        self,
        nats_client: WorkerEventBus | None,
        database_service: ComposeDatabaseService,
        sim_registry: Mapping[ComputeBackend, ComposeSimulationService] | None = None,
        slurm_ssh: Callable[[], SSHSessionService] | None = None,
    ) -> None:
        self.nats_client = nats_client
        self.database_service = database_service
        # The registry of compose services, by backend -- the router reads it too, to honour a
        # per-request ``compute_backend``. Here it is what lets non-SLURM (Ray/Batch) running jobs be
        # polled via their own get_job_status (describe_jobs) instead of squeue.
        self.sim_registry = sim_registry or {}
        # The SSH sessions SLURM is polled over, HANDED IN as a provider (P3d-3): the session service
        # is the application's to build, and a site without SLURM never has one.
        self._slurm_ssh = slurm_ssh
        self.internal_listeners = {}
        self._stop_event = asyncio.Event()
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
            hpcrun_id = await self.database_service.get_hpc_db().get_hpcrun_id_by_correlation_id(
                correlation_id=correlation_id
            )
            if hpcrun_id is not None:
                self._hpcrun_ids_by_correlation[correlation_id] = hpcrun_id
        return hpcrun_id

    async def subscribe_nats(self) -> None:
        if self.nats_client is None:
            raise RuntimeError("NATS client is not set")
        subject = get_settings().compose_nats_worker_event_subject
        logger.info(f"Subscribing to NATS messages for subject '{subject}'")

        async def message_handler(msg: WorkerEventMessage) -> None:
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
            self._polling_task = None
            logger.info("Stopped compose job status polling task.")

    async def _polling_loop(self, interval_seconds: int) -> None:
        while not self._stop_event.is_set():
            try:
                await self.update_running_jobs()
            except Exception:
                logger.exception("Error during compose job polling")
            # Sleep on the stop event, not the clock: stop_polling() then returns at once
            # instead of holding shutdown for the rest of a 30 s interval.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval_seconds)

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
        if self._slurm_ssh is None:
            raise RuntimeError("No SLURM SSH session provider was handed to the compose job monitor.")
        async with self._slurm_ssh().session() as ssh:
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
