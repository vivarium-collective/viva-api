"""In-process async task tracker for short-lived background operations.

Used for operations like SSH-based Docker image builds that should be
async from the API caller's perspective but don't have an external job
scheduler (no SLURM, no K8s Job). Tasks are tracked by UUID in an
in-memory dict and are lost on pod restart.
"""

import asyncio
import logging
import uuid
from collections.abc import Coroutine
from typing import Any

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.models import JobId, JobStatus

logger = logging.getLogger(__name__)


class LocalTaskService:
    """Track in-process async tasks by UUID."""

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task[Any]] = {}

    def submit(self, coro: Coroutine[Any, Any, Any], name: str | None = None) -> JobId:
        """Spawn an async task and return a LOCAL JobId for tracking.

        Args:
            coro: The coroutine to run in the background.
            name: Optional human-readable name (for logging).

        Returns:
            A JobId.local(uuid) that can be used to check status or cancel.
        """
        task_id = str(uuid.uuid4())[:8]
        task = asyncio.create_task(coro, name=name or f"local-{task_id}")
        task.add_done_callback(lambda t: self._on_done(task_id, t))
        self._tasks[task_id] = task
        logger.info(f"Spawned local task {task_id} ({name})")
        return JobId.local(task_id)

    def get_status(self, task_id: str) -> JobStatusInfo | None:
        """Get the status of a local task."""
        task = self._tasks.get(task_id)
        if task is None:
            return None

        job_id = JobId.local(task_id)

        if task.done():
            if task.cancelled():
                return JobStatusInfo(job_id=job_id, status=JobStatus.CANCELLED)
            exc = task.exception()
            if exc is not None:
                return JobStatusInfo(
                    job_id=job_id,
                    status=JobStatus.FAILED,
                    error_message=str(exc),
                )
            return JobStatusInfo(job_id=job_id, status=JobStatus.COMPLETED)

        return JobStatusInfo(job_id=job_id, status=JobStatus.RUNNING)

    def cancel(self, task_id: str) -> bool:
        """Cancel a local task. Returns True if cancelled, False if not found or already done."""
        task = self._tasks.get(task_id)
        if task is None or task.done():
            return False
        task.cancel()
        logger.info(f"Cancelled local task {task_id}")
        return True

    def _on_done(self, task_id: str, task: asyncio.Task[Any]) -> None:
        """Log completion. Tasks stay in the dict for status queries."""
        if task.cancelled():
            logger.info(f"Local task {task_id} was cancelled")
        elif task.exception():
            logger.error(f"Local task {task_id} failed: {task.exception()}")
        else:
            logger.info(f"Local task {task_id} completed")

    def cleanup_completed(self) -> int:
        """Remove completed/failed/cancelled tasks from the dict. Returns count removed."""
        done_ids = [tid for tid, t in self._tasks.items() if t.done()]
        for tid in done_ids:
            del self._tasks[tid]
        return len(done_ids)
