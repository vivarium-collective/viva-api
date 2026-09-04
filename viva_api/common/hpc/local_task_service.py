"""In-process async task tracker for short-lived background operations.

Used for operations like DooD Docker image builds that should be async from
the API caller's perspective but don't have an external job scheduler of their
own (no SLURM, no K8s Job): the LOCAL task submits the real work to AWS Batch
and polls it. Tasks are tracked by UUID in an in-memory dict and are lost on
pod restart -- which is exactly the problem viva-api#414 describes: the DB row
that points at a LOCAL id stayed ``running`` forever once the process that
owned the task was gone, while the external work it was watching finished
normally.

Two things here make that recoverable:

- **A task's durable record is bound to it** (``bind_hpcrun``): the caller
  that inserts the ``HpcRun`` row for a LOCAL id tells this service, and from
  then on (a) the row is finalized from the task's own outcome when it ends,
  and (b) the task can persist the external handles it is watching
  (``record_external_job_ids`` -> ``hpcrun.external_job_ids``), so the work is
  addressable from ANY process, not only the one holding the ``asyncio.Task``.
- **Ownership is queryable** (``owns``): ``JobScheduler.reconcile_local_tasks``
  scans active LOCAL rows on every tick and, for every one this process does
  NOT own, finishes the row from the external work's true terminal state
  instead of leaving it indistinguishable from healthy work.
"""

import asyncio
import contextlib
import contextvars
import datetime
import logging
import uuid
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from viva_api.common.hpc.job_service import JobStatusInfo, JobStatusUpdate
from viva_api.common.models import JobId, JobStatus

if TYPE_CHECKING:
    from viva_api.simulation.database_service import DatabaseService

logger = logging.getLogger(__name__)

# The LOCAL task id of the task currently executing, set in the context
# ``submit`` runs every coroutine in. A coroutine that was NOT started through
# ``submit`` (a unit test calling ``_run_build`` directly, say) sees ``None``
# and ``record_external_job_ids`` becomes a logged no-op.
_current_local_task_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("viva_local_task_id", default=None)


@dataclass(frozen=True)
class _Binding:
    hpcrun_id: int
    database_service: "DatabaseService"


class LocalTaskService:
    """Track in-process async tasks by UUID."""

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._bindings: dict[str, _Binding] = {}
        # External (AWS Batch) job ids a task has recorded, kept here so a
        # binding that lands AFTER the record can still persist them.
        self._external_job_ids: dict[str, list[str]] = {}
        # The in-flight finalize write per bound task, so a caller (tests,
        # shutdown) can wait for the row to actually be written.
        self._finalizers: dict[str, asyncio.Future[None]] = {}

    def submit(self, coro: Coroutine[Any, Any, Any], name: str | None = None) -> JobId:
        """Spawn an async task and return a LOCAL JobId for tracking.

        Args:
            coro: The coroutine to run in the background.
            name: Optional human-readable name (for logging).

        Returns:
            A JobId.local(uuid) that can be used to check status or cancel.
        """
        task_id = str(uuid.uuid4())[:8]
        # Run the coroutine itself as the task (no wrapper: a wrapper cancelled
        # before its first step would leave `coro` never awaited), in a context
        # that carries its own LOCAL id for record_external_job_ids.
        context = contextvars.copy_context()
        context.run(_current_local_task_id.set, task_id)
        task = asyncio.create_task(coro, name=name or f"local-{task_id}", context=context)
        task.add_done_callback(lambda t: self._on_done(task_id, t))
        self._tasks[task_id] = task
        logger.info(f"Spawned local task {task_id} ({name})")
        return JobId.local(task_id)

    @staticmethod
    def current_task_id() -> str | None:
        """The LOCAL task id of the task running this coroutine, or None if it
        was not started through ``submit``."""
        return _current_local_task_id.get()

    def owns(self, task_id: str) -> bool:
        """Whether THIS process holds the asyncio.Task for ``task_id``.

        The question ``JobScheduler.reconcile_local_tasks`` asks of every active
        LOCAL HpcRun row: a row whose id nobody owns is an orphan (its owner
        died with a previous pod) and must be finished from external truth.
        """
        return task_id in self._tasks

    async def bind_hpcrun(self, task_id: str, hpcrun_id: int, database_service: "DatabaseService") -> bool:
        """Attach the durable ``HpcRun`` row for a LOCAL task.

        Once bound: the row is finalized (COMPLETED / FAILED with the exception
        text / CANCELLED, plus ``end_time``) when the task ends -- the
        done-callback the build handler used to register by hand -- and the
        external job ids the task records are persisted onto it. Ids the task
        recorded BEFORE this call are persisted right here, so the task itself
        never has to wait for (or even know about) the binding. Returns False
        (and does nothing) if the task is unknown.

        Safe to call after the task has already finished: ``add_done_callback``
        on a done task schedules the callback immediately.
        """
        task = self._tasks.get(task_id)
        if task is None:
            logger.warning("bind_hpcrun: no local task %s to bind HpcRun %s to", task_id, hpcrun_id)
            return False
        self._bindings[task_id] = _Binding(hpcrun_id=hpcrun_id, database_service=database_service)
        task.add_done_callback(
            lambda t: self._finalizers.__setitem__(task_id, asyncio.ensure_future(self._finalize_hpcrun(task_id, t)))
        )
        pending = self._external_job_ids.get(task_id)
        if pending:
            await self._persist_external_job_ids(task_id, pending)
        return True

    async def wait_finalized(self, task_id: str) -> None:
        """Wait until the task has ended AND its bound row's terminal status
        has been written (a no-op for an unbound or unknown task). The task's
        own exception is swallowed here; read it via ``get_status``."""
        task = self._tasks.get(task_id)
        if task is None:
            return
        # The task's own outcome is the row's business (and get_status's), not
        # this waiter's -- swallow it, including CancelledError from cancel().
        with contextlib.suppress(BaseException):
            await task
        if task_id not in self._bindings:
            return
        # the done-callback is scheduled with call_soon; give the loop a turn
        for _ in range(10):
            if task_id in self._finalizers:
                break
            await asyncio.sleep(0)
        finalizer = self._finalizers.get(task_id)
        if finalizer is not None:
            await finalizer

    def bound_hpcrun_id(self, task_id: str) -> int | None:
        """The HpcRun id bound to ``task_id``, or None if unbound/unknown."""
        binding = self._bindings.get(task_id)
        return binding.hpcrun_id if binding is not None else None

    async def record_external_job_ids(self, job_ids: list[str]) -> bool:
        """Called FROM INSIDE a running local task: record the external (AWS
        Batch) job ids this task is now watching, and persist them onto its
        bound HpcRun row, so a recovering process can find the work without
        re-deriving it (viva-api#414, "persist the external handle").

        If the row is not bound yet (the caller's insert is still in flight),
        the ids are held here and ``bind_hpcrun`` persists them the moment it
        runs. Never raises: persistence is best-effort and must not fail the
        task's real work. Returns False only when not running inside a
        ``submit``-ted task at all.
        """
        task_id = _current_local_task_id.get()
        if task_id is None or task_id not in self._tasks:
            logger.debug("record_external_job_ids(%s): not running inside a local task; skipping", job_ids)
            return False
        self._external_job_ids[task_id] = list(job_ids)
        if task_id in self._bindings:
            await self._persist_external_job_ids(task_id, list(job_ids))
        return True

    async def _persist_external_job_ids(self, task_id: str, job_ids: list[str]) -> bool:
        binding = self._bindings.get(task_id)
        if binding is None:
            return False
        try:
            await binding.database_service.set_hpcrun_external_job_ids(binding.hpcrun_id, job_ids)
        except Exception:
            logger.exception("Failed to persist external job ids %s for HpcRun %s", job_ids, binding.hpcrun_id)
            return False
        logger.info("Local task %s -> HpcRun %s now points at external jobs %s", task_id, binding.hpcrun_id, job_ids)
        return True

    async def _finalize_hpcrun(self, task_id: str, task: asyncio.Task[Any]) -> None:
        """Write the task's own terminal outcome to its bound HpcRun row."""
        binding = self._bindings.get(task_id)
        if binding is None:
            return
        error: str | None = None
        if task.cancelled():
            status = JobStatus.CANCELLED
        else:
            exc = task.exception()
            if exc is not None:
                status = JobStatus.FAILED
                error = str(exc) or type(exc).__name__
            else:
                status = JobStatus.COMPLETED
        try:
            await binding.database_service.update_hpcrun_status(
                hpcrun_id=binding.hpcrun_id,
                update=JobStatusUpdate(
                    job_id=JobId.local(task_id),
                    status=status,
                    end_time=datetime.datetime.now().isoformat(),
                    error_message=error,
                ),
            )
        except Exception:
            logger.exception("Failed to update HpcRun %s status to %s", binding.hpcrun_id, status)

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
            self._bindings.pop(tid, None)
            self._external_job_ids.pop(tid, None)
            self._finalizers.pop(tid, None)
        return len(done_ids)
