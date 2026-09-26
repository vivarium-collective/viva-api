"""An in-memory ``JobStore`` -- the conformance target and a test double, and nothing else.

Fenced here, away from ``store.py``, on purpose: this repository's in-memory failures (an
``alru_cache`` that cached ``None`` and dropped worker events, a ``BackgroundTask`` that stranded
rows, viva-api#414) were all an in-memory thing that ended up on the runtime path. A
``tests/core/`` guard fails if anything outside ``tests/`` imports this module; a container with
no job store answers 503 by name, never with one of these.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import replace

from viva_core.models import JobStatus
from viva_core.tasks.store import Job, JobIsTerminal


class InMemoryJobStore:
    """The reference ``JobStore``. ``seed`` is how a test puts a row in -- the seam itself has no
    ``put`` (see ``store.py``); the oracle takes a seeding callable, and this is the in-memory one."""

    def __init__(self) -> None:
        self._rows: dict[str, Job] = {}

    async def seed(self, job: Job) -> Job:
        self._rows[job.id] = job
        return job

    async def get(self, job_id: str) -> Job | None:
        return self._rows.get(job_id)

    async def by_owner(self, owner_kind: str, owner_id: str) -> list[Job]:
        return [job for job in self._rows.values() if job.owner_kind == owner_kind and job.owner_id == owner_id]

    async def by_kind_status(
        self, statuses: Collection[JobStatus], *, kind: str | None = None, backend: str | None = None
    ) -> list[Job]:
        return [
            job
            for job in self._rows.values()
            if job.status in statuses
            and (kind is None or job.kind == kind)
            and (backend is None or job.backend == backend)
        ]

    async def set_status(self, job_id: str, status: JobStatus) -> Job:
        current = self._rows[job_id]
        if current.status.is_terminal:
            raise JobIsTerminal(f"job {job_id} is {current.status.value}; a finished job never changes again")
        updated = replace(current, status=status)
        self._rows[job_id] = updated
        return updated

    async def set_external_job_ids(self, job_id: str, job_ids: Sequence[str]) -> Job:
        updated = replace(self._rows[job_id], external_job_ids=tuple(job_ids))
        self._rows[job_id] = updated
        return updated
