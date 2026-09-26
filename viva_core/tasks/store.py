"""The JobStore seam (plan P4b): one Protocol and one owner-ref ``Job`` record
that every "run something" persists behind -- so the two task subsystems, and
eventually the job records themselves, converge on a single core shape.

Domain-neutral and standalone: this imports nothing from ``viva_api`` (the
``core-is-standalone`` contract) and names no application concept. A job's
*owner* is a two-part reference (``owner_kind`` + ``owner_id``) rather than a
foreign key per table, so a task run, a campaign member, or an env-worker run
are the same record with a different owner -- and cancelling an owner is a fold
over its jobs, not per-mechanism routing.

This module defines the seam only: the ``Job`` record, the ``JobStore``
Protocol, and an in-memory reference store. The SMS-side adapters that put the
existing ``hpcrun`` and ``task`` tables behind this Protocol -- keeping those
tables authoritative until the P7 collapse to one ``core.job`` -- are the next
step. Their intended shape:

* ``TaskJobStore`` wraps ``DatabaseService.record_task`` / ``get_task`` /
  ``update_task_status`` over ``ORMTask``; ``owner_kind = TASK``; status maps via
  ``TaskStatusDB.to_job_status()`` / ``from_job_status()``.
* ``HpcRunJobStore`` wraps ``insert_hpcrun`` / ``get_hpcrun`` over ``ORMHpcRun``;
  ``owner_kind = CAMPAIGN`` for chain/campaign rows (else ``USER``); status maps
  via ``JobStatusDB.to_job_status()``; ``external_id`` from ``job_id_ext`` (and,
  for a campaign, its members' ``external_job_ids``).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Protocol, runtime_checkable

from viva_core.models import JobStatus, StrEnumBase


class JobOwnerKind(StrEnumBase):
    """Who a job belongs to. Domain-neutral: a campaign, a task fan-out, an
    env-worker batch, or a user are all owners of the same ``Job`` record."""

    USER = "user"
    TASK = "task"
    CAMPAIGN = "campaign"
    ENV_WORKER = "env_worker"


@dataclass(frozen=True, slots=True)
class Job:
    """One run's record, owner-referenced -- the single shape the two task
    subsystems (and eventually ``hpcrun`` / ``task`` / ``compose_hpcrun`` /
    ``env_worker_task``) converge on."""

    id: str
    kind: str
    owner_kind: JobOwnerKind
    owner_id: str | None
    status: JobStatus = JobStatus.QUEUED
    external_id: str | None = None  # the live backend handle (Batch/Ray/k8s), for cancel
    output_uri: str | None = None


@runtime_checkable
class JobStore(Protocol):
    """The seam. Async, because every real store here is async (SQLAlchemy).
    P4b's interim adapters implement this over the existing tables; P7 swaps the
    store for one ``core.job`` table without changing a caller."""

    async def put(self, job: Job) -> Job:
        """Insert or replace ``job`` and return the stored record."""
        ...

    async def get(self, job_id: str) -> Job:
        """Return the job with ``job_id``; raise ``KeyError`` if absent."""
        ...

    async def by_owner(self, owner_kind: JobOwnerKind, owner_id: str) -> list[Job]:
        """Every job belonging to ``(owner_kind, owner_id)`` -- the query behind
        campaign progress and cancel."""
        ...

    async def set_status(self, job_id: str, status: JobStatus) -> Job:
        """Transition ``job_id`` to ``status`` and return the updated record."""
        ...


class InMemoryJobStore:
    """Reference ``JobStore`` -- the conformance target and a test double. The
    SMS adapters are the production stores; this proves the shape and lets
    callers (and the cancel fold) be written and tested without a database."""

    def __init__(self) -> None:
        self._rows: dict[str, Job] = {}

    async def put(self, job: Job) -> Job:
        self._rows[job.id] = job
        return job

    async def get(self, job_id: str) -> Job:
        return self._rows[job_id]

    async def by_owner(self, owner_kind: JobOwnerKind, owner_id: str) -> list[Job]:
        return [job for job in self._rows.values() if job.owner_kind == owner_kind and job.owner_id == owner_id]

    async def set_status(self, job_id: str, status: JobStatus) -> Job:
        updated = replace(self._rows[job_id], status=status)
        self._rows[job_id] = updated
        return updated
