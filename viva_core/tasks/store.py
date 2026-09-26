"""The JobStore seam (plan P4b, reshaped per D15): one owner-referenced ``Job`` record and the two
Protocols every "run something" is read and driven through -- so the task subsystems, the
campaign driver (P4c) and, at P7, the job records themselves converge on a single core shape.

Domain-neutral and standalone: this imports nothing from ``viva_api`` (the ``core-is-standalone``
contract) and names no application concept. A job's *owner* is a two-part reference --
``owner_kind`` (the owning record's kind: in an application, the owning table's name; in core,
one of the constants below) and ``owner_id`` -- rather than a foreign key per table, so a task
run, a campaign member or an env-worker call are the same record with a different owner, and
cancelling an owner is a fold over its jobs, not per-mechanism routing. ``owner_kind`` is a plain
string on purpose: an application adds a kind by writing rows, never by changing core (the P4a-1
rule, viva-api#790).

Two Protocols, because the interim stores are adapters over tables that already exist:

* :class:`JobReader` -- ``get`` / ``by_owner`` / ``by_kind_status``: what the campaign driver, the
  scheduler's ticks and ``/viva/v1/jobs`` need. Every adapter implements it.
* :class:`JobStore` -- a reader that can also ``set_status`` and ``set_external_job_ids``. There
  is **no ``put``** here: an ``hpcrun`` row needs a ``JobId``, a job type and a reference the
  ``Job`` record does not carry, so the interim adapters cannot insert -- the application's
  dispatchers keep inserting their own rows until P7, whose ``core.job`` store is the first that
  creates records through this seam.

Superseding viva-api#776 (eagmon), whose ``Job``, Protocol and conformance oracle this keeps; what
changed and why is the 2026-09-26 log entry.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import Protocol

from viva_core.models import JobStatus

#: Owner kinds core names itself. An application's kinds are its own (its table names) and are
#: never enumerated here.
OWNER_TASK = "task"
OWNER_ENV_WORKER = "env_worker"
OWNER_CAMPAIGN = "campaign"

#: Statuses a job is still in flight in -- what a tick asks a store for.
ACTIVE_STATUSES: frozenset[JobStatus] = frozenset({
    JobStatus.WAITING,
    JobStatus.PENDING,
    JobStatus.QUEUED,
    JobStatus.RUNNING,
})


class JobIsTerminal(RuntimeError):
    """A transition out of a terminal status was asked for. A finished job never changes again
    (``JobStatus.is_terminal``); a caller that wants to re-run makes a new job."""


@dataclass(frozen=True, slots=True)
class Job:
    """One run's record, owner-referenced -- the single shape the task subsystems and, at P7, the
    ``hpcrun`` / ``task`` / ``compose_hpcrun`` / ``env_worker_task`` rows converge on.

    ``id`` is the store's own identity as text (an adapter over one table renders its row id; a
    composite store over several prefixes it with the table). ``kind`` is what the job IS
    (``simulation``, ``analysis``, ``task``, ``env_worker``, ``build`` ...), ``backend`` the
    mechanism its handles belong to (a ``JobBackend`` value) -- the scheduler's ticks and the cancel
    fold select on both. ``external_job_ids`` are the live handles the backend knows the job by;
    ``companion_job_ids`` are what must die with it (viva-api#710) and are cancelled first."""

    id: str
    kind: str
    owner_kind: str
    owner_id: str
    status: JobStatus = JobStatus.QUEUED
    backend: str | None = None
    external_job_ids: tuple[str, ...] = ()
    companion_job_ids: tuple[str, ...] = ()
    output_uri: str | None = None
    trace_id: str | None = None


class JobReader(Protocol):
    """Reads over jobs. Async, because every real store here is SQLAlchemy."""

    async def get(self, job_id: str) -> Job | None:
        """The job with ``job_id``, or ``None``."""
        ...

    async def by_owner(self, owner_kind: str, owner_id: str) -> list[Job]:
        """Every job belonging to ``(owner_kind, owner_id)`` -- the query behind campaign progress
        and cancel."""
        ...

    async def by_kind_status(
        self, statuses: Collection[JobStatus], *, kind: str | None = None, backend: str | None = None
    ) -> list[Job]:
        """Every job in one of ``statuses``, narrowed to a ``kind`` and / or a ``backend`` -- the
        scheduler's tick query ("the active Nextflow heads", "the running tasks")."""
        ...


class JobStore(JobReader, Protocol):
    """A reader that can also drive a job's status and handles.

    ``set_status`` raises :class:`JobIsTerminal` when the job is already in a terminal status: a
    finished job never changes again. ``set_external_job_ids`` records the handles a dispatch
    obtained after the row existed (a build's jobs, a campaign's members)."""

    async def set_status(self, job_id: str, status: JobStatus) -> Job:
        """Transition ``job_id`` to ``status`` and return the updated record; ``KeyError`` for an
        unknown id, :class:`JobIsTerminal` when it has already finished."""
        ...

    async def set_external_job_ids(self, job_id: str, job_ids: Sequence[str]) -> Job:
        """Replace the job's live handles and return the updated record; ``KeyError`` for an
        unknown id."""
        ...
