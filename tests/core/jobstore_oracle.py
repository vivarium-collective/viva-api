"""The conformance oracle for the JobStore seam (plan P4b): every store, in memory or over a table,
must satisfy it. Not a test module -- the adapter tests import it and run it against their store.

``seed`` is how the TEST puts a row in: the seam has no ``put`` (an interim adapter over an
existing table cannot insert a ``Job``), so the in-memory store seeds itself and an adapter test
seeds through the table's own writer (``insert_hpcrun``, ``record_task``, ``insert_task``) on the
Postgres testcontainer and hands back the ``Job`` the store will read. The oracle only ever
compares what it seeded with what the store answers, so a table that cannot represent some field
(a status the table has no value for) is a seeding choice, not a hidden failure.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from viva_core.models import JobStatus
from viva_core.tasks import ACTIVE_STATUSES, Job, JobIsTerminal, JobReader, JobStore

Seed = Callable[[Job], Awaitable[Job]]


def _ids(jobs: list[Job]) -> set[str]:
    return {job.id for job in jobs}


async def assert_job_reader_conformance(store: JobReader, seed: Seed, *, owner_kind: str, other_kind: str) -> None:
    """``get`` round-trips a record; ``by_owner`` returns exactly the owner's rows; ``by_kind_status``
    filters by status set, kind and backend. ``owner_kind`` / ``other_kind`` are two owner kinds the
    store can represent (an application's are its table names)."""
    a1 = await seed(
        Job(id="a1", kind="task", owner_kind=owner_kind, owner_id="A", backend="ray", external_job_ids=("b-1",))
    )
    a2 = await seed(Job(id="a2", kind="task", owner_kind=owner_kind, owner_id="A", status=JobStatus.RUNNING))
    b1 = await seed(Job(id="b1", kind="member", owner_kind=other_kind, owner_id="B", status=JobStatus.COMPLETED))
    for seeded in (a1, a2, b1):
        found = await store.get(seeded.id)
        assert found is not None and found == seeded, (found, seeded)
    assert await store.get("no-such-job") is None

    assert _ids(await store.by_owner(owner_kind, "A")) == {a1.id, a2.id}
    assert _ids(await store.by_owner(other_kind, "B")) == {b1.id}
    assert await store.by_owner(owner_kind, "nobody") == []

    active = await store.by_kind_status(ACTIVE_STATUSES)
    assert {a1.id, a2.id} <= _ids(active) and b1.id not in _ids(active)
    assert _ids(await store.by_kind_status({JobStatus.RUNNING}, kind="task")) >= {a2.id}
    assert a1.id not in _ids(await store.by_kind_status({JobStatus.RUNNING}, kind="task"))
    assert _ids(await store.by_kind_status(ACTIVE_STATUSES, kind="member")) == set()
    assert _ids(await store.by_kind_status(ACTIVE_STATUSES, backend="ray")) >= {a1.id}
    assert a2.id not in _ids(await store.by_kind_status(ACTIVE_STATUSES, backend="ray"))


async def assert_job_store_conformance(store: JobStore, seed: Seed, *, owner_kind: str) -> None:
    """``set_status`` transitions and ``get`` sees it; a terminal job refuses to leave its status;
    ``set_external_job_ids`` replaces the handles; unknown ids raise ``KeyError``."""
    job = await seed(Job(id="s1", kind="task", owner_kind=owner_kind, owner_id="S"))
    running = await store.set_status(job.id, JobStatus.RUNNING)
    assert running.status is JobStatus.RUNNING
    found = await store.get(job.id)
    assert found is not None and found.status is JobStatus.RUNNING

    handled = await store.set_external_job_ids(job.id, ["h-1", "h-2"])
    assert handled.external_job_ids == ("h-1", "h-2")
    found = await store.get(job.id)
    assert found is not None and found.external_job_ids == ("h-1", "h-2")

    done = await store.set_status(job.id, JobStatus.COMPLETED)
    assert done.status is JobStatus.COMPLETED and done.status.is_terminal
    with pytest.raises(JobIsTerminal):
        await store.set_status(job.id, JobStatus.RUNNING)
    found = await store.get(job.id)
    assert found is not None and found.status is JobStatus.COMPLETED

    with pytest.raises(KeyError):
        await store.set_status("no-such-job", JobStatus.RUNNING)
    with pytest.raises(KeyError):
        await store.set_external_job_ids("no-such-job", [])
