"""Conformance + behaviour for the P4b JobStore seam (``viva_core.tasks``).

Pure and offline -- no database, no Docker. ``assert_jobstore_conformance`` is
the reusable oracle the SMS adapter tests (``HpcRunJobStore`` / ``TaskJobStore``)
will run against a real store."""

from __future__ import annotations

import pytest

from viva_core.models import JobStatus
from viva_core.tasks import InMemoryJobStore, Job, JobOwnerKind, JobStore


async def assert_jobstore_conformance(store: JobStore) -> None:
    """Every ``JobStore`` must satisfy this -- reused by the adapter tests."""
    job = await store.put(Job(id="j1", kind="task", owner_kind=JobOwnerKind.TASK, owner_id="t1"))
    assert job.status is JobStatus.QUEUED
    assert (await store.get("j1")).id == "j1"

    await store.put(Job(id="j2", kind="task", owner_kind=JobOwnerKind.TASK, owner_id="t1"))
    await store.put(Job(id="j3", kind="seed", owner_kind=JobOwnerKind.CAMPAIGN, owner_id="c1"))
    owned = await store.by_owner(JobOwnerKind.TASK, "t1")
    assert {j.id for j in owned} == {"j1", "j2"}

    running = await store.set_status("j1", JobStatus.RUNNING)
    assert running.status is JobStatus.RUNNING
    assert (await store.get("j1")).status is JobStatus.RUNNING


@pytest.mark.asyncio
async def test_inmemory_jobstore_conforms() -> None:
    await assert_jobstore_conformance(InMemoryJobStore())


@pytest.mark.asyncio
async def test_both_task_subsystems_use_one_shape() -> None:
    """/api/v1/tasks and /env-worker/v1/tasks differ only by ``kind`` --
    the record and the store are one."""
    store = InMemoryJobStore()
    api_task = await store.put(Job(id="a", kind="task", owner_kind=JobOwnerKind.TASK, owner_id="A"))
    env_task = await store.put(Job(id="e", kind="env_worker", owner_kind=JobOwnerKind.TASK, owner_id="B"))
    assert type(api_task) is type(env_task) is Job
    assert {j.owner_kind for j in (api_task, env_task)} == {JobOwnerKind.TASK}


@pytest.mark.asyncio
async def test_campaign_cancel_is_a_fold_over_records() -> None:
    """The #742 mechanic: cancelling an owner is a fold over its jobs that skips
    terminal ones -- and it holds in both phases. (Models a completed lead job
    plus running members -- e.g. a ParCa lead then per-seed member runs.)"""
    store = InMemoryJobStore()
    await store.put(
        Job(id="lead", kind="lead", owner_kind=JobOwnerKind.CAMPAIGN, owner_id="c", status=JobStatus.COMPLETED)
    )
    for n in range(3):
        await store.put(
            Job(
                id=f"member-{n}",
                kind="member",
                owner_kind=JobOwnerKind.CAMPAIGN,
                owner_id="c",
                status=JobStatus.RUNNING,
                external_id=f"batch::{n}",
            )
        )

    killed: list[str] = []
    for job in await store.by_owner(JobOwnerKind.CAMPAIGN, "c"):
        if job.status.is_terminal:
            continue
        if job.external_id is not None:
            killed.append(job.external_id)
        await store.set_status(job.id, JobStatus.CANCELLED)

    final = await store.by_owner(JobOwnerKind.CAMPAIGN, "c")
    assert sum(j.status is JobStatus.CANCELLED for j in final) == 3
    assert sum(j.status is JobStatus.COMPLETED for j in final) == 1  # the completed lead is untouched
    assert killed == ["batch::0", "batch::1", "batch::2"]
