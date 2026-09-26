"""The JobStore seam (plan P4b, D15): the oracle on the in-memory store, the two properties the seam
buys, and the fence around the test double.

Pure and offline. The adapter tests (``HpcRunJobStore`` / ``TaskJobStore`` in ``tests/simulation/``,
``EnvWorkerTaskJobStore`` here) run the same oracle against a real store on the Postgres
testcontainer -- that is the real test; this is the fast specification check beside it.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.core.jobstore_oracle import assert_job_reader_conformance, assert_job_store_conformance
from viva_core.models import JobStatus
from viva_core.tasks import OWNER_CAMPAIGN, OWNER_TASK, Job, JobReader, JobStore
from viva_core.tasks.testing import InMemoryJobStore


def test_the_in_memory_store_is_a_job_store() -> None:
    reader: JobReader = InMemoryJobStore()
    store: JobStore = InMemoryJobStore()
    assert reader is not None and store is not None


@pytest.mark.asyncio
async def test_inmemory_jobstore_conforms() -> None:
    store = InMemoryJobStore()
    await assert_job_reader_conformance(store, store.seed, owner_kind=OWNER_TASK, other_kind=OWNER_CAMPAIGN)
    await assert_job_store_conformance(store, store.seed, owner_kind=OWNER_TASK)


@pytest.mark.asyncio
async def test_both_task_subsystems_use_one_shape() -> None:
    """/api/v1/tasks and /env-worker/v1/tasks differ only by ``kind`` and owner -- the record and the
    store are one, and an owner kind is a string the application chooses."""
    store = InMemoryJobStore()
    api_task = await store.seed(Job(id="a", kind="task", owner_kind=OWNER_TASK, owner_id="A"))
    env_task = await store.seed(Job(id="e", kind="env_worker", owner_kind="env_worker", owner_id="B"))
    app_run = await store.seed(Job(id="s", kind="simulation", owner_kind="simulation", owner_id="1002"))
    assert type(api_task) is type(env_task) is type(app_run) is Job
    assert [job.owner_kind for job in (api_task, env_task, app_run)] == ["task", "env_worker", "simulation"]


async def _cancel_owner(store: InMemoryJobStore, owner_kind: str, owner_id: str) -> list[str]:
    """The #742 mechanic as D15 states it: cancelling an owner is a fold over its non-terminal
    jobs -- companions first (viva-api#710), then the job's own handles -- and the record is marked."""
    killed: list[str] = []
    for job in await store.by_owner(owner_kind, owner_id):
        if job.status.is_terminal:
            continue
        killed.extend(job.companion_job_ids)
        killed.extend(job.external_job_ids)
        await store.set_status(job.id, JobStatus.CANCELLED)
    return killed


@pytest.mark.asyncio
async def test_campaign_cancel_is_a_fold_over_records_in_both_phases() -> None:
    """Phase 2 (the lead done, members running): the completed lead is untouched, every member's
    companions die before its own handle, and every member is CANCELLED. Phase 1 (nothing but the
    lead, still running -- viva-api#709): the lead's handle dies and its record is CANCELLED."""
    store = InMemoryJobStore()
    await store.seed(Job(id="lead", kind="lead", owner_kind=OWNER_CAMPAIGN, owner_id="c", status=JobStatus.COMPLETED))
    for n in range(3):
        await store.seed(
            Job(
                id=f"member-{n}",
                kind="member",
                owner_kind=OWNER_CAMPAIGN,
                owner_id="c",
                status=JobStatus.RUNNING,
                backend="ray",
                external_job_ids=(f"batch::{n}",),
                companion_job_ids=(f"batch::{n}-gather",),
            )
        )
    killed = await _cancel_owner(store, OWNER_CAMPAIGN, "c")
    final = await store.by_owner(OWNER_CAMPAIGN, "c")
    assert sum(job.status is JobStatus.CANCELLED for job in final) == 3
    assert sum(job.status is JobStatus.COMPLETED for job in final) == 1  # the completed lead is untouched
    assert killed == [
        "batch::0-gather",
        "batch::0",
        "batch::1-gather",
        "batch::1",
        "batch::2-gather",
        "batch::2",
    ]

    early = InMemoryJobStore()
    await early.seed(
        Job(
            id="lead",
            kind="lead",
            owner_kind=OWNER_CAMPAIGN,
            owner_id="d",
            status=JobStatus.RUNNING,
            backend="ray",
            external_job_ids=("batch::lead",),
        )
    )
    assert await _cancel_owner(early, OWNER_CAMPAIGN, "d") == ["batch::lead"]
    lead = await early.get("lead")
    assert lead is not None and lead.status is JobStatus.CANCELLED


def _imported_modules(tree: ast.AST) -> list[str]:
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            # ``from viva_core.tasks import testing`` reaches the module too.
            names.extend(f"{node.module}.{alias.name}" for alias in node.names)
    return names


def test_the_in_memory_store_is_fenced_to_tests() -> None:
    """Nothing outside ``tests/`` imports ``viva_core.tasks.testing``: an in-memory store must never
    become a runtime default (the #414 family of failures)."""
    repo = Path(__file__).resolve().parents[2]
    offenders = [
        str(source.relative_to(repo))
        for root in ("viva_core", "viva_api", "app", "scripts")
        for source in (repo / root).rglob("*.py")
        if any(
            name.startswith("viva_core.tasks.testing")
            for name in _imported_modules(ast.parse(source.read_text(encoding="utf-8")))
        )
    ]
    assert not offenders, offenders
