"""The env-worker task tier (plan §E option (e)) — durable record + per-worker FIFO.

Two properties carry most of the weight and are tested against a real database:
a task's record survives what its execution cannot, and the one ownership rule
refuses the accident it exists to refuse.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from viva_api.compose.database_service import EnvWorkerTaskORMExecutor
from viva_api.compose.models import ComposeJobStatus, EnvWorkerTask
from viva_api.compose.tables_orm import create_compose_db


@pytest_asyncio.fixture
async def task_db(postgres_url: str) -> AsyncGenerator[EnvWorkerTaskORMExecutor]:
    engine: AsyncEngine = create_async_engine(postgres_url, echo=False)
    await create_compose_db(engine)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    db = EnvWorkerTaskORMExecutor(maker)
    # Start from a clean slate: the fixture hands out one shared database.
    await db.fail_unfinished_tasks("test setup")
    yield db
    await engine.dispose()


async def _new(
    db: EnvWorkerTaskORMExecutor,
    method: str = "run_study",
    job: str = "job-1",
    by: str | None = None,
    corr: str | None = None,
) -> EnvWorkerTask:
    import secrets

    return await db.insert_task(
        job_name=job,
        method=method,
        params={"study": "s1"},
        correlation_id=corr or f"c-{secrets.token_hex(6)}",
        created_by=by,
    )


async def _get(db: EnvWorkerTaskORMExecutor, task_id: int) -> EnvWorkerTask:
    """Fetch a task that must exist. A missing row is a test failure with a
    useful message, not an AttributeError on None three lines later."""
    task = await db.get_task(task_id)
    assert task is not None, f"task {task_id} vanished"
    return task


# --- the record -------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_task_is_queued_and_readable_immediately(task_db: EnvWorkerTaskORMExecutor) -> None:
    """The row exists before any caller could poll — compose's contract, and it
    matters more here because the client is told to poll and holds nothing else."""
    task = await _new(task_db)
    assert task.status is ComposeJobStatus.QUEUED
    assert (await _get(task_db, task.database_id)).method == "run_study"


@pytest.mark.asyncio
async def test_the_lifecycle_records_its_times(task_db: EnvWorkerTaskORMExecutor) -> None:
    task = await _new(task_db)
    await task_db.start_task(task.database_id)
    running = await _get(task_db, task.database_id)
    assert running.status is ComposeJobStatus.RUNNING and running.started_at

    await task_db.finish_task(task.database_id, ComposeJobStatus.COMPLETED, result={"run_refs": [1]})
    done = await _get(task_db, task.database_id)
    assert done.status is ComposeJobStatus.COMPLETED
    assert done.result == {"run_refs": [1]}
    assert done.ended_at


@pytest.mark.asyncio
async def test_a_non_dict_result_is_wrapped_rather_than_refused(task_db: EnvWorkerTaskORMExecutor) -> None:
    """Worker methods return dicts, lists and scalars; JSONB takes an object, so
    the odd ones are wrapped instead of losing the result."""
    task = await _new(task_db, method="list_generators")
    await task_db.finish_task(task.database_id, ComposeJobStatus.COMPLETED, result=["a", "b"])
    assert (await _get(task_db, task.database_id)).result == {"value": ["a", "b"]}


@pytest.mark.asyncio
async def test_batch_read_returns_only_what_exists(task_db: EnvWorkerTaskORMExecutor) -> None:
    a, b = await _new(task_db), await _new(task_db)
    got = await task_db.get_tasks([a.database_id, b.database_id, 10_000_000])
    assert {t.database_id for t in got} == {a.database_id, b.database_id}


@pytest.mark.asyncio
async def test_batch_read_of_nothing_does_not_query(task_db: EnvWorkerTaskORMExecutor) -> None:
    assert await task_db.get_tasks([]) == []


# --- restart honesty --------------------------------------------------------


@pytest.mark.asyncio
async def test_unfinished_tasks_are_settled_not_left_running(task_db: EnvWorkerTaskORMExecutor) -> None:
    """THE durability property, and its exact limit: a socket cannot outlive its
    process, so a restart ends the work. What survives is the row — and a row
    that still says `running` is a lie the next reader has no way to detect."""
    queued = await _new(task_db)
    started = await _new(task_db)
    await task_db.start_task(started.database_id)
    finished = await _new(task_db)
    await task_db.finish_task(finished.database_id, ComposeJobStatus.COMPLETED, result={})

    settled = await task_db.fail_unfinished_tasks("lost to a viva-api restart")
    ids = {t.database_id for t in settled}
    assert {queued.database_id, started.database_id} <= ids
    assert finished.database_id not in ids, "a completed task must not be re-terminated"

    for tid in (queued.database_id, started.database_id):
        t = await _get(task_db, tid)
        assert t.status is ComposeJobStatus.FAILED
        assert t.error_message and "restart" in t.error_message


@pytest.mark.asyncio
async def test_settling_can_be_scoped_to_one_worker(task_db: EnvWorkerTaskORMExecutor) -> None:
    """Which is what makes worker-stop attribution possible: only that worker's
    tasks are settled, and they are told why."""
    mine = await _new(task_db, job="job-A")
    theirs = await _new(task_db, job="job-B")
    settled = await task_db.fail_unfinished_tasks("worker job-A was stopped by kr0@stanford.edu", job_name="job-A")
    assert [t.database_id for t in settled] == [mine.database_id]
    reason = (await _get(task_db, mine.database_id)).error_message
    assert reason and "kr0@stanford.edu" in reason
    assert (await _get(task_db, theirs.database_id)).status is ComposeJobStatus.QUEUED


@pytest.mark.asyncio
async def test_list_unfinished_excludes_terminal_states(task_db: EnvWorkerTaskORMExecutor) -> None:
    live = await _new(task_db)
    for status in (ComposeJobStatus.COMPLETED, ComposeJobStatus.FAILED, ComposeJobStatus.CANCELLED):
        t = await _new(task_db)
        await task_db.finish_task(t.database_id, status)
    ids = {t.database_id for t in await task_db.list_unfinished_tasks()}
    assert live.database_id in ids
    assert len(ids) == 1


@pytest.mark.asyncio
async def test_correlation_id_is_unique(task_db: EnvWorkerTaskORMExecutor) -> None:
    """The idempotency key. A resubmitted request must not silently become a
    second run of the same work — the failure this whole arc removed."""
    await _new(task_db, corr="fixed-correlation")
    # IntegrityError specifically: the point is that the DATABASE refuses it, so
    # the guarantee holds against any caller and any race, not just this code
    # path remembering to check.
    with pytest.raises(IntegrityError):
        await _new(task_db, corr="fixed-correlation")


@pytest.mark.asyncio
async def test_created_by_is_recorded_when_present_and_null_otherwise(task_db: EnvWorkerTaskORMExecutor) -> None:
    anon = await _new(task_db)
    owned = await _new(task_db, by="kr0@stanford.edu")
    assert (await _get(task_db, anon.database_id)).created_by is None
    assert (await _get(task_db, owned.database_id)).created_by == "kr0@stanford.edu"


# --- the batch endpoint's shape --------------------------------------------


def test_batch_status_omits_result_payloads() -> None:
    """A *status* endpoint returns status.

    Measured on dev before this: five tasks came back as 1.19 MB, of which
    1.185 MB was result payloads. The endpoint exists so a campaign can be polled
    without N round trips; inlining every result reintroduced the cost in a
    different dimension, and polling twenty tasks every few seconds would have
    moved megabytes per cycle over an SSM tunnel.

    Asserted on the MODEL rather than through HTTP because the guarantee is the
    schema: `result` must not be a field a caller can receive here at all.
    """
    from viva_api.api.routers.env_worker import TaskResponse, TaskStatusResponse

    assert "result" not in TaskStatusResponse.model_fields, "the batch endpoint must not ship result payloads"
    # ...while the singular endpoint still must, since that is where a caller
    # goes to collect it.
    assert "result" in TaskResponse.model_fields


def test_batch_status_says_whether_a_result_is_waiting() -> None:
    """Omitting the payload must not force a caller to guess. `has_result` is
    what makes one targeted GET obviously worthwhile."""
    from viva_api.api.routers.env_worker import TaskStatusResponse, _to_status
    from viva_api.compose.models import ComposeJobStatus, EnvWorkerTask

    def _task(result: object | None) -> EnvWorkerTask:
        return EnvWorkerTask(
            database_id=1,
            job_name="j",
            method="m",
            status=ComposeJobStatus.COMPLETED,
            result=result,
            correlation_id="c",
        )

    assert _to_status(_task({"generators": ["a"]})).has_result is True
    assert _to_status(_task(None)).has_result is False
    assert isinstance(_to_status(_task(None)), TaskStatusResponse)


@pytest.mark.asyncio
async def test_an_unrecognised_status_is_left_alone_not_destroyed(
    task_db: EnvWorkerTaskORMExecutor,
) -> None:
    """THE regression. The sweep used to select `status NOT IN terminal`, so any
    value it did not recognise was treated as unfinished and overwritten.

    That destroyed real work: rows written by 0.9.63 stored the enum's NAME
    ('COMPLETED') rather than its value ('completed'), and a later sweep replaced
    eleven finished tasks' results with "lost to a viva-api restart". Housekeeping
    is not allowed to delete a result because it did not recognise a string.
    """
    from sqlalchemy import text

    task = await _new(task_db)
    async with task_db.async_session_maker() as session, session.begin():
        await session.execute(
            text("UPDATE env_worker_task SET status='COMPLETED', result='{\"ok\": true}'::jsonb WHERE id=:i"),
            {"i": task.database_id},
        )

    settled = await task_db.fail_unfinished_tasks("a sweep that should not touch it")
    assert task.database_id not in {t.database_id for t in settled}

    async with task_db.async_session_maker() as session:
        row = (
            await session.execute(
                text("SELECT status, result FROM env_worker_task WHERE id=:i"),
                {"i": task.database_id},
            )
        ).first()
    assert row is not None
    assert row[0] == "COMPLETED", "an unknown status must survive the sweep"
    assert row[1] == {"ok": True}, "and so must its result"


@pytest.mark.asyncio
async def test_recognised_unfinished_statuses_are_still_settled(
    task_db: EnvWorkerTaskORMExecutor,
) -> None:
    """Failing closed must not mean failing to do the job: every status the
    system actually writes for unfinished work is still swept."""
    from sqlalchemy import text

    from viva_api.compose.tables_orm import ComposeJobStatusDB

    ids = []
    for status in (
        ComposeJobStatusDB.QUEUED,
        ComposeJobStatusDB.RUNNING,
        ComposeJobStatusDB.PENDING,
        ComposeJobStatusDB.WAITING,
    ):
        t = await _new(task_db)
        async with task_db.async_session_maker() as session, session.begin():
            await session.execute(
                text("UPDATE env_worker_task SET status=:s WHERE id=:i"),
                {"s": status.value, "i": t.database_id},
            )
        ids.append(t.database_id)

    settled = {t.database_id for t in await task_db.fail_unfinished_tasks("restart")}
    assert set(ids) <= settled, f"missed: {set(ids) - settled}"
