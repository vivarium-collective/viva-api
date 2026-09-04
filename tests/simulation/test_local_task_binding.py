"""LocalTaskService's durable-record binding (viva-api#414).

The service's existing submit/status/cancel behavior is covered in
test_k8s_backend.py::TestLocalTaskService. These tests cover what #414 added:
binding a LOCAL task to its HpcRun row so the row is finalized from the task's
own outcome, and recording the external (AWS Batch) job ids the task watches
so a recovering process can find the work.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from viva_api.common.hpc.local_task_service import LocalTaskService
from viva_api.common.models import JobId, JobStatus


def _mock_db() -> MagicMock:
    db = MagicMock()
    db.update_hpcrun_status = AsyncMock()
    db.set_hpcrun_external_job_ids = AsyncMock()
    return db


@pytest.mark.asyncio
class TestBindHpcrun:
    async def test_completed_task_finalizes_row_completed_with_end_time(self) -> None:
        service = LocalTaskService()
        db = _mock_db()

        async def quick() -> str:
            return "ok"

        job_id = service.submit(quick(), name="t")
        assert await service.bind_hpcrun(job_id.value, hpcrun_id=42, database_service=db) is True
        assert service.bound_hpcrun_id(job_id.value) == 42
        await service.wait_finalized(job_id.value)

        db.update_hpcrun_status.assert_awaited_once()
        kwargs = db.update_hpcrun_status.await_args.kwargs
        assert kwargs["hpcrun_id"] == 42
        update = kwargs["update"]
        assert update.job_id == JobId.local(job_id.value)
        assert update.status == JobStatus.COMPLETED
        assert update.end_time is not None  # the symptom in #414 was end_time=null forever
        assert update.error_message is None

    async def test_failed_task_finalizes_row_failed_with_exception_text(self) -> None:
        service = LocalTaskService()
        db = _mock_db()

        async def boom() -> None:
            raise RuntimeError("build exploded")

        job_id = service.submit(boom(), name="t")
        await service.bind_hpcrun(job_id.value, hpcrun_id=7, database_service=db)
        await service.wait_finalized(job_id.value)

        update = db.update_hpcrun_status.await_args.kwargs["update"]
        assert update.status == JobStatus.FAILED
        assert "build exploded" in (update.error_message or "")

    async def test_cancelled_task_finalizes_row_cancelled(self) -> None:
        service = LocalTaskService()
        db = _mock_db()

        async def slow() -> None:
            await asyncio.sleep(10)

        job_id = service.submit(slow(), name="t")
        await service.bind_hpcrun(job_id.value, hpcrun_id=9, database_service=db)
        assert service.cancel(job_id.value) is True
        await service.wait_finalized(job_id.value)

        update = db.update_hpcrun_status.await_args.kwargs["update"]
        assert update.status == JobStatus.CANCELLED

    async def test_bind_after_the_task_already_finished_still_finalizes(self) -> None:
        """add_done_callback on a done task schedules the callback immediately --
        the binding must not be lost just because the caller's insert was slow."""
        service = LocalTaskService()
        db = _mock_db()

        async def quick() -> None:
            pass

        job_id = service.submit(quick(), name="t")
        await service._tasks[job_id.value]
        await service.bind_hpcrun(job_id.value, hpcrun_id=3, database_service=db)
        await service.wait_finalized(job_id.value)
        assert db.update_hpcrun_status.await_args.kwargs["update"].status == JobStatus.COMPLETED

    async def test_bind_unknown_task_is_refused(self) -> None:
        service = LocalTaskService()
        assert await service.bind_hpcrun("nope", hpcrun_id=1, database_service=_mock_db()) is False
        assert service.bound_hpcrun_id("nope") is None

    async def test_db_failure_in_finalize_does_not_raise(self) -> None:
        service = LocalTaskService()
        db = _mock_db()
        db.update_hpcrun_status = AsyncMock(side_effect=RuntimeError("db down"))

        async def quick() -> None:
            pass

        job_id = service.submit(quick(), name="t")
        await service.bind_hpcrun(job_id.value, hpcrun_id=1, database_service=db)
        await service.wait_finalized(job_id.value)  # would raise here if the finalizer let it escape
        db.update_hpcrun_status.assert_awaited_once()


@pytest.mark.asyncio
class TestRecordExternalJobIds:
    async def test_recorded_after_bind_is_persisted_immediately(self) -> None:
        service = LocalTaskService()
        db = _mock_db()
        bound = asyncio.Event()

        async def build() -> None:
            await bound.wait()
            assert await service.record_external_job_ids(["batch-1", "batch-2"]) is True

        job_id = service.submit(build(), name="build")
        await service.bind_hpcrun(job_id.value, hpcrun_id=11, database_service=db)
        bound.set()
        await service._tasks[job_id.value]

        db.set_hpcrun_external_job_ids.assert_awaited_once_with(11, ["batch-1", "batch-2"])

    async def test_recorded_before_bind_is_persisted_by_bind(self) -> None:
        """The real ordering in upload_simulator: submit -> (task submits to
        Batch and records) -> insert row -> bind. The record must survive
        arriving first."""
        service = LocalTaskService()
        db = _mock_db()
        recorded = asyncio.Event()

        async def build() -> None:
            await service.record_external_job_ids(["batch-1"])
            recorded.set()
            await asyncio.sleep(10)

        job_id = service.submit(build(), name="build")
        await recorded.wait()
        db.set_hpcrun_external_job_ids.assert_not_awaited()  # nothing to persist onto yet

        await service.bind_hpcrun(job_id.value, hpcrun_id=12, database_service=db)
        db.set_hpcrun_external_job_ids.assert_awaited_once_with(12, ["batch-1"])
        service.cancel(job_id.value)
        await service.wait_finalized(job_id.value)

    async def test_wait_finalized_is_a_noop_for_unbound_or_unknown_tasks(self) -> None:
        service = LocalTaskService()

        async def quick() -> None:
            pass

        job_id = service.submit(quick(), name="t")
        await service.wait_finalized(job_id.value)
        await service.wait_finalized("nope")

    async def test_outside_a_submitted_task_is_a_noop(self) -> None:
        service = LocalTaskService()
        assert await service.record_external_job_ids(["batch-1"]) is False
        assert LocalTaskService.current_task_id() is None

    async def test_each_task_sees_its_own_id(self) -> None:
        service = LocalTaskService()
        seen: dict[str, str | None] = {}

        async def probe(label: str) -> None:
            await asyncio.sleep(0)
            seen[label] = LocalTaskService.current_task_id()

        a = service.submit(probe("a"), name="a")
        b = service.submit(probe("b"), name="b")
        await asyncio.gather(service._tasks[a.value], service._tasks[b.value])
        assert seen == {"a": a.value, "b": b.value}

    async def test_db_failure_in_persist_does_not_fail_the_task(self) -> None:
        service = LocalTaskService()
        db = _mock_db()
        db.set_hpcrun_external_job_ids = AsyncMock(side_effect=RuntimeError("db down"))

        async def build() -> str:
            await service.record_external_job_ids(["batch-1"])
            return "built"

        job_id = service.submit(build(), name="build")
        await service.bind_hpcrun(job_id.value, hpcrun_id=1, database_service=db)
        assert await service._tasks[job_id.value] == "built"


@pytest.mark.asyncio
class TestOwnership:
    async def test_owns_only_tasks_this_service_submitted(self) -> None:
        service = LocalTaskService()

        async def quick() -> None:
            pass

        job_id = service.submit(quick(), name="t")
        assert service.owns(job_id.value) is True
        assert service.owns("someone-elses") is False
        await service._tasks[job_id.value]
        assert service.owns(job_id.value) is True  # finished tasks are still owned until cleanup
        service.cleanup_completed()
        assert service.owns(job_id.value) is False
