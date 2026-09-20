"""The build and task services on their own: no ``SimulationServiceRay`` anywhere.

That they CAN be tested like this is the reason they are services and not mixins
(``docs/plan-core.md`` decision log, 2026-09-20). ``test_ray_backend.py`` and
``test_task_run.py`` still exercise them through the real service.
"""

from collections.abc import Coroutine
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.simulation.test_ray_backend import _ray_settings, _v2ecoli_simulator
from viva_api.common.models import JobId, JobStatus
from viva_api.simulation import batch_build
from viva_api.simulation.models import TaskRunRequest
from viva_api.simulation.ray.build import RayImageBuilder
from viva_api.simulation.ray.image_paths import TASK_OUT_DIR
from viva_api.simulation.ray.tasks import RayTaskService


class FakeLocalTasks:
    def __init__(self) -> None:
        self.submitted: list[str] = []
        self.recorded: list[list[str]] = []

    def submit(self, coro: Coroutine[Any, Any, None], name: str) -> JobId:
        coro.close()  # the build itself is exercised by ``run`` below
        self.submitted.append(name)
        return JobId.local("local-1")

    async def record_external_job_ids(self, job_ids: list[str]) -> bool:
        self.recorded.append(job_ids)
        return True


@pytest.mark.asyncio
async def test_the_builder_needs_a_local_task_service_and_nothing_else() -> None:
    local = FakeLocalTasks()
    builder = RayImageBuilder(local)  # type: ignore[arg-type]
    with patch("viva_api.simulation.ray._seams.get_settings", _ray_settings):
        job_id = await builder.submit(_v2ecoli_simulator())
    assert job_id == JobId.local("local-1")
    assert local.submitted == [f"ray-build-{_v2ecoli_simulator().git_commit_hash}"]


@pytest.mark.asyncio
async def test_a_build_records_its_batch_job_before_it_waits_on_it() -> None:
    """viva-api#414: the Batch handle must be on the row before the long poll, so the build's
    outcome survives the pod that started it."""
    local = FakeLocalTasks()
    order: list[str] = []

    async def submit(**kwargs: Any) -> str:
        order.append("submit")
        assert kwargs["queue"] == _ray_settings().build_amd64_queue
        return "build-job-1"

    async def poll(job_ids: list[str]) -> None:
        assert local.recorded == [["build-job-1"]], "polled before the job id was recorded"
        order.append("poll")

    with (
        patch("viva_api.simulation.ray._seams.get_settings", _ray_settings),
        patch.object(batch_build, "submit_batch_build", submit),
        patch.object(batch_build, "poll_batch_jobs", poll),
    ):
        await RayImageBuilder(local).run(_v2ecoli_simulator())  # type: ignore[arg-type]
    assert order == ["submit", "poll"]


class FakeDispatch:
    """The whole of ``TaskDispatch``, in thirty lines -- which is the point."""

    def __init__(self) -> None:
        self.submitted: list[dict[str, Any]] = []
        self.statuses: dict[str, JobStatus] = {}

    async def get_latest_commit_hash(self) -> str:
        return "latestsha"

    def _image_uri(self, commit: str) -> str:
        return f"registry/image:{commit}"

    def _ensure_container_job_def(self, image: str, commit: str) -> str:
        return f"jobdef-{commit}:1"

    def _results_s3_uri(self, experiment_id: str) -> str:
        return f"s3://bucket/out/{experiment_id}/"

    def _submit_container(self, **kwargs: Any) -> str:
        self.submitted.append(kwargs)
        return "batch-job-1"

    def get_batch_job_statuses(self, job_ids: list[str]) -> dict[str, JobStatus]:
        return {j: self.statuses[j] for j in job_ids if j in self.statuses}

    def _batch(self) -> Any:
        return MagicMock()

    def _resolve_log_group(self, job_definition: str | None) -> str | None:
        return None


@pytest.mark.asyncio
async def test_a_task_is_one_container_job_recorded_on_the_task_table() -> None:
    dispatch, database = FakeDispatch(), MagicMock()
    database.record_task = AsyncMock(return_value="the-task-row")
    request = TaskRunRequest(script="scripts/x.py", args=["--k", "v w"], sim_data_refs={"a": "s3://x"})

    result: Any = await RayTaskService(dispatch).submit_task(request, database)
    assert result == "the-task-row"

    (job,) = dispatch.submitted
    assert job["job_definition"] == "jobdef-latestsha:1"  # no commit given: the latest
    assert job["job_cmd"] == "python scripts/x.py --k 'v w'"
    assert job["out_dir"] == TASK_OUT_DIR
    assert job["out_s3"].startswith("s3://bucket/out/tasks/x-")
    assert job["task_env"] == {"TASK_SIM_DATA_REFS": '{"a": "s3://x"}'}
    recorded = database.record_task.await_args.kwargs
    assert (recorded["job_id_ext"], recorded["out_uri"]) == ("batch-job-1", job["out_s3"])


@pytest.mark.asyncio
async def test_a_task_status_is_what_batch_says_or_unchanged_when_batch_does_not_know() -> None:
    dispatch, database = FakeDispatch(), MagicMock()
    task = SimpleNamespace(job_id_ext="batch-job-1", status=JobStatus.RUNNING)
    database.get_task = AsyncMock(return_value=task)
    database.update_task_status = AsyncMock(return_value="updated")
    service = RayTaskService(dispatch)

    unchanged: Any = await service.get_task_status(1, database)
    assert unchanged is task  # Batch has not heard of it yet
    database.update_task_status.assert_not_awaited()

    dispatch.statuses["batch-job-1"] = JobStatus.FAILED
    updated: Any = await service.get_task_status(1, database)
    assert updated == "updated"
