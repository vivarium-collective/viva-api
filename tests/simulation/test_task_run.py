"""In-region task-run verb (viva-api#631 slice 1).

``SimulationServiceRay.submit_task``/``get_task_status`` reuse the SAME
standalone container path (``_ensure_container_job_def`` + ``_submit_container``)
ParCa and the analysis DAG node already use -- the only new behavior is
building the ``python <script> <args...>`` command line and recording/polling
the result through ``DatabaseService.record_task``/``get_task``/
``update_task_status``.

Offline: an explicit batch double, settings doubles, a mocked database_service,
no sockets.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.simulation.test_ray_backend import (
    _container_settings,
    _fake_container_batch,
)
from viva_api.common.models import JobStatus
from viva_api.simulation.models import TaskDTO, TaskRunRequest
from viva_api.simulation.simulation_service_ray import SimulationServiceRay
from viva_api.simulation.tables_orm import TaskStatusDB


def _submitted_task_dto(**overrides: object) -> TaskDTO:
    base: dict[str, object] = {
        "database_id": 1,
        "name": "footask",
        "script": "scripts/foo.py",
        "args": ["--x", "1"],
        "sim_data_refs": None,
        "memory_class": "standard",
        "status": JobStatus.RUNNING,
        "job_id_ext": "c-1",
        "out_uri": "s3://mybucket/vecoli-output/tasks/footask-abcdef",
        "result_uri": None,
        "error_message": None,
    }
    base.update(overrides)
    return TaskDTO(**base)  # type: ignore[arg-type]


def _submit_task(
    request: TaskRunRequest, *, database_service: AsyncMock, submit_ids: list[str]
) -> tuple[TaskDTO, MagicMock]:
    batch = _fake_container_batch(submit_ids)
    service = SimulationServiceRay()
    with (
        patch("viva_api.simulation.ray._seams.get_settings", _container_settings),
        patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
        patch("viva_api.simulation.ray._seams.boto3.client", return_value=batch),
    ):
        import asyncio

        result = asyncio.run(service.submit_task(request, database_service))
    return result, batch


def test_submit_task_builds_expected_job_cmd_and_memory_class() -> None:
    request = TaskRunRequest(
        script="scripts/foo.py",
        args=["--x", "1"],
        memory_class="standard",
        commit="abc1234",
        name="footask",
    )
    database_service = AsyncMock()
    database_service.record_task.return_value = _submitted_task_dto()

    result, batch = _submit_task(request, database_service=database_service, submit_ids=["c-1"])

    (call,) = batch.submit_job.call_args_list
    env = {e["name"]: e["value"] for e in call.kwargs["containerOverrides"]["environment"]}
    assert env["CONTAINER_JOB_CMD"] == "python scripts/foo.py --x 1"
    assert call.kwargs["jobQueue"] == "smscdk-ray-standalone"  # memory_class=standard -> standard queue

    database_service.record_task.assert_awaited_once()
    kwargs = database_service.record_task.call_args.kwargs
    assert kwargs["name"] == "footask"
    assert kwargs["script"] == "scripts/foo.py"
    assert kwargs["args"] == ["--x", "1"]
    assert kwargs["status"] == TaskStatusDB.COMPUTING
    assert kwargs["job_id_ext"] == "c-1"
    assert kwargs["out_uri"] and "tasks/" in kwargs["out_uri"]

    assert result.database_id == 1
    assert result.job_id_ext == "c-1"


def test_submit_task_routes_large_memory_class_to_large_queue() -> None:
    request = TaskRunRequest(script="scripts/foo.py", memory_class="large", commit="abc1234")
    database_service = AsyncMock()
    database_service.record_task.return_value = _submitted_task_dto(memory_class="large")

    batch = _fake_container_batch(["c-2"])
    service = SimulationServiceRay()
    settings = _container_settings(ray_container_large_queue="smscdk-ray-standalone-large")
    with (
        patch("viva_api.simulation.ray._seams.get_settings", lambda: settings),
        patch("viva_api.common.storage.data_layout.get_settings", lambda: settings),
        patch("viva_api.simulation.ray._seams.boto3.client", return_value=batch),
    ):
        import asyncio

        asyncio.run(service.submit_task(request, database_service))

    (call,) = batch.submit_job.call_args_list
    assert call.kwargs["jobQueue"] == "smscdk-ray-standalone-large"


def test_submit_task_no_args_omits_trailing_space() -> None:
    request = TaskRunRequest(script="scripts/foo.py", commit="abc1234")
    database_service = AsyncMock()
    database_service.record_task.return_value = _submitted_task_dto(args=[])

    _, batch = _submit_task(request, database_service=database_service, submit_ids=["c-3"])
    (call,) = batch.submit_job.call_args_list
    env = {e["name"]: e["value"] for e in call.kwargs["containerOverrides"]["environment"]}
    assert env["CONTAINER_JOB_CMD"] == "python scripts/foo.py"


def test_submit_task_passes_sim_data_refs_as_json_env() -> None:
    request = TaskRunRequest(
        script="scripts/foo.py",
        sim_data_refs={"chassis": "s3://bucket/chassis.tar"},
        commit="abc1234",
    )
    database_service = AsyncMock()
    database_service.record_task.return_value = _submitted_task_dto(sim_data_refs=request.sim_data_refs)

    _, batch = _submit_task(request, database_service=database_service, submit_ids=["c-4"])
    (call,) = batch.submit_job.call_args_list
    env = {e["name"]: e["value"] for e in call.kwargs["containerOverrides"]["environment"]}
    assert env["TASK_SIM_DATA_REFS"] == '{"chassis": "s3://bucket/chassis.tar"}'


@pytest.mark.asyncio
async def test_get_task_status_maps_succeeded_to_ready() -> None:
    service = SimulationServiceRay()
    database_service = AsyncMock()
    database_service.get_task.return_value = _submitted_task_dto(status=JobStatus.RUNNING)
    database_service.update_task_status.return_value = _submitted_task_dto(status=JobStatus.COMPLETED)

    with patch.object(service, "get_batch_job_statuses", return_value={"c-1": JobStatus.COMPLETED}):
        result = await service.get_task_status(1, database_service)

    database_service.update_task_status.assert_awaited_once_with(1, TaskStatusDB.READY)
    assert result.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_get_task_status_maps_failed_to_failed() -> None:
    service = SimulationServiceRay()
    database_service = AsyncMock()
    database_service.get_task.return_value = _submitted_task_dto(status=JobStatus.RUNNING)
    database_service.update_task_status.return_value = _submitted_task_dto(status=JobStatus.FAILED)

    with patch.object(service, "get_batch_job_statuses", return_value={"c-1": JobStatus.FAILED}):
        result = await service.get_task_status(1, database_service)

    database_service.update_task_status.assert_awaited_once_with(1, TaskStatusDB.FAILED)
    assert result.status == JobStatus.FAILED


@pytest.mark.asyncio
async def test_get_task_status_without_job_id_ext_skips_batch_poll() -> None:
    service = SimulationServiceRay()
    database_service = AsyncMock()
    database_service.get_task.return_value = _submitted_task_dto(job_id_ext=None)

    result = await service.get_task_status(1, database_service)

    database_service.update_task_status.assert_not_called()
    assert result.job_id_ext is None


@pytest.mark.asyncio
async def test_get_task_status_not_yet_visible_in_batch_leaves_status_unchanged() -> None:
    """An id absent from describe_jobs (eventual-consistency lag right after
    submission) must not be treated as terminal -- same discipline
    get_batch_job_statuses already documents for its other callers."""
    service = SimulationServiceRay()
    database_service = AsyncMock()
    database_service.get_task.return_value = _submitted_task_dto(status=JobStatus.RUNNING)

    with patch.object(service, "get_batch_job_statuses", return_value={}):
        result = await service.get_task_status(1, database_service)

    database_service.update_task_status.assert_not_called()
    assert result.status == JobStatus.RUNNING


def test_submit_uploaded_task_stages_script_and_runs_staged_path() -> None:
    """slice 2: an uploaded script is put to S3 and run from the staged dir; the
    container's CONTAINER_STAGE_S3 -> CONTAINER_STAGE_DIR sync pulls it in."""
    from viva_api.simulation.ray.image_paths import TASK_STAGE_DIR

    request = TaskRunRequest(script="ignored.py", args=["--x", "1"], memory_class="standard", commit="abc1234")
    database_service = AsyncMock()
    database_service.record_task.return_value = _submitted_task_dto(
        script="s3://mybucket/vecoli-output/tasks/scripts/x/myscript.py"
    )

    batch = _fake_container_batch(["c-9"])
    service = SimulationServiceRay()
    with (
        patch("viva_api.simulation.ray._seams.get_settings", _container_settings),
        patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
        patch("viva_api.simulation.ray._seams.boto3.client", return_value=batch),
    ):
        import asyncio

        asyncio.run(
            service.submit_uploaded_task(
                request, script_bytes=b"print('hi')\n", filename="myscript.py", database_service=database_service
            )
        )

    # The script was PUT to S3 under a tasks/scripts/ prefix.
    assert batch.put_object.called
    put_kwargs = batch.put_object.call_args.kwargs
    assert put_kwargs["Key"].endswith("/myscript.py") and "tasks/scripts/" in put_kwargs["Key"]
    assert put_kwargs["Body"] == b"print('hi')\n"

    (call,) = batch.submit_job.call_args_list
    env = {e["name"]: e["value"] for e in call.kwargs["containerOverrides"]["environment"]}
    # Runs the STAGED path, and the entrypoint is told to sync the script in.
    assert env["CONTAINER_JOB_CMD"] == f"python {TASK_STAGE_DIR}/myscript.py --x 1"
    assert "tasks/scripts/" in env["CONTAINER_STAGE_S3"]
    assert env["CONTAINER_STAGE_DIR"] == TASK_STAGE_DIR

    # Recorded with the staged S3 uri as the script label.
    kwargs = database_service.record_task.call_args.kwargs
    assert kwargs["script"].startswith("s3://") and kwargs["script"].endswith("/myscript.py")


def test_submit_uploaded_task_sanitizes_filename_to_basename() -> None:
    """A path-y upload filename never escapes the stage dir."""
    from viva_api.simulation.ray.image_paths import TASK_STAGE_DIR

    request = TaskRunRequest(script="ignored.py", commit="abc1234")
    database_service = AsyncMock()
    database_service.record_task.return_value = _submitted_task_dto()

    batch = _fake_container_batch(["c-10"])
    service = SimulationServiceRay()
    with (
        patch("viva_api.simulation.ray._seams.get_settings", _container_settings),
        patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
        patch("viva_api.simulation.ray._seams.boto3.client", return_value=batch),
    ):
        import asyncio

        asyncio.run(
            service.submit_uploaded_task(
                request, script_bytes=b"x", filename="../../etc/evil.py", database_service=database_service
            )
        )
    (call,) = batch.submit_job.call_args_list
    env = {e["name"]: e["value"] for e in call.kwargs["containerOverrides"]["environment"]}
    assert env["CONTAINER_JOB_CMD"] == f"python {TASK_STAGE_DIR}/evil.py"


def test_task_run_request_rejects_bad_name() -> None:
    """A name with characters outside the Batch jobName charset is a 422 at the
    model boundary, not a 500 from submit_job."""
    import pydantic
    import pytest

    with pytest.raises(pydantic.ValidationError):
        TaskRunRequest(script="scripts/foo.py", name="bad name!")
    # A clean name is accepted.
    TaskRunRequest(script="scripts/foo.py", name="run_4-fss")


def test_derived_task_name_is_sanitized_for_jobname() -> None:
    """When name is omitted, the derived script stem (which can carry dots) is
    sanitized to the Batch jobName charset."""
    request = TaskRunRequest(script="scripts/foo.bar.py", commit="abc1234")
    database_service = AsyncMock()
    database_service.record_task.return_value = _submitted_task_dto()

    _, batch = _submit_task(request, database_service=database_service, submit_ids=["c-11"])
    (call,) = batch.submit_job.call_args_list
    job_name = call.kwargs["jobName"]
    assert job_name.startswith("task-foo-bar-")  # dots -> dashes, no dots in the jobName
    assert "." not in job_name


def _logs_fake(
    events: list[str], *, log_stream: str | None = "stream/abc", group: str | None = "/aws/batch/job"
) -> MagicMock:
    """A boto3 double answering both the batch (describe_jobs/-job_definitions)
    and CloudWatch-logs (get_log_events) calls get_task_logs makes."""
    fake = MagicMock()
    container = {"logStreamName": log_stream} if log_stream else {}
    fake.describe_jobs.return_value = {"jobs": [{"container": container, "jobDefinition": "jd:1"}]}
    fake.describe_job_definitions.return_value = {
        "jobDefinitions": [{"containerProperties": {"logConfiguration": {"options": {"awslogs-group": group}}}}]
    }
    fake.get_log_events.return_value = {"events": [{"message": m} for m in events]}
    return fake


def test_get_task_logs_reads_cloudwatch_stream() -> None:
    database_service = AsyncMock()
    database_service.get_task.return_value = _submitted_task_dto(job_id_ext="c-42")
    fake = _logs_fake(["hello", "world"])
    settings = _container_settings(ray_batch_log_group="", ray_log_s3_prefix="s3://b/logs")
    service = SimulationServiceRay()
    with (
        patch("viva_api.simulation.ray._seams.get_settings", lambda: settings),
        patch("viva_api.simulation.ray._seams.boto3.client", return_value=fake),
    ):
        import asyncio

        result = asyncio.run(service.get_task_logs(1, database_service))
    assert result.lines == ["hello", "world"]
    assert result.log_stream == "stream/abc"
    # group resolved from the job def's logConfiguration
    fake.get_log_events.assert_called_once()
    assert fake.get_log_events.call_args.kwargs["logGroupName"] == "/aws/batch/job"
    assert result.report_uri == "s3://b/logs/c-42/report.json"


def test_get_task_logs_empty_before_container_starts() -> None:
    database_service = AsyncMock()
    database_service.get_task.return_value = _submitted_task_dto(job_id_ext="c-43")
    fake = _logs_fake([], log_stream=None)  # no stream yet
    settings = _container_settings(ray_batch_log_group="", ray_log_s3_prefix="")
    service = SimulationServiceRay()
    with (
        patch("viva_api.simulation.ray._seams.get_settings", lambda: settings),
        patch("viva_api.simulation.ray._seams.boto3.client", return_value=fake),
    ):
        import asyncio

        result = asyncio.run(service.get_task_logs(1, database_service))
    assert result.lines == []
    fake.get_log_events.assert_not_called()


def test_get_task_logs_prefers_configured_log_group() -> None:
    database_service = AsyncMock()
    database_service.get_task.return_value = _submitted_task_dto(job_id_ext="c-44")
    fake = _logs_fake(["x"])
    settings = _container_settings(ray_batch_log_group="/configured/group", ray_log_s3_prefix="")
    service = SimulationServiceRay()
    with (
        patch("viva_api.simulation.ray._seams.get_settings", lambda: settings),
        patch("viva_api.simulation.ray._seams.boto3.client", return_value=fake),
    ):
        import asyncio

        result = asyncio.run(service.get_task_logs(1, database_service))
    assert result.lines == ["x"]
    assert fake.get_log_events.call_args.kwargs["logGroupName"] == "/configured/group"
    fake.describe_job_definitions.assert_not_called()  # configured group short-circuits resolution
