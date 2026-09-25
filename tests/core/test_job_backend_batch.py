"""Core's ``JobBackend`` Protocol, AWS Batch implementation, over the engine and a fake client
(U2g). The same four operations the SLURM implementation proves live in
``test_job_backend_slurm.py``: here, what each one tells Batch and how Batch's answer comes back.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

from tests.core.test_batch_backend import FakeBatch, _engine
from viva_core.backends.base import JobBackend, JobHandle, JobSpec, Resources
from viva_core.backends.batch_backend import BatchContainerJobBackend
from viva_core.backends.slurm_backend import SlurmJobBackend
from viva_core.models import JobStatus

if TYPE_CHECKING:
    from types_boto3_logs import CloudWatchLogsClient

BASE_DEFINITIONS = {"base": [{"revision": 3, "containerProperties": {"image": "old", "vcpus": 2}}]}


class FakeLogs:
    def __init__(self, lines: list[str]) -> None:
        self.lines = lines
        self.calls: list[dict[str, Any]] = []

    def get_log_events(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        chosen = self.lines[-kwargs["limit"] :] if "limit" in kwargs else self.lines
        return {"events": [{"message": line} for line in chosen]}


def _backend(fake: FakeBatch, logs: FakeLogs | None = None) -> BatchContainerJobBackend:
    return BatchContainerJobBackend(
        _engine(fake),
        job_queue="q",
        base_job_definition="base",
        definition_suffix="core",
        report_path="/work/report.json",
        logs_client=(lambda: cast("CloudWatchLogsClient", logs)) if logs is not None else None,
    )


def test_both_implementations_satisfy_the_protocol() -> None:
    """The point of declaring it now: two shapes, one Protocol, checked structurally."""
    batch: JobBackend = _backend(FakeBatch(definitions=BASE_DEFINITIONS))
    slurm: JobBackend = SlurmJobBackend(lambda: None, partition="p", work_dir=Path("/w"))  # type: ignore[arg-type,return-value]
    assert {batch.kind, slurm.kind} == {"batch-container", "slurm"}


@pytest.mark.asyncio
async def test_submit_derives_the_definition_for_the_image_and_puts_the_spec_in_one_request() -> None:
    fake = FakeBatch(definitions=BASE_DEFINITIONS)
    earlier = JobHandle(backend="batch-container", id="job-0", name="earlier")
    handle = await _backend(fake).submit(
        JobSpec(
            name="probe",
            command="python -c 'print(1)'",
            image="ghcr.io/x/y:1",
            env={"GREETING": "hi"},
            labels={"owner": "core"},
            depends_on=(earlier,),
        )
    )
    assert handle == JobHandle(backend="batch-container", id="job-1", name="probe")
    (registered,) = fake.named("register_job_definition")
    assert registered["containerProperties"]["image"] == "ghcr.io/x/y:1"
    (submitted,) = fake.named("submit_job")
    assert (
        submitted["jobName"] == "probe" and submitted["jobQueue"] == "q" and submitted["jobDefinition"] == "base-core:7"
    )
    env = {e["name"]: e["value"] for e in submitted["containerOverrides"]["environment"]}
    assert env["CONTAINER_JOB_CMD"] == "python -c 'print(1)'"
    assert env["CONTAINER_REPORT_PATH"] == "/work/report.json"
    assert env["GREETING"] == "hi"
    assert submitted["dependsOn"] == [{"jobId": "job-0", "type": "SEQUENTIAL"}]
    assert submitted["tags"] == {"owner": "core"} and submitted["propagateTags"] is True


@pytest.mark.asyncio
async def test_a_bare_command_or_a_foreign_dependency_is_refused_before_batch_is_asked() -> None:
    fake = FakeBatch(definitions=BASE_DEFINITIONS)
    with pytest.raises(ValueError, match="needs an image"):
        await _backend(fake).submit(JobSpec(name="x", command="true"))
    with pytest.raises(ValueError, match="only wait on Batch jobs"):
        await _backend(fake).submit(
            JobSpec(name="x", command="true", image="i", depends_on=(JobHandle(backend="slurm", id="7"),))
        )
    assert fake.calls == []


@pytest.mark.asyncio
async def test_resources_the_definition_fixes_are_reported_not_applied(caplog: pytest.LogCaptureFixture) -> None:
    fake = FakeBatch(definitions=BASE_DEFINITIONS)
    with caplog.at_level("WARNING"):
        await _backend(fake).submit(JobSpec(name="x", command="true", image="i", resources=Resources(cpus=16)))
    assert "job definition's, not the spec's" in caplog.text
    (submitted,) = fake.named("submit_job")
    assert "resourceRequirements" not in submitted.get("containerOverrides", {})


@pytest.mark.asyncio
async def test_status_maps_what_batch_said_and_forgets_what_batch_forgot() -> None:
    fake = FakeBatch(
        jobs=[
            {"jobId": "a", "jobName": "a", "status": "SUCCEEDED", "container": {"exitCode": 0}, "attempts": [{}]},
            {
                "jobId": "b",
                "jobName": "b",
                "status": "FAILED",
                "statusReason": "Essential container in task exited",
                "container": {"exitCode": 137},
                "attempts": [{}, {}],
            },
            {"jobId": "c", "jobName": "c", "status": "RUNNABLE"},
        ]
    )
    backend = _backend(fake)
    handles = [JobHandle(backend="batch-container", id=i) for i in ("a", "b", "c", "ghost")]
    seen = await backend.status(handles)
    assert set(seen) == {"a", "b", "c"}
    assert seen["a"].status is JobStatus.COMPLETED and seen["a"].exit_code == 0 and seen["a"].attempt == 1
    assert seen["b"].status is JobStatus.FAILED and seen["b"].exit_code == 137
    assert seen["b"].reason == "Essential container in task exited" and seen["b"].attempt == 2
    assert seen["c"].status is JobStatus.QUEUED and seen["c"].exit_code is None and seen["c"].attempt is None
    assert await backend.status([]) == {} and fake.named("describe_jobs") != []


@pytest.mark.asyncio
async def test_cancel_terminates_whatever_state_the_job_is_in() -> None:
    fake = FakeBatch()
    await _backend(fake).cancel(JobHandle(backend="batch-container", id="a"))
    (terminated,) = fake.named("terminate_job")
    assert terminated["jobId"] == "a" and "viva_core" in terminated["reason"]


@pytest.mark.asyncio
async def test_logs_come_from_the_definitions_group_and_the_jobs_stream() -> None:
    fake = FakeBatch(
        definitions={
            "base-core:7": [
                {
                    "revision": 7,
                    "containerProperties": {"logConfiguration": {"options": {"awslogs-group": "/aws/batch/core"}}},
                }
            ]
        },
        jobs=[
            {"jobId": "a", "jobDefinition": "base-core:7", "status": "RUNNING", "container": {"logStreamName": "s/1"}}
        ],
    )
    logs = FakeLogs(["one", "two", "three"])
    backend = _backend(fake, logs)
    handle = JobHandle(backend="batch-container", id="a")
    assert await backend.logs(handle) == ["one", "two", "three"]
    assert logs.calls[-1] == {"logGroupName": "/aws/batch/core", "logStreamName": "s/1", "startFromHead": True}
    assert await backend.logs(handle, tail=2) == ["two", "three"]
    assert logs.calls[-1]["startFromHead"] is False and logs.calls[-1]["limit"] == 2
    # not started yet: no stream, no lines, no error
    fake.jobs[0]["container"] = {}
    assert await backend.logs(handle) == []
    assert await backend.logs(JobHandle(backend="batch-container", id="ghost")) == []


@pytest.mark.asyncio
async def test_logs_without_a_logs_client_say_so() -> None:
    with pytest.raises(RuntimeError, match="no CloudWatch Logs client"):
        await _backend(FakeBatch()).logs(JobHandle(backend="batch-container", id="a"))
