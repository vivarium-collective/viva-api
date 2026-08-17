"""ComposeJobMonitor.update_analysis_retries: the OOM-retry-escalation poller
(item 50 Gap 6) — no AWS, no Postgres; sim_registry/database_service are mocked."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.models import JobId, JobStatus
from viva_api.compose.job_monitor import ComposeJobMonitor
from viva_api.compose.models import ComposeAnalysis, ComposeAnalysisStatus
from viva_api.config import ComputeBackend


def _analysis(**overrides: object) -> ComposeAnalysis:
    base: dict[str, object] = {
        "database_id": 1,
        "name": "analysis-exp-1-abcdef",
        "config": {
            "n_seeds": 2,
            "n_generations": 2,
            "modules": "applicable",
            "analysis_name": "analysis-exp-1-abcdef",
        },
        "simulation_id": 42,
        "job_id_ext": "batch-job-1",
        "status": ComposeAnalysisStatus.COMPUTING,
        "attempt": 1,
    }
    base.update(overrides)
    return ComposeAnalysis(**base)  # type: ignore[arg-type]


def _monitor(*, ray_service: object | None, active_analyses: list[ComposeAnalysis]) -> ComposeJobMonitor:
    db_service = MagicMock()
    db_service.get_analysis_db.return_value.list_active_analyses = AsyncMock(return_value=active_analyses)
    db_service.get_analysis_db.return_value.update_analysis_status = AsyncMock()
    db_service.get_analysis_db.return_value.update_analysis_job_id = AsyncMock()
    registry = {ComputeBackend.RAY: ray_service} if ray_service is not None else {}
    return ComposeJobMonitor(nats_client=None, database_service=db_service, sim_registry=registry)


def _ray_service(job_status_info: JobStatusInfo | None) -> MagicMock:
    svc = MagicMock()
    svc.get_job_status_info = AsyncMock(return_value=job_status_info)
    svc.resubmit_analysis = AsyncMock(return_value="retry-batch-job")
    return svc


@pytest.mark.asyncio
async def test_no_ray_backend_registered_is_a_clean_noop() -> None:
    monitor = _monitor(ray_service=None, active_analyses=[_analysis()])
    await monitor.update_analysis_retries()
    monitor.database_service.get_analysis_db.return_value.list_active_analyses.assert_not_awaited()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_ray_backend_without_analysis_methods_is_a_clean_noop() -> None:
    """A registry entry that can't do resubmit_analysis/get_job_status_info (e.g. a
    stub or a future non-Ray backend keyed under RAY somehow) is skipped, not crashed
    on — OOM-retry-escalation is fundamentally a Batch/Ray-memory concept."""
    bare_service = MagicMock(spec=[])  # no attributes at all
    monitor = _monitor(ray_service=bare_service, active_analyses=[_analysis()])
    await monitor.update_analysis_retries()
    monitor.database_service.get_analysis_db.return_value.list_active_analyses.assert_not_awaited()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_no_active_analyses_is_a_clean_noop() -> None:
    ray_service = _ray_service(None)
    monitor = _monitor(ray_service=ray_service, active_analyses=[])
    await monitor.update_analysis_retries()
    ray_service.get_job_status_info.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_terminal_status_waits_no_db_write() -> None:
    info = JobStatusInfo(
        job_id=JobId.ray("batch-job-1"),
        status=JobStatus.RUNNING,
        start_time=None,
        end_time=None,
        exit_code=None,
        error_message=None,
    )
    ray_service = _ray_service(info)
    analysis = _analysis()
    monitor = _monitor(ray_service=ray_service, active_analyses=[analysis])

    await monitor.update_analysis_retries()

    analysis_db = monitor.database_service.get_analysis_db.return_value  # type: ignore[attr-defined]
    analysis_db.update_analysis_status.assert_not_awaited()
    analysis_db.update_analysis_job_id.assert_not_awaited()
    ray_service.resubmit_analysis.assert_not_awaited()


@pytest.mark.asyncio
async def test_completed_marks_ready() -> None:
    info = JobStatusInfo(
        job_id=JobId.ray("batch-job-1"),
        status=JobStatus.COMPLETED,
        start_time=None,
        end_time=None,
        exit_code=None,
        error_message=None,
    )
    ray_service = _ray_service(info)
    analysis = _analysis()
    monitor = _monitor(ray_service=ray_service, active_analyses=[analysis])

    await monitor.update_analysis_retries()

    analysis_db = monitor.database_service.get_analysis_db.return_value  # type: ignore[attr-defined]
    analysis_db.update_analysis_status.assert_awaited_once_with(1, ComposeAnalysisStatus.READY)


@pytest.mark.asyncio
async def test_oom_escalates_and_records_the_new_job_id_and_attempt() -> None:
    info = JobStatusInfo(
        job_id=JobId.ray("batch-job-1"),
        status=JobStatus.FAILED,
        start_time=None,
        end_time=None,
        exit_code="137",
        error_message="OutOfMemoryError",
    )
    ray_service = _ray_service(info)
    analysis = _analysis(attempt=1)
    monitor = _monitor(ray_service=ray_service, active_analyses=[analysis])

    await monitor.update_analysis_retries()

    ray_service.resubmit_analysis.assert_awaited_once()
    call = ray_service.resubmit_analysis.call_args
    assert call.args[0] is analysis
    assert call.kwargs["memory_mib"] == 58 * 1024 * 2  # default baseline x (attempt+1)

    analysis_db = monitor.database_service.get_analysis_db.return_value  # type: ignore[attr-defined]
    analysis_db.update_analysis_job_id.assert_awaited_once_with(1, job_id_ext="retry-batch-job", attempt=2)
    analysis_db.update_analysis_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_oom_at_max_attempts_marks_failed_not_escalated() -> None:
    # error_message=None here (unlike the OOM-escalate test above) so this test
    # exercises decide_analysis_retry's own "OOM: retries exhausted" fallback
    # message, not just whatever string Batch happened to report.
    info = JobStatusInfo(
        job_id=JobId.ray("batch-job-1"),
        status=JobStatus.FAILED,
        start_time=None,
        end_time=None,
        exit_code="137",
        error_message=None,
    )
    ray_service = _ray_service(info)
    analysis = _analysis(attempt=3)
    monitor = _monitor(ray_service=ray_service, active_analyses=[analysis])

    await monitor.update_analysis_retries()

    ray_service.resubmit_analysis.assert_not_awaited()
    analysis_db = monitor.database_service.get_analysis_db.return_value  # type: ignore[attr-defined]
    analysis_db.update_analysis_status.assert_awaited_once_with(
        1, ComposeAnalysisStatus.FAILED, error_message="OOM: retries exhausted"
    )


@pytest.mark.asyncio
async def test_non_oom_failure_marks_failed_with_the_real_error_message() -> None:
    info = JobStatusInfo(
        job_id=JobId.ray("batch-job-1"),
        status=JobStatus.FAILED,
        start_time=None,
        end_time=None,
        exit_code="1",
        error_message="ModuleNotFoundError: no module named 'reports'",
    )
    ray_service = _ray_service(info)
    analysis = _analysis(attempt=1)
    monitor = _monitor(ray_service=ray_service, active_analyses=[analysis])

    await monitor.update_analysis_retries()

    ray_service.resubmit_analysis.assert_not_awaited()
    analysis_db = monitor.database_service.get_analysis_db.return_value  # type: ignore[attr-defined]
    analysis_db.update_analysis_status.assert_awaited_once_with(
        1, ComposeAnalysisStatus.FAILED, error_message="ModuleNotFoundError: no module named 'reports'"
    )


@pytest.mark.asyncio
async def test_explicit_memory_mib_hint_in_config_overrides_the_default_baseline() -> None:
    info = JobStatusInfo(
        job_id=JobId.ray("batch-job-1"),
        status=JobStatus.FAILED,
        start_time=None,
        end_time=None,
        exit_code="137",
        error_message="OutOfMemoryError",
    )
    ray_service = _ray_service(info)
    analysis = _analysis(
        attempt=1,
        config={
            "n_seeds": 2,
            "n_generations": 2,
            "modules": "applicable",
            "analysis_name": "analysis-exp-1-abcdef",
            "memory_mib": 30000,
        },
    )
    monitor = _monitor(ray_service=ray_service, active_analyses=[analysis])

    await monitor.update_analysis_retries()

    assert ray_service.resubmit_analysis.call_args.kwargs["memory_mib"] == 30000 * 2


@pytest.mark.asyncio
async def test_one_analysis_erroring_does_not_stop_the_others_in_the_same_poll() -> None:
    ray_service = MagicMock()
    ray_service.get_job_status_info = AsyncMock(side_effect=[RuntimeError("boom"), None])
    ray_service.resubmit_analysis = AsyncMock()
    a1, a2 = _analysis(database_id=1, job_id_ext="job-1"), _analysis(database_id=2, job_id_ext="job-2")
    monitor = _monitor(ray_service=ray_service, active_analyses=[a1, a2])

    await monitor.update_analysis_retries()  # must not raise

    assert ray_service.get_job_status_info.await_count == 2
