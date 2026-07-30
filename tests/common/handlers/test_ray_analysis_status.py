"""handle_get_ray_analysis_status resolves a Ray-native standalone analysis's
status via S3-exists, since there is no persistent job-status API for the
backing K8s Job (ttl_seconds_after_finished=86400 expires it after 24h).
This is the wiring analysis-results-design.md already designed (DB columns
and service methods existed, unused) but never connected for the K8s path."""

from unittest.mock import AsyncMock, patch

import pytest

from sms_api.analysis.models import AnalysisConfig, AnalysisConfigOptions, ExperimentAnalysisDTO
from sms_api.common.handlers.analyses import handle_get_ray_analysis_status
from sms_api.common.models import JobId, JobStatus
from sms_api.simulation.tables_orm import AnalysisStatusDB


def _make_record(*, status: JobStatus = JobStatus.RUNNING) -> ExperimentAnalysisDTO:
    config = AnalysisConfig(analysis_options=AnalysisConfigOptions(experiment_id=["exp1"]))
    return ExperimentAnalysisDTO(
        database_id=1,
        name="analysis-exp1-ab12",
        config=config,
        last_updated="now",
        backend="ray",
        result_uri="s3://bucket/exp/analyses/analysis-exp1-ab12",
        job_id_ext="ana-exp1-ab12",
        status=status,
    )


@pytest.mark.asyncio
async def test_returns_terminal_status_without_any_probe() -> None:
    """A row already marked terminal (READY/FAILED) is returned as-is --
    never re-probes S3 or the K8s job for an analysis that already resolved."""
    record = _make_record(status=JobStatus.COMPLETED)
    db_service = AsyncMock()

    result = await handle_get_ray_analysis_status(db_service=db_service, record=record)

    assert result.status == JobStatus.COMPLETED
    db_service.update_analysis_status.assert_not_called()


@pytest.mark.asyncio
async def test_manifest_present_with_output_marks_ready() -> None:
    record = _make_record()
    db_service = AsyncMock()
    manifest = (
        b'{"written": ["s3://bucket/exp/analyses/analysis-exp1-ab12/doubling_time_distribution.json"], "errors": []}'
    )
    fake_file_service = AsyncMock()
    fake_file_service.get_file_contents.return_value = manifest

    with patch("sms_api.common.handlers.analyses.get_file_service", return_value=fake_file_service):
        result = await handle_get_ray_analysis_status(db_service=db_service, record=record)

    assert result.status == JobStatus.COMPLETED
    db_service.update_analysis_status.assert_called_once_with(
        1,
        AnalysisStatusDB.READY,
        result_uri=record.result_uri,
    )


@pytest.mark.asyncio
async def test_manifest_present_with_only_errors_marks_failed() -> None:
    record = _make_record()
    db_service = AsyncMock()
    manifest = b'{"written": [], "errors": [{"name": "doubling_time_distribution", "error": "boom"}]}'
    fake_file_service = AsyncMock()
    fake_file_service.get_file_contents.return_value = manifest

    with patch("sms_api.common.handlers.analyses.get_file_service", return_value=fake_file_service):
        result = await handle_get_ray_analysis_status(db_service=db_service, record=record)

    assert result.status == JobStatus.FAILED
    assert "boom" in (result.error_log or "")
    db_service.update_analysis_status.assert_called_once()
    assert db_service.update_analysis_status.call_args.args[1] == AnalysisStatusDB.FAILED


@pytest.mark.asyncio
async def test_no_manifest_yet_and_job_still_running_stays_computing() -> None:
    record = _make_record()
    db_service = AsyncMock()
    fake_file_service = AsyncMock()
    fake_file_service.get_file_contents.return_value = None
    fake_sim_service = AsyncMock()
    fake_sim_service.get_job_status.return_value = None  # job vanished/still scheduling

    with (
        patch("sms_api.common.handlers.analyses.get_file_service", return_value=fake_file_service),
        patch("sms_api.common.handlers.analyses.get_simulation_service", return_value=fake_sim_service),
    ):
        result = await handle_get_ray_analysis_status(db_service=db_service, record=record)

    assert result.status == JobStatus.RUNNING
    db_service.update_analysis_status.assert_not_called()


@pytest.mark.asyncio
async def test_no_manifest_but_k8s_job_failed_marks_failed_early() -> None:
    """Catches a hard failure (ImagePullBackOff, scheduling error) before the
    24h TTL would otherwise leave this stuck COMPUTING forever with no
    manifest ever coming."""
    from sms_api.common.hpc.job_service import JobStatusInfo

    record = _make_record()
    db_service = AsyncMock()
    fake_file_service = AsyncMock()
    fake_file_service.get_file_contents.return_value = None
    fake_sim_service = AsyncMock()
    fake_sim_service.get_job_status.return_value = JobStatusInfo(
        job_id=JobId.k8s("ana-exp1-ab12"),
        status=JobStatus.FAILED,
        error_message="ImagePullBackOff",
    )

    with (
        patch("sms_api.common.handlers.analyses.get_file_service", return_value=fake_file_service),
        patch("sms_api.common.handlers.analyses.get_simulation_service", return_value=fake_sim_service),
    ):
        result = await handle_get_ray_analysis_status(db_service=db_service, record=record)

    assert result.status == JobStatus.FAILED
    assert result.error_log == "ImagePullBackOff"
    db_service.update_analysis_status.assert_called_once_with(
        1,
        AnalysisStatusDB.FAILED,
        error_message="ImagePullBackOff",
    )
