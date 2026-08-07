"""handle_get_ray_analysis_status resolves a Ray-native standalone analysis's
status via S3-exists, since there is no persistent job-status API for the
backing K8s Job (ttl_seconds_after_finished=86400 expires it after 24h).
This is the wiring analysis-results-design.md already designed (DB columns
and service methods existed, unused) but never connected for the K8s path."""

from unittest.mock import AsyncMock, patch

import pytest

from viva_api.analysis.models import AnalysisConfig, AnalysisConfigOptions, ExperimentAnalysisDTO
from viva_api.common.handlers.analyses import handle_get_ray_analysis_status
from viva_api.common.models import JobId, JobStatus
from viva_api.simulation.tables_orm import AnalysisStatusDB


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
async def test_manifest_lookup_uses_a_bucket_relative_key_not_the_full_uri() -> None:
    """Regression: record.result_uri is a full s3://<bucket>/... URI, but
    S3FilePath.s3_path is documented (and FileServiceS3 relies on it) as
    BUCKET-RELATIVE -- FileServiceS3 resolves the bucket separately from
    settings and uses s3_path.s3_path as the literal object key. Passing the
    full URI straight into S3FilePath (via Path(f"{result_uri}/_manifest.json"))
    double-prefixed the bucket into the key AND, via Path()'s slash-collapsing,
    mangled "s3://" into "s3:/", so the constructed key never matched a real
    object -- every manifest existence check silently 404'd regardless of
    whether the manifest genuinely existed. Live-reproduced 2026-08-05:
    atlantis analysis status kept reporting "running" 20+ minutes after the
    K8s pod had completed and written a real, valid manifest to S3. This test
    inspects the ACTUAL key handed to get_file_contents (the prior test only
    asserted on a hardcoded mock return value, which can't catch a wrong-key
    bug since the mock ignores its input entirely)."""
    record = _make_record()
    db_service = AsyncMock()
    manifest = (
        b'{"written": ["s3://bucket/exp/analyses/analysis-exp1-ab12/doubling_time_distribution.json"], "errors": []}'
    )
    fake_file_service = AsyncMock()
    fake_file_service.get_file_contents.return_value = manifest

    with patch("viva_api.common.handlers.analyses.get_file_service", return_value=fake_file_service):
        await handle_get_ray_analysis_status(db_service=db_service, record=record)

    fake_file_service.get_file_contents.assert_called_once()
    requested_path = str(fake_file_service.get_file_contents.call_args.args[0])
    assert requested_path == "exp/analyses/analysis-exp1-ab12/_manifest.json"
    assert "s3:" not in requested_path
    assert "bucket" not in requested_path


@pytest.mark.asyncio
async def test_manifest_present_with_output_marks_ready() -> None:
    record = _make_record()
    db_service = AsyncMock()
    manifest = (
        b'{"written": ["s3://bucket/exp/analyses/analysis-exp1-ab12/doubling_time_distribution.json"], "errors": []}'
    )
    fake_file_service = AsyncMock()
    fake_file_service.get_file_contents.return_value = manifest

    with patch("viva_api.common.handlers.analyses.get_file_service", return_value=fake_file_service):
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

    with patch("viva_api.common.handlers.analyses.get_file_service", return_value=fake_file_service):
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
        patch("viva_api.common.handlers.analyses.get_file_service", return_value=fake_file_service),
        patch("viva_api.common.handlers.analyses.get_simulation_service", return_value=fake_sim_service),
    ):
        result = await handle_get_ray_analysis_status(db_service=db_service, record=record)

    assert result.status == JobStatus.RUNNING
    db_service.update_analysis_status.assert_not_called()


@pytest.mark.asyncio
async def test_no_manifest_but_k8s_job_failed_marks_failed_early() -> None:
    """Catches a hard failure (ImagePullBackOff, scheduling error) before the
    24h TTL would otherwise leave this stuck COMPUTING forever with no
    manifest ever coming."""
    from viva_api.common.hpc.job_service import JobStatusInfo

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
        patch("viva_api.common.handlers.analyses.get_file_service", return_value=fake_file_service),
        patch("viva_api.common.handlers.analyses.get_simulation_service", return_value=fake_sim_service),
    ):
        result = await handle_get_ray_analysis_status(db_service=db_service, record=record)

    assert result.status == JobStatus.FAILED
    assert result.error_log == "ImagePullBackOff"
    db_service.update_analysis_status.assert_called_once_with(
        1,
        AnalysisStatusDB.FAILED,
        error_message="ImagePullBackOff",
    )
