"""list_simulation_analyses (item 111) -- unlike handle_get_ray_analysis_status
(the per-ID endpoint, already correct), this LIST endpoint used to be a plain
DB read with zero live resolution: a Ray-backend row could show a stale,
non-terminal status here indefinitely, confirmed live -- 90+ minutes frozen
at "running" after the real job had already succeeded, resolved instantly
once the per-ID endpoint was queried directly. These tests prove the list
endpoint now reuses that same resolver rather than needing a caller to know
the separate endpoint exists."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from viva_api.analysis.models import AnalysisConfig, AnalysisConfigOptions, ExperimentAnalysisDTO
from viva_api.common.handlers.analyses import list_simulation_analyses
from viva_api.common.models import JobStatus


def _make_record(
    *, database_id: int, status: JobStatus, backend: str | None = "ray", result_uri: str | None = "s3://bucket/exp"
) -> ExperimentAnalysisDTO:
    config = AnalysisConfig(analysis_options=AnalysisConfigOptions(experiment_id=["exp1"]))
    return ExperimentAnalysisDTO(
        database_id=database_id,
        name=f"analysis-{database_id}",
        config=config,
        last_updated="now",
        backend=backend,
        result_uri=result_uri,
        job_id_ext=f"job-{database_id}",
        status=status,
    )


@pytest.mark.asyncio
async def test_terminal_and_non_ray_rows_are_returned_without_any_live_probe() -> None:
    """A COMPLETED/FAILED row, and a non-ray-backend row regardless of its
    status, must never trigger a live resolution -- only a non-terminal
    backend="ray" row is a candidate for one."""
    completed = _make_record(database_id=1, status=JobStatus.COMPLETED)
    non_ray = _make_record(database_id=2, status=JobStatus.RUNNING, backend="slurm")
    db_service = AsyncMock()
    db_service.get_simulation.return_value = SimpleNamespace(experiment_id="exp1")
    db_service.list_analyses.return_value = [completed, non_ray]

    with patch("viva_api.common.handlers.analyses.handle_get_ray_analysis_status") as fake_resolve:
        result = await list_simulation_analyses(db_service, simulation_id=42)

    fake_resolve.assert_not_called()
    assert [r.status for r in result] == [JobStatus.COMPLETED, JobStatus.RUNNING]


@pytest.mark.asyncio
async def test_non_terminal_ray_row_is_lazily_resolved_and_reflects_the_fresh_status() -> None:
    """The real fix: a non-terminal backend='ray' row gets the SAME live
    resolution the per-ID endpoint already does, and the LIST response
    reflects it in this same call -- a caller of only this endpoint sees
    fresh data, not the stale stored value."""
    from viva_api.analysis.models import AnalysisRun

    stale = _make_record(database_id=3, status=JobStatus.RUNNING)
    db_service = AsyncMock()
    db_service.get_simulation.return_value = SimpleNamespace(experiment_id="exp1")
    db_service.list_analyses.return_value = [stale]

    with patch(
        "viva_api.common.handlers.analyses.handle_get_ray_analysis_status",
        return_value=AnalysisRun(id=3, status=JobStatus.COMPLETED),
    ) as fake_resolve:
        result = await list_simulation_analyses(db_service, simulation_id=42)

    fake_resolve.assert_called_once()
    assert fake_resolve.call_args.args[1].database_id == 3
    assert len(result) == 1
    assert result[0].status == JobStatus.COMPLETED
    # The original record object is not mutated in place -- a fresh copy is
    # returned, so nothing else holding a reference to `stale` is surprised.
    assert stale.status == JobStatus.RUNNING


@pytest.mark.asyncio
async def test_a_failed_live_resolution_falls_back_to_the_stored_status_not_a_crash() -> None:
    """A transient failure in the live probe (e.g. S3 hiccup) must not break
    the list endpoint -- the caller still gets the stored (possibly stale)
    status rather than a 500."""
    stale = _make_record(database_id=4, status=JobStatus.RUNNING)
    db_service = AsyncMock()
    db_service.get_simulation.return_value = SimpleNamespace(experiment_id="exp1")
    db_service.list_analyses.return_value = [stale]

    with patch(
        "viva_api.common.handlers.analyses.handle_get_ray_analysis_status",
        side_effect=RuntimeError("S3 hiccup"),
    ):
        result = await list_simulation_analyses(db_service, simulation_id=42)

    assert len(result) == 1
    assert result[0].status == JobStatus.RUNNING


@pytest.mark.asyncio
async def test_unknown_simulation_id_still_raises() -> None:
    db_service = AsyncMock()
    db_service.get_simulation.return_value = None

    with pytest.raises(ValueError, match="not found"):
        await list_simulation_analyses(db_service, simulation_id=999)
