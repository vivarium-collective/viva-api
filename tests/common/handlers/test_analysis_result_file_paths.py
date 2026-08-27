"""fetch_analysis_data / handle_get_ray_analysis_plots regression coverage,
independent of the DB-backed e2e tests in tests/api/ecoli/test_analysis_endpoints.py
(which need a real Postgres testcontainer). Mirrors
tests/common/handlers/test_ray_analysis_status.py's pattern: mock db_service and
file_service directly, and assert on the ACTUAL s3_path handed to the file
service -- not just a hardcoded mock return value, which can't catch a wrong-key
bug since the mock would ignore its input entirely.

Regression for the real bug (cplong90, live on smsvpctest, 2026-08-27):
GET /analyses/{id}/data returned 200 [] for a real, completed, non-empty
analysis, and GET /analyses/{id}/plots 500'd unconditionally for every
Ray/K8s-backend analysis. Both traced to viva_api/common/handlers/analyses.py."""

import datetime
from unittest.mock import AsyncMock, patch

import pytest

from viva_api.common.handlers.analyses import (
    AnalysisNotReadyError,
    fetch_analysis_data,
    handle_get_ray_analysis_plots,
)
from viva_api.common.models import JobStatus
from viva_api.common.storage.file_service import ListingItem


def _listing_item(key: str, size: int = 10) -> ListingItem:
    return ListingItem(Key=key, LastModified=datetime.datetime(2026, 1, 1), ETag="x", Size=size)


@pytest.mark.asyncio
async def test_fetch_analysis_data_strips_the_s3_scheme_before_listing() -> None:
    """The stored result_uri is a full s3://<bucket>/... URI; S3FilePath.s3_path
    is bucket-relative. Passing the raw URI through Path(...) mangles
    "s3://" into "s3:/" (pathlib collapses the double slash), so the listing
    silently returns nothing -- this asserts the LISTING call receives a clean,
    bucket-relative prefix, not just that the final response happens to be
    non-empty (which a differently-broken implementation could also produce)."""
    db_service = AsyncMock()
    db_service.get_analysis.return_value = AsyncMock(
        database_id=13,
        status=JobStatus.COMPLETED,
        result_uri="s3://some-bucket/vecoli-output/exp/analyses/name",
    )
    file_service = AsyncMock()
    file_service.get_listing.return_value = [
        _listing_item("vecoli-output/exp/analyses/name/variant=0/gen=0/proteomics.tsv"),
    ]
    file_service.get_file_contents.return_value = b"a\tb\n1\t2\n"

    with patch("viva_api.common.handlers.analyses.get_file_service", return_value=file_service):
        outputs = await fetch_analysis_data(db_service=db_service, analysis_id=13)

    requested_prefix = str(file_service.get_listing.call_args.args[0])
    assert requested_prefix == "vecoli-output/exp/analyses/name"
    assert "s3:" not in requested_prefix
    assert "some-bucket" not in requested_prefix
    assert len(outputs) == 1
    assert outputs[0].filename == "proteomics.tsv"


@pytest.mark.asyncio
async def test_fetch_analysis_data_filters_non_output_extensions() -> None:
    """A run's own analysis.json summary (or any other non tsv/csv/txt/html
    object under the same prefix) must not come back as a "data" file."""
    db_service = AsyncMock()
    db_service.get_analysis.return_value = AsyncMock(
        database_id=13, status=JobStatus.COMPLETED, result_uri="s3://bucket/exp/analyses/name"
    )
    file_service = AsyncMock()
    file_service.get_listing.return_value = [
        _listing_item("exp/analyses/name/proteomics.tsv"),
        _listing_item("exp/analyses/name/analysis.json"),
        _listing_item("exp/analyses/name/_manifest.json"),
    ]
    file_service.get_file_contents.return_value = b"a\tb\n1\t2\n"

    with patch("viva_api.common.handlers.analyses.get_file_service", return_value=file_service):
        outputs = await fetch_analysis_data(db_service=db_service, analysis_id=13)

    assert [o.filename for o in outputs] == ["proteomics.tsv"]


@pytest.mark.asyncio
async def test_fetch_analysis_data_not_ready_raises() -> None:
    db_service = AsyncMock()
    db_service.get_analysis.return_value = AsyncMock(status=JobStatus.RUNNING, result_uri=None)

    with pytest.raises(AnalysisNotReadyError):
        await fetch_analysis_data(db_service=db_service, analysis_id=13)


@pytest.mark.asyncio
async def test_ray_plots_strips_the_s3_scheme_and_filters_to_html() -> None:
    """Regression: handle_get_ray_analysis_plots didn't exist at all before this
    fix -- GET /analyses/{id}/plots always 500'd for a Ray-backend analysis
    (only the legacy SLURM local-filesystem path was implemented)."""
    record = AsyncMock(
        database_id=13,
        status=JobStatus.COMPLETED,
        result_uri="s3://some-bucket/vecoli-output/exp/analyses/name",
    )
    db_service = AsyncMock()
    file_service = AsyncMock()
    file_service.get_listing.return_value = [
        _listing_item("vecoli-output/exp/analyses/name/plots/mass_fraction.html"),
        _listing_item("vecoli-output/exp/analyses/name/analysis.json"),
    ]
    file_service.get_file_contents.return_value = b"<html>plot</html>"

    with patch("viva_api.common.handlers.analyses.get_file_service", return_value=file_service):
        outputs = await handle_get_ray_analysis_plots(db_service=db_service, record=record)

    requested_prefix = str(file_service.get_listing.call_args.args[0])
    assert requested_prefix == "vecoli-output/exp/analyses/name"
    assert "s3:" not in requested_prefix
    assert len(outputs) == 1
    assert outputs[0].name == "mass_fraction.html"
    assert outputs[0].content == "<html>plot</html>"


@pytest.mark.asyncio
async def test_ray_plots_not_ready_raises() -> None:
    record = AsyncMock(status=JobStatus.RUNNING, result_uri=None, database_id=13)
    db_service = AsyncMock()

    with pytest.raises(AnalysisNotReadyError):
        await handle_get_ray_analysis_plots(db_service=db_service, record=record)
