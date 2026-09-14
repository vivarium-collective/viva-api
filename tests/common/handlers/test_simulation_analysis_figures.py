"""Coverage for the simulation analysis-figure serve endpoints (viva-api#648).

These handlers let the API serve rendered analysis artifacts (viz/*.html,
ptools/*.tsv) from S3 with its OWN credentials, so a credential-less client (the
hosted workbench pod) can reach them. The load-bearing behaviours:

* the UNION -- a hand-dispatched "fill" analysis that created no DB record still
  surfaces, discovered by walking ``<out_uri>/analyses/*`` (source="s3"),
  alongside record-backed analyses (source="record");
* the LISTING call receives a clean, bucket-relative ``.../analyses/`` prefix
  (the s3://-scheme-mangling bug class that already bit this file twice);
* the fetch path is confined to ``viz/``/``ptools/`` with no ``..`` traversal.

Mirrors tests/common/handlers/test_analysis_result_file_paths.py: mock db_service
+ file_service directly, assert on the ACTUAL s3_path handed to the file service.
"""

import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from viva_api.common.handlers.analyses import (
    fetch_simulation_analysis_figure,
    list_simulation_analysis_figures,
)
from viva_api.common.models import JobStatus
from viva_api.common.storage.file_service import ListingItem


def _listing_item(key: str, size: int = 10) -> ListingItem:
    return ListingItem(Key=key, LastModified=datetime.datetime(2026, 1, 1), ETag="x", Size=size)


def _record(result_uri: str | None, name: str, status: JobStatus = JobStatus.COMPLETED) -> SimpleNamespace:
    return SimpleNamespace(result_uri=result_uri, name=name, status=status)


@pytest.mark.asyncio
async def test_list_unions_record_backed_and_s3_only_fills() -> None:
    """A record-backed analysis AND a record-less S3 fill both surface, tagged by
    ``source``; a clean bucket-relative analyses/ prefix is listed once."""
    db_service = AsyncMock()
    db_service.get_simulation.return_value = SimpleNamespace(
        out_uri="s3://bucket/vecoli-output/exp", experiment_id="exp"
    )
    db_service.list_analyses.return_value = [
        _record("s3://bucket/vecoli-output/exp/analyses/analysis-mnp-x", "analysis-mnp-x"),
    ]
    file_service = AsyncMock()
    file_service.get_listing.return_value = [
        _listing_item("vecoli-output/exp/analyses/analysis-mnp-x/viz/chromosome_state_view__variant=0_seed=0.html"),
        _listing_item("vecoli-output/exp/analyses/analysis-mnp-x/ptools/ptools_overview__variant=0_seed=0.tsv"),
        _listing_item("vecoli-output/exp/analyses/analysis-mnp-x/analysis.json"),  # ignored (not viz/ptools)
        _listing_item("vecoli-output/exp/analyses/analysis-ptools-multiseed/ptools/ptools_rna__variant=0.tsv"),
    ]

    with patch("viva_api.common.handlers.analyses.get_file_service", return_value=file_service):
        result = await list_simulation_analysis_figures(db_service=db_service, simulation_id=7)

    # Listed exactly once, with a clean bucket-relative analyses/ prefix.
    requested_prefix = str(file_service.get_listing.call_args.args[0])
    assert requested_prefix == "vecoli-output/exp/analyses"
    assert "s3:" not in requested_prefix and "bucket" not in requested_prefix

    assert result.available is True and result.reason == "ok"
    by_name = {g.name: g for g in result.analyses}

    mnp = by_name["analysis-mnp-x"]
    assert mnp.source == "record"
    assert mnp.status != "s3-only"
    assert [f.path for f in mnp.figures] == ["viz/chromosome_state_view__variant=0_seed=0.html"]
    assert [p.path for p in mnp.ptools] == ["ptools/ptools_overview__variant=0_seed=0.tsv"]

    fill = by_name["analysis-ptools-multiseed"]
    assert fill.source == "s3"
    assert fill.status == "s3-only"
    assert fill.figures == []
    assert [p.path for p in fill.ptools] == ["ptools/ptools_rna__variant=0.tsv"]
    assert fill.result_uri == "s3://bucket/vecoli-output/exp/analyses/analysis-ptools-multiseed"


@pytest.mark.asyncio
async def test_list_resolves_root_from_out_uri_when_no_records() -> None:
    """With zero DB records, the analyses root falls back to the simulation's own
    out_uri so pure-fill sims (no analysis ever registered) still surface."""
    from viva_api.common.storage import data_layout

    db_service = AsyncMock()
    db_service.get_simulation.return_value = SimpleNamespace(experiment_id="exp")
    db_service.list_analyses.return_value = []
    # The fallback root is the layout's single-nested output base for the experiment.
    expected_prefix = data_layout.RayLayout.experiment_prefix("exp") + "/analyses"
    file_service = AsyncMock()
    file_service.get_listing.return_value = [
        _listing_item(f"{expected_prefix}/analysis-ptools-single/ptools/ptools_metabolites__variant=0.tsv"),
    ]

    with patch("viva_api.common.handlers.analyses.get_file_service", return_value=file_service):
        result = await list_simulation_analysis_figures(db_service=db_service, simulation_id=7)

    assert str(file_service.get_listing.call_args.args[0]) == expected_prefix
    assert "s3:" not in str(file_service.get_listing.call_args.args[0])
    assert result.available is True
    assert [g.name for g in result.analyses] == ["analysis-ptools-single"]
    assert result.analyses[0].source == "s3"


@pytest.mark.asyncio
async def test_list_reports_no_figures_when_walk_is_empty() -> None:
    db_service = AsyncMock()
    db_service.get_simulation.return_value = SimpleNamespace(out_uri="s3://bucket/exp", experiment_id="exp")
    db_service.list_analyses.return_value = []
    file_service = AsyncMock()
    file_service.get_listing.return_value = []

    with patch("viva_api.common.handlers.analyses.get_file_service", return_value=file_service):
        result = await list_simulation_analysis_figures(db_service=db_service, simulation_id=7)

    assert result.available is False and result.reason == "no-analyses"
    assert result.analyses == []


@pytest.mark.asyncio
async def test_list_surfaces_null_result_uri_record_as_flaggable_row() -> None:
    """A completed analysis whose result_uri was never written surfaces as an
    empty record-backed row so the caller can flag the write-side gap."""
    db_service = AsyncMock()
    db_service.get_simulation.return_value = SimpleNamespace(out_uri="s3://bucket/exp", experiment_id="exp")
    db_service.list_analyses.return_value = [_record(None, "analysis-orphan")]
    file_service = AsyncMock()
    file_service.get_listing.return_value = []

    with patch("viva_api.common.handlers.analyses.get_file_service", return_value=file_service):
        result = await list_simulation_analysis_figures(db_service=db_service, simulation_id=7)

    orphan = next(g for g in result.analyses if g.name == "analysis-orphan")
    assert orphan.source == "record" and orphan.result_uri is None
    assert orphan.figures == [] and orphan.ptools == []


@pytest.mark.asyncio
async def test_fetch_resolves_record_uri_and_types_html() -> None:
    db_service = AsyncMock()
    db_service.get_simulation.return_value = SimpleNamespace(out_uri="s3://bucket/exp", experiment_id="exp")
    db_service.list_analyses.return_value = [
        _record("s3://bucket/vecoli-output/exp/analyses/analysis-mnp-x", "analysis-mnp-x"),
    ]
    file_service = AsyncMock()
    file_service.get_file_contents.return_value = b"<html>plot</html>"

    with patch("viva_api.common.handlers.analyses.get_file_service", return_value=file_service):
        body, content_type = await fetch_simulation_analysis_figure(
            db_service=db_service, simulation_id=7, analysis_name="analysis-mnp-x", relpath="viz/foo.html"
        )

    key = str(file_service.get_file_contents.call_args.args[0])
    assert key == "vecoli-output/exp/analyses/analysis-mnp-x/viz/foo.html"
    assert "s3:" not in key
    assert body == b"<html>plot</html>"
    assert content_type == "text/html; charset=utf-8"


@pytest.mark.asyncio
async def test_fetch_constructs_uri_for_s3_only_fill() -> None:
    """An analysis with no DB record is still fetchable: its uri is constructed
    from the resolved analyses root + name."""
    from viva_api.common.storage import data_layout

    db_service = AsyncMock()
    db_service.get_simulation.return_value = SimpleNamespace(experiment_id="exp")
    db_service.list_analyses.return_value = []
    file_service = AsyncMock()
    file_service.get_file_contents.return_value = b"a\tb\n1\t2\n"

    with patch("viva_api.common.handlers.analyses.get_file_service", return_value=file_service):
        body, content_type = await fetch_simulation_analysis_figure(
            db_service=db_service, simulation_id=7, analysis_name="analysis-ptools-multiseed", relpath="ptools/x.tsv"
        )

    key = str(file_service.get_file_contents.call_args.args[0])
    expected_prefix = data_layout.RayLayout.experiment_prefix("exp")
    assert key == f"{expected_prefix}/analyses/analysis-ptools-multiseed/ptools/x.tsv"
    assert "s3:" not in key
    assert content_type == "text/plain; charset=utf-8"
    assert body == b"a\tb\n1\t2\n"


@pytest.mark.asyncio
async def test_fetch_missing_object_raises_not_found() -> None:
    db_service = AsyncMock()
    db_service.get_simulation.return_value = SimpleNamespace(out_uri="s3://bucket/exp", experiment_id="exp")
    db_service.list_analyses.return_value = [_record("s3://bucket/exp/analyses/a", "a")]
    file_service = AsyncMock()
    file_service.get_file_contents.return_value = None

    with (
        patch("viva_api.common.handlers.analyses.get_file_service", return_value=file_service),
        pytest.raises(FileNotFoundError),
    ):
        await fetch_simulation_analysis_figure(
            db_service=db_service, simulation_id=7, analysis_name="a", relpath="viz/missing.html"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["../etc/passwd", "viz/../../secret", "secret/x.html", "/abs/x.html", ""])
async def test_fetch_rejects_traversal_and_out_of_scope_paths(bad: str) -> None:
    db_service = AsyncMock()
    with pytest.raises(ValueError):
        await fetch_simulation_analysis_figure(db_service=db_service, simulation_id=7, analysis_name="a", relpath=bad)
