"""Endpoint tests for the read-side analysis-result API (list + fetch-by-id)."""

import datetime

import pytest
from httpx import ASGITransport, AsyncClient

from viva_api.common.storage.file_service import ListingItem
from viva_api.dependencies import set_file_service
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import SimulationRequest
from viva_api.simulation.tables_orm import AnalysisStatusDB

_RESULT_KEY = "vecoli-output/exp-ana/exp-ana/analyses"
# The stored DB value is always a full s3://<bucket>/... URI (see
# RayLayout.results_uri -> data_layout.s3_uri) — a bucket-relative fixture here
# would let a broken S3-path construction pass silently, since S3FilePath.s3_path
# is documented (and FileServiceS3 relies on it) as bucket-relative. Regression
# coverage for the real bug (cplong90, 2026-08-27): GET /analyses/{id}/data
# returning 200 [] for a real, completed, non-empty analysis because the full
# URI was passed straight into S3FilePath, mangling "s3://" into "s3:/".
_RESULT_URI = f"s3://some-bucket/{_RESULT_KEY}"


class _FakeFileService:
    """Duck-typed file service returning one output file under the result prefix.

    Asserts the ``s3_path`` it's actually called with is bucket-relative (no
    ``s3:`` scheme, no bucket name) — a hardcoded return value regardless of
    input, as this fixture used to be, would pass whether the caller's S3-path
    construction were correct or wrong (the exact class of tautological-mock
    finding this project's own green-mock-as-go-signal rule flags)."""

    def __init__(self, files: dict[str, bytes]) -> None:
        self._files = files

    async def get_listing(self, s3_path: object) -> list[ListingItem]:
        prefix = str(s3_path)
        assert not prefix.startswith("s3:"), f"got a full URI, not a bucket-relative key: {prefix!r}"
        assert "some-bucket" not in prefix, f"bucket leaked into the key: {prefix!r}"
        assert prefix.rstrip("/") == _RESULT_KEY, f"unexpected prefix: {prefix!r}"
        return [
            ListingItem(Key=key, LastModified=datetime.datetime(2026, 1, 1), ETag="x", Size=len(content))
            for key, content in self._files.items()
        ]

    async def get_file_contents(self, s3_path: object) -> bytes:
        key = str(s3_path)
        assert not key.startswith("s3:"), f"got a full URI, not a bucket-relative key: {key!r}"
        return self._files[key]


async def _client() -> AsyncClient:
    from viva_api.api.main import app

    return AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver")


@pytest.mark.asyncio
async def test_list_simulation_analyses(
    base_router: str, database_service: DatabaseServiceSQL, experiment_request: SimulationRequest
) -> None:
    experiment_request.experiment_id = "exp-list-1"
    experiment_request.config.experiment_id = "exp-list-1"
    sim = await database_service.insert_simulation(experiment_request)
    await database_service.record_analysis(
        experiment_id="exp-list-1",
        n_tp=None,
        status=AnalysisStatusDB.READY,
        config={"analysis_options": {"experiment_id": ["exp-list-1"]}},
        name="backfill-exp-list-1",
        simulation_id=sim.database_id,
        result_uri=_RESULT_URI,
    )
    async with await _client() as client:
        resp = await client.get(f"{base_router}/simulations/{sim.database_id}/analyses")
        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 1
        assert rows[0]["experiment_id"] == "exp-list-1"
        assert rows[0]["result_uri"] == _RESULT_URI


@pytest.mark.asyncio
async def test_get_analysis_data_ready(base_router: str, database_service: DatabaseServiceSQL) -> None:
    rec = await database_service.record_analysis(
        experiment_id="exp-ana",
        n_tp=None,
        status=AnalysisStatusDB.READY,
        config={"analysis_options": {"experiment_id": ["exp-ana"]}},
        name="backfill-exp-ana",
        result_uri=_RESULT_URI,
    )
    tsv_key = f"{_RESULT_KEY}/variant=0/plots/analysis=cd1_proteomics/proteomics.tsv"
    tsv_text = "EcoCyc Reaction ID\tmean\tstd\nRXN1\t1.0\t0.1\n"
    # A non-tsv/csv/txt/html file under the same prefix (the run's own summary
    # manifest) must be filtered out, not returned as a bogus "data" file.
    set_file_service(
        _FakeFileService({  # type: ignore[arg-type]
            tsv_key: tsv_text.encode(),
            f"{_RESULT_KEY}/analysis.json": b'{"status": "done"}',
        })
    )
    try:
        async with await _client() as client:
            resp = await client.get(f"{base_router}/analyses/{rec.database_id}/data")
            assert resp.status_code == 200
            files = resp.json()
            assert len(files) == 1
            assert files[0]["filename"] == "proteomics.tsv"
            assert files[0]["variant"] == 0
            assert "EcoCyc Reaction ID" in files[0]["content"]
    finally:
        set_file_service(None)


@pytest.mark.asyncio
async def test_get_analysis_plots_ray_backend_ready(base_router: str, database_service: DatabaseServiceSQL) -> None:
    """GET /analyses/{id}/plots against a Ray/K8s-backend analysis.

    Regression: previously ALWAYS 500'd for a Ray-backend analysis — there was
    no S3-backed implementation of the plots endpoint at all, only the legacy
    SLURM local-filesystem path (found live 2026-08-27, cplong90's same report)."""
    rec = await database_service.record_analysis(
        experiment_id="exp-plots",
        n_tp=None,
        status=AnalysisStatusDB.READY,
        config={"analysis_options": {"experiment_id": ["exp-plots"]}},
        name="backfill-exp-plots",
        result_uri=_RESULT_URI,
        backend="ray",
    )
    html_key = f"{_RESULT_KEY}/plots/mass_fraction_summary.html"
    html_text = "<html><body>plot</body></html>"
    set_file_service(
        _FakeFileService({  # type: ignore[arg-type]
            html_key: html_text.encode(),
            f"{_RESULT_KEY}/analysis.json": b'{"status": "done"}',
        })
    )
    try:
        async with await _client() as client:
            resp = await client.get(f"{base_router}/analyses/{rec.database_id}/plots")
            assert resp.status_code == 200
            plots = resp.json()
            assert len(plots) == 1
            assert plots[0]["name"] == "mass_fraction_summary.html"
            assert plots[0]["content"] == html_text
    finally:
        set_file_service(None)


@pytest.mark.asyncio
async def test_get_analysis_plots_ray_backend_not_ready_returns_409(
    base_router: str, database_service: DatabaseServiceSQL
) -> None:
    rec = await database_service.record_analysis(
        experiment_id="exp-plots-computing",
        n_tp=None,
        status=AnalysisStatusDB.COMPUTING,
        config={"analysis_options": {"experiment_id": ["exp-plots-computing"]}},
        name="c",
        backend="ray",
    )
    async with await _client() as client:
        resp = await client.get(f"{base_router}/analyses/{rec.database_id}/plots")
        assert resp.status_code == 409


@pytest.mark.asyncio
async def test_get_analysis_data_not_ready_returns_409(base_router: str, database_service: DatabaseServiceSQL) -> None:
    rec = await database_service.record_analysis(
        experiment_id="exp-computing",
        n_tp=10,
        status=AnalysisStatusDB.COMPUTING,
        config={"analysis_options": {"experiment_id": ["exp-computing"]}},
        name="c",
    )
    async with await _client() as client:
        resp = await client.get(f"{base_router}/analyses/{rec.database_id}/data")
        assert resp.status_code == 409


@pytest.mark.asyncio
async def test_get_analysis_data_unknown_returns_404(base_router: str, database_service: DatabaseServiceSQL) -> None:
    async with await _client() as client:
        resp = await client.get(f"{base_router}/analyses/99999999/data")
        assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_all_analyses_exhaustive_and_filter(base_router: str, database_service: DatabaseServiceSQL) -> None:
    for exp in ("exp-all-x", "exp-all-y"):
        await database_service.record_analysis(
            experiment_id=exp,
            n_tp=None,
            status=AnalysisStatusDB.READY,
            config={"analysis_options": {"experiment_id": [exp]}},
            name=f"backfill-{exp}",
            result_uri=f"vecoli-output/{exp}/{exp}/analyses",
        )
    async with await _client() as client:
        all_resp = await client.get(f"{base_router}/analyses")
        assert all_resp.status_code == 200
        exps = {r["experiment_id"] for r in all_resp.json()}
        assert {"exp-all-x", "exp-all-y"} <= exps  # exhaustive across sims

        filtered = await client.get(f"{base_router}/analyses", params={"experiment_id": "exp-all-x"})
        assert filtered.status_code == 200
        assert {r["experiment_id"] for r in filtered.json()} == {"exp-all-x"}
