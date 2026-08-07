"""Endpoint tests for the read-side analysis-result API (list + fetch-by-id)."""

import datetime

import pytest
from httpx import ASGITransport, AsyncClient

from viva_api.common.storage.file_service import ListingItem
from viva_api.dependencies import set_file_service
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import SimulationRequest
from viva_api.simulation.tables_orm import AnalysisStatusDB

_RESULT_URI = "vecoli-output/exp-ana/exp-ana/analyses"


class _FakeFileService:
    """Duck-typed file service returning one TSV under the result prefix."""

    def __init__(self, tsv_text: str) -> None:
        self._tsv = tsv_text
        self._key = f"{_RESULT_URI}/variant=0/plots/analysis=cd1_proteomics/proteomics.tsv"

    async def get_listing(self, s3_path: object) -> list[ListingItem]:
        return [ListingItem(Key=self._key, LastModified=datetime.datetime(2026, 1, 1), ETag="x", Size=10)]

    async def get_file_contents(self, s3_path: object) -> bytes:
        return self._tsv.encode()


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
    set_file_service(_FakeFileService("EcoCyc Reaction ID\tmean\tstd\nRXN1\t1.0\t0.1\n"))  # type: ignore[arg-type]
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
