"""The analysis and simulation routes data provenance slice 1 adds or extends (docs/plan-data-provenance.md §7).

``/simulations/{id}/datasets``, ``/analyses/{id}/datasets``, ``n_datasets`` on
``/analyses/{id}``, the ``/analyses`` filters, and the coordinate filters on
``/analyses/{id}/data``. Through the ASGI app against the testcontainer Postgres.
"""

import datetime
import uuid
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from tests.simulation.test_event_ingest import _insert_run
from viva_api.common.storage.file_service import ListingItem
from viva_api.dependencies import set_file_service
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.tables_orm import AnalysisStatusDB


async def _client() -> AsyncClient:
    from viva_api.api.main import app

    return AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver")


async def _analysis(db: DatabaseServiceSQL, **overrides: Any) -> int:
    experiment_id = overrides.pop("experiment_id", f"exp-{uuid.uuid4().hex[:8]}")
    kwargs: dict[str, Any] = {
        "experiment_id": experiment_id,
        "n_tp": None,
        "status": AnalysisStatusDB.READY,
        "config": {"analysis_options": {"experiment_id": [experiment_id]}},
        "name": f"analysis-{experiment_id}",
        "backend": "k8s",
    }
    kwargs.update(overrides)
    return (await db.record_analysis(**kwargs)).database_id


def _uri(name: str) -> str:
    return f"s3://bucket/vecoli-output/exp/analyses/a-{uuid.uuid4().hex[:6]}/{name}"


def _ids(resp: Any) -> list[int]:
    assert resp.status_code == 200, resp.text
    body = resp.json()
    rows = body["datasets"] if isinstance(body, dict) else body
    return [row["database_id"] for row in rows]


@pytest.mark.asyncio
async def test_simulation_datasets_are_its_own_or_also_its_analyses(
    base_router: str, database_service: DatabaseServiceSQL
) -> None:
    simulation, _run = await _insert_run(database_service, correlation_id=f"simds-{uuid.uuid4().hex[:8]}")
    source = {"kind": "simulation", "ref": str(simulation.database_id), "resolved_id": simulation.database_id}
    own, _ = await database_service.upsert_dataset(
        uri=_uri("history/"), kind="parquet", origin="event", simulation_id=simulation.database_id, source=source
    )
    analysis_id = await _analysis(database_service, simulation_id=simulation.database_id)
    theirs, _ = await database_service.upsert_dataset(
        uri=_uri("ptools/ptools_rna_multiseed__variant=0.tsv"),
        kind="ptools-analysis",
        origin="walk",
        analysis_id=analysis_id,
        source=source,
    )
    await database_service.upsert_dataset(
        uri=_uri("ptools/unsourced.tsv"), kind="ptools-analysis", origin="walk", analysis_id=analysis_id
    )

    route = f"{base_router}/simulations/{simulation.database_id}/datasets"
    async with await _client() as client:
        assert _ids(await client.get(route)) == [own.database_id]
        assert set(_ids(await client.get(route, params={"include_analyses": True}))) == {
            own.database_id,
            theirs.database_id,
        }
        assert _ids(await client.get(route, params={"include_analyses": True, "kind": "ptools-analysis"})) == [
            theirs.database_id
        ]
        assert (await client.get(route, params={"kind": "tsv"})).status_code == 400
        assert (await client.get(f"{base_router}/simulations/999999999/datasets")).status_code == 404


@pytest.mark.asyncio
async def test_an_analysis_reports_status_and_dataset_count_apart_and_lists_its_datasets(
    base_router: str, database_service: DatabaseServiceSQL
) -> None:
    source = {"kind": "simulation", "ref": "1002"}
    analysis_id = await _analysis(database_service, status=AnalysisStatusDB.COMPUTING, tags=["cd2"], source=source)
    async with await _client() as client:
        body = (await client.get(f"{base_router}/analyses/{analysis_id}")).json()
        assert body["status"] == "running" and body["n_datasets"] == 0
        assert body["tags"] == ["cd2"] and body["source"] == source

        kept, _ = await database_service.upsert_dataset(
            uri=_uri("ptools/kept.tsv"), kind="ptools-analysis", origin="event", analysis_id=analysis_id
        )
        gone, _ = await database_service.upsert_dataset(
            uri=_uri("ptools/gone.tsv"), kind="ptools-analysis", origin="event", analysis_id=analysis_id
        )
        await database_service.set_dataset_available(gone.database_id, False)

        body = (await client.get(f"{base_router}/analyses/{analysis_id}")).json()
        assert body["status"] == "running" and body["n_datasets"] == 1

        route = f"{base_router}/analyses/{analysis_id}/datasets"
        assert _ids(await client.get(route)) == [kept.database_id]
        assert set(_ids(await client.get(route, params={"available": "any"}))) == {kept.database_id, gone.database_id}
        assert (await client.get(f"{base_router}/analyses/999999999/datasets")).status_code == 404


@pytest.mark.asyncio
async def test_list_analyses_filters_and_pages_newest_first(
    base_router: str, database_service: DatabaseServiceSQL
) -> None:
    tag = f"ana-{uuid.uuid4().hex[:6]}"
    ref = str(uuid.uuid4().int % 10**9)
    ready = await _analysis(
        database_service, backend="batch", tags=[tag], source={"kind": "simulation", "ref": ref, "resolved_id": 1}
    )
    failed = await _analysis(database_service, status=AnalysisStatusDB.FAILED, tags=[tag])
    running = await _analysis(database_service, status=AnalysisStatusDB.COMPUTING, tags=[tag, "cd2"])

    async with await _client() as client:

        async def listed(**params: Any) -> list[int]:
            return _ids(await client.get(f"{base_router}/analyses", params={"tag": tag, **params}))

        assert await listed() == [running, failed, ready]
        assert await listed(status="completed") == [ready]
        assert await listed(status="failed") == [failed]
        assert await listed(status="running") == [running]
        assert await listed(backend="k8s") == [running, failed]
        assert await listed(source=f"sim:{ref}") == [ready]
        assert await listed(tag=f"{tag},cd2") == [running]
        assert await listed(limit=1, offset=1) == [failed]
        tomorrow = datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=1)
        assert await listed(since=tomorrow.isoformat()) == []
        assert (await client.get(f"{base_router}/analyses", params={"source": "nope"})).status_code == 400


class _Bundle:
    """An analysis result prefix holding a few objects; records what was downloaded."""

    def __init__(self, prefix: str, files: dict[str, bytes]) -> None:
        self.prefix = prefix
        self.files = {f"{prefix}/{name}": data for name, data in files.items()}
        self.fetched: list[str] = []

    async def get_listing(self, s3_path: object) -> list[ListingItem]:
        assert str(s3_path).rstrip("/") == self.prefix, s3_path
        return [
            ListingItem(Key=key, LastModified=datetime.datetime(2026, 9, 15), ETag="x", Size=len(data))
            for key, data in self.files.items()
        ]

    async def get_file_contents(self, s3_path: object) -> bytes:
        key = str(s3_path)
        self.fetched.append(key)
        return self.files[key]


@pytest.mark.asyncio
async def test_analysis_data_selects_by_coordinate_and_aliases_one_file_per_view(
    base_router: str, database_service: DatabaseServiceSQL
) -> None:
    experiment_id = f"exp-data-{uuid.uuid4().hex[:8]}"
    prefix = f"vecoli-output/{experiment_id}/analyses/analysis-ptools-multiseed"
    analysis_id = await _analysis(
        database_service, experiment_id=experiment_id, result_uri=f"s3://some-bucket/{prefix}"
    )
    bundle = _Bundle(
        prefix,
        {
            "ptools/ptools_rna_multiseed__variant=0.tsv": b"rna0",
            "ptools/ptools_rna_multiseed__variant=1.tsv": b"rna1",
            "ptools/ptools_rxns_multiseed__variant=0.tsv": b"rxns0",
            "viz/ptools_rna_multiseed__variant=0.html": b"<html>",
            "analysis.json": b"{}",
        },
    )
    set_file_service(bundle)  # type: ignore[arg-type]
    route = f"{base_router}/analyses/{analysis_id}/data"
    try:
        async with await _client() as client:

            async def files(**params: Any) -> dict[str, dict[str, Any]]:
                resp = await client.get(route, params=params)
                assert resp.status_code == 200, resp.text
                return {row["filename"]: row for row in resp.json()}

            # Unfiltered: two ptools_rna tables keep their names; single-file views are aliased.
            everything = await files()
            assert set(everything) == {
                "ptools_rna_multiseed__variant=0.tsv",
                "ptools_rna_multiseed__variant=1.tsv",
                "ptools_rxns.tsv",
                "ptools_rna.html",
            }
            assert everything["ptools_rxns.tsv"]["path"] == "ptools/ptools_rxns_multiseed__variant=0.tsv"
            assert everything["ptools_rna_multiseed__variant=1.tsv"]["variant"] == 1

            variant_zero = await files(variant=0)
            assert set(variant_zero) == {"ptools_rna.tsv", "ptools_rxns.tsv", "ptools_rna.html"}
            assert variant_zero["ptools_rna.tsv"]["content"] == "rna0"
            assert variant_zero["ptools_rna.tsv"]["path"] == "ptools/ptools_rna_multiseed__variant=0.tsv"

            # The filters select before anything is downloaded.
            bundle.fetched.clear()
            one = await files(view="ptools_rna", variant=1, protocol="multiseed")
            assert list(one) == ["ptools_rna.tsv"] and one["ptools_rna.tsv"]["content"] == "rna1"
            assert [Path(key).name for key in bundle.fetched] == ["ptools_rna_multiseed__variant=1.tsv"]

            bundle.fetched.clear()
            assert await files(protocol="single") == {}
            assert await files(seed=3) == {}
            assert bundle.fetched == []
    finally:
        set_file_service(None)
