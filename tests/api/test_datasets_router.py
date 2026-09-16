"""Endpoint tests for /datasets (data provenance slice 1, docs/plan-data-provenance.md §7).

Through the ASGI app against the testcontainer Postgres (``database_service``); the file
service is an in-memory fake. Nothing here reaches S3 or a hosted database.
"""

import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from tests.simulation.test_event_ingest import _insert_run
from viva_api.common.models import JobId
from viva_api.config import get_settings
from viva_api.dependencies import set_file_service
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import JobType, SimulationSpan
from viva_api.simulation.tables_orm import AnalysisStatusDB

BUCKET = "api-bucket"


async def _client() -> AsyncClient:
    from viva_api.api.main import app

    return AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver")


async def _analysis(db: DatabaseServiceSQL, **overrides: Any) -> int:
    experiment_id = f"exp-{uuid.uuid4().hex[:8]}"
    kwargs: dict[str, Any] = {
        "experiment_id": experiment_id,
        "n_tp": None,
        "status": AnalysisStatusDB.READY,
        "config": {"analysis_options": {"experiment_id": [experiment_id]}},
        "name": f"analysis-{experiment_id}",
        "backend": "walk",
    }
    kwargs.update(overrides)
    return (await db.record_analysis(**kwargs)).database_id


def _uri(name: str, bucket: str = BUCKET) -> str:
    return f"s3://{bucket}/vecoli-output/exp/analyses/a-{uuid.uuid4().hex[:6]}/{name}"


def _key(uri: str) -> str:
    return uri.removeprefix(f"s3://{BUCKET}/")


class _Objects:
    """Objects by bucket-relative key; records every key a caller opened."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.opened: list[str] = []

    async def open_file_stream(self, s3_path: object, chunk_size: int = 1024 * 1024) -> AsyncIterator[bytes] | None:
        key = str(s3_path)
        assert not key.startswith("s3:"), f"got a full URI, not a bucket-relative key: {key!r}"
        self.opened.append(key)
        if key not in self.objects:
            return None
        data = self.objects[key]

        async def chunks() -> AsyncIterator[bytes]:
            for start in range(0, len(data), 4):
                yield data[start : start + 4]

        return chunks()


@pytest.mark.asyncio
async def test_list_datasets_filters_by_kind_tags_typed_attributes_source_and_pages(
    base_router: str, database_service: DatabaseServiceSQL
) -> None:
    simulation, _run = await _insert_run(database_service, correlation_id=f"dsr-{uuid.uuid4().hex[:8]}")
    analysis_id = await _analysis(database_service)
    tag = f"dsr-{uuid.uuid4().hex[:6]}"
    source = {"kind": "simulation", "ref": str(simulation.database_id), "resolved_id": simulation.database_id}
    rows: list[int] = []
    uris: list[str] = []
    for name, kind, attributes in (
        ("ptools/ptools_rna_multiseed__variant=0.tsv", "ptools-analysis", {"variant": 0, "protocol": "multiseed"}),
        (
            "ptools/ptools_rna__variant=0_seed=3_gen=12_agent=000.tsv",
            "ptools-analysis",
            {"variant": 0, "seed": 3, "generation": 12, "agent": "000", "protocol": "single"},
        ),
        ("viz/ptools_rna_multiseed__variant=1.html", "figure", {"variant": 1, "protocol": "multiseed"}),
    ):
        uri = _uri(name)
        uris.append(uri)
        dto, _ = await database_service.upsert_dataset(
            uri=uri,
            kind=kind,
            origin="walk",
            analysis_id=analysis_id,
            view="ptools_rna",
            attributes=attributes,
            tags=[tag],
            source=source,
        )
        rows.append(dto.database_id)
    rna_multiseed, rna_single, figure = rows

    async with await _client() as client:

        async def listed(**params: Any) -> list[int]:
            resp = await client.get(f"{base_router}/datasets", params={"tag": tag, **params})
            assert resp.status_code == 200, resp.text
            return [d["database_id"] for d in resp.json()["datasets"]]

        assert set(await listed()) == {rna_multiseed, rna_single, figure}
        assert await listed(kind="figure") == [figure]
        # Typed matching: 0 is the integer, "0" the string; 000 is not JSON, so it is a string.
        assert set(await listed(attr="variant=0")) == {rna_multiseed, rna_single}
        assert await listed(attr='variant="0"') == []
        assert await listed(**{"attr.agent": "000"}) == [rna_single]
        assert await listed(attr=["variant=0", "protocol=multiseed"]) == [rna_multiseed]
        assert set(await listed(source=f"sim:{simulation.database_id}")) == {rna_multiseed, rna_single, figure}
        assert await listed(source="sim:999999999") == []
        assert set(await listed(analysis_id=analysis_id)) == {rna_multiseed, rna_single, figure}

        first = (await client.get(f"{base_router}/datasets", params={"tag": tag, "limit": 2})).json()
        assert len(first["datasets"]) == 2 and first["next_offset"] == 2
        last = (await client.get(f"{base_router}/datasets", params={"tag": tag, "limit": 2, "offset": 2})).json()
        assert len(last["datasets"]) == 1 and last["next_offset"] is None

        # `total` counts every MATCHING row, not the page and not the table: it is what lets a
        # client say "1-2 of 3" and narrow instead of paging. Counting with the listing's own
        # filters is the whole point -- a count that ignored them would report the table.
        assert first["total"] == 3 and last["total"] == 3
        assert (await client.get(f"{base_router}/datasets", params={"tag": tag, "kind": "figure"})).json()["total"] == 1

        # uri_prefix: every row under one directory. `_uri` randomises the bundle segment per
        # call, so the prefix must come from a row that was actually inserted -- deriving it
        # from a fresh _uri() would match nothing and pass for the wrong reason.
        ptools_dir = uris[0].rsplit("/", 1)[0] + "/"
        under = (await client.get(f"{base_router}/datasets", params={"tag": tag, "uri_prefix": ptools_dir})).json()
        assert [d["database_id"] for d in under["datasets"]] == [rna_multiseed] and under["total"] == 1
        bundle = uris[0].rsplit("/", 2)[0] + "/"
        whole = (await client.get(f"{base_router}/datasets", params={"tag": tag, "uri_prefix": bundle})).json()
        assert [d["database_id"] for d in whole["datasets"]] == [rna_multiseed] and whole["total"] == 1
        assert (await client.get(f"{base_router}/datasets", params={"uri_prefix": "s3://nope/"})).json()["total"] == 0
        # Wildcards are literal, not LIKE patterns.
        assert (await client.get(f"{base_router}/datasets", params={"uri_prefix": "s3://%"})).json()["total"] == 0

        # A gone object is hidden unless asked for.
        await database_service.set_dataset_available(figure, False)
        assert figure not in await listed()
        assert await listed(available="false") == [figure]
        assert figure in await listed(available="any")

        for bad in ({"kind": "tsv"}, {"attr": "variant"}, {"source": "simulation"}, {"source": "{bad"}):
            resp = await client.get(f"{base_router}/datasets", params=bad)
            assert resp.status_code == 400, (bad, resp.text)


@pytest.mark.asyncio
async def test_get_tag_and_summarize_datasets(base_router: str, database_service: DatabaseServiceSQL) -> None:
    analysis_id = await _analysis(database_service)
    tag = f"dsr-{uuid.uuid4().hex[:6]}"
    dto, _ = await database_service.upsert_dataset(
        uri=_uri("ptools/ptools_rxns_multiseed__variant=7.tsv"),
        kind="ptools-analysis",
        origin="event",
        analysis_id=analysis_id,
        view="ptools_rxns",
        attributes={"variant": 7, "protocol": "multiseed"},
        tags=[tag],
    )
    async with await _client() as client:
        got = await client.get(f"{base_router}/datasets/{dto.database_id}")
        assert got.status_code == 200
        assert got.json()["uri"] == dto.uri and got.json()["attributes"]["origin"] == "event"
        assert (await client.get(f"{base_router}/datasets/999999999")).status_code == 404

        tagged = await client.post(f"{base_router}/datasets/{dto.database_id}/tags", json={"tags": ["cd2", tag]})
        assert tagged.status_code == 200
        assert sorted(tagged.json()["tags"]) == sorted([tag, "cd2"])
        missing = await client.post(f"{base_router}/datasets/999999999/tags", json={"tags": ["cd2"]})
        assert missing.status_code == 404

        tags = (await client.get(f"{base_router}/datasets/tags", params={"kind": "ptools-analysis"})).json()
        assert tags[tag] == 1
        values = (await client.get(f"{base_router}/datasets/attributes", params={"kind": "ptools-analysis"})).json()
        assert 7 in values["variant"] and "multiseed" in values["protocol"]
        assert (await client.get(f"{base_router}/datasets/tags", params={"kind": "nope"})).status_code == 400
        assert (await client.get(f"{base_router}/datasets/attributes", params={"kind": "nope"})).status_code == 400


@pytest.mark.asyncio
async def test_dataset_content_streams_one_object_from_the_storage_bucket(
    base_router: str, database_service: DatabaseServiceSQL, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "storage_s3_bucket", BUCKET)
    analysis_id = await _analysis(database_service)
    tsv = _uri("ptools/ptools_rna_multiseed__variant=0.tsv")
    report = _uri("analysis.json")
    gone = _uri("ptools/gone.tsv")
    foreign = _uri("ptools/elsewhere.tsv", bucket="someone-elses-bucket")
    ids: dict[str, int] = {}
    for uri, kind in ((tsv, "ptools-analysis"), (report, "report"), (gone, "ptools-analysis"), (foreign, "figure")):
        ids[uri] = (await database_service.upsert_dataset(uri=uri, kind=kind, origin="walk", analysis_id=analysis_id))[
            0
        ].database_id
    store, _ = await database_service.upsert_dataset(
        uri=f"s3://{BUCKET}/vecoli-output/exp/history/{uuid.uuid4().hex[:6]}/",
        kind="parquet",
        origin="event",
        analysis_id=analysis_id,
    )

    body = b"$\t0m\t124m\nEG10001\t0.2542\t0.2778\n"
    objects = _Objects({_key(tsv): body, _key(report): b'{"status": "done"}'})
    set_file_service(objects)  # type: ignore[arg-type]
    try:
        async with await _client() as client:
            resp = await client.get(f"{base_router}/datasets/{ids[tsv]}/content")
            assert resp.status_code == 200
            assert resp.content == body
            assert resp.headers["content-type"].startswith("text/plain")
            assert 'filename="ptools_rna_multiseed__variant=0.tsv"' in resp.headers["content-disposition"]

            resp = await client.get(f"{base_router}/datasets/{ids[report]}/content")
            assert resp.status_code == 200 and resp.headers["content-type"].startswith("application/json")

            assert (await client.get(f"{base_router}/datasets/{ids[gone]}/content")).status_code == 404
            assert (await client.get(f"{base_router}/datasets/{ids[foreign]}/content")).status_code == 409
            assert (await client.get(f"{base_router}/datasets/{store.database_id}/content")).status_code == 409
            assert (await client.get(f"{base_router}/datasets/999999999/content")).status_code == 404
        # Refused datasets are never opened: not the other bucket, not the store.
        assert sorted(objects.opened) == sorted([_key(tsv), _key(report), _key(gone)])
    finally:
        set_file_service(None)


@pytest.mark.asyncio
async def test_provenance_names_the_producer_run_its_span_and_the_datasets_it_read(
    base_router: str, database_service: DatabaseServiceSQL
) -> None:
    simulation, sim_run = await _insert_run(database_service, correlation_id=f"prov-{uuid.uuid4().hex[:8]}")
    upstream, _ = await database_service.upsert_dataset(
        uri=f"s3://{BUCKET}/vecoli-output/{simulation.experiment_id}/history/variant=0/",
        kind="parquet",
        origin="event",
        simulation_id=simulation.database_id,
        attributes={"variant": 0, "span_id": "sim-span"},
    )
    await database_service.upsert_hpcrun_spans(
        sim_run.database_id, sim_run.trace_id or "", [SimulationSpan(span_id="sim-span", name="lineage")]
    )

    analysis_id = await _analysis(
        database_service,
        status=AnalysisStatusDB.COMPUTING,
        backend="k8s",
        simulation_id=simulation.database_id,
        source={"kind": "simulation", "ref": str(simulation.database_id), "uri": upstream.uri},
        tags=["cd2"],
    )
    run = await database_service.insert_hpcrun(
        job_id=JobId.k8s_nextflow(f"ana-{analysis_id}"),
        job_type=JobType.ANALYSIS,
        ref_id=analysis_id,
        correlation_id=f"analysis-{analysis_id}-{uuid.uuid4().hex[:6]}",
    )
    await database_service.upsert_hpcrun_spans(
        run.database_id,
        run.trace_id or "",
        [SimulationSpan(span_id="other-span", name="analysis"), SimulationSpan(span_id="gather-span", name="gather")],
    )
    written, _ = await database_service.upsert_dataset(
        uri=_uri("ptools/ptools_rna_multiseed__variant=0.tsv"),
        kind="ptools-analysis",
        origin="event",
        analysis_id=analysis_id,
        attributes={"span_id": "gather-span"},
    )
    walked, _ = await database_service.upsert_dataset(
        uri=_uri("ptools/walked.tsv"),
        kind="ptools-analysis",
        origin="walk",
        analysis_id=await _analysis(database_service),
    )

    async with await _client() as client:
        body = (await client.get(f"{base_router}/datasets/{written.database_id}/provenance")).json()
        producer = body["producer"]
        assert producer["kind"] == "analysis" and producer["id"] == analysis_id
        assert producer["status"] == "running" and producer["tags"] == ["cd2"]
        assert producer["source"]["uri"] == upstream.uri
        assert producer["hpcrun_id"] == run.database_id and producer["trace_id"] == run.trace_id
        assert body["span"]["span_id"] == "gather-span" and body["span"]["name"] == "gather"
        assert [d["database_id"] for d in body["inputs"]] == [upstream.database_id]

        body = (await client.get(f"{base_router}/datasets/{upstream.database_id}/provenance")).json()
        assert body["producer"]["kind"] == "simulation"
        assert body["producer"]["name"] == simulation.experiment_id
        assert body["producer"]["hpcrun_id"] == sim_run.database_id
        assert body["span"]["span_id"] == "sim-span"
        assert body["inputs"] == []

        # A backfilled run row has no HpcRun, and a walk-found dataset no span.
        body = (await client.get(f"{base_router}/datasets/{walked.database_id}/provenance")).json()
        assert body["producer"]["kind"] == "analysis" and body["producer"]["hpcrun_id"] is None
        assert body["span"] is None and body["inputs"] == []

        assert (await client.get(f"{base_router}/datasets/999999999/provenance")).status_code == 404
