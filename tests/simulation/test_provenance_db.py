"""DatabaseService tests for data provenance slice 1 (docs/plan-data-provenance.md).

Covers the ``dataset`` registry's persistence rules, the analysis-run additions
(``source``/``tags``, dispatch-after-record, lookup by result URI, list filters and
paging) and ``JobType.ANALYSIS`` HpcRun routing.

Runs against the testcontainer Postgres from ``tests/fixtures/postgres_fixtures.py``
(schema via ``create_all``). Nothing here reaches a hosted database.
"""

import asyncio
import datetime
import uuid
from typing import Any

import pytest

from tests.simulation.test_event_ingest import _insert_run
from viva_api.common.models import JobId
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import JobType
from viva_api.simulation.tables_orm import AnalysisStatusDB


def _config(experiment_id: str) -> dict[str, Any]:
    return {"analysis_options": {"experiment_id": [experiment_id]}}


async def _analysis(db: DatabaseServiceSQL, **overrides: Any) -> int:
    experiment_id = overrides.pop("experiment_id", f"exp-{uuid.uuid4().hex[:8]}")
    kwargs: dict[str, Any] = {
        "experiment_id": experiment_id,
        "n_tp": None,
        "status": AnalysisStatusDB.COMPUTING,
        "config": _config(experiment_id),
        "name": f"analysis-{experiment_id}",
        "backend": "k8s",
    }
    kwargs.update(overrides)
    return (await db.record_analysis(**kwargs)).database_id


def _uri(name: str) -> str:
    return f"s3://bucket/vecoli-output/exp/analyses/a/ptools/{name}-{uuid.uuid4().hex[:6]}.tsv"


# ---------------------------------------------------------------------------
# JobType.ANALYSIS HpcRun routing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_analysis_hpcrun_routes_to_its_own_job_reference(database_service: DatabaseServiceSQL) -> None:
    analysis_id = await _analysis(database_service)
    run = await database_service.insert_hpcrun(
        # The backend tag is irrelevant to routing; any JobId constructor will do.
        job_id=JobId.k8s_nextflow(f"ana-{analysis_id}"),
        job_type=JobType.ANALYSIS,
        ref_id=analysis_id,
        correlation_id=f"analysis-{analysis_id}",
    )
    assert run.job_type == JobType.ANALYSIS
    assert run.ref_id == analysis_id
    assert run.trace_id  # derived from the correlation id at insert, like every run

    found = await database_service.get_hpcrun_by_ref(analysis_id, JobType.ANALYSIS)
    assert found is not None and found.database_id == run.database_id
    # The same integer as a SIMULATION reference must not find the analysis run.
    other = await database_service.get_hpcrun_by_ref(analysis_id, JobType.SIMULATION)
    assert other is None or other.database_id != run.database_id


# ---------------------------------------------------------------------------
# analysis runs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_analysis_stores_source_and_tags_and_merges_tags_on_update(
    database_service: DatabaseServiceSQL,
) -> None:
    experiment_id = f"exp-{uuid.uuid4().hex[:8]}"
    source = {"kind": "simulation", "ref": "1002", "resolved_id": 1002}
    first = await _analysis(database_service, experiment_id=experiment_id, n_tp=10, source=source, tags=["cd2", "run3"])
    # The (experiment_id, n_tp) dedup path: same row, tags union-merged, source kept.
    second = await _analysis(database_service, experiment_id=experiment_id, n_tp=10, tags=["run3", "multiseed"])
    assert second == first

    by_tags = await database_service.list_analyses(tags=["cd2", "multiseed"])
    assert [a.database_id for a in by_tags] == [first]
    by_source = await database_service.list_analyses(source={"kind": "simulation", "ref": "1002"})
    assert [a.database_id for a in by_source] == [first]
    assert await database_service.list_analyses(tags=["cd2", "not-a-tag"]) == []


@pytest.mark.asyncio
async def test_dispatch_is_recorded_after_the_row_exists(database_service: DatabaseServiceSQL) -> None:
    analysis_id = await _analysis(database_service)
    updated = await database_service.update_analysis_dispatch(analysis_id, "ana-analysis-xyz")
    assert updated.job_id_ext == "ana-analysis-xyz"
    with pytest.raises(RuntimeError):
        await database_service.update_analysis_dispatch(10_000_000, "nope")


@pytest.mark.asyncio
async def test_get_analysis_by_result_uri_ignores_a_trailing_slash(database_service: DatabaseServiceSQL) -> None:
    uri = f"s3://bucket/vecoli-output/exp-{uuid.uuid4().hex[:6]}/analyses/analysis-ptools-multiseed"
    analysis_id = await _analysis(database_service, result_uri=uri)
    for probe in (uri, f"{uri}/"):
        found = await database_service.get_analysis_by_result_uri(probe)
        assert found is not None and found.database_id == analysis_id
    assert await database_service.get_analysis_by_result_uri(f"{uri}-other") is None


@pytest.mark.asyncio
async def test_list_analyses_filters_orders_and_pages(database_service: DatabaseServiceSQL) -> None:
    tag = f"page-{uuid.uuid4().hex[:6]}"
    ids = [await _analysis(database_service, tags=[tag], backend="fill") for _ in range(3)]
    await database_service.update_analysis_status(ids[0], AnalysisStatusDB.READY)  # newest change

    default_order = await database_service.list_analyses(tags=[tag])
    assert [a.database_id for a in default_order] == ids  # id order, unchanged default

    newest = await database_service.list_analyses(tags=[tag], newest_first=True)
    assert newest[0].database_id == ids[0]

    page = await database_service.list_analyses(tags=[tag], limit=2, offset=1)
    assert [a.database_id for a in page] == ids[1:]

    ready = await database_service.list_analyses(tags=[tag], status=AnalysisStatusDB.READY)
    assert [a.database_id for a in ready] == [ids[0]]
    assert await database_service.list_analyses(tags=[tag], backend="ray") == []

    future = datetime.datetime.now() + datetime.timedelta(days=1)
    assert await database_service.list_analyses(tags=[tag], since=future) == []


# ---------------------------------------------------------------------------
# datasets
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_dataset_round_trips_and_records_its_origin(database_service: DatabaseServiceSQL) -> None:
    analysis_id = await _analysis(database_service)
    uri = _uri("ptools_rna_multiseed__variant=0")
    source = {"kind": "simulation", "ref": "1002", "coordinate": {"variant": 0, "protocol": "multiseed"}}
    dto, action = await database_service.upsert_dataset(
        uri=uri,
        kind="ptools-analysis",
        origin="event",
        analysis_id=analysis_id,
        view="ptools_rna",
        display_name="exp · ptools_rna · multiseed",
        size_bytes=12345,
        attributes={"protocol": "multiseed", "variant": 0, "n_tp": 8},
        tags=["cd2", "cd2", ""],
        source=source,
    )
    assert action == "inserted"
    assert dto.analysis_id == analysis_id and dto.simulation_id is None
    assert dto.attributes == {"protocol": "multiseed", "variant": 0, "n_tp": 8, "origin": "event"}
    assert dto.origin == "event"
    assert dto.tags == ["cd2"]  # de-duplicated, empty dropped
    assert dto.source == source and dto.available is True

    assert await database_service.get_dataset(dto.database_id) == dto
    assert await database_service.get_dataset_by_uri(uri) == dto
    assert await database_service.get_dataset(10_000_000) is None


@pytest.mark.asyncio
async def test_upsert_rejects_a_dataset_without_a_producer_or_with_an_unknown_kind(
    database_service: DatabaseServiceSQL,
) -> None:
    with pytest.raises(ValueError, match="producer"):
        await database_service.upsert_dataset(uri=_uri("orphan"), kind="figure", origin="walk")
    analysis_id = await _analysis(database_service)
    with pytest.raises(ValueError, match="kind"):
        await database_service.upsert_dataset(uri=_uri("x"), kind="spreadsheet", origin="walk", analysis_id=analysis_id)
    with pytest.raises(ValueError, match="origin"):
        await database_service.upsert_dataset(uri=_uri("x"), kind="figure", origin="guess", analysis_id=analysis_id)


@pytest.mark.asyncio
async def test_a_walk_never_overwrites_an_event_sourced_row(database_service: DatabaseServiceSQL) -> None:
    analysis_id = await _analysis(database_service)
    uri = _uri("ptools_rxns")
    event_row, _ = await database_service.upsert_dataset(
        uri=uri, kind="ptools-analysis", origin="event", analysis_id=analysis_id, attributes={"n_tp": 8}
    )
    walked, action = await database_service.upsert_dataset(
        uri=uri, kind="ptools-analysis", origin="walk", analysis_id=analysis_id, attributes={"n_tp": 99}, tags=["x"]
    )
    assert action == "skipped"
    assert walked == event_row
    assert (await database_service.get_dataset(event_row.database_id)) == event_row


@pytest.mark.asyncio
async def test_an_event_upgrades_a_walk_row_and_merges_attributes_and_tags(
    database_service: DatabaseServiceSQL,
) -> None:
    analysis_id = await _analysis(database_service)
    uri = _uri("ptools_proteins")
    walked, _ = await database_service.upsert_dataset(
        uri=uri,
        kind="ptools-analysis",
        origin="walk",
        analysis_id=analysis_id,
        attributes={"protocol": "multiseed", "n_tp": 7},
        tags=["cd2"],
    )
    upgraded, action = await database_service.upsert_dataset(
        uri=uri,
        kind="ptools-analysis",
        origin="event",
        analysis_id=analysis_id,
        view="ptools_proteins",
        attributes={"n_tp": 8, "variant": 0},
        tags=["run3"],
    )
    assert action == "updated"
    assert upgraded.database_id == walked.database_id
    assert upgraded.attributes == {"protocol": "multiseed", "n_tp": 8, "variant": 0, "origin": "event"}
    assert upgraded.tags == ["cd2", "run3"]
    assert upgraded.view == "ptools_proteins"


@pytest.mark.asyncio
async def test_an_identical_rewrite_changes_nothing_including_updated_at(
    database_service: DatabaseServiceSQL,
) -> None:
    """``updated_at`` must mean "changed", not "seen": the hourly walk re-registers
    every file, and a freshness filter would otherwise return everything."""
    analysis_id = await _analysis(database_service)
    uri = _uri("ptools_overview")
    first, _ = await database_service.upsert_dataset(
        uri=uri, kind="ptools-analysis", origin="walk", analysis_id=analysis_id, attributes={"n_tp": 8}, tags=["cd2"]
    )
    await asyncio.sleep(0.05)
    again, action = await database_service.upsert_dataset(
        uri=uri, kind="ptools-analysis", origin="walk", analysis_id=analysis_id, attributes={"n_tp": 8}, tags=["cd2"]
    )
    assert action == "unchanged"
    assert again.updated_at == first.updated_at


@pytest.mark.asyncio
async def test_concurrent_writers_of_one_uri_leave_exactly_one_row(database_service: DatabaseServiceSQL) -> None:
    analysis_id = await _analysis(database_service)
    uri = _uri("race")
    results = await asyncio.gather(*[
        database_service.upsert_dataset(
            uri=uri, kind="figure", origin="walk", analysis_id=analysis_id, tags=[f"writer-{i}"]
        )
        for i in range(4)
    ])
    assert [action for _, action in results].count("inserted") == 1
    assert len({dto.database_id for dto, _ in results}) == 1
    assert await database_service.count_datasets(analysis_id=analysis_id) == 1


@pytest.mark.asyncio
async def test_list_datasets_filters_and_pages(database_service: DatabaseServiceSQL) -> None:
    analysis_id = await _analysis(database_service)
    other_analysis = await _analysis(database_service)
    tag = f"list-{uuid.uuid4().hex[:6]}"

    rows = {}
    for view, protocol, variant in (
        ("ptools_rna", "multiseed", 0),
        ("ptools_rna", "multiseed", 1),
        ("ptools_rxns", "multiseed", 0),
        ("ptools_rna", "single", 0),
    ):
        dto, _ = await database_service.upsert_dataset(
            uri=_uri(f"{view}_{protocol}_{variant}"),
            kind="ptools-analysis",
            origin="event",
            analysis_id=analysis_id,
            view=view,
            attributes={"protocol": protocol, "variant": variant},
            tags=[tag, protocol],
        )
        rows[(view, protocol, variant)] = dto.database_id
    figure, _ = await database_service.upsert_dataset(
        uri=_uri("figure"), kind="figure", origin="walk", analysis_id=other_analysis, tags=[tag]
    )

    def ids(items: list[Any]) -> set[int]:
        return {d.database_id for d in items}

    assert ids(await database_service.list_datasets(tags=[tag])) == set(rows.values()) | {figure.database_id}
    assert ids(await database_service.list_datasets(tags=[tag], kind="figure")) == {figure.database_id}
    assert ids(await database_service.list_datasets(tags=[tag], view="ptools_rxns")) == {
        rows[("ptools_rxns", "multiseed", 0)]
    }
    assert ids(
        await database_service.list_datasets(tags=[tag], attributes={"protocol": "multiseed", "variant": 0})
    ) == {rows[("ptools_rna", "multiseed", 0)], rows[("ptools_rxns", "multiseed", 0)]}
    # Containment is typed: a string "0" is not the integer 0.
    assert await database_service.list_datasets(tags=[tag], attributes={"variant": "0"}) == []
    assert ids(await database_service.list_datasets(tags=[tag, "single"])) == {rows[("ptools_rna", "single", 0)]}
    assert ids(await database_service.list_datasets(tags=[tag], analysis_id=other_analysis)) == {figure.database_id}

    # Unavailable rows are excluded by default and included with available=None.
    gone = rows[("ptools_rna", "single", 0)]
    flipped = await database_service.set_dataset_available(gone, False)
    assert flipped is not None and flipped.available is False
    assert gone not in ids(await database_service.list_datasets(tags=[tag]))
    assert gone in ids(await database_service.list_datasets(tags=[tag], available=None))
    assert ids(await database_service.list_datasets(tags=[tag], available=False)) == {gone}

    # Newest change first, paging, and the limit ceiling.
    newest = await database_service.list_datasets(tags=[tag], available=None)
    assert newest[0].database_id == gone
    page = await database_service.list_datasets(tags=[tag], available=None, limit=2, offset=2)
    assert [d.database_id for d in page] == [d.database_id for d in newest[2:4]]
    assert len(await database_service.list_datasets(tags=[tag], available=None, limit=10_000)) == 5

    future = datetime.datetime.now() + datetime.timedelta(days=1)
    assert await database_service.list_datasets(tags=[tag], since=future) == []

    assert await database_service.count_datasets(analysis_id=analysis_id) == 4
    assert await database_service.count_datasets(analysis_id=analysis_id, available=True) == 3


@pytest.mark.asyncio
async def test_a_simulation_can_be_a_producer(database_service: DatabaseServiceSQL) -> None:
    simulation, _run = await _insert_run(database_service, correlation_id=f"prov-{uuid.uuid4().hex[:8]}")
    dto, _ = await database_service.upsert_dataset(
        uri=f"s3://bucket/vecoli-output/{simulation.experiment_id}/history/variant=0/",
        kind="parquet",
        origin="event",
        simulation_id=simulation.database_id,
        attributes={"variant": 0, "generation": 1},
    )
    assert dto.simulation_id == simulation.database_id
    assert await database_service.count_datasets(simulation_id=simulation.database_id) == 1


@pytest.mark.asyncio
async def test_tags_and_attribute_summaries(database_service: DatabaseServiceSQL) -> None:
    analysis_id = await _analysis(database_service)
    tag = f"sum-{uuid.uuid4().hex[:6]}"
    first, _ = await database_service.upsert_dataset(
        uri=_uri("a"), kind="report", origin="walk", analysis_id=analysis_id, attributes={"family": tag, "variant": 0}
    )
    await database_service.upsert_dataset(
        uri=_uri("b"), kind="report", origin="walk", analysis_id=analysis_id, attributes={"family": tag, "variant": 1}
    )

    tagged = await database_service.add_dataset_tags(first.database_id, [tag, tag, "extra"])
    assert tagged.tags == [tag, "extra"]
    with pytest.raises(RuntimeError):
        await database_service.add_dataset_tags(10_000_000, [tag])

    counts = await database_service.list_dataset_tags(kind="report")
    assert counts.get(tag) == 1 and counts.get("extra") == 1

    values = await database_service.list_dataset_attribute_values(kind="report")
    assert tag in values["family"]
    assert {0, 1} <= set(values["variant"])
    assert values["origin"] == ["walk"] or "walk" in values["origin"]


@pytest.mark.asyncio
async def test_list_datasets_filters_by_source(database_service: DatabaseServiceSQL) -> None:
    analysis_id = await _analysis(database_service)
    ref = str(uuid.uuid4().int % 10**9)
    of_simulation, _ = await database_service.upsert_dataset(
        uri=_uri("of-simulation"),
        kind="ptools-analysis",
        origin="walk",
        analysis_id=analysis_id,
        source={"kind": "simulation", "ref": ref, "resolved_id": int(ref), "coordinate": {"variant": 0}},
    )
    await database_service.upsert_dataset(
        uri=_uri("unsourced"), kind="ptools-analysis", origin="walk", analysis_id=analysis_id
    )

    found = await database_service.list_datasets(source={"kind": "simulation", "ref": ref})
    assert [d.database_id for d in found] == [of_simulation.database_id]
    # Containment reaches into the coordinate.
    assert (
        await database_service.list_datasets(source={"kind": "simulation", "ref": ref, "coordinate": {"variant": 1}})
        == []
    )


@pytest.mark.asyncio
async def test_add_analysis_tags_union_merges_and_rejects_an_unknown_id(database_service: DatabaseServiceSQL) -> None:
    analysis_id = await _analysis(database_service, tags=["a"])
    tagged = await database_service.add_analysis_tags(analysis_id, ["b", "a"])
    assert sorted(tagged.tags) == ["a", "b"]
    assert sorted((await database_service.get_analysis(analysis_id)).tags) == ["a", "b"]
    with pytest.raises(RuntimeError):
        await database_service.add_analysis_tags(999_999_999, ["x"])
