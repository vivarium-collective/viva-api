"""The trace feeder: ``artifact.written`` events become ``dataset`` rows (plan §3, §5).

Synthetic event objects go through the real ``event_ingest.ingest_run_events``
against the testcontainer Postgres, with the in-memory ``_S3`` double standing in
for the task's ``events.jsonl`` objects. Nothing reaches a hosted service.
"""

from __future__ import annotations

import json
import uuid
from typing import Any
from unittest.mock import patch

import pytest

from tests.simulation.test_event_ingest import _S3, _insert_run, _settings
from viva_api.common.models import JobId
from viva_api.simulation import event_ingest
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.dataset_registry import register_datasets
from viva_api.simulation.models import HpcRun, JobType, Simulation
from viva_api.simulation.tables_orm import AnalysisStatusDB


def _event(
    seq: int,
    trace_id: str,
    *,
    payload: dict[str, Any] | None = None,
    baggage: dict[str, str] | None = None,
    event: str = "artifact.written",
    span_id: str = "aaaabbbbccccdddd",
) -> str:
    return json.dumps({
        "v": 1,
        "ts": f"2026-09-15T00:00:{seq:02d}.000Z",
        "seq": seq,
        "source": "analysis-task",
        "component": "v2ecoli.analysis",
        "event": event,
        "level": "info",
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": None,
        "global_time": None,
        "wall_time": float(seq),
        "baggage": baggage or {},
        "tags": {},
        "payload": payload or {},
    })


def _object(*lines: str) -> bytes:
    return ("\n".join(lines) + "\n").encode()


def _key(simulation: Simulation, run: HpcRun, name: str = "task") -> str:
    return f"nextflow/work/{simulation.experiment_id}/events/{run.trace_id}/{name}.jsonl"


def _uri(name: str) -> str:
    return f"s3://work/vecoli-output/exp/analyses/a/ptools/{name}-{uuid.uuid4().hex[:8]}.tsv"


async def _analysis_run(db: DatabaseServiceSQL, simulation: Simulation) -> tuple[int, HpcRun]:
    record = await db.record_analysis(
        experiment_id=simulation.experiment_id,
        n_tp=None,
        status=AnalysisStatusDB.COMPUTING,
        config={"analysis_options": {"experiment_id": [simulation.experiment_id]}},
        name=f"analysis-{uuid.uuid4().hex[:8]}",
        simulation_id=simulation.database_id,
        backend="ray",
    )
    run = await db.insert_hpcrun(
        job_id=JobId.k8s(f"ana-{record.database_id}"),
        job_type=JobType.ANALYSIS,
        ref_id=record.database_id,
        correlation_id=f"analysis-{record.database_id}",
    )
    return record.database_id, run


# ---------------------------------------------------------------------------
# producers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_simulation_run_registers_what_it_wrote(database_service: DatabaseServiceSQL) -> None:
    """The in-run gather rides the simulation's trace: its files belong to the simulation,
    carry the coordinate from baggage, and inherit the simulation's tags."""
    simulation, run = await _insert_run(database_service, correlation_id=f"sim-{uuid.uuid4().hex[:8]}")
    simulation = await database_service.add_tags(simulation.database_id, ["cd2"])
    parquet_uri = f"s3://work/vecoli-output/{simulation.experiment_id}/history/variant=0/gen=1-{uuid.uuid4().hex[:6]}/"
    tsv_uri = _uri("ptools_rna_multiseed__variant=0")
    s3 = _S3({
        _key(simulation, run): _object(
            _event(
                1,
                run.trace_id or "",
                payload={"uri": parquet_uri, "kind": "parquet", "attributes": {"table": "history"}},
                baggage={"sim_id": str(simulation.database_id), "variant": "0", "generation": "1"},
            ),
            _event(
                2,
                run.trace_id or "",
                payload={
                    "uri": tsv_uri,
                    "kind": "ptools-analysis",
                    "name": "ptools_rna_multiseed__variant=0.tsv",
                    "view": "ptools_rna",
                    "bytes": 2048,
                    "attributes": {"protocol": "multiseed", "variant": 0, "n_tp": 8, "tags": ["run3"]},
                },
            ),
        )
    })

    result = await event_ingest.ingest_run_events(run, simulation, s3, database_service, _settings())

    assert (result.artifacts_registered, result.artifacts_skipped) == (2, 0)
    parquet = await database_service.get_dataset_by_uri(parquet_uri)
    assert parquet is not None and parquet.simulation_id == simulation.database_id
    assert parquet.kind == "parquet"
    assert parquet.attributes["variant"] == 0 and parquet.attributes["generation"] == 1
    assert parquet.attributes["origin"] == "event"
    # The job and trace that wrote it are columns (P4a-1); ``hpcrun_id`` no longer rides in the attributes.
    assert parquet.producer_job_id == run.database_id and parquet.trace_id == run.trace_id
    assert "hpcrun_id" not in parquet.attributes
    assert parquet.attributes["span_id"] == "aaaabbbbccccdddd"
    assert parquet.source == {
        "kind": "simulation",
        "ref": str(simulation.database_id),
        "resolved_id": simulation.database_id,
        "coordinate": {"variant": 0, "generation": 1},
    }

    tsv = await database_service.get_dataset_by_uri(tsv_uri)
    assert tsv is not None and tsv.view == "ptools_rna" and tsv.size_bytes == 2048
    assert tsv.attributes["n_tp"] == 8 and tsv.attributes["name"] == "ptools_rna_multiseed__variant=0.tsv"
    assert "tags" not in tsv.attributes
    assert tsv.tags == ["cd2", "run3"]
    assert tsv.display_name == f"{simulation.experiment_id} · ptools_rna · multiseed"


@pytest.mark.asyncio
async def test_an_analysis_run_attributes_its_files_to_its_analysis(database_service: DatabaseServiceSQL) -> None:
    """Baggage ``analysis_id`` wins; without it an ANALYSIS run's own analysis is the producer."""
    simulation, _ = await _insert_run(database_service, correlation_id=f"sim-{uuid.uuid4().hex[:8]}")
    analysis_id, run = await _analysis_run(database_service, simulation)
    with_baggage, without_baggage = _uri("with"), _uri("without")
    s3 = _S3({
        _key(simulation, run): _object(
            _event(
                1,
                run.trace_id or "",
                payload={"uri": with_baggage, "kind": "figure"},
                baggage={"analysis_id": str(analysis_id)},
            ),
            _event(2, run.trace_id or "", payload={"uri": without_baggage, "kind": "figure"}),
        )
    })

    result = await event_ingest.ingest_run_events(run, simulation, s3, database_service, _settings())

    assert result.artifacts_registered == 2
    for uri in (with_baggage, without_baggage):
        row = await database_service.get_dataset_by_uri(uri)
        assert row is not None and row.analysis_id == analysis_id and row.simulation_id is None
    assert await database_service.count_datasets(analysis_id=analysis_id) == 2


@pytest.mark.asyncio
async def test_a_parca_cache_belongs_to_the_parca_dataset(database_service: DatabaseServiceSQL) -> None:
    simulation, run = await _insert_run(database_service, correlation_id=f"sim-{uuid.uuid4().hex[:8]}")
    cache_uri = f"s3://work/ray-parca-cache/{uuid.uuid4().hex[:7]}/"
    events = event_ingest.parse_event_lines(
        _event(1, run.trace_id or "", payload={"uri": cache_uri, "kind": "parca-cache"})
    )[0]

    result = await register_datasets(events, hpc_run=run, simulation=simulation, db=database_service)

    assert result.registered == 1
    row = await database_service.get_dataset_by_uri(cache_uri)
    assert row is not None and row.parca_dataset_id == simulation.parca_dataset_id
    assert row.simulation_id is None and row.analysis_id is None


# ---------------------------------------------------------------------------
# availability, validation, idempotence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_failed_view_registers_unavailable_and_a_later_success_flips_it(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation, run = await _insert_run(database_service, correlation_id=f"sim-{uuid.uuid4().hex[:8]}")
    uri = _uri("ptools_metabolites_multigeneration")
    key = _key(simulation, run)
    failure = _event(1, run.trace_id or "", payload={"uri": uri, "kind": "ptools-analysis", "error": "OOM at 41.0 GiB"})
    s3 = _S3({key: _object(failure)})
    settings = _settings()

    await event_ingest.ingest_run_events(run, simulation, s3, database_service, settings)
    failed = await database_service.get_dataset_by_uri(uri)
    assert failed is not None and failed.available is False
    assert failed.attributes["error"] == "OOM at 41.0 GiB"

    # The writer rewrites the whole object; the retry's success event follows the failure.
    s3.objects[key] = _object(failure, _event(2, run.trace_id or "", payload={"uri": uri, "kind": "ptools-analysis"}))
    refreshed_run = await database_service.get_hpcrun(run.database_id)
    assert refreshed_run is not None
    await event_ingest.ingest_run_events(refreshed_run, simulation, s3, database_service, settings)
    recovered = await database_service.get_dataset_by_uri(uri)
    assert recovered is not None and recovered.available is True


@pytest.mark.asyncio
async def test_invalid_artifacts_are_skipped_and_counted_without_stopping_the_rest(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation, run = await _insert_run(database_service, correlation_id=f"sim-{uuid.uuid4().hex[:8]}")
    good = _uri("good")
    trace = run.trace_id or ""
    s3 = _S3({
        _key(simulation, run): _object(
            _event(1, trace, payload={"uri": "/local/path.tsv", "kind": "ptools-analysis"}),
            _event(2, trace, payload={"uri": _uri("bad-kind"), "kind": "spreadsheet"}),
            _event(3, trace, payload={"uri": _uri("bad-attrs"), "kind": "figure", "attributes": ["x"]}),
            _event(4, trace, payload={"uri": good, "kind": "figure"}),
            _event(5, trace, payload={"uri": _uri("read"), "kind": "parquet"}, event="artifact.read"),
        )
    })

    result = await event_ingest.ingest_run_events(run, simulation, s3, database_service, _settings())

    assert (result.artifacts_registered, result.artifacts_skipped) == (1, 3)
    assert await database_service.get_dataset_by_uri(good) is not None
    stored = {e.event for e in await database_service.list_hpcrun_events(run.database_id)}
    assert {"artifact.written", "artifact.read"} <= stored  # the events themselves are kept


@pytest.mark.asyncio
async def test_registering_the_same_events_twice_changes_nothing_and_a_walk_cannot_downgrade(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation, run = await _insert_run(database_service, correlation_id=f"sim-{uuid.uuid4().hex[:8]}")
    uri = _uri("idempotent")
    events = event_ingest.parse_event_lines(
        _event(1, run.trace_id or "", payload={"uri": uri, "kind": "analysis", "attributes": {"n_tp": 5}})
    )[0]

    first = await register_datasets(events, hpc_run=run, simulation=simulation, db=database_service)
    again = await register_datasets(events, hpc_run=run, simulation=simulation, db=database_service)
    assert (first.registered, again.registered, again.unchanged) == (1, 0, 1)

    _dto, action = await database_service.upsert_dataset(
        uri=uri, kind="analysis", origin="walk", simulation_id=simulation.database_id, attributes={"n_tp": 99}
    )
    assert action == "skipped"
    row = await database_service.get_dataset_by_uri(uri)
    assert row is not None and row.origin == "event" and row.attributes["n_tp"] == 5


@pytest.mark.asyncio
async def test_a_failed_registration_leaves_the_cursor_so_the_next_tick_retries(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation, run = await _insert_run(database_service, correlation_id=f"sim-{uuid.uuid4().hex[:8]}")
    uri = _uri("retry")
    s3 = _S3({_key(simulation, run): _object(_event(1, run.trace_id or "", payload={"uri": uri, "kind": "figure"}))})
    settings = _settings()

    with patch.object(event_ingest, "register_datasets", side_effect=RuntimeError("database went away")):
        failed = await event_ingest.ingest_run_events(run, simulation, s3, database_service, settings)
    assert failed.objects_read == 1 and failed.events_inserted == 1
    assert await database_service.get_hpcrun_events_cursor(run.database_id) is None
    assert await database_service.get_dataset_by_uri(uri) is None

    refreshed_run = await database_service.get_hpcrun(run.database_id)
    assert refreshed_run is not None
    retried = await event_ingest.ingest_run_events(refreshed_run, simulation, s3, database_service, settings)
    assert retried.objects_read == 1 and retried.artifacts_registered == 1
    assert await database_service.get_dataset_by_uri(uri) is not None
