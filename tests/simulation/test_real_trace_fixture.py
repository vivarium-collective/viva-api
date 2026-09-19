"""The ingester and the dataset registry on a REAL event stream, not only hand-written ones.

``tests/fixtures/events/real_sim1319_*_thinned.jsonl`` are thinned, hostname-scrubbed
copies of two objects a real dev run wrote (simulation 1319, 2026-09-14): a
chain-dispatch lineage task and the run's in-run analysis gather. See the fixtures
README for provenance.

No producer emits ``artifact.written`` yet, so a real stream must register nothing;
and an artifact event placed inside a real envelope (real trace, span and baggage,
including baggage coordinates sent as JSON numbers) must register exactly like the
contract says. Each test re-tags the fixture to its own run's trace id, so tests do not
collide on the ``(trace_id, source, seq)`` uniqueness of stored events.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from tests.simulation.test_event_ingest import _S3, _insert_run, _settings
from viva_api.simulation import event_ingest
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import HpcRun, Simulation

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "events"
REAL_TRACE = "b87bb2477553ee5a0c98ca6588eebec7"
LINEAGE = "real_sim1319_lineage_thinned.jsonl"
GATHER = "real_sim1319_gather_thinned.jsonl"


def _real(name: str, trace_id: str) -> str:
    text = (FIXTURES / name).read_text()
    assert REAL_TRACE in text, "fixture no longer carries the real trace id"
    return text.replace(REAL_TRACE, trace_id)


def _objects(simulation: Simulation, run: HpcRun, blobs: dict[str, str]) -> _S3:
    return _S3({
        f"nextflow/work/{simulation.experiment_id}/events/{run.trace_id}/{name}": text.encode()
        for name, text in blobs.items()
    })


def _append_artifact(text: str, *, need_generation: bool, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """A new artifact.written line built on a real line's envelope (trace, source, span, baggage)."""
    events = [json.loads(line) for line in text.splitlines() if line.strip()]
    donor = [e for e in events if not need_generation or "generation" in (e.get("baggage") or {})][-1]
    line = {
        **donor,
        "seq": max(e["seq"] for e in events if e["source"] == donor["source"]) + 1,
        "event": "artifact.written",
        "level": "info",
        "payload": payload,
    }
    return text.rstrip("\n") + "\n" + json.dumps(line) + "\n", line


@pytest.mark.asyncio
async def test_a_real_stream_ingests_cleanly_and_registers_nothing(database_service: DatabaseServiceSQL) -> None:
    simulation, run = await _insert_run(database_service, correlation_id=f"real-{uuid.uuid4().hex[:8]}")
    trace_id = run.trace_id or ""
    blobs = {name: _real(name, trace_id) for name in (LINEAGE, GATHER)}
    settings = _settings()

    expected_parsed = expected_stored = 0
    for text in blobs.values():
        events, bad = event_ingest.parse_event_lines(text, expected_trace_id=trace_id)
        assert bad == 0
        expected_parsed += len(events)
        expected_stored += len(event_ingest.storable_events(events, settings))

    result = await event_ingest.ingest_run_events(
        run, simulation, _objects(simulation, run, blobs), database_service, settings
    )

    assert result.objects_read == 2 and result.bad_lines == 0
    assert result.events_parsed == expected_parsed
    assert result.events_inserted == expected_stored
    assert result.spans_changed > 0
    assert (result.artifacts_registered, result.artifacts_unchanged, result.artifacts_skipped) == (0, 0, 0)
    assert await database_service.list_datasets(simulation_id=simulation.database_id, available=None) == []

    # The real lineage stream sends its coordinates as JSON numbers; they still promote.
    stored = await database_service.list_hpcrun_events(run.database_id, limit=10_000)
    generation_starts = [e for e in stored if e.event == "lineage.generation.start"]
    assert generation_starts and all(e.generation is not None for e in generation_starts)


@pytest.mark.asyncio
async def test_artifacts_in_real_envelopes_register_with_real_coordinates(database_service: DatabaseServiceSQL) -> None:
    simulation, run = await _insert_run(database_service, correlation_id=f"real-{uuid.uuid4().hex[:8]}")
    trace_id = run.trace_id or ""
    parquet_uri = f"s3://work/vecoli-output/{simulation.experiment_id}/history/real-{uuid.uuid4().hex[:6]}/"
    tsv_uri = f"s3://work/vecoli-output/{simulation.experiment_id}/analyses/a/ptools/rna-{uuid.uuid4().hex[:6]}.tsv"

    lineage, parquet_line = _append_artifact(
        _real(LINEAGE, trace_id),
        need_generation=True,
        payload={"uri": parquet_uri, "kind": "parquet", "attributes": {"table": "history"}},
    )
    gather, tsv_line = _append_artifact(
        _real(GATHER, trace_id),
        need_generation=False,
        payload={
            "uri": tsv_uri,
            "kind": "ptools-analysis",
            "view": "ptools_rna",
            "attributes": {"protocol": "multiseed"},
        },
    )

    result = await event_ingest.ingest_run_events(
        run, simulation, _objects(simulation, run, {LINEAGE: lineage, GATHER: gather}), database_service, _settings()
    )
    assert (result.artifacts_registered, result.artifacts_skipped) == (2, 0)

    parquet = await database_service.get_dataset_by_uri(parquet_uri)
    assert parquet is not None and parquet.simulation_id == simulation.database_id
    baggage = parquet_line["baggage"]
    expected = {
        "variant": int(baggage["variant"]),
        "seed": int(baggage["lineage_seed"]),
        "generation": int(baggage["generation"]),
    }
    assert {key: parquet.attributes[key] for key in expected} == expected
    assert parquet.source is not None and parquet.source["coordinate"] == expected
    assert parquet.attributes["span_id"] == parquet_line["span_id"]

    tsv = await database_service.get_dataset_by_uri(tsv_uri)
    # The real gather's baggage names no analysis, so its files belong to the simulation.
    assert tsv is not None and tsv.simulation_id == simulation.database_id and tsv.analysis_id is None
    assert tsv.attributes["span_id"] == tsv_line["span_id"]
