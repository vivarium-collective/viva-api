"""The event ingester in core: a run's JSON-lines objects become event rows, a span tree, progress
columns and registered artifacts through the ``EventStore`` and ``ArtifactRegistrar`` Protocols
(plan P4a-2, slice 4).

Pure and fast: an in-memory store, an object-map file service and a scripted registrar. The
application's own tests (``tests/simulation/test_event_ingest.py``, ``test_dataset_registry.py``,
``test_real_trace_fixture.py``) run the same loop against Postgres with its store and feeder.
"""

from __future__ import annotations

import datetime
import json
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import pytest

from viva_core.datasets.registry import RegistrationResult
from viva_core.events.ingest import (
    ArtifactRegistrar,
    EventStore,
    IngestResult,
    IngestRun,
    ingest_run_events,
    is_ingest_candidate,
    parse_event_lines,
)
from viva_core.events.models import SimulationEvent, SimulationSpan
from viva_core.storage.file_paths import S3FilePath
from viva_core.storage.file_service import FileService, ListingItem

TRACE = "0123456789abcdef0123456789abcdef"
PREFIX = "s3://work/runs/exp-1/events"
KEY = f"runs/exp-1/events/{TRACE}/task.jsonl"


def _settings(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "storage_s3_bucket": "work",
        "events_ingest_max_objects_per_tick": 50,
        "events_ingest_idle_seconds": 600,
        "events_ingest_terminal_grace_seconds": 900,
        "events_ingest_store_debug": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _line(seq: int, event: str, *, payload: dict[str, object] | None = None, **top: object) -> str:
    raw: dict[str, object] = {
        "v": 1,
        "ts": f"2026-09-15T00:00:{seq:02d}.000Z",
        "seq": seq,
        "source": "task",
        "component": "process_bigraph",
        "event": event,
        "level": "info",
        "trace_id": TRACE,
        "span_id": "aaaabbbbccccdddd",
        "baggage": {"generation": seq // 2},
        "payload": payload or {},
    }
    raw.update(top)
    return json.dumps(raw)


def _object(*lines: str) -> bytes:
    return ("\n".join(lines) + "\n").encode()


class _Objects(FileService):
    """A ``FileService`` double: ``objects`` maps keys to bytes; listing is by prefix."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.reads: list[str] = []

    async def get_listing(self, s3_path: S3FilePath) -> list[ListingItem]:
        prefix = str(s3_path.s3_path)
        when = datetime.datetime(2026, 9, 15, tzinfo=datetime.UTC)
        return [
            ListingItem(Key=key, LastModified=when, ETag="e", Size=len(body))
            for key, body in sorted(self.objects.items())
            if key.startswith(prefix)
        ]

    async def get_file_contents(self, s3_path: S3FilePath) -> bytes | None:
        self.reads.append(str(s3_path.s3_path))
        return self.objects.get(str(s3_path.s3_path))

    async def download_file(self, s3_path: S3FilePath, file_path: Path | None = None) -> tuple[S3FilePath, str]:
        raise NotImplementedError

    async def upload_file(self, file_path: Path, s3_path: S3FilePath) -> S3FilePath:
        raise NotImplementedError

    async def upload_bytes(self, file_contents: bytes, s3_path: S3FilePath) -> S3FilePath:
        raise NotImplementedError

    async def get_modified_date(self, s3_path: S3FilePath) -> datetime.datetime:
        raise NotImplementedError

    async def delete_file(self, s3_path: S3FilePath) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        return None


@dataclass
class _Progress:
    stage: str | None
    generation: int | None
    events_cursor: dict[str, int] | None
    events_prefix: str | None


@dataclass
class _Store:
    """An ``EventStore`` in memory: events deduplicated on ``(source, seq)``, spans by id, and every
    progress update recorded."""

    events: dict[tuple[str, int], SimulationEvent] = field(default_factory=dict)
    spans: dict[str, SimulationSpan] = field(default_factory=dict)
    cursor: dict[str, int] | None = None
    progress: list[_Progress] = field(default_factory=list)
    closed: int = 0

    async def events_cursor(self, run_id: int) -> dict[str, int] | None:
        return self.cursor

    async def list_spans(self, run_id: int) -> list[SimulationSpan]:
        return [span.model_copy() for span in self.spans.values()]

    async def insert_events(self, run_id: int, trace_id: str, events: list[SimulationEvent]) -> int:
        before = len(self.events)
        for event in events:
            self.events.setdefault((event.source, event.seq), event)
        return len(self.events) - before

    async def upsert_spans(self, run_id: int, trace_id: str, spans: list[SimulationSpan]) -> None:
        for span in spans:
            self.spans[span.span_id] = span.model_copy()

    async def close_open_spans(self, run_id: int, status: str) -> int:
        open_spans = [span for span in self.spans.values() if span.end_ts is None]
        for span in open_spans:
            span.end_ts, span.status = "closed", status
        self.closed += len(open_spans)
        return len(open_spans)

    async def update_progress(
        self,
        run_id: int,
        *,
        stage: str | None,
        generation: int | None,
        last_event_at: datetime.datetime | None,
        events_cursor: dict[str, int] | None,
        events_prefix: str | None,
    ) -> None:
        if events_cursor is not None:
            self.cursor = dict(events_cursor)
        self.progress.append(_Progress(stage, generation, events_cursor, events_prefix))


@dataclass
class _Registrar:
    """An ``ArtifactRegistrar`` that records what it was handed; ``fail`` makes it raise."""

    fail: bool = False
    handed: list[list[SimulationEvent]] = field(default_factory=list)

    async def register(self, events: list[SimulationEvent]) -> RegistrationResult:
        if self.fail:
            raise RuntimeError("database went away")
        self.handed.append(list(events))
        return RegistrationResult(registered=len(events))


def _run(**overrides: object) -> IngestRun:
    values: dict[str, object] = {"run_id": 7, "trace_id": TRACE, "events_prefix": None, "default_events_prefix": PREFIX}
    values.update(overrides)
    return IngestRun(**values)  # type: ignore[arg-type]


@dataclass
class _Pass:
    result: IngestResult
    store: _Store
    registrar: _Registrar
    files: _Objects


async def _ingest(
    objects: dict[str, bytes], *, store: _Store | None = None, registrar: _Registrar | None = None, **run: object
) -> _Pass:
    store = store or _Store()
    registrar = registrar or _Registrar()
    files = _Objects(objects)
    result = await ingest_run_events(
        _run(**run), file_service=files, store=store, registrar=registrar, settings=_settings()
    )
    return _Pass(result, store, registrar, files)


def test_the_protocols_are_satisfied_structurally() -> None:
    store: EventStore = _Store()
    registrar: ArtifactRegistrar = _Registrar()
    assert store is not None and registrar is not None


@pytest.mark.asyncio
async def test_a_pass_stores_events_folds_spans_and_progress_and_hands_artifacts_to_the_registrar() -> None:
    objects = {
        KEY: _object(
            _line(1, "span.start", payload={"name": "lineage", "attrs": {"generation": 0}}),
            _line(2, "tick"),
            _line(3, "lineage.debug", level="debug"),
            _line(4, "artifact.written", payload={"uri": "s3://work/out/x/", "kind": "store"}),
            _line(5, "step", trace_id="ffffffffffffffffffffffffffffffff"),  # another trace: skipped
            "not json",
        )
    }
    p = await _ingest(objects)

    assert (p.result.objects_listed, p.result.objects_read, p.result.events_parsed, p.result.bad_lines) == (1, 1, 4, 2)
    assert p.result.events_inserted == 2 and sorted(seq for _s, seq in p.store.events) == [
        1,
        4,
    ]  # tick + debug unstored
    assert p.result.spans_changed == 1 and p.store.spans["aaaabbbbccccdddd"].name == "lineage"
    assert p.result.stage == "lineage[generation=0]" and p.result.generation == 2
    assert p.result.last_event_at == datetime.datetime(2026, 9, 15, 0, 0, 4)
    assert p.result.artifacts_registered == 1 and [e.seq for e in p.registrar.handed[0]] == [4]
    assert p.store.cursor == {KEY: len(objects[KEY])}
    assert p.store.progress[-1].events_prefix == PREFIX  # written back: the row had none
    assert p.files.reads == [KEY]


@pytest.mark.asyncio
async def test_an_unchanged_object_is_skipped_without_a_read_and_a_rewritten_one_is_re_read() -> None:
    objects = {KEY: _object(_line(1, "step"))}
    first = await _ingest(objects)
    assert first.files.reads == [KEY]

    again = await _ingest(objects, store=first.store)
    assert again.files.reads == [] and again.result.skipped_reason is None and again.result.objects_read == 0

    objects[KEY] = _object(_line(1, "step"), _line(2, "step"))
    third = await _ingest(objects, store=first.store)
    assert third.files.reads == [KEY] and third.result.events_inserted == 1  # seq 1 was already stored


@pytest.mark.asyncio
async def test_a_failed_registration_withholds_the_cursor_so_the_next_tick_retries() -> None:
    objects = {KEY: _object(_line(1, "artifact.written", payload={"uri": "s3://work/out/x/", "kind": "store"}))}
    failed = await _ingest(objects, registrar=_Registrar(fail=True))
    assert failed.result.objects_read == 1 and failed.result.events_inserted == 1
    assert failed.result.artifacts_registered == 0 and failed.store.cursor is None

    retried = await _ingest(objects, store=failed.store)
    assert retried.result.objects_read == 1 and retried.result.artifacts_registered == 1
    assert retried.result.events_inserted == 0 and retried.store.cursor == {KEY: len(objects[KEY])}


@pytest.mark.asyncio
async def test_a_terminal_run_closes_its_open_spans_as_unknown() -> None:
    objects = {KEY: _object(_line(1, "span.start", payload={"name": "lineage"}))}
    p = await _ingest(objects, terminal=True)
    assert p.store.closed == 1 and p.store.spans["aaaabbbbccccdddd"].status == "unknown"
    assert p.result.stage is None


@pytest.mark.asyncio
async def test_a_run_without_a_trace_or_a_prefix_or_in_another_bucket_is_skipped_with_a_reason() -> None:
    assert (await _ingest({}, trace_id=None)).result.skipped_reason == "no trace_id on the row"
    assert "no events prefix" in ((await _ingest({}, default_events_prefix=None)).result.skipped_reason or "")
    elsewhere = await _ingest({}, events_prefix="s3://other/runs/exp-1/events")
    assert "not in the file service's bucket" in (elsewhere.result.skipped_reason or "")
    assert (await _ingest({})).result.skipped_reason == "no events objects yet"


def test_candidates_are_active_runs_and_recently_terminal_ones() -> None:
    now = datetime.datetime(2026, 9, 15, 1, 0, 0)
    settings = _settings()
    assert is_ingest_candidate(_run(), settings, now)
    assert is_ingest_candidate(_run(last_event_at="2026-09-15T00:55:00Z"), settings, now)
    assert not is_ingest_candidate(_run(last_event_at="2026-09-15T00:00:00Z"), settings, now)
    assert is_ingest_candidate(_run(terminal=True, end_time="2026-09-15T00:50:00Z"), settings, now)
    assert not is_ingest_candidate(_run(terminal=True, end_time="2026-09-15T00:00:00Z"), settings, now)
    assert not is_ingest_candidate(_run(terminal=True), settings, now)


def test_parsing_promotes_the_coordinate_from_baggage_and_tolerates_an_older_stream() -> None:
    modern = _line(1, "step", baggage={"variant": "2", "lineage_seed": 5, "generation": 3})
    older = json.dumps({"seq": 2, "event": "step", "ts": "", "lineage_seed": 9, "variant": None})
    events, bad = parse_event_lines("\n".join([modern, older, "{}", ""]))
    assert bad == 1 and [e.seq for e in events] == [1, 2]
    assert (events[0].variant, events[0].lineage_seed, events[0].generation) == (2, 5, 3)
    assert events[1].lineage_seed == 9 and events[1].variant is None and events[1].baggage == {"lineage_seed": 9}
