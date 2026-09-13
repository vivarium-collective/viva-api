"""Ingest a run's task-side event stream into the database (observability plan D4b).

Every task the API dispatches writes JSON-lines events (process-bigraph
``events.py``, schema D1) to stdout and -- when the run has an S3 events prefix
-- to ``<prefix>/<trace_id>/<source>.jsonl``, rewriting the WHOLE object every
``PBG_EVENT_FLUSH_S`` seconds. This module is the API-side adapter that turns
those objects into rows a person can query without AWS credentials:

* ``hpcrun_event`` -- one row per event (``tick`` heartbeats excluded: they are
  a liveness signal, not a record), idempotent on ``(trace_id, source, seq)``;
* ``hpcrun_span`` -- the trace tree, materialised from ``span.start`` /
  ``span.end`` (a ``span.end`` whose start was never seen still creates the row
  from the ``start_ts`` the end event repeats);
* the run row's ``stage`` / ``generation`` / ``last_event_at`` -- folded from
  every event seen, heartbeats included, so "is it progressing?" is one column.

Cost is bounded per scheduler tick (``events_ingest_max_objects_per_tick``);
an object whose size has not changed since the last pass is skipped without a
read (the writer rewrites whole objects, so "same size" is the cheap
unchanged-test), and rows that have been quiet longer than
``events_ingest_idle_seconds`` are skipped. The prefix is derived exactly as
:func:`viva_api.common.events_env.events_s3_prefix` derived it for the task,
so the reader and the writer cannot disagree.

This module reads S3 through the injected :class:`FileService` and never
imports boto3 -- the same discipline the engine keeps -- and it is switchable
off with ``Settings.events_ingest_enabled``.
"""

from __future__ import annotations

import datetime
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from viva_api.common.events_env import events_s3_prefix
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.simulation.models import SimulationEvent, SimulationSpan, SpanTree

if TYPE_CHECKING:
    from viva_api.common.storage.file_service import FileService, ListingItem
    from viva_api.simulation.database_service import DatabaseService
    from viva_api.simulation.models import HpcRun, Simulation

logger = logging.getLogger(__name__)

#: Events folded into the row's progress columns but never stored as rows.
UNSTORED_EVENTS: frozenset[str] = frozenset({"tick"})

#: Span boundaries (plan D1'): the settled dotted names, plus the underscore
#: names the first engine draft emitted, so an older stream still folds.
SPAN_START_EVENTS: frozenset[str] = frozenset({"span.start", "span_start"})
SPAN_END_EVENTS: frozenset[str] = frozenset({"span.end", "span_end"})
SPAN_EVENTS: frozenset[str] = SPAN_START_EVENTS | SPAN_END_EVENTS

#: Component name for the API's own dispatcher-layer events.
DISPATCH_COMPONENT = "viva_api.dispatch"

#: Span names that are the campaign itself -- a stage of "campaign" says nothing.
_STAGE_HIDDEN_SPANS: frozenset[str] = frozenset({"campaign"})


# ---------------------------------------------------------------------------
# Pure parsing / folding (unit-testable without a database)
# ---------------------------------------------------------------------------


def parse_timestamp(value: Any) -> datetime.datetime | None:
    """An event ``ts`` (ISO 8601, ``Z`` or offset) as a naive UTC datetime, or ``None``."""
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(datetime.UTC).replace(tzinfo=None)
    return parsed


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def parse_event_line(line: str, *, expected_trace_id: str | None = None) -> SimulationEvent | None:
    """One JSON line -> :class:`SimulationEvent`, or ``None`` when the line is not a
    usable event (not JSON, not an object, no ``event`` name, or -- when
    ``expected_trace_id`` is given -- stamped with a different trace)."""
    text = line.strip()
    if not text:
        return None
    try:
        raw = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(raw, dict) or not isinstance(raw.get("event"), str):
        return None
    trace_id = raw.get("trace_id")
    if expected_trace_id and isinstance(trace_id, str) and trace_id and trace_id != expected_trace_id:
        return None
    seq = _as_int(raw.get("seq"))
    if seq is None:
        return None
    payload = raw.get("payload")
    tags = raw.get("tags")
    baggage = event_baggage(raw)
    return SimulationEvent(
        seq=seq,
        source=str(raw.get("source") or "unknown"),
        ts=str(raw.get("ts") or ""),
        component=str(raw.get("component") or raw.get("layer") or "process_bigraph"),
        event=raw["event"],
        level=str(raw.get("level") or "info"),
        generation=_as_int(baggage.get("generation")),
        variant=_as_int(baggage.get("variant")),
        lineage_seed=_as_int(baggage.get("lineage_seed")),
        baggage=baggage or None,
        global_time=_as_float(raw.get("global_time")),
        wall_time=_as_float(raw.get("wall_time")),
        span_id=raw.get("span_id") if isinstance(raw.get("span_id"), str) else None,
        parent_span_id=raw.get("parent_span_id") if isinstance(raw.get("parent_span_id"), str) else None,
        payload=payload if isinstance(payload, dict) else None,
        tags=tags if isinstance(tags, dict) else None,
    )


#: Domain identity keys viva-api promotes out of an event's baggage. The engine
#: (process-bigraph >= 1.9) carries them only inside the opaque ``baggage`` map;
#: an older stream that still stamped them at the top level is read the same way.
BAGGAGE_IDENTITY_KEYS: tuple[str, ...] = ("sim_id", "experiment_id", "variant", "lineage_seed", "generation")


def event_baggage(raw: dict[str, Any]) -> dict[str, Any]:
    """The event's baggage map: the ``baggage`` object when present, else the
    identity keys an older engine wrote at the top level (``None`` values dropped)."""
    baggage = raw.get("baggage")
    merged: dict[str, Any] = dict(baggage) if isinstance(baggage, dict) else {}
    for key in BAGGAGE_IDENTITY_KEYS:
        if key not in merged and raw.get(key) is not None:
            merged[key] = raw[key]
    return {k: v for k, v in merged.items() if v is not None}


def parse_event_lines(text: str, *, expected_trace_id: str | None = None) -> tuple[list[SimulationEvent], int]:
    """Every parseable event in ``text`` (in file order) and the count of lines skipped."""
    events: list[SimulationEvent] = []
    bad = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        event = parse_event_line(line, expected_trace_id=expected_trace_id)
        if event is None:
            bad += 1
            continue
        events.append(event)
    return events, bad


def _span_for_event(spans: dict[str, SimulationSpan], event: SimulationEvent) -> SimulationSpan:
    """The span an event describes, created on first sight and updated with the
    event's name/attrs/parent/start (whichever it carries)."""
    payload = event.payload or {}
    name = payload.get("name")
    attrs = payload.get("attrs")
    span = spans.get(event.span_id or "")
    if span is None:
        span = SimulationSpan(
            span_id=event.span_id or "",
            parent_span_id=event.parent_span_id,
            name=str(name or "span"),
            attrs=attrs if isinstance(attrs, dict) else None,
        )
        spans[span.span_id] = span
    if isinstance(name, str) and name:
        span.name = name
    if isinstance(attrs, dict) and attrs:
        span.attrs = attrs
    if event.parent_span_id and not span.parent_span_id:
        span.parent_span_id = event.parent_span_id
    start_ts = payload.get("start_ts")
    if isinstance(start_ts, str) and start_ts and not span.start_ts:
        span.start_ts = start_ts
    return span


def _apply_span_end(span: SimulationSpan, event: SimulationEvent) -> None:
    payload = event.payload or {}
    end_ts = payload.get("end_ts")
    span.end_ts = end_ts if isinstance(end_ts, str) and end_ts else event.ts
    span.duration_s = _as_float(payload.get("duration_s"))
    status = payload.get("status")
    span.status = str(status) if status else ("error" if event.level == "error" else "ok")
    error = payload.get("error")
    if error:
        span.error = error if isinstance(error, str) else json.dumps(error, default=str)


def apply_span_events(spans: dict[str, SimulationSpan], events: list[SimulationEvent]) -> set[str]:
    """Fold ``span.start``/``span.end`` events into ``spans`` (keyed by span id);
    returns the ids that changed. A ``span.end`` without a prior ``span.start``
    still creates the span from the ``start_ts`` the end event repeats."""
    changed: set[str] = set()
    for event in events:
        if event.event not in SPAN_EVENTS or not event.span_id:
            continue
        span = _span_for_event(spans, event)
        if event.event in SPAN_START_EVENTS and not span.start_ts:
            span.start_ts = event.ts
        elif event.event in SPAN_END_EVENTS:
            _apply_span_end(span, event)
        changed.add(span.span_id)
    return changed


def _span_depth(spans: dict[str, SimulationSpan], span: SimulationSpan) -> int:
    seen: set[str] = set()
    depth = 0
    parent = span.parent_span_id
    while parent and parent in spans and parent not in seen:
        seen.add(parent)
        depth += 1
        parent = spans[parent].parent_span_id
    return depth


def open_span_labels(spans: dict[str, SimulationSpan]) -> list[str]:
    """Labels of the spans still open, outermost first (by depth, then start
    time), with the campaign span itself left out -- this is the run's ``stage``."""
    open_spans = [s for s in spans.values() if s.end_ts is None and s.name not in _STAGE_HIDDEN_SPANS]
    open_spans.sort(key=lambda s: (_span_depth(spans, s), s.start_ts or ""))
    return [s.label for s in open_spans]


def stage_from_spans(spans: dict[str, SimulationSpan]) -> str | None:
    labels = open_span_labels(spans)
    return " > ".join(labels) if labels else None


@dataclass
class Progress:
    """What the event stream says about a run's progress, for the row columns."""

    generation: int | None = None
    last_event_at: datetime.datetime | None = None
    newest_ts: str | None = None
    events_seen: int = 0


def fold_progress(events: list[SimulationEvent], prior: Progress | None = None) -> Progress:
    """Fold every event (heartbeats included) into ``generation`` (the newest
    generation any event reports) and ``last_event_at`` (the newest timestamp)."""
    progress = prior or Progress()
    for event in events:
        progress.events_seen += 1
        if event.generation is not None and (progress.generation is None or event.generation > progress.generation):
            progress.generation = event.generation
        ts = parse_timestamp(event.ts)
        if ts is not None and (progress.last_event_at is None or ts > progress.last_event_at):
            progress.last_event_at = ts
            progress.newest_ts = event.ts
    return progress


def build_span_tree(spans: list[SimulationSpan], events: list[SimulationEvent]) -> list[SpanTree]:
    """The forest of spans (roots = spans whose parent is unknown), each node
    carrying its own events, children ordered by start time."""
    by_id = {s.span_id: s for s in spans}
    events_by_span: dict[str | None, list[SimulationEvent]] = {}
    for event in events:
        if event.event in SPAN_EVENTS:
            continue
        events_by_span.setdefault(event.span_id, []).append(event)
    children_of: dict[str | None, list[SimulationSpan]] = {}
    for span in spans:
        parent = span.parent_span_id if span.parent_span_id in by_id else None
        children_of.setdefault(parent, []).append(span)
    for siblings in children_of.values():
        siblings.sort(key=lambda s: (s.start_ts or "", s.span_id))

    def node(span: SimulationSpan) -> SpanTree:
        return SpanTree(
            span=span,
            events=sorted(events_by_span.get(span.span_id, []), key=lambda e: (e.ts, e.source, e.seq)),
            children=[node(child) for child in children_of.get(span.span_id, [])],
        )

    return [node(root) for root in children_of.get(None, [])]


def key_from_events_uri(uri: str, storage_bucket: str | None) -> str | None:
    """The object-key prefix of an events URI for the injected FileService (which
    is bound to ONE bucket), or ``None`` when the URI names a different bucket
    -- reading it would silently return nothing, so say so instead."""
    if not uri.startswith("s3://"):
        return uri.strip("/")
    rest = uri.removeprefix("s3://")
    bucket, _, key = rest.partition("/")
    if storage_bucket and bucket != storage_bucket:
        return None
    return key.strip("/")


# ---------------------------------------------------------------------------
# The ingester
# ---------------------------------------------------------------------------


@dataclass
class IngestResult:
    hpcrun_id: int
    objects_listed: int = 0
    objects_read: int = 0
    events_parsed: int = 0
    events_inserted: int = 0
    bad_lines: int = 0
    spans_changed: int = 0
    stage: str | None = None
    generation: int | None = None
    last_event_at: datetime.datetime | None = None
    skipped_reason: str | None = None
    keys: list[str] = field(default_factory=list)


def resolve_events_prefix(hpc_run: HpcRun, simulation: Simulation | None, settings: Any) -> str | None:
    """The run's events URI: the row's own value, else derived the way the
    dispatcher derived it for the task (same function, same settings)."""
    if hpc_run.events_s3_prefix:
        return hpc_run.events_s3_prefix
    if simulation is None:
        return None
    return events_s3_prefix(settings, str(simulation.experiment_id))


def _events_key_prefix(
    hpc_run: HpcRun, simulation: Simulation | None, settings: Any, result: IngestResult
) -> tuple[str | None, str | None]:
    """``(events URI, object-key prefix)``; on a miss ``result.skipped_reason`` says why."""
    prefix_uri = resolve_events_prefix(hpc_run, simulation, settings)
    if not prefix_uri:
        result.skipped_reason = "no events prefix (S3 events disabled or no work bucket)"
        return None, None
    storage_bucket = getattr(settings, "storage_s3_bucket", None)
    key_prefix = key_from_events_uri(prefix_uri, storage_bucket if isinstance(storage_bucket, str) else None)
    if key_prefix is None:
        result.skipped_reason = f"events prefix {prefix_uri} is not in the file service's bucket"
        return prefix_uri, None
    return prefix_uri, key_prefix


@dataclass
class _Pass:
    progress: Progress = field(default_factory=Progress)
    changed_spans: set[str] = field(default_factory=set)
    cursor_changed: bool = False


async def _ingest_object(
    item: ListingItem,
    *,
    hpc_run: HpcRun,
    trace_id: str,
    file_service: FileService,
    db: DatabaseService,
    cursor: dict[str, int],
    spans: dict[str, SimulationSpan],
    result: IngestResult,
    state: _Pass,
) -> None:
    """Read one events object in full (the writer rewrites whole objects), store
    what is new (idempotent), fold the rest into progress/spans."""
    try:
        content = await file_service.get_file_contents(S3FilePath(s3_path=Path(item.Key)))
    except Exception:
        logger.exception("events: reading %s failed for HpcRun %s", item.Key, hpc_run.database_id)
        return
    result.objects_read += 1
    result.keys.append(item.Key)
    cursor[item.Key] = len(content or b"")
    state.cursor_changed = True
    if not content:
        return
    events, bad = parse_event_lines(content.decode("utf-8", errors="replace"), expected_trace_id=trace_id)
    result.events_parsed += len(events)
    result.bad_lines += bad
    state.progress = fold_progress(events, state.progress)
    storable = [e for e in events if e.event not in UNSTORED_EVENTS]
    if storable:
        result.events_inserted += await db.insert_hpcrun_events(hpc_run.database_id, trace_id, storable)
    state.changed_spans |= apply_span_events(spans, events)


async def _close_open_spans_if_terminal(
    hpc_run: HpcRun, spans: dict[str, SimulationSpan], db: DatabaseService, now: datetime.datetime
) -> None:
    terminal = hpc_run.status is not None and hpc_run.status.is_terminal
    if not terminal or not any(s.end_ts is None for s in spans.values()):
        return
    closed = await db.close_open_hpcrun_spans(hpc_run.database_id, status="unknown")
    if not closed:
        return
    for span in spans.values():
        if span.end_ts is None:
            span.end_ts = now.isoformat(timespec="milliseconds") + "Z"
            span.status = "unknown"


async def ingest_run_events(
    hpc_run: HpcRun,
    simulation: Simulation | None,
    file_service: FileService,
    db: DatabaseService,
    settings: Any,
    *,
    now: datetime.datetime | None = None,
) -> IngestResult:
    """Read whatever the run's tasks have written since the last pass and fold it
    into the database. Safe to call every tick: idempotent inserts, size-based
    skip of unchanged objects, and every failure is logged, never raised past
    the row (the scheduler loop must not stall on one run's bad object)."""
    result = IngestResult(hpcrun_id=hpc_run.database_id)
    now = now or datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    trace_id = hpc_run.trace_id
    if not trace_id:
        result.skipped_reason = "no trace_id on the row"
        return result
    prefix_uri, key_prefix = _events_key_prefix(hpc_run, simulation, settings, result)
    if key_prefix is None:
        return result

    try:
        listing = await file_service.get_listing(S3FilePath(s3_path=Path(key_prefix)))
    except Exception:
        logger.exception("events: listing %s failed for HpcRun %s", key_prefix, hpc_run.database_id)
        result.skipped_reason = "listing failed"
        return result
    objects = sorted((item for item in listing if item.Key.endswith(".jsonl")), key=lambda i: i.Key)
    result.objects_listed = len(objects)
    if not objects:
        result.skipped_reason = "no events objects yet"
        return result

    max_objects = int(getattr(settings, "events_ingest_max_objects_per_tick", 50) or 50)
    cursor: dict[str, int] = dict(await db.get_hpcrun_events_cursor(hpc_run.database_id) or {})
    spans = {s.span_id: s for s in await db.list_hpcrun_spans(hpc_run.database_id)}
    state = _Pass()
    for item in objects:
        if result.objects_read >= max_objects:
            break
        if cursor.get(item.Key) == item.Size:
            continue  # unchanged since the last pass (the writer rewrites whole objects)
        await _ingest_object(
            item,
            hpc_run=hpc_run,
            trace_id=trace_id,
            file_service=file_service,
            db=db,
            cursor=cursor,
            spans=spans,
            result=result,
            state=state,
        )

    if state.changed_spans:
        await db.upsert_hpcrun_spans(hpc_run.database_id, trace_id, [spans[s] for s in sorted(state.changed_spans)])
        result.spans_changed = len(state.changed_spans)
    await _close_open_spans_if_terminal(hpc_run, spans, db, now)

    result.stage = stage_from_spans(spans)
    result.generation = state.progress.generation if state.progress.generation is not None else hpc_run.generation
    result.last_event_at = state.progress.last_event_at
    if state.cursor_changed or state.changed_spans or state.progress.events_seen:
        await db.update_hpcrun_progress(
            hpc_run.database_id,
            stage=result.stage,
            generation=result.generation,
            last_event_at=state.progress.last_event_at,
            events_cursor=cursor if state.cursor_changed else None,
            events_s3_prefix=prefix_uri if not hpc_run.events_s3_prefix else None,
        )
    return result


def is_ingest_candidate(hpc_run: HpcRun, settings: Any, now: datetime.datetime | None = None) -> bool:
    """Whether a run is worth a listing this tick: an active run, or one that went
    terminal within ``events_ingest_terminal_grace_seconds`` (its tasks' final
    flush may land after the head exits). A run that has produced events before
    but none for ``events_ingest_idle_seconds`` is skipped until something else
    changes -- a stalled task shows up as a stale ``last_event_at``, which is the
    signal, not a reason to keep re-listing."""
    now = now or datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    idle_seconds = int(getattr(settings, "events_ingest_idle_seconds", 600) or 600)
    grace_seconds = int(getattr(settings, "events_ingest_terminal_grace_seconds", 900) or 900)
    terminal = hpc_run.status is not None and hpc_run.status.is_terminal
    if terminal:
        end = parse_timestamp(hpc_run.end_time) if hpc_run.end_time else None
        return end is not None and (now - end).total_seconds() <= grace_seconds
    last = parse_timestamp(hpc_run.last_event_at) if hpc_run.last_event_at else None
    return last is None or (now - last).total_seconds() <= idle_seconds
