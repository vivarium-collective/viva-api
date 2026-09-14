"""Render a run's spans and events as Chrome Trace Event JSON.

Why this format and not OTLP: it needs **no service and no upload**. The result
opens in `ui.perfetto.dev` (which runs entirely client-side), in speedscope, and
in `chrome://tracing` -- so a trace can be handed to someone as a file, or
attached to a report, without standing up a collector or sending anything
outside the account. That matters on GovCloud, where the SaaS trace viewers are
mostly non-starters on data-residency grounds.

It is also a better fit than a request-tracing UI for what this project actually
produces. Jaeger/X-Ray assume millisecond-to-second spans; here a ParCa step is
13 s, a condition fit is minutes, and a gather is hours. Perfetto is built for
long system traces and handles that range without complaint.

Nothing here touches the engine. ``docs/plan-observability.md`` records that the
engine carries no OTel SDK and that "an OTLP exporter is a later sink"; this is
the consumer side of that same decision -- a pure read-model transform over
``SimulationSpan``/``SimulationEvent``, importable without any AWS or engine
dependency.

Format reference: the Trace Event Format's "Complete" (``ph:"X"``) and "Instant"
(``ph:"i"``) phases. Timestamps are **microseconds** since an arbitrary origin;
we use the earliest span/event in the trace so the result starts at zero and is
comparable across runs.
"""

from __future__ import annotations

import datetime
import json
from typing import Any

from viva_api.simulation.models import SimulationEvent, SimulationSpan

__all__ = [
    "chrome_trace_document",
    "parse_ts",
    "spans_to_trace_events",
]

#: Chrome's timestamps are microseconds.
_US = 1_000_000

#: Lane assignment. Chrome groups by process (``pid``) then thread (``tid``);
#: we map the run's own structure onto that: one "process" per phase
#: (parca / lineage / analysis / other), one "thread" per span source, so
#: concurrent work lands on separate rows instead of overlapping in one lane.
_PHASE_ORDER = ("parca", "lineage", "sim", "analysis")


def parse_ts(value: str | None) -> float | None:
    """ISO-8601 (with or without ``Z``) to a POSIX timestamp, or ``None``.

    The engine writes ``...Z``; Postgres round-trips can drop it. Both parse.
    """
    if not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.UTC)
    return parsed.timestamp()


def _phase_of(span: SimulationSpan) -> str:
    name = (span.name or "").lower()
    for phase in _PHASE_ORDER:
        if name.startswith(phase) or phase in name:
            return phase
    return "other"


def _assign_lanes(intervals: list[tuple[float, float, str]]) -> dict[str, int]:
    """Pack spans into the fewest lanes such that no lane holds an overlap.

    Greedy interval partitioning, per phase: a span reuses the first lane whose
    previous occupant already ended, and only opens a new lane when it genuinely
    overlaps everything so far. That is what makes the result readable -- lane
    count becomes the CONCURRENCY of the phase rather than its span count.

    Keying lanes on identity attributes instead (condition, step, ...) looks
    equivalent and is not: measured on sim 1319, it gave 59 lanes for 59 spans,
    so ParCa's six SEQUENTIAL steps were drawn as six parallel rows. Overlap is
    the property actually being displayed, so overlap is what to partition on.
    """
    lane_free_at: list[float] = []
    assignment: dict[str, int] = {}
    for begin, end, span_id in sorted(intervals, key=lambda item: item[0]):
        placed = None
        for index, free_at in enumerate(lane_free_at):
            if free_at <= begin:
                placed = index
                break
        if placed is None:
            lane_free_at.append(end)
            placed = len(lane_free_at) - 1
        else:
            lane_free_at[placed] = end
        assignment[span_id] = placed + 1
    return assignment


def _trace_window(
    spans: list[SimulationSpan],
    events: list[SimulationEvent] | None,
    now: float | None,
) -> tuple[float, float] | None:
    """``(origin, wall_now)``, or ``None`` when nothing carries a timestamp."""
    starts = [t for t in (parse_ts(s.start_ts) for s in spans) if t is not None]
    starts += [t for t in (parse_ts(e.ts) for e in (events or [])) if t is not None]
    if not starts:
        return None
    origin = min(starts)
    ends = [t for t in (parse_ts(s.end_ts) for s in spans) if t is not None]
    return origin, (now if now is not None else max(ends or [origin]))


def _span_args(span: SimulationSpan, *, open_ended: bool) -> dict[str, Any]:
    args: dict[str, Any] = {"span_id": span.span_id}
    if span.parent_span_id:
        args["parent_span_id"] = span.parent_span_id
    if span.attrs:
        args.update(span.attrs)
    if span.status:
        args["status"] = span.status
    if span.error:
        args["error"] = span.error
    if open_ended:
        args["still_open"] = True
    return args


def _packed_lanes(spans: list[SimulationSpan], wall_now: float) -> dict[str, int]:
    """Lane per span, packed by overlap and partitioned per phase."""
    by_phase: dict[str, list[tuple[float, float, str]]] = {}
    for span in spans:
        begin = parse_ts(span.start_ts)
        if begin is None:
            continue
        raw_end = parse_ts(span.end_ts)
        end = max(wall_now, begin) if raw_end is None else raw_end
        by_phase.setdefault(_phase_of(span), []).append((begin, end, span.span_id))
    lanes: dict[str, int] = {}
    for intervals in by_phase.values():
        lanes.update(_assign_lanes(intervals))
    return lanes


def spans_to_trace_events(
    spans: list[SimulationSpan],
    events: list[SimulationEvent] | None = None,
    *,
    now: float | None = None,
) -> list[dict[str, Any]]:
    """Chrome trace events for ``spans`` (+ optional point events).

    A span still open (``end_ts is None``) is drawn to ``now`` rather than
    dropped -- an unfinished span is usually the interesting one, and omitting
    it is how a viewer ends up showing a run as complete when it is not.
    """
    window = _trace_window(spans, events, now)
    if window is None:
        return []
    origin, wall_now = window
    lanes = _packed_lanes(spans, wall_now)
    phases = {p: i + 1 for i, p in enumerate((*_PHASE_ORDER, "other"))}

    out: list[dict[str, Any]] = [
        {"ph": "M", "pid": pid, "tid": 0, "name": "process_name", "args": {"name": name}}
        for name, pid in phases.items()
    ]

    for span in spans:
        begin = parse_ts(span.start_ts)
        if begin is None:
            continue
        raw_end = parse_ts(span.end_ts)
        open_ended = raw_end is None
        end = max(wall_now, begin) if raw_end is None else raw_end
        out.append({
            "ph": "X",
            "name": span.label,
            "cat": span.name,
            "pid": phases[_phase_of(span)],
            "tid": lanes.get(span.span_id, 1),
            "ts": round((begin - origin) * _US),
            "dur": max(1, round((end - begin) * _US)),
            "args": _span_args(span, open_ended=open_ended),
        })

    for event in events or []:
        when = parse_ts(event.ts)
        if when is None:
            continue
        out.append({
            "ph": "i",
            "s": "t",
            "name": event.event,
            "cat": event.component,
            "pid": phases["other"],
            "tid": 0,
            "ts": round((when - origin) * _US),
            "args": {"level": event.level, **(event.payload or {})},
        })

    out.sort(key=lambda e: (e.get("ts", 0), e.get("ph", "")))
    return out


def chrome_trace_document(
    spans: list[SimulationSpan],
    events: list[SimulationEvent] | None = None,
    *,
    trace_id: str | None = None,
    simulation_id: int | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """The full document: ``{"traceEvents": [...], "otherData": {...}}``.

    ``otherData`` is shown by Perfetto's info panel, so the trace carries its own
    provenance -- which run and which trace id it came from -- instead of
    depending on whatever the file happened to be named.
    """
    other: dict[str, Any] = {"source": "viva-api"}
    if trace_id:
        other["trace_id"] = trace_id
    if simulation_id is not None:
        other["simulation_id"] = str(simulation_id)
    return {
        "traceEvents": spans_to_trace_events(spans, events, now=now),
        "displayTimeUnit": "ms",
        "otherData": other,
    }


def write_chrome_trace(path: str, document: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(document, handle)
