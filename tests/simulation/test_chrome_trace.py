"""Chrome Trace Event export — the offline, no-service trace view.

Shapes here are taken from sim 1319 (2026-09-14), the first campaign on an image
carrying the ParCa/gather spans: nine ``parca.step`` spans with concurrent
``parca.fit_condition`` children, then lineage, then the gather.
"""

from __future__ import annotations

from typing import Any

from viva_api.simulation.chrome_trace import (
    chrome_trace_document,
    parse_ts,
    spans_to_trace_events,
)
from viva_api.simulation.models import SimulationEvent, SimulationSpan


def _span(
    span_id: str,
    name: str,
    start: str,
    end: str | None = None,
    *,
    parent: str | None = None,
    attrs: dict[str, Any] | None = None,
    status: str | None = None,
    error: str | None = None,
) -> SimulationSpan:
    return SimulationSpan(
        span_id=span_id,
        parent_span_id=parent,
        name=name,
        attrs=attrs,
        start_ts=start,
        end_ts=end,
        status=status,
        error=error,
    )


def _complete(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in events if e.get("ph") == "X"]


def test_parse_ts_accepts_both_the_engine_and_postgres_forms() -> None:
    """The engine writes ...Z; a Postgres round-trip can drop it."""
    a = parse_ts("2026-09-14T02:40:00.000Z")
    b = parse_ts("2026-09-14 02:40:00")
    assert a is not None and b is not None and a == b
    assert parse_ts(None) is None
    assert parse_ts("not a timestamp") is None


def test_a_span_becomes_one_complete_event_in_microseconds() -> None:
    spans = [
        _span(
            "s1",
            "parca.step",
            "2026-09-14T02:40:00Z",
            "2026-09-14T02:40:13.39Z",
            attrs={"step": 1, "name": "InitializeStep"},
            status="ok",
        )
    ]
    got = _complete(spans_to_trace_events(spans))
    assert len(got) == 1
    ev = got[0]
    assert ev["ts"] == 0  # earliest span defines the origin
    assert ev["dur"] == 13_390_000  # 13.39 s in microseconds
    assert ev["args"]["step"] == 1
    assert ev["args"]["status"] == "ok"


def test_the_origin_is_the_earliest_span_so_traces_start_at_zero() -> None:
    spans = [
        _span("late", "b", "2026-09-14T02:41:00Z", "2026-09-14T02:41:10Z"),
        _span("early", "a", "2026-09-14T02:40:00Z", "2026-09-14T02:40:05Z"),
    ]
    by_name = {e["name"]: e for e in _complete(spans_to_trace_events(spans))}
    assert by_name["a"]["ts"] == 0
    assert by_name["late" if "late" in by_name else "b"]["ts"] == 60_000_000


def test_concurrent_siblings_get_separate_lanes() -> None:
    """The sim 1319 shape: seven fit_conditions under one step, all overlapping.

    If they shared a tid the viewer would stack them into one unreadable row --
    the same structural mistake the `stage` renderer made by chaining them.
    """
    spans = [
        _span("step", "parca.step", "2026-09-14T02:40:00Z", "2026-09-14T02:45:00Z", attrs={"step": 4}),
        *[
            _span(
                f"c{i}",
                "parca.fit_condition",
                "2026-09-14T02:40:10Z",
                "2026-09-14T02:44:00Z",
                parent="step",
                attrs={"condition": f"COND-{i}"},
            )
            for i in range(7)
        ],
    ]
    fits = [e for e in _complete(spans_to_trace_events(spans)) if e["cat"] == "parca.fit_condition"]
    assert len(fits) == 7
    assert len({e["tid"] for e in fits}) == 7, "siblings collapsed into one lane"


def test_an_open_span_is_drawn_to_now_not_dropped() -> None:
    """An unfinished span is usually the interesting one."""
    spans = [
        _span("open", "analysis.group", "2026-09-14T02:40:00Z", None, attrs={"name": "ptools_rna_multigeneration"})
    ]
    got = _complete(spans_to_trace_events(spans, now=parse_ts("2026-09-14T06:40:00Z")))
    assert len(got) == 1
    assert got[0]["dur"] == 4 * 3600 * 1_000_000  # four hours
    assert got[0]["args"]["still_open"] is True


def test_an_error_span_carries_its_status_and_message() -> None:
    spans = [
        _span(
            "bad",
            "parca.fit_condition",
            "2026-09-14T02:40:00Z",
            "2026-09-14T02:41:00Z",
            status="error",
            error="Exception: Fitting did not converge",
        )
    ]
    args = _complete(spans_to_trace_events(spans))[0]["args"]
    assert args["status"] == "error"
    assert "did not converge" in args["error"]


def test_point_events_become_instant_events() -> None:
    spans = [_span("s", "parca.step", "2026-09-14T02:40:00Z", "2026-09-14T02:40:30Z")]
    events = [
        SimulationEvent(
            seq=1,
            source="x",
            ts="2026-09-14T02:40:10Z",
            component="v2ecoli.lineage",
            event="lineage.division",
            level="info",
            payload={"t_division": 42.0},
        ),
    ]
    instants = [e for e in spans_to_trace_events(spans, events) if e.get("ph") == "i"]
    assert len(instants) == 1
    assert instants[0]["name"] == "lineage.division"
    assert instants[0]["ts"] == 10_000_000
    assert instants[0]["args"]["t_division"] == 42.0


def test_phases_become_named_processes() -> None:
    spans = [
        _span("p", "parca.step", "2026-09-14T02:40:00Z", "2026-09-14T02:40:10Z"),
        _span("a", "analysis.group", "2026-09-14T02:41:00Z", "2026-09-14T02:41:10Z"),
    ]
    out = spans_to_trace_events(spans)
    names = {m["args"]["name"] for m in out if m.get("ph") == "M"}
    assert {"parca", "analysis"} <= names
    pids = {e["cat"]: e["pid"] for e in _complete(out)}
    assert pids["parca.step"] != pids["analysis.group"]


def test_the_document_carries_its_own_provenance() -> None:
    doc = chrome_trace_document(
        [_span("s", "parca.step", "2026-09-14T02:40:00Z", "2026-09-14T02:40:01Z")],
        trace_id="ba540aa33b5eb4d358b2e4f3c798ac1a",
        simulation_id=1319,
    )
    assert doc["displayTimeUnit"] == "ms"
    assert doc["otherData"]["trace_id"] == "ba540aa33b5eb4d358b2e4f3c798ac1a"
    assert doc["otherData"]["simulation_id"] == "1319"
    assert doc["traceEvents"]


def test_no_timestamps_at_all_is_an_empty_trace_not_a_crash() -> None:
    assert spans_to_trace_events([_span("s", "x", "")]) == []
    assert spans_to_trace_events([]) == []


def test_sequential_spans_share_a_lane() -> None:
    """Lane count must be the phase's CONCURRENCY, not its span count.

    Keying lanes on identity attributes gave 59 lanes for 59 spans on sim 1319,
    drawing ParCa's six sequential steps as six parallel rows. Overlap is the
    property being displayed, so overlap is what is partitioned on.
    """
    spans = [
        _span(f"s{i}", "parca.step", f"2026-09-14T02:4{i}:00Z", f"2026-09-14T02:4{i}:30Z", attrs={"step": i})
        for i in range(5)
    ]
    tids = {e["tid"] for e in _complete(spans_to_trace_events(spans))}
    assert tids == {1}, f"sequential steps were split across lanes: {tids}"


def test_lane_count_equals_peak_concurrency() -> None:
    """Three overlapping + one later that can reuse a freed lane."""
    spans = [
        _span("a", "parca.fit_condition", "2026-09-14T02:40:00Z", "2026-09-14T02:40:30Z"),
        _span("b", "parca.fit_condition", "2026-09-14T02:40:05Z", "2026-09-14T02:40:30Z"),
        _span("c", "parca.fit_condition", "2026-09-14T02:40:10Z", "2026-09-14T02:40:30Z"),
        _span("d", "parca.fit_condition", "2026-09-14T02:41:00Z", "2026-09-14T02:41:10Z"),
    ]
    tids = {e["tid"] for e in _complete(spans_to_trace_events(spans))}
    assert tids == {1, 2, 3}, f"expected 3 lanes for peak concurrency 3, got {sorted(tids)}"


def test_phases_are_packed_independently() -> None:
    """Concurrency in one phase must not push another phase's rows around."""
    spans = [
        _span("p1", "parca.step", "2026-09-14T02:40:00Z", "2026-09-14T02:45:00Z"),
        _span("p2", "parca.step", "2026-09-14T02:40:00Z", "2026-09-14T02:45:00Z"),
        _span("a1", "analysis.group", "2026-09-14T02:41:00Z", "2026-09-14T02:42:00Z"),
    ]
    out = _complete(spans_to_trace_events(spans))
    analysis = next(e for e in out if e["cat"] == "analysis.group")
    assert analysis["tid"] == 1, "analysis lane was displaced by parca concurrency"
