"""Debug-level events are folded and spanned, but never stored as rows.

Found on sim 1318 (2026-09-14), the first campaign run on an image carrying the
gather/ParCa spans: 946 of the first 1000 stored ``hpcrun_event`` rows were
``lineage.debug`` -- one per SIMULATED TIMESTEP, 3.6 rows/s for a single lineage,
~13k rows/hour/lineage. A 10x10 campaign would have written hundreds of thousands
of rows of telemetry nobody queries.

``UNSTORED_EVENTS`` did not catch it because that set names ``tick`` and nothing
else. Level is the durable contract, so the bound is structural rather than a
list someone has to remember to extend.
"""

from __future__ import annotations

import itertools
import json
from types import SimpleNamespace
from typing import Any

from viva_api.simulation.event_ingest import (
    UNSTORED_EVENTS,
    UNSTORED_LEVELS,
    parse_event_lines,
    storable_events,
)

TRACE = "ba540aa33b5eb4d358b2e4f3c798ac1a"


_SEQ = itertools.count(1)


def _line(event: str, level: str, **payload: object) -> str:
    # ``seq`` is REQUIRED -- parse_event_line drops any line without it, which is
    # how the first draft of this file "failed": the fixture was wrong, not the
    # filter.
    return json.dumps({
        "v": 1,
        "ts": "2026-09-14T00:40:00.000Z",
        "seq": next(_SEQ),
        "event": event,
        "level": level,
        "component": "v2ecoli.lineage",
        "trace_id": TRACE,
        "payload": payload,
    })


def _parse(*lines: str) -> list[Any]:
    events, bad = parse_event_lines("\n".join(lines), expected_trace_id=TRACE)
    assert bad == 0
    return events


def test_lineage_debug_is_not_stored_by_default() -> None:
    """The exact shape that flooded sim 1318."""
    events = _parse(
        _line("lineage.debug", "debug", t=1.0, dry_mass=379.08, divided=False),
        _line("lineage.debug", "debug", t=2.0, dry_mass=379.13, divided=False),
        _line("run.start", "info", n_steps=45),
    )
    kept = storable_events(events, SimpleNamespace())
    assert [e.event for e in kept] == ["run.start"]


def test_tick_is_still_excluded_by_name() -> None:
    """The original exclusion must survive -- tick is debug AND named."""
    events = _parse(_line("tick", "debug", ticks=133), _line("run.end", "info", status="ok"))
    kept = storable_events(events, SimpleNamespace())
    assert [e.event for e in kept] == ["run.end"]


def test_info_and_error_events_are_always_stored() -> None:
    events = _parse(
        _line("span.start", "info", name="parca.step"),
        _line("span.end", "info", name="parca.step", duration_s=13.38),
        _line("process.exception", "error", exc_type="MemoryError"),
    )
    kept = storable_events(events, SimpleNamespace())
    assert len(kept) == 3


def test_the_setting_can_turn_debug_storage_back_on() -> None:
    """Escape hatch for debugging the ingester itself."""
    events = _parse(_line("lineage.debug", "debug", t=1.0))
    assert storable_events(events, SimpleNamespace(events_ingest_store_debug=True))
    assert not storable_events(events, SimpleNamespace(events_ingest_store_debug=False))


def test_tick_stays_excluded_even_with_debug_storage_on() -> None:
    """The two exclusions are independent; the name-based one is unconditional."""
    events = _parse(_line("tick", "debug", ticks=5))
    assert not storable_events(events, SimpleNamespace(events_ingest_store_debug=True))


def test_level_matching_is_case_insensitive_and_survives_a_missing_level() -> None:
    events = _parse(_line("lineage.debug", "DEBUG", t=1.0))
    assert not storable_events(events, SimpleNamespace())
    # a stream with no level at all must not be silently dropped
    raw = json.dumps({"v": 1, "ts": "2026-09-14T00:40:00.000Z", "seq": 99, "event": "x", "trace_id": TRACE})
    events, _ = parse_event_lines(raw, expected_trace_id=TRACE)
    assert storable_events(events, SimpleNamespace())


def test_the_constants_stay_coherent() -> None:
    assert "tick" in UNSTORED_EVENTS
    assert "debug" in UNSTORED_LEVELS
    assert "info" not in UNSTORED_LEVELS and "error" not in UNSTORED_LEVELS
