"""``stage`` must not turn parallel siblings into fake nesting.

``open_span_labels`` emitted one label per open SPAN joined with " > ", which
assumes the open spans form a single nested chain. ParCa fits its conditions
concurrently, so sim 1319 (2026-09-14) reported

    parca.step > parca.fit_condition > parca.fit_condition > ... (x7)

for what is ONE level with seven siblings — every one of them correctly carrying
the same parent span id. The spans were right; the rendering was not, and
``stage`` is the first field anyone reads off ``/status``.
"""

from __future__ import annotations

from viva_api.simulation.event_ingest import open_span_labels, stage_from_spans
from viva_api.simulation.models import SimulationSpan


def _span(
    span_id: str,
    name: str,
    parent: str | None,
    *,
    attrs: dict[str, object] | None = None,
    end: str | None = None,
    start: str = "2026-09-14T02:40:00Z",
) -> SimulationSpan:
    return SimulationSpan(
        span_id=span_id,
        parent_span_id=parent,
        name=name,
        attrs=attrs,
        start_ts=start,
        end_ts=end,
    )


def _index(*spans: SimulationSpan) -> dict[str, SimulationSpan]:
    return {s.span_id: s for s in spans}


def test_parallel_siblings_collapse_instead_of_implying_depth() -> None:
    """The exact sim 1319 shape."""
    spans = _index(
        _span("root", "campaign", None),
        _span("step", "parca.step", "root", attrs={"step": 4}),
        *[
            _span(
                f"c{i}",
                "parca.fit_condition",
                "step",
                attrs={"condition": f"COND-{i}"},
                start=f"2026-09-14T02:4{i}:00Z",
            )
            for i in range(7)
        ],
    )
    assert stage_from_spans(spans) == "parca.step > parca.fit_condition x7"


def test_a_lone_span_keeps_its_full_label() -> None:
    """No count suffix, and identity attrs survive, when nothing is parallel."""
    spans = _index(
        _span("root", "campaign", None),
        _span("task", "task", "root"),
        _span("gen", "generation", "task", attrs={"variant": 0, "generation": 1, "lineage_seed": 0}),
    )
    assert stage_from_spans(spans) == "task > generation[variant=0,generation=1,lineage_seed=0]"


def test_real_nesting_is_still_shown_as_nesting() -> None:
    spans = _index(
        _span("root", "campaign", None),
        _span("a", "parca.step", "root"),
        _span("b", "parca.fit_condition", "a"),
    )
    assert stage_from_spans(spans) == "parca.step > parca.fit_condition"


def test_distinct_names_at_one_depth_are_both_listed() -> None:
    spans = _index(
        _span("root", "campaign", None),
        _span("x", "analysis.name", "root", start="2026-09-14T02:40:00Z"),
        _span("y", "parca.step", "root", start="2026-09-14T02:41:00Z"),
    )
    assert open_span_labels(spans) == ["analysis.name, parca.step"]


def test_closed_spans_and_the_campaign_span_are_excluded() -> None:
    spans = _index(
        _span("root", "campaign", None),
        _span("done", "parca.step", "root", end="2026-09-14T02:41:00Z"),
        _span("live", "parca.fit_condition", "root"),
    )
    assert stage_from_spans(spans) == "parca.fit_condition"


def test_no_open_spans_is_no_stage() -> None:
    spans = _index(_span("root", "campaign", None, end="2026-09-14T02:50:00Z"))
    assert stage_from_spans(spans) is None
