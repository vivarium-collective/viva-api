"""A run's events and spans, as the ingester materialises them (observability plan D1).

These are the application's ``SimulationEvent`` / ``SimulationSpan`` / ``SpanTree`` moved verbatim
(P4a-2, slice 4): the names are kept because they are OpenAPI schema components of the application's
documents, and the open maps are ``dict[str, object]`` -- the same JSON schema ``dict[str, Any]``
rendered, without the ``Any`` (D12).
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class SimulationEvent(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """One structured event of a run (an event row; observability plan D1).

    The identity block mirrors the engine's JSON-lines schema; infrastructure
    identifiers (Batch job ids, log streams) live in ``tags``/``payload``, never
    in the core fields.
    """

    cursor: int | None = None  # the stored row id; pass back as ``after`` to page forward across sources
    seq: int
    source: str
    ts: str  # ISO 8601 UTC
    component: str  # who emitted it: "process_bigraph", the application's own layers, ...
    event: str
    level: str = "info"
    # Coordinate axes, promoted from the engine event's opaque ``baggage`` map (W3C baggage
    # semantics): the engine never names these keys itself; the application does, so its API and
    # CLI present them first-class.
    generation: int | None = None
    variant: int | None = None
    lineage_seed: int | None = None
    baggage: dict[str, object] | None = None  # the full baggage map as the task saw it
    global_time: float | None = None
    wall_time: float | None = None
    span_id: str | None = None
    parent_span_id: str | None = None
    payload: dict[str, object] | None = None
    tags: dict[str, object] | None = None


class SimulationSpan(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """A node of a run's trace tree (a span row), materialised from ``span.start`` /
    ``span.end`` events. ``end_ts`` is ``None`` while open; a span still open when the run
    goes terminal is closed as ``status='unknown'``."""

    span_id: str
    parent_span_id: str | None = None
    name: str
    attrs: dict[str, object] | None = None
    start_ts: str | None = None
    end_ts: str | None = None
    duration_s: float | None = None
    status: str | None = None  # ok | error | unknown | None while open
    error: str | None = None

    @property
    def label(self) -> str:
        """``name[key=value,...]`` -- the human form used for ``stage`` and the tree."""
        attrs = self.attrs or {}
        shown = {k: v for k, v in attrs.items() if k in ("variant", "lineage_seed", "seed", "generation")}
        if not shown:
            return self.name
        return f"{self.name}[{','.join(f'{k}={v}' for k, v in shown.items())}]"


class SpanTree(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """The span tree with each span's own events attached (events whose ``span_id`` matches;
    ``tick`` heartbeats are never stored, so they never appear here)."""

    span: SimulationSpan
    events: list[SimulationEvent] = Field(default_factory=list)
    children: list[SpanTree] = Field(default_factory=list)
