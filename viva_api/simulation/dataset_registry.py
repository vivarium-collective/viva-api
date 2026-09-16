"""Register the files a run wrote as ``dataset`` rows, from its trace (plan §3, §5).

This is the **trace feeder** of docs/plan-data-provenance.md: a producer emits one
``artifact.written`` event per consumable file set, inside the span that wrote it, and
the event ingester hands every such event it parses to :func:`register_datasets`.

Contract (docs/plan-data-provenance.md §3)::

    event:   "artifact.written"   (info level -- debug is stream-only and never reaches here)
    payload: uri         s3://bucket/key  (an object, or a prefix for multi-file kinds)
             kind        parquet | parca-cache | ptools-analysis | analysis | figure | report | other
             name, view, bytes, sha256        optional
             attributes  {} open map (variant, seed, generation, agent, protocol, n_tp, ...)
             error       optional: the file was NOT produced, and why
    baggage: sim_id, experiment_id, variant, lineage_seed, generation, analysis_id

Rules:

* A dataset row is born here or from the reconciliation walk -- never pre-created
  (§2a). Rows written here carry ``attributes.origin = "event"`` and a walk never
  overwrites them (``DatabaseService.upsert_dataset``).
* **Producer resolution uses baggage and the run row, never span parentage**: spans
  from worker processes can attach to the trace root (OBSERVABILITY.md). A ParCa cache
  belongs to the simulation's ParCa dataset; otherwise an ``analysis_id`` in baggage
  that names a real analysis wins; otherwise the run's own reference.
* An invalid payload or an unresolvable producer is **skipped and counted**, never
  raised: one malformed event must not stop a run's other files from registering.
* ``error`` registers the row with ``available = false``: "this coordinate was
  expected and failed" is provenance, and a later successful event for the same
  ``uri`` flips it back.
* ``artifact.read`` is left to the event store in slice 1 (consumer derivation is a
  later step).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from viva_api.analysis.models import (
    DATASET_KINDS,
    DATASET_ORIGIN_EVENT,
    DatasetWrite,
    JsonDict,
    ProducerRef,
)
from viva_api.simulation.models import JobType

if TYPE_CHECKING:
    from viva_api.analysis.models import ExperimentAnalysisDTO
    from viva_api.simulation.database_service import DatabaseService
    from viva_api.simulation.models import HpcRun, Simulation, SimulationEvent

logger = logging.getLogger(__name__)

ARTIFACT_WRITTEN = "artifact.written"
ARTIFACT_READ = "artifact.read"

#: Coordinate axes copied into ``source.coordinate``. ``seed`` / ``generation`` /
#: ``variant`` fall back to the event's baggage (``lineage_seed`` on the wire).
_COORDINATE_KEYS: tuple[str, ...] = ("variant", "seed", "generation", "agent", "protocol")

#: How many skip reasons to keep for the log line (the count is always exact).
_MAX_REASONS = 5


class _Skip(Exception):
    """This event cannot become a dataset row; the message says why."""


@dataclass
class RegistrationResult:
    registered: int = 0  # rows inserted or changed
    unchanged: int = 0  # already registered exactly like this
    skipped: int = 0  # invalid payload, unresolvable producer, or refused by the store
    reasons: list[str] = field(default_factory=list)

    def skip(self, reason: str) -> None:
        self.skipped += 1
        if len(self.reasons) < _MAX_REASONS:
            self.reasons.append(reason)


def is_artifact_written(event: SimulationEvent) -> bool:
    return event.event == ARTIFACT_WRITTEN


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _str_or_none(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


# ---------------------------------------------------------------------------
# payload -> dataset fields (pure)
# ---------------------------------------------------------------------------


def _validated(event: SimulationEvent) -> tuple[str, str, JsonDict, JsonDict]:
    """``(uri, kind, attributes copy, payload)``, or ``_Skip`` naming what is wrong."""
    payload = event.payload or {}
    uri = payload.get("uri")
    if not isinstance(uri, str) or not uri.startswith("s3://") or len(uri) <= len("s3://"):
        raise _Skip(f"seq {event.seq}: uri must be an s3:// URI, got {uri!r}")
    kind = payload.get("kind")
    if not isinstance(kind, str) or kind not in DATASET_KINDS:
        raise _Skip(f"seq {event.seq}: unknown kind {kind!r}")
    attributes = payload.get("attributes", {})
    if not isinstance(attributes, dict):
        raise _Skip(f"seq {event.seq}: attributes must be an object")
    return uri, kind, dict(attributes), payload


def _coordinate(attributes: JsonDict, event: SimulationEvent) -> JsonDict:
    """The event's coordinate: explicit attributes first, then the promoted baggage."""
    fallback: JsonDict = {"variant": event.variant, "seed": event.lineage_seed, "generation": event.generation}
    coordinate: JsonDict = {}
    for key in _COORDINATE_KEYS:
        value = attributes.get(key, fallback.get(key))
        if value is not None:
            coordinate[key] = value
    return coordinate


def _display_name(
    explicit: object, *, experiment_id: object, view: str | None, name: str | None, kind: str, protocol: object
) -> str:
    if isinstance(explicit, str) and explicit:
        return explicit
    label = view or name or kind
    return " · ".join(str(part) for part in (experiment_id, label, protocol) if part)


def _tags(simulation: Simulation | None, extra: object) -> list[str]:
    tags = list(simulation.tags) if simulation is not None else []
    if isinstance(extra, list):
        tags.extend(str(tag) for tag in extra if tag)
    return tags


def _source(simulation: Simulation | None, coordinate: JsonDict) -> JsonDict | None:
    if simulation is None:
        return None
    source: JsonDict = {
        "kind": "simulation",
        "ref": str(simulation.database_id),
        "resolved_id": simulation.database_id,
    }
    if coordinate:
        source["coordinate"] = coordinate
    return source


def dataset_fields(event: SimulationEvent, *, hpc_run: HpcRun, simulation: Simulation | None) -> DatasetWrite:
    """Everything ``upsert_dataset`` needs except the producer. Pure; raises ``_Skip``."""
    uri, kind, attributes, payload = _validated(event)
    extra_tags = attributes.pop("tags", None)
    explicit_name = attributes.pop("display_name", None) or payload.get("display_name")
    name = _str_or_none(payload.get("name"))
    view = _str_or_none(payload.get("view"))
    if name is not None:
        attributes.setdefault("name", name)

    coordinate = _coordinate(attributes, event)
    for key in ("variant", "seed", "generation"):
        if key in coordinate:
            attributes.setdefault(key, coordinate[key])
    # Where in the trace this file was recorded: enough to walk back to the span.
    attributes["hpcrun_id"] = hpc_run.database_id
    if event.span_id:
        attributes["span_id"] = event.span_id
    error = payload.get("error")
    if error:
        attributes["error"] = str(error)

    experiment_id = simulation.experiment_id if simulation is not None else (event.baggage or {}).get("experiment_id")
    return {
        "uri": uri,
        "kind": kind,
        "origin": DATASET_ORIGIN_EVENT,
        "view": view,
        "display_name": _display_name(
            explicit_name,
            experiment_id=experiment_id,
            view=view,
            name=name,
            kind=kind,
            protocol=coordinate.get("protocol"),
        ),
        "size_bytes": _as_int(payload.get("bytes")),
        "sha256": _str_or_none(payload.get("sha256")),
        "attributes": attributes,
        "tags": _tags(simulation, extra_tags),
        "source": _source(simulation, coordinate),
        "available": not error,
    }


# ---------------------------------------------------------------------------
# producer resolution
# ---------------------------------------------------------------------------


async def _known_analysis(
    candidates: list[int], db: DatabaseService, analyses: dict[int, ExperimentAnalysisDTO | None]
) -> int | None:
    """The first candidate id that names a real analysis (lookups cached per pass)."""
    for analysis_id in candidates:
        if analysis_id not in analyses:
            try:
                analyses[analysis_id] = await db.get_analysis(database_id=analysis_id)
            except Exception:
                analyses[analysis_id] = None
        if analyses[analysis_id] is not None:
            return analysis_id
    return None


async def _producer(
    kind: str,
    event: SimulationEvent,
    *,
    hpc_run: HpcRun,
    simulation: Simulation | None,
    db: DatabaseService,
    analyses: dict[int, ExperimentAnalysisDTO | None],
) -> ProducerRef:
    """The producer foreign key for one artifact, or ``_Skip`` when none resolves."""
    if kind == "parca-cache":
        if simulation is not None and simulation.parca_dataset_id:
            return {"parca_dataset_id": simulation.parca_dataset_id}
        raise _Skip(f"seq {event.seq}: a parca-cache needs the run's simulation and its ParCa dataset")

    candidates = [_as_int((event.baggage or {}).get("analysis_id"))]
    if hpc_run.job_type == JobType.ANALYSIS:
        candidates.append(hpc_run.ref_id)
    analysis_id = await _known_analysis([c for c in candidates if c is not None], db, analyses)
    if analysis_id is not None:
        return {"analysis_id": analysis_id}

    if hpc_run.job_type == JobType.SIMULATION:
        return {"simulation_id": hpc_run.ref_id}
    if simulation is not None:
        return {"simulation_id": simulation.database_id}
    raise _Skip(f"seq {event.seq}: no producer (no analysis, simulation or ParCa dataset resolves)")


async def register_datasets(
    events: list[SimulationEvent],
    *,
    hpc_run: HpcRun,
    simulation: Simulation | None,
    db: DatabaseService,
) -> RegistrationResult:
    """Upsert one ``dataset`` row per ``artifact.written`` event, in stream order.

    Database errors other than a per-row refusal propagate, so the caller can retry
    the pass; everything attributable to one bad event is skipped and counted.
    """
    result = RegistrationResult()
    analyses: dict[int, ExperimentAnalysisDTO | None] = {}
    for event in events:
        if not is_artifact_written(event):
            continue
        try:
            fields = dataset_fields(event, hpc_run=hpc_run, simulation=simulation)
            producer = await _producer(
                fields["kind"], event, hpc_run=hpc_run, simulation=simulation, db=db, analyses=analyses
            )
        except _Skip as skip:
            result.skip(str(skip))
            continue
        try:
            _dto, action = await db.upsert_dataset(**fields, **producer)
        except ValueError as refused:
            result.skip(f"seq {event.seq}: {refused}")
            continue
        if action in ("inserted", "updated"):
            result.registered += 1
        else:
            result.unchanged += 1
    return result
