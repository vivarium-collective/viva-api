"""The application's half of the trace feeder (core split, ``docs/plan-core.md`` P4a-2).

The registration rules -- payload validation, the coordinate, display names, availability, the
skip-and-count discipline -- moved verbatim to :mod:`viva_core.datasets.registry`. What stays
here is what core cannot know: **which of this application's records produced an artifact**
(:class:`SmsOwnerResolver`, over the run row and the ``analysis`` and ``parca_dataset`` tables),
how a :class:`SimulationEvent` maps onto core's :class:`ArtifactEvent` (the coordinate axes and
the experiment id promoted from baggage), and the store
(:class:`~viva_api.simulation.dataset_store.SmsDatasetStore`).

:func:`register_datasets` keeps the signature the event ingester and the tests call -- run row,
simulation, database service -- and hands core its implementations. This is not a
``sys.modules`` shim: the old function takes the application's types, so the name stays a
thin adapter (the P3d-3 pattern), and the docstring of the rules is now core's.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from viva_api.analysis.models import DATASET_KINDS
from viva_api.simulation.dataset_store import SmsDatasetStore
from viva_api.simulation.models import JobType
from viva_core.datasets.models import OwnerRef
from viva_core.datasets.registry import (
    ARTIFACT_READ,
    ARTIFACT_WRITTEN,
    ArtifactEvent,
    OwnerUnresolved,
    RegistrationResult,
    RunContext,
)
from viva_core.datasets.registry import register_datasets as _register_datasets

if TYPE_CHECKING:
    from viva_api.analysis.models import ExperimentAnalysisDTO
    from viva_api.simulation.database_service import DatabaseService
    from viva_api.simulation.models import HpcRun, Simulation, SimulationEvent

__all__ = [
    "ARTIFACT_READ",
    "ARTIFACT_WRITTEN",
    "RegistrationResult",
    "SmsOwnerResolver",
    "artifact_event",
    "is_artifact_written",
    "register_datasets",
    "run_context",
]


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


def artifact_event(event: SimulationEvent) -> ArtifactEvent:
    """Core's view of one stored event: the payload, the baggage, the span, and the coordinate
    axes this application promotes from baggage (``lineage_seed`` on the wire is ``seed``)."""
    promoted = {"variant": event.variant, "seed": event.lineage_seed, "generation": event.generation}
    baggage = dict(event.baggage or {})
    label = baggage.get("experiment_id")
    return ArtifactEvent(
        seq=event.seq,
        payload=dict(event.payload or {}),
        baggage=baggage,
        span_id=event.span_id,
        coordinate={key: value for key, value in promoted.items() if value is not None},
        label=label if isinstance(label, str) and label else None,
    )


def run_context(hpc_run: HpcRun, simulation: Simulation | None) -> RunContext:
    """The run as core needs it: its id, and -- when it has one -- the simulation the data is OF,
    whose experiment id labels display names and whose tags every row inherits."""
    if simulation is None:
        return RunContext(run_id=hpc_run.database_id, trace_id=hpc_run.trace_id)
    return RunContext(
        run_id=hpc_run.database_id,
        trace_id=hpc_run.trace_id,
        label=simulation.experiment_id,
        tags=list(simulation.tags),
        subject={"kind": "simulation", "ref": str(simulation.database_id), "resolved_id": simulation.database_id},
    )


class SmsOwnerResolver:
    """Which of this application's records produced an artifact (core's ``OwnerResolver``).

    Resolution uses baggage and the run row, never span parentage: spans from worker processes
    can attach to the trace root (OBSERVABILITY.md). A ParCa cache belongs to the simulation's
    ParCa dataset; otherwise an ``analysis_id`` in baggage that names a real analysis wins;
    otherwise the run's own reference. Analysis lookups are cached for the resolver's lifetime
    (one registration pass)."""

    def __init__(self, *, hpc_run: HpcRun, simulation: Simulation | None, db: DatabaseService) -> None:
        self._hpc_run = hpc_run
        self._simulation = simulation
        self._db = db
        self._analyses: dict[int, ExperimentAnalysisDTO | None] = {}

    async def _known_analysis(self, candidates: list[int]) -> int | None:
        """The first candidate id that names a real analysis (lookups cached per pass)."""
        for analysis_id in candidates:
            if analysis_id not in self._analyses:
                try:
                    self._analyses[analysis_id] = await self._db.get_analysis(database_id=analysis_id)
                except Exception:
                    self._analyses[analysis_id] = None
            if self._analyses[analysis_id] is not None:
                return analysis_id
        return None

    async def resolve(self, kind: str, event: ArtifactEvent) -> OwnerRef:
        hpc_run, simulation = self._hpc_run, self._simulation
        if kind == "parca-cache":
            if simulation is not None and simulation.parca_dataset_id:
                return {"owner_kind": "parca_dataset", "owner_id": str(simulation.parca_dataset_id)}
            raise OwnerUnresolved(f"seq {event.seq}: a parca-cache needs the run's simulation and its ParCa dataset")

        candidates = [_as_int(event.baggage.get("analysis_id"))]
        if hpc_run.job_type == JobType.ANALYSIS:
            candidates.append(hpc_run.ref_id)
        analysis_id = await self._known_analysis([c for c in candidates if c is not None])
        if analysis_id is not None:
            return {"owner_kind": "analysis", "owner_id": str(analysis_id)}

        if hpc_run.job_type == JobType.SIMULATION:
            return {"owner_kind": "simulation", "owner_id": str(hpc_run.ref_id)}
        if simulation is not None:
            return {"owner_kind": "simulation", "owner_id": str(simulation.database_id)}
        raise OwnerUnresolved(f"seq {event.seq}: no producer (no analysis, simulation or ParCa dataset resolves)")


async def register_datasets(
    events: list[SimulationEvent],
    *,
    hpc_run: HpcRun,
    simulation: Simulation | None,
    db: DatabaseService,
) -> RegistrationResult:
    """Upsert one ``dataset`` row per ``artifact.written`` event, in stream order: core's
    :func:`viva_core.datasets.registry.register_datasets` with this application's owner
    resolver, store and kind vocabulary handed in. Events that are not ``artifact.written``
    are ignored, as before."""
    return await _register_datasets(
        [artifact_event(event) for event in events if is_artifact_written(event)],
        run=run_context(hpc_run, simulation),
        owner_resolver=SmsOwnerResolver(hpc_run=hpc_run, simulation=simulation, db=db),
        store=SmsDatasetStore(db),
        kinds=DATASET_KINDS,
    )
