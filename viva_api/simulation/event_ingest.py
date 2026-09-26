"""The application's half of the event ingester (core split, ``docs/plan-core.md`` P4a-2, slice 4).

Parsing, span folding, progress, the per-tick loop and its cursor discipline moved verbatim to
:mod:`viva_core.events.ingest`; the event and span models to :mod:`viva_core.events.models`. What
stays here is what core cannot know: **the run row** (an :class:`HpcRun` read once into core's
``IngestRun``, with the events prefix the dispatcher would derive for its simulation), **where the
rows live** (:class:`SmsEventStore` over :class:`DatabaseService`: ``hpcrun_event``, ``hpcrun_span``
and the progress columns of ``hpcrun``), and **the trace feeder** (:class:`SmsArtifactRegistrar`
over :func:`viva_api.simulation.dataset_registry.register_datasets`).

:func:`ingest_run_events` and :func:`is_ingest_candidate` keep the signatures the scheduler and the
tests call, and the pure functions are re-exported, so every importer -- the handlers, the CLI, the
database service's own use of ``parse_timestamp`` -- is unchanged. Not a ``sys.modules`` shim: the
entry points take the application's types (the P3d-3 pattern).
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from viva_api.common.events_env import events_s3_prefix
from viva_api.simulation.dataset_registry import register_datasets
from viva_core.datasets.registry import RegistrationResult
from viva_core.events.ingest import (
    BAGGAGE_IDENTITY_KEYS,
    SPAN_END_EVENTS,
    SPAN_EVENTS,
    SPAN_START_EVENTS,
    UNSTORED_EVENTS,
    UNSTORED_LEVELS,
    IngestResult,
    IngestRun,
    Progress,
    _as_int,
    apply_span_events,
    build_span_tree,
    event_baggage,
    fold_progress,
    key_from_events_uri,
    open_span_labels,
    parse_event_line,
    parse_event_lines,
    parse_timestamp,
    stage_from_spans,
    storable_events,
)
from viva_core.events.ingest import ingest_run_events as _ingest_run_events
from viva_core.events.ingest import is_ingest_candidate as _is_ingest_candidate

if TYPE_CHECKING:
    from viva_api.simulation.database_service import DatabaseService
    from viva_api.simulation.models import HpcRun, Simulation
    from viva_core.events.models import SimulationEvent, SimulationSpan
    from viva_core.storage.file_service import FileService

__all__ = [
    "BAGGAGE_IDENTITY_KEYS",
    "DISPATCH_COMPONENT",
    "SPAN_END_EVENTS",
    "SPAN_EVENTS",
    "SPAN_START_EVENTS",
    "UNSTORED_EVENTS",
    "UNSTORED_LEVELS",
    "IngestResult",
    "Progress",
    "SmsArtifactRegistrar",
    "SmsEventStore",
    "_as_int",
    "apply_span_events",
    "build_span_tree",
    "event_baggage",
    "fold_progress",
    "ingest_run",
    "ingest_run_events",
    "is_ingest_candidate",
    "key_from_events_uri",
    "open_span_labels",
    "parse_event_line",
    "parse_event_lines",
    "parse_timestamp",
    "resolve_events_prefix",
    "stage_from_spans",
    "storable_events",
]

#: Component name for the API's own dispatcher-layer events.
DISPATCH_COMPONENT = "viva_api.dispatch"


def ingest_run(hpc_run: HpcRun, simulation: Simulation | None, settings: object) -> IngestRun:
    """Core's view of a run row: its id, trace, the prefix it recorded and -- for its simulation --
    the one the dispatcher derives (same function, same settings), and its liveness."""
    return IngestRun(
        run_id=hpc_run.database_id,
        trace_id=hpc_run.trace_id,
        events_prefix=hpc_run.events_s3_prefix,
        default_events_prefix=events_s3_prefix(settings, str(simulation.experiment_id)) if simulation else None,
        terminal=hpc_run.status is not None and hpc_run.status.is_terminal,
        end_time=hpc_run.end_time,
        last_event_at=hpc_run.last_event_at,
        generation=hpc_run.generation,
    )


def resolve_events_prefix(hpc_run: HpcRun, simulation: Simulation | None, settings: object) -> str | None:
    """The run's events URI: the row's own value, else derived the way the
    dispatcher derived it for the task (same function, same settings)."""
    return ingest_run(hpc_run, simulation, settings).events_prefix or (
        events_s3_prefix(settings, str(simulation.experiment_id)) if simulation is not None else None
    )


class SmsEventStore:
    """Core's ``EventStore`` over the application's :class:`DatabaseService`."""

    def __init__(self, db: DatabaseService) -> None:
        self._db = db

    async def events_cursor(self, run_id: int) -> dict[str, int] | None:
        return await self._db.get_hpcrun_events_cursor(run_id)

    async def list_spans(self, run_id: int) -> list[SimulationSpan]:
        return await self._db.list_hpcrun_spans(run_id)

    async def insert_events(self, run_id: int, trace_id: str, events: list[SimulationEvent]) -> int:
        return await self._db.insert_hpcrun_events(run_id, trace_id, events)

    async def upsert_spans(self, run_id: int, trace_id: str, spans: list[SimulationSpan]) -> None:
        await self._db.upsert_hpcrun_spans(run_id, trace_id, spans)

    async def close_open_spans(self, run_id: int, status: str) -> int:
        return await self._db.close_open_hpcrun_spans(run_id, status=status)

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
        await self._db.update_hpcrun_progress(
            run_id,
            stage=stage,
            generation=generation,
            last_event_at=last_event_at,
            events_cursor=events_cursor,
            events_s3_prefix=events_prefix,
        )


class SmsArtifactRegistrar:
    """Core's ``ArtifactRegistrar``: this application's trace feeder for one run."""

    def __init__(self, *, hpc_run: HpcRun, simulation: Simulation | None, db: DatabaseService) -> None:
        self._hpc_run = hpc_run
        self._simulation = simulation
        self._db = db

    async def register(self, events: list[SimulationEvent]) -> RegistrationResult:
        # Looked up at call time on purpose: tests patch ``event_ingest.register_datasets``.
        return await register_datasets(events, hpc_run=self._hpc_run, simulation=self._simulation, db=self._db)


async def ingest_run_events(
    hpc_run: HpcRun,
    simulation: Simulation | None,
    file_service: FileService,
    db: DatabaseService,
    settings: object,
    *,
    now: datetime.datetime | None = None,
) -> IngestResult:
    """Read whatever the run's tasks have written since the last pass and fold it into the
    database: core's :func:`viva_core.events.ingest.ingest_run_events` with this application's
    run view, store and trace feeder handed in."""
    return await _ingest_run_events(
        ingest_run(hpc_run, simulation, settings),
        file_service=file_service,
        store=SmsEventStore(db),
        registrar=SmsArtifactRegistrar(hpc_run=hpc_run, simulation=simulation, db=db),
        settings=settings,
        now=now,
    )


def is_ingest_candidate(hpc_run: HpcRun, settings: object, now: datetime.datetime | None = None) -> bool:
    """Whether a run is worth a listing this tick (core's rule over the row's liveness)."""
    return _is_ingest_candidate(ingest_run(hpc_run, None, settings), settings, now)
