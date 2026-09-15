"""Dataset registry reads behind ``/datasets`` and the per-run dataset routes.

docs/plan-data-provenance.md §7. Everything here reads: rows are written by the trace feeder
(:mod:`viva_api.simulation.dataset_registry`) and the reconciliation walk
(:mod:`viva_api.simulation.dataset_walk`); the only API write is union-merging tags.
"""

import datetime
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from fastapi import Query

from viva_api.analysis.models import (
    DATASET_KINDS,
    DATASET_LIST_MAX_LIMIT,
    DatasetDTO,
    DatasetListDTO,
    DatasetProducerDTO,
    DatasetProvenanceDTO,
)
from viva_api.common.handlers.analyses import _figure_content_type
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service import FileService
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.dataset_walk import split_s3_uri
from viva_api.simulation.models import HpcRun, JobType, SimulationSpan

#: Kinds whose ``uri`` names a store (a parquet prefix, a ParCa cache) rather than one object
#: small enough to hand back through the API; read those from ``uri`` with storage credentials.
UNSERVED_KINDS: tuple[str, ...] = ("parquet", "parca-cache")

#: ``source`` filter shorthands (``sim:1002``) and the ``ProvenanceRef.kind`` each means.
_SOURCE_KINDS = {"sim": "simulation", "simulation": "simulation", "analysis": "analysis", "task": "task"}

#: Spans read to find the one a dataset was written in (the events tree's cap).
_MAX_SPAN_ROWS = 5_000

Availability = Literal["true", "false", "any"]
_AVAILABILITY: dict[str, bool | None] = {"true": True, "false": False, "any": None}


class DatasetQueryError(ValueError):
    """A malformed dataset or analysis filter (400)."""


class DatasetNotFoundError(LookupError):
    """An unknown dataset, or one whose object is gone (404)."""


class DatasetNotServableError(RuntimeError):
    """A dataset whose bytes this API does not serve (409)."""


# ---------------------------------------------------------------------------
# query parsing
# ---------------------------------------------------------------------------


def parse_tags(tag: str | None) -> list[str] | None:
    """Comma-separated tags, blanks dropped; ``None`` when there are none."""
    tags = [part.strip() for part in (tag or "").split(",") if part.strip()]
    return tags or None


def naive_utc(moment: datetime.datetime | None) -> datetime.datetime | None:
    """``updated_at`` columns are naive UTC: convert an aware ``since``, take a naive one as UTC."""
    if moment is None or moment.tzinfo is None:
        return moment
    return moment.astimezone(datetime.UTC).replace(tzinfo=None)


def parse_attribute_value(raw: str) -> Any:
    """A filter value typed the way JSONB containment compares it.

    A JSON scalar keeps its type (``0`` is an integer, ``true`` a boolean, ``"0"`` a string);
    anything that is not JSON (``000``, ``ptools_rna``) is a string."""
    try:
        value = json.loads(raw)
    except ValueError:
        return raw
    return value if isinstance(value, str | int | float | bool) else raw


def parse_attribute_filters(pairs: list[str], query_items: list[tuple[str, str]]) -> dict[str, Any] | None:
    """``attr=<key>=<value>`` pairs plus ``attr.<key>=<value>`` parameters, as one containment filter."""
    attributes: dict[str, Any] = {}
    for pair in pairs:
        key, sep, raw = pair.partition("=")
        if not sep or not key.strip():
            raise DatasetQueryError(f"attr filter {pair!r} is not <key>=<value>")
        attributes[key.strip()] = parse_attribute_value(raw)
    for name, raw in query_items:
        key = name.removeprefix("attr.")
        if key != name and key:
            attributes[key] = parse_attribute_value(raw)
    return attributes or None


def parse_source_filter(source: str | None) -> dict[str, Any] | None:
    """A ``source`` filter: ``sim:1002`` / ``analysis:7`` / ``task:3``, or a JSON ProvenanceRef fragment."""
    text = (source or "").strip()
    if not text:
        return None
    if text.startswith("{"):
        try:
            value = json.loads(text)
        except ValueError as e:
            raise DatasetQueryError(f"source {source!r} is not valid JSON") from e
        if not isinstance(value, dict):
            raise DatasetQueryError(f"source {source!r} is not a JSON object")
        return value
    prefix, sep, ref = text.partition(":")
    kind = _SOURCE_KINDS.get(prefix.strip().lower())
    if not sep or kind is None or not ref.strip():
        raise DatasetQueryError(
            f"source {source!r} is not <kind>:<id> (kind one of {', '.join(sorted(_SOURCE_KINDS))}) or a JSON object"
        )
    return {"kind": kind, "ref": ref.strip()}


def check_kind(kind: str | None) -> None:
    if kind is not None and kind not in DATASET_KINDS:
        raise DatasetQueryError(f"unknown dataset kind {kind!r}; expected one of {', '.join(DATASET_KINDS)}")


@dataclass(frozen=True)
class DatasetListParams:
    """The filters every dataset listing shares, parsed from the query string."""

    kind: str | None
    view: str | None
    tags: list[str] | None
    available: bool | None
    since: datetime.datetime | None
    limit: int
    offset: int

    def filters(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "view": self.view,
            "tags": self.tags,
            "available": self.available,
            "since": self.since,
        }


def dataset_list_params(
    kind: str | None = Query(default=None, description=f"Dataset kind: {', '.join(DATASET_KINDS)}."),
    view: str | None = Query(default=None, description="Logical view, e.g. 'ptools_rna'."),
    tag: str | None = Query(default=None, description="Comma-separated tags; a dataset must carry all of them."),
    available: Availability = Query(
        default="true",
        description="'true' (default): datasets whose object exists; 'false': those whose object is gone; 'any'.",
    ),
    since: datetime.datetime | None = Query(default=None, description="Only datasets changed at or after this time."),
    limit: int = Query(default=100, ge=1, le=DATASET_LIST_MAX_LIMIT, description="Page size."),
    offset: int = Query(default=0, ge=0, description="Rows to skip (pagination, with limit)."),
) -> DatasetListParams:
    """FastAPI dependency: the query parameters every dataset listing takes."""
    return DatasetListParams(
        kind=kind,
        view=view,
        tags=parse_tags(tag),
        available=_AVAILABILITY[available],
        since=naive_utc(since),
        limit=limit,
        offset=offset,
    )


# ---------------------------------------------------------------------------
# reads
# ---------------------------------------------------------------------------


async def list_page(db: DatabaseService, params: DatasetListParams, **scope: Any) -> DatasetListDTO:
    """One page of datasets, newest change first: the shared filters plus a route's own
    (a producer id, attributes, source). ``next_offset`` is ``None`` once a page comes back short."""
    check_kind(params.kind)
    datasets = await db.list_datasets(**params.filters(), **scope, limit=params.limit, offset=params.offset)
    next_offset = params.offset + params.limit if len(datasets) == params.limit else None
    return DatasetListDTO(datasets=datasets, limit=params.limit, offset=params.offset, next_offset=next_offset)


async def require_dataset(db: DatabaseService, dataset_id: int) -> DatasetDTO:
    dataset = await db.get_dataset(dataset_id)
    if dataset is None:
        raise DatasetNotFoundError(f"Dataset {dataset_id} not found")
    return dataset


async def open_content(
    db: DatabaseService, file_service: FileService, dataset_id: int, *, storage_bucket: str | None
) -> tuple[DatasetDTO, AsyncIterator[bytes]]:
    """A dataset's bytes, streamed from the API's storage bucket.

    ``DatasetNotServableError`` for a store kind, a non-object uri or another bucket;
    ``DatasetNotFoundError`` for an unknown id or a gone object. Whether the object exists is
    settled before the first byte, so the caller can still answer 404."""
    dataset = await require_dataset(db, dataset_id)
    if dataset.kind in UNSERVED_KINDS:
        raise DatasetNotServableError(
            f"Dataset {dataset_id} is a {dataset.kind} store and is not served through the API; read {dataset.uri}"
        )
    bucket, key = split_s3_uri(dataset.uri)
    if not dataset.uri.startswith("s3://") or not key:
        raise DatasetNotServableError(f"Dataset {dataset_id} does not name an S3 object: {dataset.uri}")
    if storage_bucket and bucket != storage_bucket:
        raise DatasetNotServableError(f"Dataset {dataset_id} is in bucket {bucket!r}, not this API's storage bucket")
    stream = await file_service.open_file_stream(S3FilePath(s3_path=Path(key)))
    if stream is None:
        raise DatasetNotFoundError(f"Dataset {dataset_id}'s object is gone: {dataset.uri}")
    return dataset, stream


def content_headers(dataset: DatasetDTO) -> tuple[str, dict[str, str]]:
    """Media type and headers for serving a dataset inline."""
    name = dataset.uri.rstrip("/").rsplit("/", 1)[-1]
    media_type = "application/json" if dataset.kind == "report" else _figure_content_type(name)
    return media_type, {"Content-Disposition": f'inline; filename="{name}"', "X-Content-Type-Options": "nosniff"}


async def provenance(db: DatabaseService, dataset_id: int) -> DatasetProvenanceDTO:
    """One hop back from a dataset: the run that wrote it, the span it was written in, and the
    registered datasets that run's declared ``source`` points at (matched on ``uri``)."""
    dataset = await require_dataset(db, dataset_id)
    producer, run = await _producer(db, dataset)
    span = await _span(db, run, dataset.attributes.get("span_id"))
    inputs = await _inputs(db, producer.source if producer is not None else None, exclude=dataset.database_id)
    return DatasetProvenanceDTO(dataset=dataset, producer=producer, span=span, inputs=inputs)


def _with_run(producer: DatasetProducerDTO, run: HpcRun | None) -> DatasetProducerDTO:
    if run is None:
        return producer
    run_status = run.status.value if run.status is not None else None
    return producer.model_copy(
        update={
            "hpcrun_id": run.database_id,
            "trace_id": run.trace_id,
            "correlation_id": run.correlation_id,
            "status": producer.status or run_status,
        }
    )


async def _analysis_producer(db: DatabaseService, analysis_id: int) -> tuple[DatasetProducerDTO | None, HpcRun | None]:
    try:
        analysis = await db.get_analysis(database_id=analysis_id)
    except RuntimeError:
        return None, None
    run = await db.get_hpcrun_by_ref(analysis_id, JobType.ANALYSIS)  # none for a walk-created run row
    producer = DatasetProducerDTO(
        kind="analysis",
        id=analysis_id,
        name=analysis.name,
        status=analysis.status.value if analysis.status is not None else None,
        source=analysis.source,
        tags=list(analysis.tags),
    )
    return _with_run(producer, run), run


async def _simulation_producer(
    db: DatabaseService, simulation_id: int
) -> tuple[DatasetProducerDTO | None, HpcRun | None]:
    simulation = await db.get_simulation(simulation_id)
    run = await db.get_hpcrun_by_ref(simulation_id, JobType.SIMULATION)
    producer = DatasetProducerDTO(
        kind="simulation",
        id=simulation_id,
        name=simulation.experiment_id if simulation is not None else None,
        tags=list(simulation.tags) if simulation is not None else [],
    )
    return _with_run(producer, run), run


async def _producer(db: DatabaseService, dataset: DatasetDTO) -> tuple[DatasetProducerDTO | None, HpcRun | None]:
    if dataset.analysis_id is not None:
        return await _analysis_producer(db, dataset.analysis_id)
    if dataset.simulation_id is not None:
        return await _simulation_producer(db, dataset.simulation_id)
    if dataset.parca_dataset_id is not None:
        run = await db.get_hpcrun_by_ref(dataset.parca_dataset_id, JobType.PARCA)
        return _with_run(DatasetProducerDTO(kind="parca", id=dataset.parca_dataset_id), run), run
    return None, None


async def _span(db: DatabaseService, run: HpcRun | None, span_id: Any) -> SimulationSpan | None:
    if run is None or not isinstance(span_id, str) or not span_id:
        return None
    for span in await db.list_hpcrun_spans(run.database_id, limit=_MAX_SPAN_ROWS):
        if span.span_id == span_id:
            return span
    return None


async def _inputs(db: DatabaseService, source: Any, *, exclude: int) -> list[DatasetDTO]:
    """Registered datasets a declared source resolves to: one ProvenanceRef or a list (slice 2's
    ``task.inputs``), matched on ``uri``. A reference to a whole run (``sim:1002``) has no uri and
    resolves to nothing here; the producer's ``source`` already names it."""
    refs = source if isinstance(source, list) else [source]
    inputs: dict[int, DatasetDTO] = {}
    for ref in refs:
        uri = ref.get("uri") if isinstance(ref, dict) else None
        if not isinstance(uri, str) or not uri:
            continue
        found = await db.get_dataset_by_uri(uri)
        if found is not None and found.database_id != exclude:
            inputs[found.database_id] = found
    return list(inputs.values())
