"""Register the files a run wrote as dataset rows, from its trace (the trace feeder).

A producer emits one ``artifact.written`` event per consumable file set, inside the span that
wrote it, and whoever ingests the run's events hands every such event to
:func:`register_datasets`.

Contract (``docs/plan-data-provenance.md`` §3)::

    event:   "artifact.written"   (info level -- debug is stream-only and never reaches here)
    payload: uri         s3://bucket/key  (an object, or a prefix for multi-file kinds)
             kind        one of the application's dataset kinds
             name, view, bytes, sha256        optional
             attributes  {} open map (variant, seed, generation, agent, protocol, n_tp, ...);
                         ``lineage_seed`` is read as ``seed`` when ``seed`` is absent
             error       optional: the file was NOT produced, and why

Rules:

* A dataset row is born here or from the reconciliation walk -- never pre-created. Rows
  written here carry ``attributes.origin = "event"`` and a walk never overwrites them (the
  store's ``upsert``).
* **Owner resolution is the application's** (:class:`OwnerResolver`): it uses the event's
  baggage and the run row, never span parentage -- spans from worker processes can attach to
  the trace root.
* An invalid payload or an unresolvable owner is **skipped and counted**, never raised: one
  malformed event must not stop a run's other files from registering.
* ``error`` registers the row with ``available = false``: "this coordinate was expected and
  failed" is provenance, and a later successful event for the same ``uri`` flips it back.
* ``artifact.read`` is left to the event store (consumer derivation is a later step).

This is the application's ``dataset_registry`` moved verbatim (P4a-2), with the run, the event
and the store as core's own shapes: :class:`RunContext`, :class:`ArtifactEvent` and
:class:`~viva_core.datasets.models.DatasetWriter`.
"""

from __future__ import annotations

import logging
from collections.abc import Container, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from viva_core.datasets.models import (
    DATASET_ORIGIN_EVENT,
    DatasetWrite,
    DatasetWriter,
    JsonDict,
    OwnerRef,
)

logger = logging.getLogger(__name__)

ARTIFACT_WRITTEN = "artifact.written"
ARTIFACT_READ = "artifact.read"

#: Coordinate axes copied into ``source.coordinate``. ``seed`` / ``generation`` / ``variant``
#: fall back to the axes the application promoted from the event's baggage.
_COORDINATE_KEYS: tuple[str, ...] = ("variant", "seed", "generation", "agent", "protocol")

#: Attribute spellings a producer may use for a coordinate axis, read only when the axis's own
#: key is absent: history partitions name the seed ``lineage_seed`` (as the baggage does), and
#: the registry records it as ``seed`` either way (P4d). The spelling itself stays in attributes.
_COORDINATE_ALIASES: dict[str, str] = {"seed": "lineage_seed"}

#: How many skip reasons to keep for the log line (the count is always exact).
_MAX_REASONS = 5


class _Skip(Exception):
    """This event cannot become a dataset row; the message says why."""


class OwnerUnresolved(Exception):
    """No owner resolves for an artifact; the message says why. Raised by an :class:`OwnerResolver`,
    counted as a skip by the registry."""


@dataclass(frozen=True)
class ArtifactEvent:
    """One ``artifact.written`` event as the registry reads it.

    ``coordinate`` and ``label`` are what the application promoted from the event's opaque
    baggage (the engine never names those keys; the application does): the coordinate axes an
    explicit ``attributes`` value does not override, and the run's label for display names."""

    seq: int
    payload: dict[str, object]  # as read off the wire: every field is checked before use
    baggage: dict[str, object] = field(default_factory=dict)
    span_id: str | None = None
    coordinate: JsonDict = field(default_factory=dict)
    label: str | None = None


@dataclass(frozen=True)
class RunContext:
    """The run whose trace the events came from, as the registry needs it.

    ``subject`` is what the run's data is OF, without a coordinate (the registry adds the
    event's) -- ``None`` when the run has no subject the application wants recorded."""

    run_id: int
    trace_id: str | None = None
    label: str | None = None
    tags: Sequence[str] = ()
    subject: JsonDict | None = None


class OwnerResolver(Protocol):
    """Who produced one artifact. The application answers from the event's baggage and its own
    tables; it raises :class:`OwnerUnresolved` when nothing resolves."""

    async def resolve(self, kind: str, event: ArtifactEvent) -> OwnerRef: ...


@dataclass
class RegistrationResult:
    registered: int = 0  # rows inserted or changed
    unchanged: int = 0  # already registered exactly like this
    skipped: int = 0  # invalid payload, unresolvable owner, or refused by the store
    reasons: list[str] = field(default_factory=list)

    def skip(self, reason: str) -> None:
        self.skipped += 1
        if len(self.reasons) < _MAX_REASONS:
            self.reasons.append(reason)


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


def _validated(event: ArtifactEvent, kinds: Container[str]) -> tuple[str, str, JsonDict, dict[str, object]]:
    """``(uri, kind, attributes copy, payload)``, or ``_Skip`` naming what is wrong."""
    payload = event.payload or {}
    uri = payload.get("uri")
    if not isinstance(uri, str) or not uri.startswith("s3://") or len(uri) <= len("s3://"):
        raise _Skip(f"seq {event.seq}: uri must be an s3:// URI, got {uri!r}")
    kind = payload.get("kind")
    if not isinstance(kind, str) or kind not in kinds:
        raise _Skip(f"seq {event.seq}: unknown kind {kind!r}")
    attributes = payload.get("attributes", {})
    if not isinstance(attributes, dict):
        raise _Skip(f"seq {event.seq}: attributes must be an object")
    return uri, kind, dict(attributes), payload


def _coordinate(attributes: JsonDict, event: ArtifactEvent) -> JsonDict:
    """The event's coordinate: explicit attributes first (an axis's own key, then its alias),
    then the promoted baggage."""
    fallback = event.coordinate
    coordinate: JsonDict = {}
    for key in _COORDINATE_KEYS:
        alias = _COORDINATE_ALIASES.get(key)
        if key in attributes:
            value = attributes[key]
        elif alias is not None and alias in attributes:
            value = attributes[alias]
        else:
            value = fallback.get(key)
        if value is not None:
            coordinate[key] = value
    return coordinate


def _display_name(
    explicit: object, *, label: object, view: str | None, name: str | None, kind: str, protocol: object
) -> str:
    if isinstance(explicit, str) and explicit:
        return explicit
    part = view or name or kind
    return " · ".join(str(item) for item in (label, part, protocol) if item)


def _tags(run: RunContext, extra: object) -> list[str]:
    tags = list(run.tags)
    if isinstance(extra, list):
        tags.extend(str(tag) for tag in extra if tag)
    return tags


def _source(run: RunContext, coordinate: JsonDict) -> JsonDict | None:
    if run.subject is None:
        return None
    source: JsonDict = dict(run.subject)
    if coordinate:
        source["coordinate"] = coordinate
    return source


def dataset_fields(event: ArtifactEvent, *, run: RunContext, kinds: Container[str]) -> DatasetWrite:
    """Everything the store's ``upsert`` needs except the owner. Pure; raises ``_Skip``."""
    uri, kind, attributes, payload = _validated(event, kinds)
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
    # Where in the trace this file was recorded: the job and trace are columns (P4a-1); the span,
    # which has no column, rides in the attributes -- enough to walk back to it.
    if event.span_id:
        attributes["span_id"] = event.span_id
    error = payload.get("error")
    if error:
        attributes["error"] = str(error)

    return {
        "uri": uri,
        "kind": kind,
        "origin": DATASET_ORIGIN_EVENT,
        "view": view,
        "display_name": _display_name(
            explicit_name,
            label=run.label if run.label is not None else event.label,
            view=view,
            name=name,
            kind=kind,
            protocol=coordinate.get("protocol"),
        ),
        "size_bytes": _as_int(payload.get("bytes")),
        "sha256": _str_or_none(payload.get("sha256")),
        "attributes": attributes,
        "tags": _tags(run, extra_tags),
        "source": _source(run, coordinate),
        "producer_job_id": run.run_id,
        "trace_id": run.trace_id,
        "available": not error,
    }


# ---------------------------------------------------------------------------
# registration
# ---------------------------------------------------------------------------


async def register_datasets(
    events: Iterable[ArtifactEvent],
    *,
    run: RunContext,
    owner_resolver: OwnerResolver,
    store: DatasetWriter,
    kinds: Container[str],
) -> RegistrationResult:
    """Upsert one dataset row per ``artifact.written`` event, in stream order.

    ``kinds`` is the application's dataset vocabulary; an event naming another kind is
    skipped. Store errors other than a per-row refusal propagate, so the caller can retry the
    pass; everything attributable to one bad event is skipped and counted.
    """
    result = RegistrationResult()
    for event in events:
        try:
            fields = dataset_fields(event, run=run, kinds=kinds)
            owner = await owner_resolver.resolve(fields["kind"], event)
        except (_Skip, OwnerUnresolved) as skip:
            result.skip(str(skip))
            continue
        try:
            _record, action = await store.upsert(fields, owner=owner)
        except ValueError as refused:
            result.skip(f"seq {event.seq}: {refused}")
            continue
        if action in ("inserted", "updated"):
            result.registered += 1
        else:
            result.unchanged += 1
    return result
