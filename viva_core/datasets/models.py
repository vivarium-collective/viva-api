"""What a dataset row is made of, and the store Protocol the registry writes through.

These are the shapes the application's ``dataset`` table already has (viva-api#661), minus the
application's three producer foreign keys: core names an owner as one ``(owner_kind, owner_id)``
pair -- the owning table's name and its id as a string, the rule the P4a-1 owner-ref expand
backfills -- and the application's store translates. Nothing here reaches a database.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, Protocol, TypedDict

from pydantic import JsonValue

#: A JSONB payload as it round-trips through the API and the database: ``attributes``, ``source``,
#: a coordinate. ``JsonValue`` says "arbitrary JSON" precisely, where ``Any`` would say "unchecked".
JsonDict = dict[str, JsonValue]

#: How a dataset row came to exist, recorded as ``attributes["origin"]``: scraped from a run's
#: trace, or found by the reconciliation walk. A walk never overwrites an event-sourced row.
DATASET_ORIGIN_EVENT = "event"
DATASET_ORIGIN_WALK = "walk"
DATASET_ORIGINS: tuple[str, ...] = (DATASET_ORIGIN_EVENT, DATASET_ORIGIN_WALK)

#: What one ``upsert`` did. ``skipped``: the store refused to let a walk rewrite an event-sourced row.
UpsertAction = Literal["inserted", "updated", "unchanged", "skipped"]


class OwnerRef(TypedDict):
    """Who produced a dataset: the owning record's kind and id.

    ``owner_kind`` is the owning table's name in the application (``simulation``, ``analysis``,
    ...) and ``owner_id`` its id as a string. A plain string, not an enum: an application adds a
    kind by writing rows, never by changing core."""

    owner_kind: str
    owner_id: str


class DatasetFields(TypedDict, total=False):
    """What a registration contributes to an upsert: everything but uri, kind, origin and owner."""

    view: str | None
    display_name: str | None
    size_bytes: int | None
    sha256: str | None
    attributes: JsonDict
    tags: list[str]
    source: JsonDict | None


class DatasetWrite(DatasetFields, total=False):
    """A complete upsert bar the owner: what the trace feeder derives from one event."""

    uri: str
    kind: str
    origin: str
    available: bool


class DatasetRecord(Protocol):
    """The little the registry needs to know about a stored row. The application's DTO satisfies
    it structurally; core's own record model arrives with the read side (plan P4a-2, slice 3)."""

    @property
    def database_id(self) -> int: ...

    @property
    def uri(self) -> str: ...

    @property
    def available(self) -> bool: ...


class DatasetStore(Protocol):
    """Where dataset rows live. The application implements it over its database service.

    ``upsert`` is keyed on ``uri``: it inserts a new row, updates a changed one, reports an
    identical rewrite as ``unchanged``, and refuses -- ``skipped`` -- a walk's rewrite of an
    event-sourced row. It raises ``ValueError`` when the write itself is unacceptable (an unknown
    kind, an owner kind the store has no column for): the registry counts that as a skip.

    ``list_under`` is every row whose uri starts with a prefix, available or not (the walk
    reconciles against it); ``set_available`` flips one row's availability and is a no-op for
    an unknown id."""

    async def upsert(self, write: DatasetWrite, *, owner: OwnerRef) -> tuple[DatasetRecord, UpsertAction]: ...

    async def list_under(self, uri_prefix: str) -> Sequence[DatasetRecord]: ...

    async def set_available(self, dataset_id: int, available: bool) -> None: ...
