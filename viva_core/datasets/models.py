"""What a dataset row is made of, and the store Protocol core reads and writes through.

Core names a dataset's owner as one ``(owner_kind, owner_id)`` pair -- the owning table's name and
its id as a string, the rule the P4a-1 owner-ref expand (viva-api#790) backfilled -- and the run
that produced it by ``producer_job_id`` and ``trace_id``. The application's ``dataset`` table says
the owner with three producer foreign keys as well; its store translates. Nothing here reaches a
database.
"""

from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Literal, Protocol, TypedDict

from pydantic import BaseModel, Field, JsonValue

#: A JSONB payload as it round-trips through the API and the database: ``attributes``, ``source``,
#: a coordinate. ``JsonValue`` says "arbitrary JSON" precisely, where ``Any`` would say "unchecked".
JsonDict = dict[str, JsonValue]

#: How a dataset row came to exist, recorded as ``attributes["origin"]``: scraped from a run's
#: trace, or found by the reconciliation walk. A walk never overwrites an event-sourced row.
DATASET_ORIGIN_EVENT = "event"
DATASET_ORIGIN_WALK = "walk"
DATASET_ORIGINS: tuple[str, ...] = (DATASET_ORIGIN_EVENT, DATASET_ORIGIN_WALK)

#: Page-size ceiling for dataset listings.
DATASET_LIST_MAX_LIMIT = 200

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
    producer_job_id: int | None  # the job row that wrote it (the trace feeder knows; a walk does not)
    trace_id: str | None


class DatasetWrite(DatasetFields, total=False):
    """A complete upsert bar the owner: what the trace feeder derives from one event."""

    uri: str
    kind: str
    origin: str
    available: bool


class Dataset(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """One consumable file set a run actually wrote, as core serves it (``GET /viva/v1/datasets``).

    Never pre-created: it exists only once a run's trace was scraped or the walk found the object.
    ``owner_kind`` / ``owner_id`` say whose it is; ``producer_job_id`` / ``trace_id`` say which job
    wrote it and under which trace (rows registered before those columns existed carry ``None``, or
    the older ``attributes["hpcrun_id"]``). ``available`` is false once the object is gone; the row
    itself is kept so provenance never dangles."""

    id: int
    uri: str
    kind: str
    owner_kind: str | None = None
    owner_id: str | None = None
    producer_job_id: int | None = None
    trace_id: str | None = None
    view: str | None = None
    display_name: str | None = None
    size_bytes: int | None = None
    sha256: str | None = None
    attributes: JsonDict = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    source: JsonDict | None = None
    available: bool = True
    created_at: str | None = None
    updated_at: str | None = None

    @property
    def origin(self) -> str | None:
        value = self.attributes.get("origin")
        return value if isinstance(value, str) else None


class DatasetPage(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """A page of datasets, newest change first. ``next_offset`` is ``None`` on the last page;
    ``total`` counts every row the same filters match, so a client can say "1-100 of 43,182"."""

    datasets: list[Dataset]
    limit: int
    offset: int
    next_offset: int | None = None
    total: int = 0


class DatasetQuery(TypedDict, total=False):
    """The filters a listing and its count share. Every key is optional; ``None`` means unfiltered."""

    kind: str | None
    view: str | None
    tags: list[str] | None
    available: bool | None
    since: datetime.datetime | None
    uri_prefix: str | None
    q: str | None
    owner_kind: str | None
    owner_id: str | None
    attributes: JsonDict | None
    source: JsonDict | None


class DatasetRecord(Protocol):
    """The little the registry and the walk need to know about a stored row; :class:`Dataset`
    satisfies it, and so may an application's own record."""

    @property
    def id(self) -> int: ...

    @property
    def uri(self) -> str: ...

    @property
    def available(self) -> bool: ...


class DatasetWriter(Protocol):
    """What the feeders write through: the registry and the walk need only this.

    ``upsert`` is keyed on ``uri``; it inserts a new row, updates a changed one, reports an
    identical rewrite as ``unchanged``, and refuses -- ``skipped`` -- a walk's rewrite of an
    event-sourced row. It raises ``ValueError`` when the write itself is unacceptable (an unknown
    kind, an owner kind the store has no column for): the registry counts that as a skip.
    ``list_under`` is every row whose uri starts with a prefix, available or not (the walk
    reconciles against it); ``set_available`` flips one row's availability and is a no-op for an
    unknown id."""

    async def upsert(self, write: DatasetWrite, *, owner: OwnerRef) -> tuple[DatasetRecord, UpsertAction]: ...

    async def list_under(self, uri_prefix: str) -> Sequence[DatasetRecord]: ...

    async def set_available(self, dataset_id: int, available: bool) -> None: ...


class DatasetStore(DatasetWriter, Protocol):
    """Where dataset rows live, reads included -- what ``/viva/v1/datasets`` is served from. The
    application implements it over its database service.

    ``page`` and ``count`` take the same :class:`DatasetQuery`, so a page and its total can never
    disagree; ``attribute_values`` and ``tag_counts`` feed pickers; ``add_tags`` union-merges and
    answers ``None`` for an unknown id."""

    async def get(self, dataset_id: int) -> Dataset | None: ...

    async def page(self, query: DatasetQuery, *, limit: int, offset: int) -> list[Dataset]: ...

    async def count(self, query: DatasetQuery) -> int: ...

    async def add_tags(self, dataset_id: int, tags: list[str]) -> Dataset | None: ...

    async def attribute_values(self, kind: str | None) -> dict[str, list[JsonValue]]: ...

    async def tag_counts(self, kind: str | None) -> dict[str, int]: ...
