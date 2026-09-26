"""The application's ``dataset`` table behind core's :class:`~viva_core.datasets.models.DatasetStore`.

Core names a dataset's owner as one ``(owner_kind, owner_id)`` pair; this table says the same with
three producer foreign keys AND, since the P4a-1 owner-ref expand (viva-api#790), with the pair
itself. The translation lives here and nowhere else: on a write, ``owner_kind`` (the owning table's
name -- ``simulation``, ``parca_dataset``, ``analysis``) selects the producer column the row has
always carried, and the database service dual-writes the pair; on a read, the pair comes straight
off its columns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import JsonValue

from viva_api.analysis.models import DATASET_LIST_MAX_LIMIT, DatasetDTO, ProducerRef
from viva_core.datasets.models import Dataset, DatasetQuery, DatasetWrite, OwnerRef, UpsertAction

if TYPE_CHECKING:
    from viva_api.simulation.database_service import DatabaseService

#: The producer column each owner kind names. The owning table's name, not a column name, is the
#: contract; the column is this table's way of storing it.
PRODUCER_COLUMN_BY_OWNER_KIND: dict[str, str] = {
    "simulation": "simulation_id",
    "parca_dataset": "parca_dataset_id",
    "analysis": "analysis_id",
}
_OWNER_KIND_BY_PRODUCER_COLUMN = {column: kind for kind, column in PRODUCER_COLUMN_BY_OWNER_KIND.items()}


def producer_ref(owner: OwnerRef) -> dict[str, int]:
    """``{<producer column>: id}`` for an owner, or ``ValueError`` when this table has no column for
    its kind (the store refuses the write; the registry counts it)."""
    column = PRODUCER_COLUMN_BY_OWNER_KIND.get(owner["owner_kind"])
    if column is None:
        raise ValueError(f"no producer column for owner kind {owner['owner_kind']!r}")
    try:
        return {column: int(owner["owner_id"])}
    except ValueError as e:
        raise ValueError(f"owner id {owner['owner_id']!r} is not an integer id") from e


def owner_ref(producer: ProducerRef) -> OwnerRef:
    """The inverse: the ONE producer a ``ProducerRef`` names, as core's owner. The walk's callers
    (the CD2 importer) still speak producer columns."""
    named = [(column, value) for column, value in producer.items() if value is not None]
    if len(named) != 1:
        raise ValueError(f"a producer names exactly one column, got {sorted(producer)}")
    column, value = named[0]
    return {"owner_kind": _OWNER_KIND_BY_PRODUCER_COLUMN[column], "owner_id": str(value)}


def dataset_record(dto: DatasetDTO) -> Dataset:
    """Core's record of a row this application read: the same row, the owner off its own columns."""
    return Dataset(
        id=dto.database_id,
        uri=dto.uri,
        kind=dto.kind,
        owner_kind=dto.owner_kind,
        owner_id=dto.owner_id,
        producer_job_id=dto.producer_job_id,
        trace_id=dto.trace_id,
        view=dto.view,
        display_name=dto.display_name,
        size_bytes=dto.size_bytes,
        sha256=dto.sha256,
        attributes=dict(dto.attributes),
        tags=list(dto.tags),
        source=dict(dto.source) if dto.source is not None else None,
        available=dto.available,
        created_at=dto.created_at,
        updated_at=dto.updated_at,
    )


class SmsDatasetStore:
    """``DatasetStore`` over the application's :class:`DatabaseService`."""

    def __init__(self, db: DatabaseService) -> None:
        self._db = db

    # ---- writes (the registry and the walk) ----

    async def upsert(self, write: DatasetWrite, *, owner: OwnerRef) -> tuple[Dataset, UpsertAction]:
        dto, action = await self._db.upsert_dataset(**write, **producer_ref(owner))
        return dataset_record(dto), action

    async def list_under(self, uri_prefix: str) -> list[Dataset]:
        """Every row under a prefix, available or not, paged through the listing's ceiling."""
        rows: list[Dataset] = []
        offset = 0
        while True:
            page = await self._db.list_datasets(
                uri_prefix=uri_prefix, available=None, limit=DATASET_LIST_MAX_LIMIT, offset=offset
            )
            rows.extend(dataset_record(dto) for dto in page)
            if len(page) < DATASET_LIST_MAX_LIMIT:
                return rows
            offset += len(page)

    async def set_available(self, dataset_id: int, available: bool) -> None:
        await self._db.set_dataset_available(dataset_id, available)

    # ---- reads (/viva/v1/datasets) ----

    async def get(self, dataset_id: int) -> Dataset | None:
        dto = await self._db.get_dataset(dataset_id)
        return dataset_record(dto) if dto is not None else None

    async def page(self, query: DatasetQuery, *, limit: int, offset: int) -> list[Dataset]:
        page = await self._db.list_datasets(**query, limit=limit, offset=offset)
        return [dataset_record(dto) for dto in page]

    async def count(self, query: DatasetQuery) -> int:
        return await self._db.count_datasets(**query)

    async def add_tags(self, dataset_id: int, tags: list[str]) -> Dataset | None:
        try:
            dto = await self._db.add_dataset_tags(dataset_id, tags)
        except RuntimeError:  # the database service's "not found"
            return None
        return dataset_record(dto)

    async def attribute_values(self, kind: str | None) -> dict[str, list[JsonValue]]:
        return await self._db.list_dataset_attribute_values(kind=kind)

    async def tag_counts(self, kind: str | None) -> dict[str, int]:
        return await self._db.list_dataset_tags(kind=kind)
