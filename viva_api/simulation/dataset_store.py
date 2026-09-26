"""The application's ``dataset`` table behind core's :class:`~viva_core.datasets.models.DatasetStore`.

Core names a dataset's owner as one ``(owner_kind, owner_id)`` pair; this table says the same with
three producer foreign keys. The translation lives here and nowhere else: ``owner_kind`` is the
owning table's name (``simulation``, ``parca_dataset``, ``analysis`` -- the P4a-1 rule, so the
owner-ref columns of migration ``a4b6c8d0e2f4`` agree with these rows the day they land) and the
column it selects is the one the row has always carried.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from viva_core.datasets.models import DatasetWrite, OwnerRef, UpsertAction

if TYPE_CHECKING:
    from viva_api.analysis.models import DatasetDTO
    from viva_api.simulation.database_service import DatabaseService

#: The producer column each owner kind names. The owning table's name, not a column name, is the
#: contract; the column is this table's way of storing it.
PRODUCER_COLUMN_BY_OWNER_KIND: dict[str, str] = {
    "simulation": "simulation_id",
    "parca_dataset": "parca_dataset_id",
    "analysis": "analysis_id",
}


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


class SmsDatasetStore:
    """``DatasetStore`` over the application's :class:`DatabaseService`."""

    def __init__(self, db: DatabaseService) -> None:
        self._db = db

    async def upsert(self, write: DatasetWrite, *, owner: OwnerRef) -> tuple[DatasetDTO, UpsertAction]:
        return await self._db.upsert_dataset(**write, **producer_ref(owner))
