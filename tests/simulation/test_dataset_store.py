"""The application's ``DatasetStore``: core's ``(owner_kind, owner_id)`` becomes this table's producer
column, and nothing else does that translation (plan P4a-2, slice 1)."""

import pytest

from viva_api.simulation.dataset_store import PRODUCER_COLUMN_BY_OWNER_KIND, producer_ref


@pytest.mark.parametrize(
    ("owner_kind", "column"),
    [("simulation", "simulation_id"), ("parca_dataset", "parca_dataset_id"), ("analysis", "analysis_id")],
)
def test_an_owner_kind_is_the_owning_tables_name_and_selects_its_producer_column(owner_kind: str, column: str) -> None:
    assert producer_ref({"owner_kind": owner_kind, "owner_id": "7"}) == {column: 7}


def test_the_three_producer_columns_are_the_whole_vocabulary() -> None:
    """The P4a-1 rule (#790's ``dataset_owner``) spells the same three names; a fourth arrives as a
    column here, never as an enum in core."""
    assert set(PRODUCER_COLUMN_BY_OWNER_KIND) == {"simulation", "parca_dataset", "analysis"}


def test_an_owner_this_table_cannot_store_is_refused_as_a_value_error() -> None:
    """A refusal, not a crash: the registry counts a ``ValueError`` from the store as a skip."""
    with pytest.raises(ValueError, match="no producer column for owner kind 'task'"):
        producer_ref({"owner_kind": "task", "owner_id": "1"})
    with pytest.raises(ValueError, match="not an integer id"):
        producer_ref({"owner_kind": "analysis", "owner_id": "seven"})
