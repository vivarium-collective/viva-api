"""bring the three baseline tables the models outgrew into the models' shape (viva-api#637)

Revision ID: c3f7a1e5b9d4
Revises: b9e1d5a3c7f2
Create Date: 2026-09-19

Making ``upgrade head`` RUN from an empty database (``b9e1d5a3c7f2``) is not the same as
making it build a schema the app can use. The baseline, ``fb7621a73e24``, captured an older
shape of three tables, and the models moved on through ``create_all`` alone:

==============  ===============================================  ==========================================
table           the baseline has, the models do not              the models have, the baseline does not
==============  ===============================================  ==========================================
``hpcrun``      --                                               ``correlation_id`` (+ index)
``simulation``  ``variant_config``, ``variant_config_hash``      ``config``, ``config_filename`` (+ index),
                                                                 ``experiment_id`` (unique)
``worker_event`` ``sim_data``, ``global_time``, ``error_message`` ``correlation_id`` (+ index), ``mass``,
                                                                 ``bulk``, ``bulk_index``, ``time``
==============  ===============================================  ==========================================

No real database has the baseline's shape -- every one was made by ``create_all`` and looks
like the models (checked on ``sms-api-stanford-test``, 2026-09-19). A database built from this
chain, though, could not take a single ``hpcrun`` insert: ``correlation_id`` is NOT NULL in
the model and absent from the table, and ``simulation.variant_config`` is NOT NULL in the
table and unknown to the model.

Guarded, like ``b9e1d5a3c7f2``, because a LEGACY database stamped below this revision is
upgraded through it and already has the models' shape:

* a column is added only when it is missing;
* a stale column is dropped only when it is present;
* and when either change is needed on a table that HOLDS ROWS, this revision stops instead.
  The new columns are NOT NULL with no sensible default, and a populated table that still has
  the baseline's columns is one the app could never have written -- someone should look at
  it rather than have a migration guess.

On a ``create_all`` database every check finds nothing to do.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "c3f7a1e5b9d4"
down_revision: str | Sequence[str] | None = "b9e1d5a3c7f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_STALE: dict[str, tuple[str, ...]] = {
    "simulation": ("variant_config", "variant_config_hash"),
    "worker_event": ("sim_data", "global_time", "error_message"),
}


def _missing_columns() -> dict[str, list[sa.Column[object]]]:
    return {
        "hpcrun": [sa.Column("correlation_id", sa.String(), nullable=False)],
        "simulation": [
            sa.Column("config_filename", sa.String(), nullable=False),
            sa.Column("experiment_id", sa.String(), nullable=False),
            sa.Column("config", postgresql.JSONB(), nullable=False),
        ],
        "worker_event": [
            sa.Column("correlation_id", sa.String(), nullable=False),
            sa.Column("mass", postgresql.JSONB(), nullable=False),
            sa.Column("bulk", postgresql.JSONB(), nullable=True),
            sa.Column("bulk_index", postgresql.JSONB(), nullable=True),
            sa.Column("time", sa.Float(), nullable=True),
        ],
    }


#: index name -> (table, column, unique)
_INDEXES: dict[str, tuple[str, str, bool]] = {
    "ix_hpcrun_correlation_id": ("hpcrun", "correlation_id", False),
    "ix_simulation_config_filename": ("simulation", "config_filename", False),
    "ix_worker_event_correlation_id": ("worker_event", "correlation_id", False),
}


def _refuse_if_populated(table: str, change: str) -> None:
    rows = op.get_bind().execute(sa.text(f"SELECT EXISTS (SELECT 1 FROM {table})")).scalar()  # noqa: S608 - fixed names
    if rows:
        raise RuntimeError(
            f"c3f7a1e5b9d4: '{table}' holds rows and needs a structural change ({change}). This revision "
            "only reshapes EMPTY baseline tables; a populated table in the baseline's shape was not written "
            "by this application. Inspect it by hand (uv run python -m viva_api.simulation.schema_diff)."
        )


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    for table, columns in _missing_columns().items():
        present = {c["name"] for c in inspector.get_columns(table)}
        to_add = [c for c in columns if c.name not in present]
        to_drop = [name for name in _STALE.get(table, ()) if name in present]
        if not to_add and not to_drop:
            continue
        _refuse_if_populated(table, f"add {[c.name for c in to_add]}, drop {to_drop}")
        for column in to_add:
            op.add_column(table, column)
        for name in to_drop:
            op.drop_column(table, name)

    inspector = sa.inspect(op.get_bind())
    for name, (table, column, unique) in _INDEXES.items():
        if not any(ix["name"] == name for ix in inspector.get_indexes(table)):
            op.create_index(name, table, [column], unique=unique)
    uniques = {tuple(u["column_names"]) for u in inspector.get_unique_constraints("simulation")}
    if ("experiment_id",) not in uniques:
        op.create_unique_constraint("simulation_experiment_id_key", "simulation", ["experiment_id"])


def downgrade() -> None:
    """A no-op, on purpose. The upgrade only ever changes a database this chain built from
    empty, and there is nothing to go back TO: the baseline's shape is one the application
    cannot use. Going further down reaches ``b9e1d5a3c7f2``'s downgrade, which drops tables."""
