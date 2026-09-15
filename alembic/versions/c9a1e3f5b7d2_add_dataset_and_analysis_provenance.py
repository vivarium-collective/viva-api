"""add the dataset table, analysis.source/tags, and hpcrun.jobref_analysis_id

Revision ID: c9a1e3f5b7d2
Revises: b2f6d8e0a4c7
Create Date: 2026-09-15

Data provenance, slice 1 (docs/plan-data-provenance.md §6, viva-api#655/#657).

* ``dataset`` -- one row per consumable file set a run actually wrote. Never
  pre-created (§2a): born from an ``artifact.written`` trace event or from the
  reconciliation walk finding the object. Producer foreign keys to the run that
  wrote it (simulation / ParCa / analysis; slice 2 adds task), ``uri`` unique,
  extensible GIN-indexed ``attributes`` and ``tags``, ``available`` flag.
* ``analysis.source`` (a ProvenanceRef) and ``analysis.tags`` (same declaration and
  GIN index as ``simulation.tags``, c1a2b3d4e5f6).
* ``hpcrun.jobref_analysis_id`` -- the job reference of a ``JobTypeDB.ANALYSIS`` run
  (label added by b2f6d8e0a4c7).

**Guarded everywhere, typed, no raw DDL** -- the shape d7e2f4a6c8b0 and a3b5c7d9e1f2
settled on. Two production shapes must both work:

* a MANAGED database (both Stanford sites) arrives here with none of these objects;
* a LEGACY ``create_all`` database already has all of them (every object below is
  in ``Base.metadata``), gets stamped by ``db_reconcile``, and must pass through as a
  no-op.

**The ``analysis`` table guard is defensive, not a #637 fix.** No migration creates
``analysis`` (only ``create_all`` does), so this revision creates it, in its
pre-this-revision shape, when it is absent. A genuinely empty database still fails
earlier, at d3f9a1c72b84's unguarded ``add_column("analysis", ...)``; #637 is fixed
there, separately.

Column definitions here mirror ``ORMDataset`` / ``ORMAnalysis`` / ``ORMHpcRun``
exactly (types, nullability, server defaults); the create_all-vs-migration type
comparison in tests/simulation/test_dataset_migration.py is what holds them together.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c9a1e3f5b7d2"
down_revision: str | Sequence[str] | None = "b2f6d8e0a4c7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# create_type=False: the explicit .create(checkfirst=True) below owns the type, so a
# create_table referencing it cannot emit a second, unguarded CREATE TYPE (the
# DuplicateObjectError d7e2f4a6c8b0 documents).
_analysis_status = postgresql.ENUM("COMPUTING", "READY", "FAILED", name="analysisstatusdb", create_type=False)


def _has_table(name: str) -> bool:
    # A fresh inspector per question: Inspector caches reflection, and this revision
    # creates objects between questions.
    return sa.inspect(op.get_bind()).has_table(name)


def _has_column(table: str, column: str) -> bool:
    return any(c["name"] == column for c in sa.inspect(op.get_bind()).get_columns(table))


def _has_index(table: str, name: str) -> bool:
    return any(ix["name"] == name for ix in sa.inspect(op.get_bind()).get_indexes(table))


def _ensure_analysis_table() -> None:
    """Create ``analysis`` in its pre-c9a1e3f5b7d2 shape when absent (see docstring)."""
    if _has_table("analysis"):
        return
    _analysis_status.create(op.get_bind(), checkfirst=True)
    op.create_table(
        "analysis",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("config", postgresql.JSONB(), nullable=False),
        sa.Column("last_updated", sa.String(), nullable=False),
        sa.Column("job_name", sa.String(), nullable=True),
        sa.Column("job_id", sa.Integer(), nullable=True),
        sa.Column("experiment_id", sa.String(), nullable=True),
        sa.Column("n_tp", sa.Integer(), nullable=True),
        sa.Column("status", _analysis_status, nullable=True),
        sa.Column("result_uri", sa.String(), nullable=True),
        sa.Column("backend", sa.String(), nullable=True, server_default="batch"),
        sa.Column("simulation_id", sa.Integer(), sa.ForeignKey("simulation.id"), nullable=True),
        sa.Column("job_id_ext", sa.String(), nullable=True),
        sa.Column("error_message", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
    )
    op.create_index("ix_analysis_experiment_id", "analysis", ["experiment_id"])
    op.create_index("ix_analysis_n_tp", "analysis", ["n_tp"])
    op.create_index("ix_analysis_simulation_id", "analysis", ["simulation_id"])


def upgrade() -> None:
    _ensure_analysis_table()

    # --- analysis provenance columns ---
    if not _has_column("analysis", "source"):
        op.add_column("analysis", sa.Column("source", postgresql.JSONB(), nullable=True))
    if not _has_column("analysis", "tags"):
        op.add_column("analysis", sa.Column("tags", postgresql.JSONB(), nullable=False, server_default="[]"))
    if not _has_index("analysis", "ix_analysis_tags"):
        op.create_index("ix_analysis_tags", "analysis", ["tags"], postgresql_using="gin")

    # --- hpcrun job reference for ANALYSIS runs ---
    if not _has_column("hpcrun", "jobref_analysis_id"):
        op.add_column(
            "hpcrun",
            sa.Column("jobref_analysis_id", sa.Integer(), sa.ForeignKey("analysis.id"), nullable=True),
        )
    if not _has_index("hpcrun", "ix_hpcrun_jobref_analysis_id"):
        op.create_index("ix_hpcrun_jobref_analysis_id", "hpcrun", ["jobref_analysis_id"])

    # --- dataset ---
    if not _has_table("dataset"):
        op.create_table(
            "dataset",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("simulation_id", sa.Integer(), sa.ForeignKey("simulation.id"), nullable=True),
            sa.Column("parca_dataset_id", sa.Integer(), sa.ForeignKey("parca_dataset.id"), nullable=True),
            sa.Column("analysis_id", sa.Integer(), sa.ForeignKey("analysis.id"), nullable=True),
            sa.Column("kind", sa.String(), nullable=False),
            sa.Column("view", sa.String(), nullable=True),
            sa.Column("display_name", sa.String(), nullable=True),
            sa.Column("uri", sa.String(), nullable=False, unique=True),
            sa.Column("size_bytes", sa.BigInteger(), nullable=True),
            sa.Column("sha256", sa.String(), nullable=True),
            sa.Column("attributes", postgresql.JSONB(), nullable=False, server_default="{}"),
            sa.Column("tags", postgresql.JSONB(), nullable=False, server_default="[]"),
            sa.Column("source", postgresql.JSONB(), nullable=True),
            sa.Column("available", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
            sa.CheckConstraint(
                "simulation_id IS NOT NULL OR parca_dataset_id IS NOT NULL OR analysis_id IS NOT NULL",
                name="ck_dataset_producer",
            ),
        )
    for name, columns, using in (
        ("ix_dataset_simulation_id", ["simulation_id"], None),
        ("ix_dataset_parca_dataset_id", ["parca_dataset_id"], None),
        ("ix_dataset_analysis_id", ["analysis_id"], None),
        ("ix_dataset_kind_view", ["kind", "view"], None),
        ("ix_dataset_attributes", ["attributes"], "gin"),
        ("ix_dataset_tags", ["tags"], "gin"),
    ):
        if not _has_index("dataset", name):
            if using:
                op.create_index(name, "dataset", columns, postgresql_using=using)
            else:
                op.create_index(name, "dataset", columns)


def downgrade() -> None:
    """Drop what this revision added. The ``analysis`` table itself is left in place:
    every other deployment path (create_all) owns it, and dropping it would destroy
    data this revision never created."""
    op.drop_table("dataset", if_exists=True)
    op.drop_index("ix_hpcrun_jobref_analysis_id", table_name="hpcrun", if_exists=True)
    if _has_column("hpcrun", "jobref_analysis_id"):
        op.drop_column("hpcrun", "jobref_analysis_id")  # its FK constraint goes with the column
    op.drop_index("ix_analysis_tags", table_name="analysis", if_exists=True)
    for column in ("tags", "source"):
        if _has_table("analysis") and _has_column("analysis", column):
            op.drop_column("analysis", column)
