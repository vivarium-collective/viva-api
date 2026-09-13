"""add task table (in-region task-run verb, viva-api#631)

Revision ID: d7e2f4a6c8b0
Revises: f76e43d01841
Create Date: 2026-09-12

Adds the ``task`` table backing ``POST /api/v1/tasks`` — a self-contained script
run on the in-region task compute (slice 1). Distinct from ``analysis`` because a
task is an arbitrary script, not a named analysis over a sweep. Introduces the
``taskstatusdb`` enum (labels match the TaskStatusDB member names, as create_all
uses member names).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d7e2f4a6c8b0"
down_revision: str | Sequence[str] | None = "f76e43d01841"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Enum labels match the TaskStatusDB member names (create_all uses member names).
# create_type=False: the explicit ``.create(..., checkfirst=True)`` in upgrade() owns
# type creation. Without this, ``op.create_table`` emits a SECOND, unguarded
# CREATE TYPE for the column, and the migration fails with
# DuplicateObjectError: type "taskstatusdb" already exists -- on every run,
# including a fresh database. Caught by the real-alembic-chain test this branch
# adds (tests/simulation/test_observability_migration.py); nothing on main ran
# the chain end to end, which is why it was not seen at merge (#636 carried the
# same fix to main ahead of this branch, with the citation adapted).
_task_status = postgresql.ENUM("COMPUTING", "READY", "FAILED", name="taskstatusdb", create_type=False)


def upgrade() -> None:
    # Guarded with the inspector rather than raw ``CREATE TABLE IF NOT EXISTS`` so the
    # column definitions stay in ONE place, typed, next to the ORM -- hand-written DDL
    # here would be a second definition free to drift from ORMTask.
    #
    # The guard is needed because a create_all database ALREADY has this table:
    # ORMTask is a Base table, so create_db makes it at startup, which this revision's
    # own fingerprint marker relies on. An unguarded create_table fails with
    # DuplicateTableError on exactly the databases db_reconcile's LEGACY path produces
    # (stamp, then upgrade head) -- the normal production shape.
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    # checkfirst on the explicit create, and create_type=False on the column type:
    # otherwise create_table emits a SECOND, unguarded CREATE TYPE and the migration
    # dies with DuplicateObjectError on every run, fresh databases included.
    _task_status.create(bind, checkfirst=True)

    if not inspector.has_table("task"):
        op.create_table(
            "task",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("script", sa.String(), nullable=False),
            sa.Column("args", postgresql.JSONB(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("sim_data_refs", postgresql.JSONB(), nullable=True),
            sa.Column("memory_class", sa.String(), nullable=True, server_default="standard"),
            sa.Column("status", _task_status, nullable=True),
            sa.Column("job_name", sa.String(), nullable=True),
            sa.Column("job_id_ext", sa.String(), nullable=True),
            sa.Column("out_uri", sa.String(), nullable=True),
            sa.Column("result_uri", sa.String(), nullable=True),
            sa.Column("error_message", sa.String(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=True, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(), nullable=True, server_default=sa.func.now()),
        )

    if not any(ix["name"] == "ix_task_job_id_ext" for ix in inspector.get_indexes("task")):
        op.create_index("ix_task_job_id_ext", "task", ["job_id_ext"])


def downgrade() -> None:
    op.drop_index("ix_task_job_id_ext", table_name="task", if_exists=True)
    op.drop_table("task", if_exists=True)
    _task_status.drop(op.get_bind(), checkfirst=True)
