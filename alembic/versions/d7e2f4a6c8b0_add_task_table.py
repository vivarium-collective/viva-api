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
# including a fresh database. Nothing on main runs the alembic chain end to end,
# which is why this merged green; it was caught by a real-alembic-chain test on
# the observability branch (viva-api#609), which is where that coverage lands.
_task_status = postgresql.ENUM("COMPUTING", "READY", "FAILED", name="taskstatusdb", create_type=False)


def upgrade() -> None:
    # Every statement guarded, matching b4d7e9c02a15's reasoning: a create_all
    # database ALREADY has this table -- ORMTask is a Base table, so create_db makes
    # it at startup, which this migration's own fingerprint-marker docstring relies
    # on. An unguarded op.create_table therefore fails with
    # DuplicateTableError: relation "task" already exists on exactly the databases
    # db_reconcile's LEGACY path produces (stamp, then upgrade head) -- which is the
    # normal production shape.
    _task_status.create(op.get_bind(), checkfirst=True)
    op.execute("""
        CREATE TABLE IF NOT EXISTS task (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            script VARCHAR NOT NULL,
            args JSONB NOT NULL DEFAULT '[]',
            sim_data_refs JSONB,
            memory_class VARCHAR DEFAULT 'standard',
            status taskstatusdb,
            job_name VARCHAR,
            job_id_ext VARCHAR,
            out_uri VARCHAR,
            result_uri VARCHAR,
            error_message VARCHAR,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
            updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_task_job_id_ext ON task (job_id_ext)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_task_job_id_ext")
    op.execute("DROP TABLE IF EXISTS task")
    _task_status.drop(op.get_bind(), checkfirst=True)
