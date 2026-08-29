"""add env_worker_task

Revision ID: b4d7e9c02a15
Revises: 9c2e6b1f4a73
Create Date: 2026-08-29

Plan §E option (e): the durable record for an env-worker call that cannot be a
synchronous HTTP request — the job-class methods (``run_study``,
``run_study_analyses``, ``run_investigation_analysis``), which run a study's
simulations to completion.

**Idempotent by necessity, not by taste.** ``env_worker_task`` lives in the
compose metadata, and compose tables are bootstrapped at startup by
``create_compose_db`` (``create_all``, via ``dependencies.py``). So on any DB the
app has already touched, this table ALREADY EXISTS by the time Alembic runs, and
a plain ``op.create_table`` would fail re-creating it. ``CREATE TABLE IF NOT
EXISTS`` makes this a no-op there and a real create on a DB that Alembic owns —
the same reasoning as ``e5a7c9d10f21``'s ``ADD COLUMN IF NOT EXISTS``, applied to
a whole table because there is no in-repo precedent for a post-baseline create.

The status column is plain VARCHAR rather than a PG enum. ``create_all`` would
mint a ``composejobstatusdb`` enum type, and an enum created by two different
mechanisms is exactly the drift ``db_reconcile`` exists to diagnose; a string
column is identical from SQLAlchemy's side (the Python enum still validates) and
cannot disagree with itself.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b4d7e9c02a15"
down_revision: str | Sequence[str] | None = "9c2e6b1f4a73"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS env_worker_task (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
            job_name VARCHAR NOT NULL,
            method VARCHAR NOT NULL,
            params JSONB,
            status VARCHAR NOT NULL,
            result JSONB,
            error_message VARCHAR,
            started_at TIMESTAMP WITHOUT TIME ZONE,
            ended_at TIMESTAMP WITHOUT TIME ZONE,
            created_by VARCHAR,
            correlation_id VARCHAR NOT NULL
        )
    """)
    # Separate statements, each guarded: a create_all DB already has these from
    # the ORM's index=True/unique=True, and IF NOT EXISTS is per-object.
    op.execute("CREATE INDEX IF NOT EXISTS ix_env_worker_task_job_name ON env_worker_task (job_name)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_env_worker_task_created_by ON env_worker_task (created_by)")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_env_worker_task_correlation_id ON env_worker_task (correlation_id)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_env_worker_task_correlation_id")
    op.execute("DROP INDEX IF EXISTS ix_env_worker_task_created_by")
    op.execute("DROP INDEX IF EXISTS ix_env_worker_task_job_name")
    op.execute("DROP TABLE IF EXISTS env_worker_task")
