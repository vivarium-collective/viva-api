"""add job_id_ext/job_backend to compose_hpcrun

Revision ID: e5a7c9d10f21
Revises: d3f9a1c72b84
Create Date: 2026-07-21

The compose subsystem's ``compose_hpcrun`` table is normally bootstrapped by
``create_compose_db`` (``create_all``), not Alembic — so a DB the app already
touched has the OLD shape (``slurmjobid`` int only) and ``create_all`` never
ALTERs it. This adds the two backend-agnostic columns (mirroring
``hpcrun.job_id_ext``/``job_backend``) so an AWS Batch/Ray compose job's
UUID string id can be tracked. Idempotent: ``ADD COLUMN IF NOT EXISTS`` so it's
a no-op on a fresh ``create_all`` DB that already has the columns.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e5a7c9d10f21"
down_revision: str | Sequence[str] | None = "d3f9a1c72b84"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE compose_hpcrun ADD COLUMN IF NOT EXISTS job_id_ext VARCHAR")
    op.execute("ALTER TABLE compose_hpcrun ADD COLUMN IF NOT EXISTS job_backend VARCHAR NOT NULL DEFAULT 'ray'")


def downgrade() -> None:
    op.execute("ALTER TABLE compose_hpcrun DROP COLUMN IF EXISTS job_backend")
    op.execute("ALTER TABLE compose_hpcrun DROP COLUMN IF EXISTS job_id_ext")
