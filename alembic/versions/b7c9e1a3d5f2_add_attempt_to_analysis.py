"""add attempt column to analysis (OOM-retry-escalation, item 38 track B)

Revision ID: b7c9e1a3d5f2
Revises: f2b8e4a6c9d1
Create Date: 2026-08-10

Backlog item 38 track B: one new nullable-never column, ``analysis.attempt``,
defaulting existing and future rows to ``1``. Lets one logical ``analysis`` row
track multiple physical retry submissions (``job_id_ext`` is swapped in place
on each retry by ``DatabaseService.update_analysis_job_id``) instead of
spawning a new row per attempt -- mirrors vEcoli-private's own Nextflow trace
(one logical task, an incrementing attempt, a new native job id each retry).
Purely additive; every pre-existing row backfills to 1 via the column default.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7c9e1a3d5f2"
down_revision: str | Sequence[str] | None = "f2b8e4a6c9d1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "analysis",
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="1"),
    )


def downgrade() -> None:
    op.drop_column("analysis", "attempt")
