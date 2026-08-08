"""add wave_index/wave_seed_indices columns to hpcrun

Revision ID: f2b8e4a6c9d1
Revises: e5a7c9d10f21
Create Date: 2026-08-08

Backlog item 33 (per-generation wave dispatch): adds two nullable columns to
the existing ``hpcrun`` table so a wave-dispatch campaign can track, per HpcRun
row, which 0-based generation its AWS Batch Array job ran (``wave_index``) and
the real seed dispatched at each local array position (``wave_seed_indices``,
JSONB list of ints -- needed to remap a sparse survivor set back to real seeds
after Spot/OOM attrition). Both are NULL for every pre-existing row and every
non-wave HpcRun going forward (SLURM, K8s, MNP, single-shot Array) -- purely
additive, no new table (ORMHpcRun already supports multiple rows per
simulation, one per wave/generation).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f2b8e4a6c9d1"
down_revision: str | Sequence[str] | None = "e5a7c9d10f21"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("hpcrun", sa.Column("wave_index", sa.Integer(), nullable=True))
    op.add_column("hpcrun", sa.Column("wave_seed_indices", postgresql.JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column("hpcrun", "wave_seed_indices")
    op.drop_column("hpcrun", "wave_index")
