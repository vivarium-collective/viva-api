"""add chain-dispatch campaign columns to hpcrun

Revision ID: f2b8e4a6c9d1
Revises: e5a7c9d10f21
Create Date: 2026-08-08

Backlog item 33 (per-generation task decomposition): adds two nullable columns
to the existing ``hpcrun`` table so ONE row can track a whole per-seed
chain-dispatch campaign -- ``chain_n_generations`` (the campaign's total
generation count G, also doubling as the "is this row a chain-campaign
tracker" discriminator via IS NOT NULL) and ``chain_final_job_ids`` (JSONB
list of the AWS Batch job id AWS assigned to each seed's own last
successfully-submitted generation job -- the set the analysis-fan-in poller,
``JobScheduler.update_chain_campaigns``, watches for all-terminal). Both are
NULL for every pre-existing row and every non-chain-campaign HpcRun going
forward (SLURM, K8s, MNP, single-shot Array) -- purely additive, no new table
(ORMHpcRun already supports multiple rows per simulation).

Amended in place (still unmerged -- PR #237) rather than stacking a second
migration on top: this revision originally shipped as
``wave_index``/``wave_seed_indices``, sized for a fundamentally different
design (one HpcRun row per GENERATION-WIDE array job, "wave_seed_indices"
holding the real seed at each local array position). That design was
superseded before merge by individual per-seed AWS Batch job chains (each
generation its own job, chained via native ``dependsOn`` -- see
``SimulationServiceRay.submit_chain_dispatch_job``), which collapses the
per-campaign bookkeeping to ONE row holding the N seeds' own final job ids
directly, rather than N array-position indices. Since nothing was ever
deployed against the old column names (this capability is still unwired from
any HTTP router), renaming here costs nothing and avoids a churny
add-then-drop pair of migrations for a shape that never shipped.
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
    op.add_column("hpcrun", sa.Column("chain_n_generations", sa.Integer(), nullable=True))
    op.add_column("hpcrun", sa.Column("chain_final_job_ids", postgresql.JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column("hpcrun", "chain_final_job_ids")
    op.drop_column("hpcrun", "chain_n_generations")
