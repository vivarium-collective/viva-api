"""add chain-dispatch per-seed progress columns to hpcrun

Revision ID: 71a5478673a8
Revises: 44335812e447
Create Date: 2026-08-18 19:57:00.056157

Backlog item 71 Phase 4 (V2 non-Nextflow chain-dispatch redesign): adds three
nullable columns to the existing ``hpcrun`` table so a chain-dispatch campaign's
generations are submitted incrementally by ``JobScheduler``'s own poll loop
(app-level gating) instead of all N*G jobs being submitted upfront via native
AWS Batch ``dependsOn`` chains (item 68's own scaling-stall root cause).

- ``chain_current_job_ids``: JSONB list, one entry per seed, the AWS Batch job
  id of that seed's CURRENT in-flight job, or ``null`` once that seed's chain
  has resolved (succeeded all generations, or permanently failed one). This is
  what the scheduler polls each tick and what a campaign-wide cancel walks.
- ``chain_current_generation``: JSONB list, one entry per seed, the generation
  index the seed's current tracked job represents — real per-seed progress
  visibility, not previously available.
- ``chain_parca_done``: nullable boolean, gates generation-0 submission for
  every seed at once (ParCa is a single, campaign-wide prerequisite, not
  per-seed) — avoids a hybrid dependsOn+app-level state machine for that one
  edge the way native ``dependsOn`` used to handle it.

All three are NULL for every pre-existing row and every non-chain-campaign
HpcRun going forward (SLURM, K8s, MNP, single-shot Array, and the plain
comparison-ensemble/phase0 paths) — purely additive, no new table.
``chain_final_job_ids`` (f2b8e4a6c9d1) is UNCHANGED in shape — still a flat
list of each seed's own last job id — only WHEN it's written changes
(incrementally, as each seed's chain resolves, instead of all at submission
time), so the existing analysis-fan-in consumer (``JobScheduler.
_advance_chain_campaign`` / ``submit_campaign_analysis``) keeps working once
every seed has contributed its entry.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "71a5478673a8"
down_revision: str | Sequence[str] | None = "44335812e447"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("hpcrun", sa.Column("chain_current_job_ids", postgresql.JSONB(), nullable=True))
    op.add_column("hpcrun", sa.Column("chain_current_generation", postgresql.JSONB(), nullable=True))
    op.add_column("hpcrun", sa.Column("chain_parca_done", sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column("hpcrun", "chain_parca_done")
    op.drop_column("hpcrun", "chain_current_generation")
    op.drop_column("hpcrun", "chain_current_job_ids")
