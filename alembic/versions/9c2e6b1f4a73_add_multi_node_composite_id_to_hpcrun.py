"""add multi_node_composite_id to hpcrun

Revision ID: 9c2e6b1f4a73
Revises: 71a5478673a8
Create Date: 2026-08-25 00:00:00.000000

Backlog item 88 (colony composite scalable dispatch): adds one nullable
column to the existing ``hpcrun`` table so a generic multi-node
process-bigraph composite dispatch (``SimulationServiceRay.
_submit_multi_node_composite`` — e.g. a colony composite spread across N
Ray-cluster nodes) can join the same auto-triggered "Analysis flush" pattern
chain-dispatch campaigns already get, without touching any chain-dispatch
code.

- ``multi_node_composite_id``: the dispatched composite's id (e.g.
  ``v2ecoli.composites.ecoli_colony.ecoli_colony``), written once at
  submission time. Doubles as the discriminator ``JobScheduler.
  update_multi_node_jobs`` polls for (via IS NOT NULL), the same role
  ``chain_n_generations`` already plays for chain-dispatch campaigns — the
  two columns are independent and mutually exclusive by construction (a row
  is written by exactly one dispatch shape), so the existing
  ``list_active_chain_campaigns`` query (filtered on
  ``chain_n_generations IS NOT NULL``) is structurally guaranteed to never
  see a multi-node-composite row, and vice versa.

NULL for every pre-existing row and every non-multi-node-composite HpcRun
going forward (SLURM, K8s, chain-dispatch, the older comparison-ensemble/
phase0 MNP paths) — purely additive, no new table.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9c2e6b1f4a73"
down_revision: str | Sequence[str] | None = "71a5478673a8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("hpcrun", sa.Column("multi_node_composite_id", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("hpcrun", "multi_node_composite_id")
