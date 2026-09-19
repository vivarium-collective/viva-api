"""add query/result columns to analysis

Revision ID: d3f9a1c72b84
Revises: c3f7a1e5b9d4
Create Date: 2026-07-14

Generalizes the ``analysis`` table for the analysis-result endpoints
(analysis-results-design.md): adds nullable, indexed query columns
(experiment_id, n_tp, simulation_id) plus status/result/audit columns. The
existing ``config`` JSONB remains the authoritative store; legacy rows keep the
new columns NULL. Introduces the ``analysisstatusdb`` enum (labels match the
ORM member names).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d3f9a1c72b84"
down_revision: str | Sequence[str] | None = "c3f7a1e5b9d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Enum labels match the AnalysisStatusDB member names (create_all uses member names).
_analysis_status = postgresql.ENUM("COMPUTING", "READY", "FAILED", name="analysisstatusdb")


def upgrade() -> None:
    _analysis_status.create(op.get_bind(), checkfirst=True)
    op.add_column("analysis", sa.Column("experiment_id", sa.String(), nullable=True))
    op.add_column("analysis", sa.Column("n_tp", sa.Integer(), nullable=True))
    op.add_column("analysis", sa.Column("status", _analysis_status, nullable=True))
    op.add_column("analysis", sa.Column("result_uri", sa.String(), nullable=True))
    op.add_column("analysis", sa.Column("backend", sa.String(), nullable=True, server_default="batch"))
    op.add_column("analysis", sa.Column("simulation_id", sa.Integer(), nullable=True))
    op.add_column("analysis", sa.Column("job_id_ext", sa.String(), nullable=True))
    op.add_column("analysis", sa.Column("error_message", sa.String(), nullable=True))
    op.add_column("analysis", sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True))
    op.add_column("analysis", sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=True))
    op.create_foreign_key("fk_analysis_simulation_id", "analysis", "simulation", ["simulation_id"], ["id"])
    op.create_index("ix_analysis_experiment_id", "analysis", ["experiment_id"])
    op.create_index("ix_analysis_n_tp", "analysis", ["n_tp"])
    op.create_index("ix_analysis_simulation_id", "analysis", ["simulation_id"])


def downgrade() -> None:
    op.drop_index("ix_analysis_simulation_id", table_name="analysis")
    op.drop_index("ix_analysis_n_tp", table_name="analysis")
    op.drop_index("ix_analysis_experiment_id", table_name="analysis")
    op.drop_constraint("fk_analysis_simulation_id", "analysis", type_="foreignkey")
    for col in (
        "updated_at",
        "created_at",
        "error_message",
        "job_id_ext",
        "simulation_id",
        "backend",
        "result_uri",
        "status",
        "n_tp",
        "experiment_id",
    ):
        op.drop_column("analysis", col)
    _analysis_status.drop(op.get_bind(), checkfirst=True)
