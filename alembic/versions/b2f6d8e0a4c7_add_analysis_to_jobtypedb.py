"""add 'ANALYSIS' to the jobtypedb enum

Revision ID: b2f6d8e0a4c7
Revises: e3a9c1d70b62
Create Date: 2026-09-15

Data provenance, slice 1 (docs/plan-data-provenance.md §4b, viva-api#655/#657): a
standalone analysis run (``POST /simulations/{id}/analysis``) becomes a traced run
with its own ``hpcrun`` row, so the event ingester can read its trace and attribute
the files it writes. ``hpcrun.job_type`` therefore needs an ``ANALYSIS`` value.

The label is the member NAME, upper-case: ``ORMHpcRun.job_type`` binds a plain
``enum.Enum`` by ``.name`` (see ``44335812e447`` for the case bug that taught this
repo that lesson), and ``create_all`` derives the type's labels the same way.

Kept in its own revision, before the tables revision, and written exactly like
``44335812e447``: ``ADD VALUE IF NOT EXISTS`` is idempotent (a no-op on a
``create_all`` database, which already has the label) and legal inside Alembic's
single upgrade transaction on Postgres >= 12, because no later revision in the same
upgrade *uses* the new label -- the one thing Postgres forbids before commit.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b2f6d8e0a4c7"
down_revision: str | Sequence[str] | None = "e3a9c1d70b62"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TYPE jobtypedb ADD VALUE IF NOT EXISTS 'ANALYSIS'")


def downgrade() -> None:
    """No-op: PostgreSQL has no ``DROP VALUE`` for enum types.

    Removing the label would mean recreating the type and every dependent column,
    which is unsafe to automate (same position as ``a1c3e5f7b9d2`` and
    ``44335812e447``). An unused label is harmless.
    """
