"""add PENDING and uppercase CANCELLED to jobstatusdb

Revision ID: 44335812e447
Revises: f2b8e4a6c9d1
Create Date: 2026-08-16 13:13:33.356973

Backlog item 40: POST /api/v1/simulations/{id}/cancel raised a real Postgres
error writing the terminal job status:

    InvalidTextRepresentationError: invalid input value for enum jobstatusdb: "CANCELLED"

Root cause: ``ORMHpcRun.status: Mapped[JobStatusDB]`` has no ``values_callable``,
so SQLAlchemy's default ``Enum`` type binds/compiles a plain Python
``enum.Enum`` member by its **name**, not its ``.value`` -- confirmed directly
against this repo's SQLAlchemy 2.0.49 pin, and consistent with the baseline
migration's own literal enum labels (``WAITING``, ``QUEUED``, ``RUNNING``,
``COMPLETED``, ``FAILED`` -- all upper-case ``.name``-shaped strings, not the
lower-case ``.value``s the Python enum also carries). ``JobStatusDB``'s member
names are ``WAITING, PENDING, QUEUED, RUNNING, COMPLETED, CANCELLED, FAILED``,
so the app writes upper-case ``"CANCELLED"`` (and would write ``"PENDING"``
too) whenever it persists those statuses.

The prior migration (``a1c3e5f7b9d2``, "formalises the live patch applied to
the production database on 2026-05-11") added the *wrong* value: lower-case
``'cancelled'``, not the ``.name``-shaped ``'CANCELLED'`` the app actually
sends -- an off-by-case bug in that migration itself, not a second, separate
defect. ``'PENDING'`` has never been added by any migration at all, even
though it is also a real, currently-live risk: ``DatabaseServiceSQL.
list_active_hpcruns``/``list_active_chain_campaigns`` (``viva_api/simulation/
database_service.py``) both bind ``JobStatusDB.PENDING`` into a
``status IN (...)`` clause typed against this same enum column, which fails
the identical way for the identical reason -- confirmed directly (empirically,
against a real Postgres 15 database built from this repo's own real Alembic
migration chain up to a1c3e5f7b9d2, not assumed): the real, currently-deployed
``jobstatusdb`` label set is exactly
``{WAITING, QUEUED, RUNNING, COMPLETED, FAILED, cancelled}`` -- missing
``PENDING`` entirely, and carrying only the never-written, orphaned
lower-case ``cancelled`` instead of the upper-case value the app needs.

This migration adds both real, currently-needed values. The stray lower-case
``'cancelled'`` label added by ``a1c3e5f7b9d2`` is harmless and left in place:
Postgres has no ``DROP VALUE`` for enum types (removing a label safely
requires recreating the type and every dependent column/default, which
``a1c3e5f7b9d2``'s own ``downgrade()`` already declined to automate for the
same reason), and nothing ever writes it, so it is simply unused clutter, not
a live risk.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "44335812e447"
down_revision: str | Sequence[str] | None = "f2b8e4a6c9d1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add 'PENDING' and 'CANCELLED' to the jobstatusdb enum.

    Uses IF NOT EXISTS to be idempotent -- safe to run against a database
    that already has either value (e.g. a create_all-bootstrapped database,
    where both already exist because create_all always reflects the current
    ORM model's .name-shaped labels).
    """
    op.execute("ALTER TYPE jobstatusdb ADD VALUE IF NOT EXISTS 'PENDING'")
    op.execute("ALTER TYPE jobstatusdb ADD VALUE IF NOT EXISTS 'CANCELLED'")


def downgrade() -> None:
    """Downgrade is a no-op.

    PostgreSQL does not support removing enum values. Removing 'PENDING'/
    'CANCELLED' would require recreating the type and all columns that
    depend on it, which is unsafe to automate. If a rollback is needed,
    handle manually.
    """
