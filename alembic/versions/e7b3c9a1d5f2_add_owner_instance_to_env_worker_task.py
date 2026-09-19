"""add owner_instance to env_worker_task

Revision ID: e7b3c9a1d5f2
Revises: c9a1e3f5b7d2
Create Date: 2026-09-19

The env-worker boot sweep (``fail_unfinished_tasks``) settles every task left unfinished by a
process that no longer exists. It was UNSCOPED: correct only while exactly one process uses the
table. A second process sharing the database -- the core/SMS split, ``docs/plan-core.md`` P9 --
would, on boot, fail every task the first one is running right now.

``owner_instance`` records the ROLE of the process that accepted a task (a stable name such as
``api``; never a pod name, because the sweep runs in the NEXT incarnation of that role). The
sweep settles only its own role's rows -- and rows with no owner, which predate this column.

Idempotent (``IF NOT EXISTS``), like the other compose-side revisions: ``create_all`` on a laptop
or a test database may already have added the column. Fully reversible.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "e7b3c9a1d5f2"
down_revision: str | Sequence[str] | None = "c9a1e3f5b7d2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE env_worker_task ADD COLUMN IF NOT EXISTS owner_instance VARCHAR")
    op.execute("CREATE INDEX IF NOT EXISTS ix_env_worker_task_owner_instance ON env_worker_task (owner_instance)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_env_worker_task_owner_instance")
    op.execute("ALTER TABLE env_worker_task DROP COLUMN IF EXISTS owner_instance")
