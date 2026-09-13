"""rename hpcrun_event.layer to hpcrun_event.component

Revision ID: e3a9c1d70b62
Revises: a3b5c7d9e1f2
Create Date: 2026-09-13

A draft of ``a3b5c7d9e1f2`` named the event column ``layer`` (the fixed
``engine|runner|dispatcher|api`` vocabulary of the first design). The settled
schema -- plan D1', matching process-bigraph#209 -- calls it ``component``, a
FREE string: ``"process_bigraph"``, ``"v2ecoli.lineage"``,
``"viva_api.dispatch"``. ``ORMHpcRunEvent.component`` and the ingester both
write that name.

**Why this is its own revision rather than an edit to a3b5c7d9e1f2.**

The rename originally lived inside ``a3b5c7d9e1f2.upgrade()``. That cannot work
for the databases it exists to rescue. A site that deployed the draft is
*stamped* at ``a3b5c7d9e1f2``; ``db_reconcile`` classifies a stamped database as
MANAGED and does exactly one thing -- ``alembic upgrade head``. Alembic never
re-runs an applied revision, so the rename would be dead code precisely where it
was needed, and the app would then write ``component`` against a table that only
has ``layer``: ``UndefinedColumn`` on every event insert, silently, because the
migration Job exits 0 having applied nothing.

Carrying it in a NEW revision makes ``upgrade head`` do the work on exactly
those databases, and nothing anywhere else.

Idempotent and origin-agnostic, so it is a no-op on every other shape:

* a database built by this chain from base already has ``component``;
* a ``create_all`` database has ``component`` (it reflects the current ORM);
* a database that somehow has both is left alone rather than guessed at.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e3a9c1d70b62"
down_revision: str | Sequence[str] | None = "a3b5c7d9e1f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_name = 'hpcrun_event' AND column_name = 'layer')
               AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name = 'hpcrun_event' AND column_name = 'component') THEN
                ALTER TABLE hpcrun_event RENAME COLUMN layer TO component;
            END IF;
        END $$
        """
    )


def downgrade() -> None:
    """Back to the draft name, under the mirror-image guard."""
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_name = 'hpcrun_event' AND column_name = 'component')
               AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name = 'hpcrun_event' AND column_name = 'layer') THEN
                ALTER TABLE hpcrun_event RENAME COLUMN component TO layer;
            END IF;
        END $$
        """
    )
