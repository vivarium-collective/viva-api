"""add temporary / label / image_tag to simulator

Revision ID: f4c8a2e6d0b3
Revises: e7b3c9a1d5f2
Create Date: 2026-09-20

A simulator record, its container image and its image tag are the provenance of every
simulation that ran on them, so they are WRITE-ONCE: never overwritten, force-rebuilt,
re-tagged or deleted (``docs/plan-core.md`` decision D11). The one exception is a simulator
that says so about itself:

* ``temporary`` -- this simulator is a test artifact, not an authoritative build. It may be
  overwritten or removed, and every end-user surface marks it and leaves it out of any
  "latest" or default choice. False for every existing row: they are all authoritative.
* ``label`` -- who or what made it (``atlantis smoke …``). Required when ``temporary``.
* ``image_tag`` -- the image tag, when it is NOT the commit. NULL for every existing row,
  whose image is ``<repository>:<git_commit_hash>`` as before. A temporary simulator gets
  ``tmp-<commit>-<nonce>``, so it can never claim -- or overwrite -- the tag an authoritative
  build of the same commit owns.

Additive with defaults, so the previous image keeps working against the migrated database.
Idempotent (``IF NOT EXISTS``). Fully reversible: no row written before this revision uses
any of the three columns.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "f4c8a2e6d0b3"
down_revision: str | Sequence[str] | None = "e7b3c9a1d5f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE simulator ADD COLUMN IF NOT EXISTS temporary BOOLEAN NOT NULL DEFAULT false")
    op.execute("ALTER TABLE simulator ADD COLUMN IF NOT EXISTS label VARCHAR")
    op.execute("ALTER TABLE simulator ADD COLUMN IF NOT EXISTS image_tag VARCHAR")


def downgrade() -> None:
    op.execute("ALTER TABLE simulator DROP COLUMN IF EXISTS image_tag")
    op.execute("ALTER TABLE simulator DROP COLUMN IF EXISTS label")
    op.execute("ALTER TABLE simulator DROP COLUMN IF EXISTS temporary")
