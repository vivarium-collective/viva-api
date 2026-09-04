"""add external_job_ids to hpcrun

Revision ID: c7d1f3a9b2e4
Revises: b4d7e9c02a15
Create Date: 2026-09-04

viva-api#414 (orphaned job polling): a LOCAL-backend ``hpcrun`` row -- an
in-process asyncio task that submitted real work to AWS Batch and is polling
it (today: the DooD image builds on both the K8s/Nextflow and Ray backends) --
stored ONLY the LOCAL uuid in ``job_id_ext``. That uuid dies with the pod that
minted it, so after a rollout mid-build the row sat ``running`` forever while
the Batch job finished normally (measured live 2026-09-04: hpcrun 506, build
``v2ecoli-ray-build-10ebc4c``, SUCCEEDED 6 minutes after the pod that owned it
was replaced; the only recovery was a redundant 10-minute rebuild).

- ``external_job_ids``: JSONB list of the AWS Batch job ids the LOCAL task is
  watching, written by the task right after it submits them
  (``LocalTaskService.record_external_job_ids``). This is the durable pointer
  ``JobScheduler.reconcile_local_tasks`` uses to finish an orphaned row from
  the external work's true terminal state, from any process. NULL for every
  non-LOCAL row and every pre-existing row (a legacy orphan is resolved by
  the reconciler's deterministic-job-name fallback instead).

``ADD COLUMN IF NOT EXISTS`` rather than a bare ``op.add_column``: the app
still bootstraps schema with ``create_all`` at startup, so a database first
touched by an app version carrying this column already has it by the time the
migration Job runs. Same reasoning as ``e5a7c9d10f21``.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c7d1f3a9b2e4"
down_revision: str | Sequence[str] | None = "b4d7e9c02a15"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE hpcrun ADD COLUMN IF NOT EXISTS external_job_ids JSONB")


def downgrade() -> None:
    op.execute("ALTER TABLE hpcrun DROP COLUMN IF EXISTS external_job_ids")
