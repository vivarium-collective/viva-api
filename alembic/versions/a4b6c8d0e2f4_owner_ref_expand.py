"""owner-ref expand: (owner_kind, owner_id) on hpcrun and dataset, beside the foreign keys

Revision ID: a4b6c8d0e2f4
Revises: f4c8a2e6d0b3
Create Date: 2026-09-25

Plan P4a (``docs/plan-core.md``): the first brick of P4. A run and a dataset each say who they
belong to as ONE ``(owner_kind, owner_id)`` pair -- the shape core's ``Job`` record has -- instead
of a foreign key per owning table. The foreign keys stay, and stay authoritative, until the P7
collapse to ``core.job``; the new columns are dual-written from now on and backfilled here from
the keys with the same rule the writer uses (``viva_api/simulation/owner_ref.py``): the owning
table's name and the id as a string.

``owner_kind`` is a VARCHAR, deliberately not an enum: a new kind is a row, not a migration, which
is what keeps this step reversible (the P4b row of the plan). ``hpcrun.output_uri``,
``dataset.producer_job_id`` and ``dataset.trace_id`` are added now, NULL, for the writers that
follow (P4b's adapters and the ingest hook).

Additive, idempotent (``IF NOT EXISTS``), and fully reversible: dropping the columns loses only
what the foreign keys still hold.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "a4b6c8d0e2f4"
down_revision: str | Sequence[str] | None = "f4c8a2e6d0b3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE hpcrun ADD COLUMN IF NOT EXISTS owner_kind VARCHAR")
    op.execute("ALTER TABLE hpcrun ADD COLUMN IF NOT EXISTS owner_id VARCHAR")
    op.execute("ALTER TABLE hpcrun ADD COLUMN IF NOT EXISTS output_uri VARCHAR")
    op.execute("CREATE INDEX IF NOT EXISTS ix_hpcrun_owner_kind ON hpcrun (owner_kind)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_hpcrun_owner_id ON hpcrun (owner_id)")

    op.execute("ALTER TABLE dataset ADD COLUMN IF NOT EXISTS owner_kind VARCHAR")
    op.execute("ALTER TABLE dataset ADD COLUMN IF NOT EXISTS owner_id VARCHAR")
    op.execute("ALTER TABLE dataset ADD COLUMN IF NOT EXISTS producer_job_id INTEGER REFERENCES hpcrun (id)")
    op.execute("ALTER TABLE dataset ADD COLUMN IF NOT EXISTS trace_id VARCHAR")
    op.execute("CREATE INDEX IF NOT EXISTS ix_dataset_owner_kind ON dataset (owner_kind)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_dataset_owner_id ON dataset (owner_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_dataset_producer_job_id ON dataset (producer_job_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_dataset_trace_id ON dataset (trace_id)")

    # Backfill from the foreign keys, with the writer's rule. Only rows with no owner yet, so a
    # re-run (the reconciler is idempotent) never overwrites what the dual-write recorded.
    op.execute(
        "UPDATE hpcrun SET owner_kind = 'simulation', owner_id = jobref_simulation_id::varchar "
        "WHERE owner_kind IS NULL AND jobref_simulation_id IS NOT NULL"
    )
    op.execute(
        "UPDATE hpcrun SET owner_kind = 'parca_dataset', owner_id = jobref_parca_dataset_id::varchar "
        "WHERE owner_kind IS NULL AND jobref_parca_dataset_id IS NOT NULL"
    )
    op.execute(
        "UPDATE hpcrun SET owner_kind = 'simulator', owner_id = jobref_simulator_id::varchar "
        "WHERE owner_kind IS NULL AND jobref_simulator_id IS NOT NULL"
    )
    op.execute(
        "UPDATE hpcrun SET owner_kind = 'analysis', owner_id = jobref_analysis_id::varchar "
        "WHERE owner_kind IS NULL AND jobref_analysis_id IS NOT NULL"
    )
    # A dataset names up to three producers; the first present wins, in the order the table has
    # always preferred them (simulation, then ParCa dataset, then analysis).
    op.execute(
        "UPDATE dataset SET owner_kind = 'simulation', owner_id = simulation_id::varchar "
        "WHERE owner_kind IS NULL AND simulation_id IS NOT NULL"
    )
    op.execute(
        "UPDATE dataset SET owner_kind = 'parca_dataset', owner_id = parca_dataset_id::varchar "
        "WHERE owner_kind IS NULL AND parca_dataset_id IS NOT NULL"
    )
    op.execute(
        "UPDATE dataset SET owner_kind = 'analysis', owner_id = analysis_id::varchar "
        "WHERE owner_kind IS NULL AND analysis_id IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_dataset_trace_id")
    op.execute("DROP INDEX IF EXISTS ix_dataset_producer_job_id")
    op.execute("DROP INDEX IF EXISTS ix_dataset_owner_id")
    op.execute("DROP INDEX IF EXISTS ix_dataset_owner_kind")
    op.execute("ALTER TABLE dataset DROP COLUMN IF EXISTS trace_id")
    op.execute("ALTER TABLE dataset DROP COLUMN IF EXISTS producer_job_id")
    op.execute("ALTER TABLE dataset DROP COLUMN IF EXISTS owner_id")
    op.execute("ALTER TABLE dataset DROP COLUMN IF EXISTS owner_kind")
    op.execute("DROP INDEX IF EXISTS ix_hpcrun_owner_id")
    op.execute("DROP INDEX IF EXISTS ix_hpcrun_owner_kind")
    op.execute("ALTER TABLE hpcrun DROP COLUMN IF EXISTS output_uri")
    op.execute("ALTER TABLE hpcrun DROP COLUMN IF EXISTS owner_id")
    op.execute("ALTER TABLE hpcrun DROP COLUMN IF EXISTS owner_kind")
