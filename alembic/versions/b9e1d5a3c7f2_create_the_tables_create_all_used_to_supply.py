"""create the tables that only ``create_all`` ever created (viva-api#637)

Revision ID: b9e1d5a3c7f2
Revises: c1a2b3d4e5f6
Create Date: 2026-09-19

Nine tables in the models were never created by any migration: ``analysis`` and the eight
``compose_*`` tables. They exist on every real database only because the app runs
``Base.metadata.create_all`` / ``ComposeBase.metadata.create_all`` at startup. So
``alembic upgrade head`` from an EMPTY database died at ``d3f9a1c72b84`` -- the first
revision to touch one of them -- with ``relation "analysis" does not exist``, and
``db_reconcile``'s FRESH path, which promises "'upgrade head' builds it from base", was
false. Dormant while every database was touched by the app first; live the first time a new
site runs the ``alembic-migrate`` Job before the app, which is the documented order.

Decision on #637 (Option 1): make the chain honest. This revision is INSERTED before
``d3f9a1c72b84`` rather than appended at the head, because a from-base run never reaches
the head -- it has to find these tables before the first revision that assumes them.

Two properties matter, and both are tested against a real Postgres:

* **Guarded.** Every create is skipped when the table is already there. A database the
  reconciler adopts as LEGACY is stamped at an early revision and then upgraded THROUGH this
  one, and it already has all nine tables from ``create_all`` -- an unguarded
  ``CREATE TABLE`` would fail on exactly the sites that work today (the ``d7e2f4a6c8b0``
  double-create that #636 fixed). A MANAGED database is already past this revision and never
  runs it.
* **Historical shape, not today's.** Each table is created as it was at THIS point in the
  chain, so that the later revisions still apply: ``analysis`` without the ten columns
  ``d3f9a1c72b84`` adds (and the two ``c9a1e3f5b7d2`` adds); ``compose_hpcrun`` without
  ``job_id_ext`` / ``job_backend`` (``e5a7c9d10f21``); ``compose_simulation`` without
  ``analysis_options`` (``f76e43d01841``). The definitions are written out here, frozen,
  rather than imported from the ORM: a model change must never retroactively change what an
  old revision creates. ``tests/simulation/test_fresh_database_migrations.py`` holds the
  result to account -- the schema this chain builds from empty must equal the schema
  ``create_all`` builds.

Enum labels are the ORM member NAMES (upper case), which is what ``create_all`` emits.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "b9e1d5a3c7f2"
down_revision: str | Sequence[str] | None = "c1a2b3d4e5f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_package_type = postgresql.ENUM("PYPI", "CONDA", name="packagetypedb", create_type=False)
_compute_type = postgresql.ENUM("PROCESS", "STEP", name="bigraphcomputetypedb", create_type=False)
_compose_job_type = postgresql.ENUM("SIMULATION", "BUILD_CONTAINER", name="composejobtypedb", create_type=False)
_compose_job_status = postgresql.ENUM(
    "WAITING",
    "QUEUED",
    "RUNNING",
    "COMPLETED",
    "FAILED",
    "PENDING",
    "CANCELLED",
    "OUT_OF_MEMORY",
    "SUSPENDED",
    "TIMEOUT",
    "UNKNOWN",
    name="composejobstatusdb",
    create_type=False,
)
_ENUMS = (_package_type, _compute_type, _compose_job_type, _compose_job_status)

#: Reverse dependency order, for downgrade.
_TABLES = (
    "compose_worker_event",
    "compose_hpcrun",
    "compose_simulator_to_package",
    "compose_simulation",
    "compose_bigraph_compute",
    "compose_simulator",
    "compose_packages",
    "compose_allow_list",
    "analysis",
)


def _created_at(name: str = "created_at") -> sa.Column[object]:
    return sa.Column(name, sa.DateTime(), nullable=False, server_default=sa.func.now())


def upgrade() -> None:  # noqa: C901 - one guarded create per table; splitting it would only hide the order
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for enum in _ENUMS:
        # create_type=False on the column types + checkfirst here: otherwise create_table emits
        # a second, unguarded CREATE TYPE (the d7e2f4a6c8b0 lesson).
        enum.create(bind, checkfirst=True)

    if not inspector.has_table("analysis"):
        op.create_table(
            "analysis",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("config", postgresql.JSONB(), nullable=False),
            sa.Column("last_updated", sa.String(), nullable=False),
            sa.Column("job_name", sa.String(), nullable=True),
            sa.Column("job_id", sa.Integer(), nullable=True),
        )

    if not inspector.has_table("compose_allow_list"):
        op.create_table(
            "compose_allow_list",
            sa.Column("id", sa.Integer(), primary_key=True),
            _created_at("approved_at"),
            sa.Column("package_name", sa.String(), nullable=False),
            sa.Column("package_type", _package_type, nullable=False),
            sa.Column("package_version", sa.String(), nullable=False),
        )
        op.create_index("ix_compose_allow_list_package_name", "compose_allow_list", ["package_name"])

    if not inspector.has_table("compose_packages"):
        op.create_table(
            "compose_packages",
            sa.Column("id", sa.Integer(), primary_key=True),
            _created_at(),
            sa.Column("package_type", _package_type, nullable=False),
            sa.Column("name", sa.String(), nullable=False),
            sa.UniqueConstraint("name", "package_type", name="uq_compose_package_name_type"),
            sa.UniqueConstraint("name"),
        )

    if not inspector.has_table("compose_simulator"):
        op.create_table(
            "compose_simulator",
            sa.Column("id", sa.Integer(), primary_key=True),
            _created_at(),
            sa.Column("singularity_def", sa.String(), nullable=False),
            sa.Column("singularity_def_hash", sa.String(), nullable=False),
            sa.UniqueConstraint("singularity_def_hash"),
        )

    if not inspector.has_table("compose_bigraph_compute"):
        op.create_table(
            "compose_bigraph_compute",
            sa.Column("id", sa.Integer(), primary_key=True),
            _created_at("inserted_at"),
            sa.Column("package_ref", sa.Integer(), sa.ForeignKey("compose_packages.id"), nullable=False),
            sa.Column("module", sa.String(), nullable=False),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("compute_type", _compute_type, nullable=False),
            sa.Column("inputs", sa.String(), nullable=True),
            sa.Column("outputs", sa.String(), nullable=True),
        )
        op.create_index("ix_compose_bigraph_compute_package_ref", "compose_bigraph_compute", ["package_ref"])

    if not inspector.has_table("compose_simulation"):
        op.create_table(
            "compose_simulation",
            sa.Column("id", sa.Integer(), primary_key=True),
            _created_at(),
            sa.Column("experiment_id", sa.String(), nullable=False),
            sa.Column("simulator_id", sa.Integer(), sa.ForeignKey("compose_simulator.id"), nullable=False),
            sa.Column("document", sa.String(), nullable=True),
            sa.UniqueConstraint("experiment_id"),
        )
        op.create_index("ix_compose_simulation_simulator_id", "compose_simulation", ["simulator_id"])

    if not inspector.has_table("compose_simulator_to_package"):
        op.create_table(
            "compose_simulator_to_package",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("simulator_id", sa.Integer(), sa.ForeignKey("compose_simulator.id"), nullable=False),
            sa.Column("package_id", sa.Integer(), sa.ForeignKey("compose_packages.id"), nullable=False),
        )
        op.create_index("ix_compose_simulator_to_package_package_id", "compose_simulator_to_package", ["package_id"])
        op.create_index(
            "ix_compose_simulator_to_package_simulator_id", "compose_simulator_to_package", ["simulator_id"]
        )

    if not inspector.has_table("compose_hpcrun"):
        op.create_table(
            "compose_hpcrun",
            sa.Column("id", sa.Integer(), primary_key=True),
            _created_at(),
            sa.Column("job_type", _compose_job_type, nullable=False),
            sa.Column("correlation_id", sa.String(), nullable=False),
            sa.Column("slurmjobid", sa.Integer(), nullable=True),
            sa.Column("start_time", sa.DateTime(), nullable=True),
            sa.Column("end_time", sa.DateTime(), nullable=True),
            sa.Column("status", _compose_job_status, nullable=False),
            sa.Column("error_message", sa.String(), nullable=True),
            sa.Column("simulation_id", sa.Integer(), sa.ForeignKey("compose_simulation.id"), nullable=True),
            sa.Column("simulator_id", sa.Integer(), sa.ForeignKey("compose_simulator.id"), nullable=True),
        )
        # UNIQUE, and created before compose_worker_event: that table's foreign key targets it.
        op.create_index("ix_compose_hpcrun_correlation_id", "compose_hpcrun", ["correlation_id"], unique=True)
        op.create_index("ix_compose_hpcrun_simulation_id", "compose_hpcrun", ["simulation_id"])
        op.create_index("ix_compose_hpcrun_simulator_id", "compose_hpcrun", ["simulator_id"])

    if not inspector.has_table("compose_worker_event"):
        op.create_table(
            "compose_worker_event",
            sa.Column("id", sa.Integer(), primary_key=True),
            _created_at(),
            sa.Column(
                "correlation_id",
                sa.String(),
                sa.ForeignKey("compose_hpcrun.correlation_id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("sequence_number", sa.Integer(), nullable=False),
            sa.Column("mass", postgresql.JSONB(), nullable=False),
            sa.Column("time", sa.Float(), nullable=True),
            sa.Column(
                "hpcrun_id", sa.Integer(), sa.ForeignKey("compose_hpcrun.id", ondelete="CASCADE"), nullable=False
            ),
        )
        op.create_index("ix_compose_worker_event_hpcrun_id", "compose_worker_event", ["hpcrun_id"])
        op.create_index("ix_compose_worker_event_sequence_number", "compose_worker_event", ["sequence_number"])


def downgrade() -> None:
    """Drops the nine tables and the four compose enums -- and with them their DATA. That is
    what undoing a create means; it is only ever the right thing on a database this chain
    built. On a ``create_all`` database the tables predate this revision: do not downgrade
    through it there."""
    inspector = sa.inspect(op.get_bind())
    for table in _TABLES:
        if inspector.has_table(table):
            op.drop_table(table)
    for enum in _ENUMS:
        enum.drop(op.get_bind(), checkfirst=True)
