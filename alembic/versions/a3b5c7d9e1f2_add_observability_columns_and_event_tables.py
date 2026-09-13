"""add observability columns and the event/span tables

Revision ID: a3b5c7d9e1f2
Revises: d7e2f4a6c8b0
Create Date: 2026-09-10

Observability plan (docs/plan-observability.md), viva-api part A/B storage:

* ``hpcrun`` gains ``exit_code``, ``attempt``, ``error_source``, ``trace_id``
  (indexed), ``campaign_span_id``, ``events_s3_prefix``, ``events_cursor``
  (JSONB), ``stage``, ``generation``, ``last_event_at`` -- all nullable, so
  every existing row is untouched.
* ``hpcrun_event``: one structured event per row from a run's task stream
  (``tick`` heartbeats are never stored); unique on ``(trace_id, source, seq)``
  so re-ingesting an ``events.jsonl`` object is idempotent.
* ``hpcrun_span``: the run's trace tree (campaign > parca / lineage / analysis >
  generation > ...), materialised from ``span.start``/``span.end`` events.
* ``jobstatusdb`` is deliberately UNCHANGED. An earlier draft added a
  ``PARTIAL`` label; it was dropped because nothing branched on it (it was a
  terminal-set member and a CLI colour), because Postgres cannot drop an enum
  label once added, and because "which tasks survived" belongs in the per-task
  rows and ``error_message``, not in a status. Keeping the enum fixed also keeps
  this migration free of the deploy-ordering constraint an ``ADD VALUE`` imposes.

**Typed DDL, guarded by the inspector** rather than raw ``CREATE TABLE IF NOT
EXISTS`` strings. Both forms are idempotent; this one is type-checked and states
each column once in the same vocabulary the ORM uses, which is the shape
``d7e2f4a6c8b0`` was corrected to for the same reason. Idempotency is required,
not stylistic: ``create_db``'s ``Base.metadata.create_all`` bootstraps these
tables at app startup, so on any database the app has touched they ALREADY EXIST
by the time Alembic runs -- the normal production shape that ``db_reconcile``'s
LEGACY path produces (stamp, then upgrade head).

A draft of this revision named the event column ``layer``; the rename to
``component`` lives in ``e3a9c1d70b62``, NOT here -- see that revision's
docstring for why a rename inside an already-applied revision can never run.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a3b5c7d9e1f2"
down_revision: str | Sequence[str] | None = "d7e2f4a6c8b0"
# Re-pointed from f76e43d01841 when this branch merged main: viva-api#631 landed
# d7e2f4a6c8b0 (the `task` table) on the SAME parent, so leaving this as-is gave
# TWO alembic heads and `alembic upgrade head` fails outright on a multi-head
# chain. The revision chain must stay linear -- pinned by
# tests/simulation/test_observability_migration.py.
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


#: The columns this revision adds to the existing ``hpcrun`` table, in
#: ``ORMHpcRun`` order. All nullable: existing rows predate observability.
_HPCRUN_COLUMNS: tuple[sa.Column, ...] = (
    sa.Column("exit_code", sa.Integer(), nullable=True),
    sa.Column("attempt", sa.Integer(), nullable=True),
    sa.Column("error_source", sa.String(), nullable=True),
    sa.Column("trace_id", sa.String(), nullable=True),
    sa.Column("campaign_span_id", sa.String(), nullable=True),
    sa.Column("events_s3_prefix", sa.String(), nullable=True),
    sa.Column("events_cursor", postgresql.JSONB(), nullable=True),
    sa.Column("stage", sa.String(), nullable=True),
    sa.Column("generation", sa.Integer(), nullable=True),
    sa.Column("last_event_at", sa.DateTime(), nullable=True),
)


def _existing_columns(inspector: sa.Inspector, table: str) -> set[str]:
    if not inspector.has_table(table):
        return set()
    return {c["name"] for c in inspector.get_columns(table)}


def _existing_indexes(inspector: sa.Inspector, table: str) -> set[str]:
    if not inspector.has_table(table):
        return set()
    return {ix["name"] for ix in inspector.get_indexes(table) if ix.get("name")}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    present = _existing_columns(inspector, "hpcrun")
    for column in _HPCRUN_COLUMNS:
        if column.name not in present:
            # A fresh Column object per add: SQLAlchemy binds a Column to the
            # table it is appended to, so the module-level tuple cannot be
            # reused directly across upgrade() calls.
            op.add_column("hpcrun", column.copy())
    if "ix_hpcrun_trace_id" not in _existing_indexes(inspector, "hpcrun"):
        op.create_index("ix_hpcrun_trace_id", "hpcrun", ["trace_id"])

    if not inspector.has_table("hpcrun_event"):
        op.create_table(
            "hpcrun_event",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("hpcrun_id", sa.Integer(), sa.ForeignKey("hpcrun.id"), nullable=False),
            sa.Column("trace_id", sa.String(), nullable=False),
            # The writer: a Batch job id, a host-pid, or "api".
            sa.Column("source", sa.String(), nullable=False),
            sa.Column("seq", sa.Integer(), nullable=False),
            sa.Column("ts", sa.DateTime(), nullable=False),
            # Free string -- "process_bigraph", "v2ecoli.lineage", ... (plan D1').
            sa.Column("component", sa.String(), nullable=False),
            sa.Column("event", sa.String(), nullable=False),
            sa.Column("level", sa.String(), nullable=False, server_default="info"),
            sa.Column("generation", sa.Integer(), nullable=True),
            sa.Column("global_time", sa.Float(), nullable=True),
            sa.Column("wall_time", sa.Float(), nullable=True),
            sa.Column("span_id", sa.String(), nullable=True),
            sa.Column("parent_span_id", sa.String(), nullable=True),
            sa.Column("payload", postgresql.JSONB(), nullable=True),
            sa.Column("tags", postgresql.JSONB(), nullable=True),
            sa.UniqueConstraint("trace_id", "source", "seq", name="uq_hpcrun_event_trace_source_seq"),
        )
    event_indexes = _existing_indexes(inspector, "hpcrun_event")
    for name, columns in (
        ("ix_hpcrun_event_hpcrun_id", ["hpcrun_id"]),
        ("ix_hpcrun_event_trace_id", ["trace_id"]),
        ("ix_hpcrun_event_trace_span", ["trace_id", "span_id"]),
    ):
        if name not in event_indexes:
            op.create_index(name, "hpcrun_event", columns)

    if not inspector.has_table("hpcrun_span"):
        op.create_table(
            "hpcrun_span",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("hpcrun_id", sa.Integer(), sa.ForeignKey("hpcrun.id"), nullable=False),
            sa.Column("trace_id", sa.String(), nullable=False),
            sa.Column("span_id", sa.String(), nullable=False),
            sa.Column("parent_span_id", sa.String(), nullable=True),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("attrs", postgresql.JSONB(), nullable=True),
            sa.Column("start_ts", sa.DateTime(), nullable=True),
            sa.Column("end_ts", sa.DateTime(), nullable=True),
            # ok | error | unknown
            sa.Column("status", sa.String(), nullable=True),
            sa.Column("error", sa.String(), nullable=True),
            sa.UniqueConstraint("trace_id", "span_id", name="uq_hpcrun_span_trace_span"),
        )
    span_indexes = _existing_indexes(inspector, "hpcrun_span")
    for name, columns in (
        ("ix_hpcrun_span_hpcrun_id", ["hpcrun_id"]),
        ("ix_hpcrun_span_trace_id", ["trace_id"]),
    ):
        if name not in span_indexes:
            op.create_index(name, "hpcrun_span", columns)


def downgrade() -> None:
    """Drop the tables and columns. The enum label stays: Postgres has no DROP
    VALUE (see 44335812e447's downgrade for the same reasoning)."""
    op.drop_table("hpcrun_span", if_exists=True)
    op.drop_table("hpcrun_event", if_exists=True)
    op.drop_index("ix_hpcrun_trace_id", table_name="hpcrun", if_exists=True)
    for column in _HPCRUN_COLUMNS:
        op.drop_column("hpcrun", column.name, if_exists=True)
