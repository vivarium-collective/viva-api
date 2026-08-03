"""Read-only schema drift check: live database vs the current ORM.

The migration reconciler (``db_reconcile.py``) fixes the *Alembic-tracked* delta.
This tool catches the other gap: schema the ORM expects that neither Alembic nor
``create_all`` will supply on an existing database. That matters most when a
database has jumped many app versions (e.g. prod 0.7.2 → 0.9.20), because
``create_all`` only creates *missing tables* — it never adds a column to an
existing table, and most historical schema changes were made via ``create_all``,
not migrations.

Findings, by severity:

* **BLOCKING** — a column the ORM expects is missing from a table that already
  exists, or an enum value the ORM expects is missing. ``create_all`` will not
  add these; the app will error querying them. Needs a migration or manual DDL
  before deploying.
* **INFO** — a table the ORM expects is entirely absent (``create_all`` creates
  it on boot), or the database has extra tables/columns the ORM no longer knows
  about (usually harmless legacy).

Connection is resolved exactly like the reconciler (``SQLALCHEMY_DATABASE_URL``
or ``POSTGRES_*``). This tool NEVER writes. Run it before a big-jump deploy:

    uv run python -m viva_api.simulation.schema_diff      # or scripts/db_schema_diff.py

Exit code 0 = no blocking drift; 2 = blocking drift found.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import sys

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from viva_api.simulation.db_reconcile import resolve_database_url

# Tables that legitimately exist in the database but are not ORM-declared app
# schema, so they must not be reported as drift.
_IGNORED_TABLES = frozenset({"alembic_version"})


@dataclasses.dataclass
class DbSchema:
    """A schema snapshot: table -> set of column names, and enum type -> set of labels (lowercased)."""

    tables: dict[str, set[str]]
    enums: dict[str, set[str]]
    # enum type name -> tables whose columns use it (populated for the ORM side only).
    # Used to tell "blocking" enum drift (a live table uses it) from enums that
    # simply arrive with a not-yet-created table via create_all.
    enum_tables: dict[str, set[str]] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class SchemaDiff:
    missing_tables: list[str]  # expected by ORM, absent in DB (create_all will create — INFO)
    missing_columns: dict[str, list[str]]  # existing table -> columns the ORM expects but DB lacks (BLOCKING)
    missing_enum_values: dict[str, list[str]]  # existing enum -> labels the ORM expects but DB lacks (BLOCKING)
    missing_enum_types: list[str]  # enum types absent AND used by an EXISTING table (BLOCKING)
    pending_enum_types: list[str]  # enum types absent but used only by missing tables (create_all creates — INFO)
    extra_tables: list[str]  # in DB, not in ORM (INFO)
    extra_columns: dict[str, list[str]]  # existing table -> columns in DB the ORM no longer declares (INFO)

    @property
    def has_blocking_drift(self) -> bool:
        return bool(self.missing_columns or self.missing_enum_values or self.missing_enum_types)


def _expected_schema_from_orm() -> DbSchema:
    """Build the expected schema from the ORM metadata (no database access)."""
    from viva_api.simulation.tables_orm import Base

    metadatas: list[sa.MetaData] = [Base.metadata]
    try:  # compose subsystem tables live under a separate declarative base
        from viva_api.compose.tables_orm import ComposeBase

        metadatas.append(ComposeBase.metadata)
    except ImportError:
        pass

    tables: dict[str, set[str]] = {}
    enums: dict[str, set[str]] = {}
    enum_tables: dict[str, set[str]] = {}
    for metadata in metadatas:
        for table in metadata.tables.values():
            tables[table.name] = {col.name for col in table.columns}
            for col in table.columns:
                col_type = col.type
                if isinstance(col_type, sa.Enum) and col_type.name:
                    # Compare case-insensitively: the ORM enum labels are the Python
                    # member names (e.g. WAITING) while some DB labels are lowercased
                    # values (e.g. 'cancelled'); lowercasing both avoids false positives.
                    enums[col_type.name] = {label.lower() for label in col_type.enums}
                    enum_tables.setdefault(col_type.name, set()).add(table.name)
    return DbSchema(tables=tables, enums=enums, enum_tables=enum_tables)


def _collect_db_schema(sync_conn: Connection) -> DbSchema:
    """Reflect the live database schema (runs inside conn.run_sync)."""
    inspector = sa.inspect(sync_conn)
    tables: dict[str, set[str]] = {}
    for table_name in inspector.get_table_names():
        tables[table_name] = {col["name"] for col in inspector.get_columns(table_name)}

    enums: dict[str, set[str]] = {}
    get_enums = getattr(inspector, "get_enums", None)  # PostgreSQL-only
    if get_enums is not None:
        for enum in get_enums():
            enums[enum["name"]] = {label.lower() for label in enum["labels"]}
    return DbSchema(tables=tables, enums=enums)


async def _reflect_db(url: str) -> DbSchema:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            return await conn.run_sync(_collect_db_schema)
    finally:
        await engine.dispose()


def diff_schemas(expected: DbSchema, actual: DbSchema) -> SchemaDiff:
    """Pure comparison of expected (ORM) vs actual (DB) schema — unit-tested."""
    missing_tables = sorted(t for t in expected.tables if t not in actual.tables)
    extra_tables = sorted(t for t in actual.tables if t not in expected.tables and t not in _IGNORED_TABLES)

    missing_columns: dict[str, list[str]] = {}
    extra_columns: dict[str, list[str]] = {}
    for table, expected_cols in expected.tables.items():
        if table not in actual.tables:
            continue  # whole table missing — reported under missing_tables (create_all handles it)
        actual_cols = actual.tables[table]
        missing = sorted(expected_cols - actual_cols)
        extra = sorted(actual_cols - expected_cols)
        if missing:
            missing_columns[table] = missing
        if extra:
            extra_columns[table] = extra

    # A missing enum type only blocks if a table that already EXISTS uses it; if
    # its only users are missing tables, create_all creates the type with them.
    def _used_by_existing_table(enum_name: str) -> bool:
        users = expected.enum_tables.get(enum_name, set())
        return any(t in actual.tables for t in users) if users else True

    missing_enum_types: list[str] = []
    pending_enum_types: list[str] = []
    for name in sorted(name for name in expected.enums if name not in actual.enums):
        (missing_enum_types if _used_by_existing_table(name) else pending_enum_types).append(name)

    missing_enum_values: dict[str, list[str]] = {}
    for name, expected_labels in expected.enums.items():
        if name not in actual.enums:
            continue  # whole type missing — reported under missing/pending_enum_types
        missing_labels = sorted(expected_labels - actual.enums[name])
        if missing_labels:
            missing_enum_values[name] = missing_labels

    return SchemaDiff(
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        missing_enum_values=missing_enum_values,
        missing_enum_types=missing_enum_types,
        pending_enum_types=pending_enum_types,
        extra_tables=extra_tables,
        extra_columns=extra_columns,
    )


def render_report(diff: SchemaDiff) -> str:
    lines = ["Schema drift report (live database vs ORM)", "==========================================", ""]

    lines.append("BLOCKING — create_all/Alembic will NOT supply these; migrate or DDL before deploy:")
    if not diff.has_blocking_drift:
        lines.append("  (none)")
    if diff.missing_columns:
        lines.append("  Missing columns on existing tables:")
        for table, cols in sorted(diff.missing_columns.items()):
            lines.append(f"    {table}: {', '.join(cols)}")
    if diff.missing_enum_types:
        lines.append(f"  Missing enum types: {', '.join(diff.missing_enum_types)}")
    if diff.missing_enum_values:
        lines.append("  Missing enum values:")
        for name, labels in sorted(diff.missing_enum_values.items()):
            lines.append(f"    {name}: {', '.join(labels)}")

    lines.append("")
    lines.append("INFO — expected by ORM but absent (create_all creates these on boot):")
    lines.append(f"  Missing tables: {', '.join(diff.missing_tables) if diff.missing_tables else '(none)'}")
    if diff.pending_enum_types:
        lines.append(f"  Enum types for missing tables: {', '.join(diff.pending_enum_types)}")
    lines.append("")
    lines.append("INFO — present in DB but not declared by the ORM (usually harmless legacy):")
    lines.append(f"  Extra tables: {', '.join(diff.extra_tables) if diff.extra_tables else '(none)'}")
    if diff.extra_columns:
        lines.append("  Extra columns:")
        for table, cols in sorted(diff.extra_columns.items()):
            lines.append(f"    {table}: {', '.join(cols)}")

    lines.append("")
    lines.append(
        "RESULT: BLOCKING DRIFT — resolve, OR confirm each item is added by a pending Alembic\n"
        "        migration (cross-check `db_reconcile.py --analyze`) before deploying."
        if diff.has_blocking_drift
        else "RESULT: no blocking drift."
    )
    lines.append("(Presence only — no column-type comparison. Items a pending migration adds are expected here.)")
    return "\n".join(lines)


def diagnose(url: str) -> SchemaDiff:
    actual = asyncio.run(_reflect_db(url))
    return diff_schemas(_expected_schema_from_orm(), actual)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="db_schema_diff",
        description="Read-only: compare the live database schema against the current ORM.",
    )
    parser.parse_args(argv)

    try:
        url = resolve_database_url()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    diff = diagnose(url)
    print(render_report(diff))
    return 2 if diff.has_blocking_drift else 0


if __name__ == "__main__":
    raise SystemExit(main())
