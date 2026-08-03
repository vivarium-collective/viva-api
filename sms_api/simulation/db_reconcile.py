"""Self-diagnosing Alembic reconciliation for customer-controlled deployments.

Historically the app bootstrapped its schema with ``Base.metadata.create_all``
at startup (see ``tables_orm.create_db``). ``create_all`` creates *missing*
tables/types but never *alters* existing ones, and it never writes an
``alembic_version`` row. Any environment where the app pod touches the database
before the migration job therefore ends up in a state Alembic cannot manage:

* tables exist, but ``alembic_version`` is absent, so ``alembic upgrade head``
  runs from base and fails re-``CREATE``-ing tables that already exist; and
* enums/columns added by later migrations may be missing (a frozen enum, a
  never-dropped column), because ``create_all`` could not alter them.

Because the final deployment is customer-controlled we cannot remotely curate
each database. This module inspects *any* database state and does the correct,
idempotent thing:

======================  ===========================================  ==============================================
State                   Signal                                       Action
======================  ===========================================  ==============================================
``FRESH``               no app tables, no ``alembic_version``         ``upgrade head`` (base creates everything)
``MANAGED``             ``alembic_version`` present                  ``upgrade head`` (normal path, no-op if current)
``LEGACY``              app tables exist, no ``alembic_version``      ``stamp <matched>`` then ``upgrade head``
``INCONSISTENT``        fingerprints non-linear (a gap)              refuse; print manual instructions
======================  ===========================================  ==============================================

The ``LEGACY`` match is found by walking a small, ordered fingerprint table —
one detectable schema marker per revision that predates Alembic adoption. That
table is frozen at the head which existed when adoption was introduced: once a
database is stamped it is ``alembic_version``-managed forever, so migrations
added *after* adoption never need a new fingerprint.

Wire the migration Job to ``python -m sms_api.simulation.db_reconcile --apply``
(or ``scripts/db_reconcile.py``) so it is correct for fresh installs, our own
drifted databases, and already-managed ones on a single code path. Run
``--analyze`` (or ``scripts/db_analyze.py``) first for a read-only report.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import enum
import os
import sys
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

# Repo root = two levels up from sms_api/simulation/db_reconcile.py
REPO_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_INI = REPO_ROOT / "alembic.ini"

# Ordered markers, one per revision that predates Alembic adoption, used ONLY to
# adopt un-stamped (create_all-bootstrapped) databases. Each entry is
# ``(revision, human label, async predicate)``. FROZEN at the adoption-era head:
# never add entries for migrations authored after adoption — those databases
# already carry an ``alembic_version`` row and take the MANAGED path.


async def _table_exists(conn: AsyncConnection, name: str) -> bool:
    q = text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :n)")
    return bool((await conn.execute(q, {"n": name})).scalar())


async def _column_exists(conn: AsyncConnection, table: str, column: str) -> bool:
    q = text("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = :t AND column_name = :c)")
    return bool((await conn.execute(q, {"t": table, "c": column})).scalar())


async def _enum_has_value(conn: AsyncConnection, enum_name: str, value: str) -> bool:
    q = text(
        "SELECT EXISTS (SELECT 1 FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid "
        "WHERE t.typname = :t AND e.enumlabel = :v)"
    )
    return bool((await conn.execute(q, {"t": enum_name, "v": value})).scalar())


async def _marker_baseline(conn: AsyncConnection) -> bool:
    return await _table_exists(conn, "simulation")


async def _marker_hpcrun_k8s(conn: AsyncConnection) -> bool:
    has_new = await _column_exists(conn, "hpcrun", "job_id_ext")
    has_old = await _column_exists(conn, "hpcrun", "slurmjobid")
    return has_new and not has_old


async def _marker_jobstatus_cancelled(conn: AsyncConnection) -> bool:
    return await _enum_has_value(conn, "jobstatusdb", "cancelled")


async def _marker_simulation_tags(conn: AsyncConnection) -> bool:
    return await _column_exists(conn, "simulation", "tags")


async def _marker_analysis_query_columns(conn: AsyncConnection) -> bool:
    return await _column_exists(conn, "analysis", "n_tp")


async def _marker_compose_hpcrun_job_id_ext(conn: AsyncConnection) -> bool:
    return await _column_exists(conn, "compose_hpcrun", "job_id_ext")


# (revision, human-readable marker description, async predicate)
# One marker per revision reachable by a legacy create_all database. New entries
# are needed ONLY while create_all still bootstraps prod DBs (see module docstring):
# a create_all DB advanced past the top marker would stamp stale and re-apply an
# already-made migration. Guarding create_all off in prod freezes this list.
LEGACY_FINGERPRINTS: list[tuple[str, str]] = [
    ("fb7621a73e24", "baseline: table 'simulation' exists"),
    ("0f991fad32ba", "hpcrun.job_id_ext present and hpcrun.slurmjobid dropped"),
    ("a1c3e5f7b9d2", "enum jobstatusdb has value 'cancelled'"),
    ("c1a2b3d4e5f6", "simulation.tags column exists"),
    ("d3f9a1c72b84", "analysis.n_tp column exists"),
    ("e5a7c9d10f21", "compose_hpcrun.job_id_ext column exists"),
]
_LEGACY_PREDICATES = [
    _marker_baseline,
    _marker_hpcrun_k8s,
    _marker_jobstatus_cancelled,
    _marker_simulation_tags,
    _marker_analysis_query_columns,
    _marker_compose_hpcrun_job_id_ext,
]


class DbState(enum.Enum):
    FRESH = "fresh"
    MANAGED = "managed"
    LEGACY = "legacy"
    INCONSISTENT = "inconsistent"


@dataclasses.dataclass
class Diagnosis:
    """The result of inspecting a database against the migration history."""

    state: DbState
    head_revision: str | None
    current_revision: str | None  # from alembic_version, when MANAGED
    matched_revision: str | None  # stamp target, when LEGACY
    markers: list[tuple[str, bool]]  # (label, present) for every legacy fingerprint
    message: str

    @property
    def needs_stamp(self) -> bool:
        return self.state is DbState.LEGACY

    @property
    def can_upgrade(self) -> bool:
        return self.state in (DbState.FRESH, DbState.MANAGED, DbState.LEGACY)


def classify(
    *,
    alembic_revision: str | None,
    fingerprint: list[bool],
    head_revision: str | None,
    legacy_fingerprints: list[tuple[str, str]] = LEGACY_FINGERPRINTS,
) -> Diagnosis:
    """Pure state-machine over inspection facts (no database access — unit-tested).

    ``fingerprint[i]`` is whether the marker for ``legacy_fingerprints[i]`` is
    present. ``alembic_revision`` is the value in ``alembic_version`` (or None).
    """
    labels = [label for _, label in legacy_fingerprints]
    revs = [rev for rev, _ in legacy_fingerprints]
    markers = list(zip(labels, fingerprint, strict=True))

    if alembic_revision is not None:
        return Diagnosis(
            state=DbState.MANAGED,
            head_revision=head_revision,
            current_revision=alembic_revision,
            matched_revision=None,
            markers=markers,
            message=(
                f"Database is Alembic-managed at revision {alembic_revision}. "
                f"'upgrade head' will apply any pending migrations."
            ),
        )

    # No alembic_version row. With no markers at all the database is empty —
    # let 'upgrade head' build everything from base.
    if not any(fingerprint):
        return Diagnosis(
            state=DbState.FRESH,
            head_revision=head_revision,
            current_revision=None,
            matched_revision=None,
            markers=markers,
            message=(
                "No application tables and no alembic_version — fresh database. 'upgrade head' builds it from base."
            ),
        )

    # Legacy (create_all-bootstrapped): walk the fingerprint. A valid legacy
    # database has all-True markers followed by all-False; a True after a False
    # is a non-linear gap (e.g. baseline table missing while a later marker is
    # present) we refuse to guess about.
    matched_idx = -1
    seen_false = False
    for i, present in enumerate(fingerprint):
        if present:
            if seen_false:
                return Diagnosis(
                    state=DbState.INCONSISTENT,
                    head_revision=head_revision,
                    current_revision=None,
                    matched_revision=None,
                    markers=markers,
                    message=(
                        f"Inconsistent schema: marker for {revs[i]} is present but an earlier marker is missing. "
                        f"The database does not match any single revision; manual reconciliation required."
                    ),
                )
            matched_idx = i
        else:
            seen_false = True

    matched = revs[matched_idx]
    return Diagnosis(
        state=DbState.LEGACY,
        head_revision=head_revision,
        current_revision=None,
        matched_revision=matched,
        markers=markers,
        message=(
            f"Legacy create_all database (no alembic_version). Schema matches revision {matched}. "
            f"Will 'stamp {matched}' then 'upgrade head'."
        ),
    )


def _normalize_async_url(url: str) -> str:
    """Ensure an asyncpg driver so the same URL works for the async engine and Alembic."""
    if url.startswith("postgresql+asyncpg://"):
        return url
    for bare in ("postgresql://", "postgres://"):
        if url.startswith(bare):
            return "postgresql+asyncpg://" + url[len(bare) :]
    return url


def resolve_database_url() -> str:
    """Resolve the connection URL, preferring SQLALCHEMY_DATABASE_URL, else POSTGRES_*.

    Also writes the normalized URL back to ``SQLALCHEMY_DATABASE_URL`` so Alembic's
    ``env.py`` (which reads that variable) uses the identical connection.
    """
    url = os.environ.get("SQLALCHEMY_DATABASE_URL")
    if not url:
        missing = [
            name
            for name in ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DATABASE")
            if not os.environ.get(name)
        ]
        if missing:
            raise RuntimeError(
                "No database connection configured. Set SQLALCHEMY_DATABASE_URL, or all of: " + ", ".join(missing)
            )
        user = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]
        host = os.environ["POSTGRES_HOST"]
        port = os.environ["POSTGRES_PORT"]
        database = os.environ["POSTGRES_DATABASE"]
        url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
    url = _normalize_async_url(url)
    os.environ["SQLALCHEMY_DATABASE_URL"] = url
    return url


def _alembic_config(url: str) -> Config:
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def _head_revision(cfg: Config) -> str | None:
    return ScriptDirectory.from_config(cfg).get_current_head()


async def _inspect(url: str, cfg: Config) -> Diagnosis:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            if await _table_exists(conn, "alembic_version"):
                current = (await conn.execute(text("SELECT version_num FROM alembic_version LIMIT 1"))).scalar()
                alembic_revision: str | None = str(current) if current is not None else None
            else:
                alembic_revision = None
            fingerprint = [await predicate(conn) for predicate in _LEGACY_PREDICATES]
    finally:
        await engine.dispose()
    return classify(alembic_revision=alembic_revision, fingerprint=fingerprint, head_revision=_head_revision(cfg))


def diagnose(url: str, cfg: Config) -> Diagnosis:
    """Inspect the database and return a Diagnosis (read-only)."""
    return asyncio.run(_inspect(url, cfg))


def render_report(diag: Diagnosis) -> str:
    lines = [
        "Database reconciliation report",
        "------------------------------",
        f"  state:            {diag.state.value}",
        f"  head revision:    {diag.head_revision}",
        f"  current revision: {diag.current_revision if diag.current_revision else '(none — no alembic_version)'}",
    ]
    if diag.matched_revision:
        lines.append(f"  matched revision: {diag.matched_revision}  (stamp target)")
    lines.append("  schema markers:")
    for label, present in diag.markers:
        lines.append(f"    [{'x' if present else ' '}] {label}")
    lines.append("")
    lines.append(diag.message)
    return "\n".join(lines)


def _manual_instructions(diag: Diagnosis) -> str:
    return (
        "\nManual reconciliation required — the tool made NO changes.\n"
        "The schema does not correspond to a single known revision. Inspect it and, once you have\n"
        "determined the revision it truly matches, adopt it explicitly:\n\n"
        "    uv run alembic stamp <revision>   # the rev the schema matches; creates alembic_version\n"
        "    uv run alembic upgrade head        # apply the remaining migrations\n\n"
        f"Detected markers: {diag.markers}\n"
    )


def apply(url: str, cfg: Config, diag: Diagnosis) -> int:
    """Perform the reconciliation implied by ``diag``. Idempotent. Returns an exit code."""
    if diag.state is DbState.INCONSISTENT:
        print(_manual_instructions(diag), file=sys.stderr)
        return 2

    if diag.state is DbState.LEGACY and diag.matched_revision is not None:
        print(f"→ stamping database at {diag.matched_revision} (adopting create_all schema into Alembic)")
        command.stamp(cfg, diag.matched_revision)

    print("→ running 'alembic upgrade head'")
    command.upgrade(cfg, "head")
    print("✓ database reconciled and upgraded to head")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="db_reconcile",
        description="Diagnose and (optionally) reconcile the database's Alembic state.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--analyze", action="store_true", help="Read-only: print the diagnosis and exit (default).")
    group.add_argument("--apply", action="store_true", help="Stamp/upgrade the database as diagnosed (mutating).")
    args = parser.parse_args(argv)

    try:
        url = resolve_database_url()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    cfg = _alembic_config(url)
    diag = diagnose(url, cfg)
    print(render_report(diag))

    if not args.apply:
        # Read-only mode: signal INCONSISTENT via exit code so automation can gate.
        return 2 if diag.state is DbState.INCONSISTENT else 0

    print()
    return apply(url, cfg, diag)


if __name__ == "__main__":
    raise SystemExit(main())
