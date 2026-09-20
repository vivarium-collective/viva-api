"""Data provenance slice 1 migrations, exercised against the REAL Alembic chain.

``b2f6d8e0a4c7`` adds the ``ANALYSIS`` label to ``jobtypedb``; ``c9a1e3f5b7d2`` adds
the ``dataset`` table, ``analysis.source`` / ``analysis.tags`` and
``hpcrun.jobref_analysis_id`` (docs/plan-data-provenance.md §6).

Every database here is a throwaway ``postgres:15`` testcontainer. Nothing connects
to a hosted database.

The production shapes rehearsed, because each has bitten this repo before:

* **MANAGED** (both Stanford sites): stamped at the previous head, rows present,
  ``upgrade head`` is the migration Job's only action. Includes writing the new
  enum label in a later transaction -- the thing Postgres forbids inside the
  transaction that added it.
* **LEGACY, current ORM**: ``create_all`` already built every object; the migration
  must pass through as a no-op, and ``db_reconcile`` must stamp at the new head.
* **LEGACY, previous release**: a ``create_all`` database from before this work
  (old enum labels, no new objects). ``db_reconcile`` must stamp at
  ``e3a9c1d70b62`` and ``upgrade head`` must add everything.
* **create_all and the migration agree on types, defaults, indexes and
  constraints** -- not just column names (see the precedent in
  ``test_observability_migration`` for why names alone lie).
* downgrade / upgrade round trip.
"""

import asyncio
from collections.abc import Generator
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from alembic import command
from tests.docker_utils import SKIP_DOCKER_REASON, SKIP_DOCKER_TESTS
from tests.simulation.test_jobstatusdb_cancel_migration import _migrate_to, _shim_missing_hpcrun_columns
from viva_api.compose.tables_orm import ComposeBase
from viva_api.simulation import db_reconcile
from viva_api.simulation.db_reconcile import DbState, _alembic_config
from viva_api.simulation.tables_orm import Base

PRE_REVISION = "e3a9c1d70b62"
JOBTYPE_REVISION = "b2f6d8e0a4c7"
DATASET_REVISION = "c9a1e3f5b7d2"
#: `upgrade head` and the reconciler go past the dataset revision now; this is where they stop.
HEAD_REVISION = "f4c8a2e6d0b3"

_NEW_COLUMNS = (("analysis", "source"), ("analysis", "tags"), ("hpcrun", "jobref_analysis_id"))
_NEW_INDEXES = ("ix_analysis_tags", "ix_hpcrun_jobref_analysis_id")


@pytest.fixture(scope="function")
def fresh_postgres_url() -> Generator[str]:
    """A dedicated, empty Postgres per test (same shape as the precedent files)."""
    if SKIP_DOCKER_TESTS:
        pytest.skip(SKIP_DOCKER_REASON)
    with PostgresContainer("postgres:15") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql+asyncpg://")


# ---------------------------------------------------------------------------
# helpers (each opens and disposes its own engine, so no transaction spans them)
# ---------------------------------------------------------------------------


async def _fetch(url: str, sql: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            return [tuple(r) for r in (await conn.execute(text(sql), params or {})).all()]
    finally:
        await engine.dispose()


async def _execute(url: str, *statements: str) -> None:
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            for sql in statements:
                await conn.execute(text(sql))
    finally:
        await engine.dispose()


async def _create_all(url: str) -> None:
    """What app startup does: ``create_db`` (Base) and ``create_compose_db``
    (ComposeBase, a separate metadata). Two fingerprint markers check compose
    tables, so a Base-only database is not a shape production ever has."""
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.run_sync(ComposeBase.metadata.create_all)
    finally:
        await engine.dispose()


async def _shim_hpcrun(url: str) -> None:
    """The migration chain's ``hpcrun`` predates several ORM columns that only
    ``create_all`` ever added (pre-existing, unrelated gap; same shim the precedent
    migration tests use)."""
    engine = create_async_engine(url)
    try:
        await _shim_missing_hpcrun_columns(engine)
    finally:
        await engine.dispose()


async def _version(url: str) -> str | None:
    rows = await _fetch(url, "SELECT version_num FROM alembic_version")
    return str(rows[0][0]) if rows else None


async def _jobtype_labels(url: str) -> set[str]:
    rows = await _fetch(
        url,
        "SELECT e.enumlabel FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid WHERE t.typname = 'jobtypedb'",
    )
    return {r[0] for r in rows}


async def _column_facts(url: str) -> dict[tuple[str, str], tuple[Any, ...]]:
    """(table, column) -> (data_type, is_nullable, column_default) for everything this
    migration owns: all of ``dataset`` plus the three added columns."""
    rows = await _fetch(
        url,
        "SELECT table_name, column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns WHERE table_name IN ('dataset', 'analysis', 'hpcrun')",
    )
    owned = set(_NEW_COLUMNS)
    return {(r[0], r[1]): (r[2], r[3], r[4]) for r in rows if r[0] == "dataset" or (r[0], r[1]) in owned}


async def _index_defs(url: str) -> dict[str, str]:
    rows = await _fetch(
        url,
        "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'dataset' OR indexname = ANY(:names)",
        {"names": list(_NEW_INDEXES)},
    )
    return {r[0]: r[1] for r in rows}


async def _dataset_constraints(url: str) -> dict[str, str]:
    rows = await _fetch(
        url,
        "SELECT conname, pg_get_constraintdef(c.oid) FROM pg_constraint c "
        "JOIN pg_class t ON t.oid = c.conrelid WHERE t.relname = 'dataset'",
    )
    return {r[0]: r[1] for r in rows}


async def _drop_provenance_objects(url: str) -> None:
    """Take a current create_all database back to the pre-provenance object set.

    The enum label cannot be removed (Postgres has no DROP VALUE); a caller that
    needs the old label set pre-creates the type before ``create_all`` instead."""
    await _execute(
        url,
        "DROP TABLE dataset",
        "ALTER TABLE hpcrun DROP COLUMN jobref_analysis_id",
        "DROP INDEX ix_analysis_tags",
        "ALTER TABLE analysis DROP COLUMN tags",
        "ALTER TABLE analysis DROP COLUMN source",
        # ...and nothing NEWER than provenance either: a database bootstrapped on that release
        # cannot have a column a later revision adds. Left in place it reads as a marker that is
        # True above two that are False, and the reconciler rightly calls that INCONSISTENT.
        "DROP INDEX IF EXISTS ix_env_worker_task_owner_instance",
        "ALTER TABLE env_worker_task DROP COLUMN IF EXISTS owner_instance",
        "ALTER TABLE simulator DROP COLUMN IF EXISTS image_tag",
        "ALTER TABLE simulator DROP COLUMN IF EXISTS label",
        "ALTER TABLE simulator DROP COLUMN IF EXISTS temporary",
    )


def _cfg(url: str, monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", url)
    return _alembic_config(url)


# ---------------------------------------------------------------------------
# MANAGED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upgrade_head_on_a_managed_database_adds_everything_and_keeps_rows(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The migration Job's path on both Stanford sites: stamped at the previous
    head, an existing analysis row, then ``upgrade head`` and nothing else."""
    url = fresh_postgres_url
    await _migrate_to(url, PRE_REVISION, monkeypatch)
    assert "ANALYSIS" not in await _jobtype_labels(url)
    await _execute(url, "INSERT INTO analysis (name, config, last_updated) VALUES ('before', '{}', 'x')")

    await asyncio.to_thread(command.upgrade, _cfg(url, monkeypatch), "head")

    assert await _version(url) == HEAD_REVISION
    assert "ANALYSIS" in await _jobtype_labels(url)
    facts = await _column_facts(url)
    for table, column in _NEW_COLUMNS:
        assert (table, column) in facts, f"{table}.{column} missing after upgrade head"
    assert ("dataset", "uri") in facts

    # The existing row picked up the server defaults, not NULLs.
    assert await _fetch(url, "SELECT tags, source FROM analysis WHERE name = 'before'") == [([], None)]


@pytest.mark.asyncio
async def test_the_new_label_and_tables_are_writable_after_the_upgrade_commits(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ANALYSIS run row, a dataset row attributed to it, and the two constraints
    that make a dataset a dataset: a producer is required and ``uri`` is unique."""
    url = fresh_postgres_url
    await _migrate_to(url, PRE_REVISION, monkeypatch)
    await asyncio.to_thread(command.upgrade, _cfg(url, monkeypatch), "head")
    await _shim_hpcrun(url)

    analysis_id = (
        await _fetch(url, "INSERT INTO analysis (name, config, last_updated) VALUES ('a', '{}', 'x') RETURNING id")
    )[0][0]
    # A separate transaction from the ALTER TYPE: this is the write Postgres refuses
    # if the label were used before the adding transaction committed.
    hpcrun_rows = await _fetch(
        url,
        "INSERT INTO hpcrun (job_type, correlation_id, job_backend, status, jobref_analysis_id) "
        "VALUES ('ANALYSIS', :c, 'k8s', 'RUNNING', :a) RETURNING id",
        {"c": f"analysis-{analysis_id}", "a": analysis_id},
    )
    assert len(hpcrun_rows) == 1

    dataset_rows = await _fetch(
        url,
        "INSERT INTO dataset (analysis_id, kind, uri) "
        "VALUES (:a, 'ptools-analysis', 's3://b/analyses/x/ptools/ptools_rna.tsv') "
        "RETURNING available, attributes, tags, source",
        {"a": analysis_id},
    )
    assert dataset_rows == [(True, {}, [], None)]

    with pytest.raises(DBAPIError, match="ck_dataset_producer"):
        await _execute(url, "INSERT INTO dataset (kind, uri) VALUES ('figure', 's3://b/orphan.html')")
    with pytest.raises(DBAPIError, match="dataset_uri_key"):
        await _fetch(
            url,
            "INSERT INTO dataset (analysis_id, kind, uri) "
            "VALUES (:a, 'ptools-analysis', 's3://b/analyses/x/ptools/ptools_rna.tsv') RETURNING id",
            {"a": analysis_id},
        )


@pytest.mark.asyncio
async def test_downgrade_then_upgrade_round_trips(fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    url = fresh_postgres_url
    await _migrate_to(url, PRE_REVISION, monkeypatch)
    cfg = _cfg(url, monkeypatch)
    await asyncio.to_thread(command.upgrade, cfg, "head")

    await asyncio.to_thread(command.downgrade, cfg, JOBTYPE_REVISION)
    dropped = await _column_facts(url)
    assert not any(table == "dataset" for table, _ in dropped)
    assert not any(key in dropped for key in _NEW_COLUMNS)
    assert await _fetch(url, "SELECT 1 FROM information_schema.tables WHERE table_name = 'analysis'"), (
        "downgrade must leave the analysis table itself in place"
    )

    await asyncio.to_thread(command.upgrade, cfg, "head")
    restored = await _column_facts(url)
    assert all(key in restored for key in _NEW_COLUMNS)
    assert ("dataset", "uri") in restored


# ---------------------------------------------------------------------------
# LEGACY (create_all) and the reconciler
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upgrade_head_is_a_noop_on_a_create_all_database(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every object already exists; the guards must turn both revisions into no-ops
    rather than DuplicateTable / DuplicateObject / DuplicateColumn."""
    url = fresh_postgres_url
    await _create_all(url)
    before = (await _column_facts(url), await _index_defs(url), await _dataset_constraints(url))

    cfg = _cfg(url, monkeypatch)
    await asyncio.to_thread(command.stamp, cfg, PRE_REVISION)
    await asyncio.to_thread(command.upgrade, cfg, "head")

    assert await _version(url) == HEAD_REVISION
    after = (await _column_facts(url), await _index_defs(url), await _dataset_constraints(url))
    assert after == before


@pytest.mark.asyncio
async def test_reconciler_adopts_a_current_create_all_database_at_the_new_head(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = fresh_postgres_url
    await _create_all(url)
    cfg = _cfg(url, monkeypatch)

    diag = await asyncio.to_thread(db_reconcile.diagnose, url, cfg)
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == HEAD_REVISION

    assert await asyncio.to_thread(db_reconcile.apply, url, cfg, diag) == 0
    assert await _version(url) == HEAD_REVISION


@pytest.mark.asyncio
async def test_reconciler_carries_a_previous_release_create_all_database_to_head(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The most faithful LEGACY rehearsal: the app bootstrapped this database on the
    previous release, so ``jobtypedb`` has only the old labels and none of the new
    objects exist. Pre-creating the enum with the old labels makes ``create_all``
    keep them (it creates types only when absent)."""
    url = fresh_postgres_url
    await _execute(url, "CREATE TYPE jobtypedb AS ENUM ('SIMULATION', 'PARCA', 'BUILD_IMAGE')")
    await _create_all(url)
    await _drop_provenance_objects(url)
    assert "ANALYSIS" not in await _jobtype_labels(url)

    cfg = _cfg(url, monkeypatch)
    diag = await asyncio.to_thread(db_reconcile.diagnose, url, cfg)
    assert diag.state is DbState.LEGACY
    assert diag.matched_revision == PRE_REVISION, diag.message

    assert await asyncio.to_thread(db_reconcile.apply, url, cfg, diag) == 0
    assert await _version(url) == HEAD_REVISION
    assert "ANALYSIS" in await _jobtype_labels(url)
    facts = await _column_facts(url)
    assert all(key in facts for key in _NEW_COLUMNS)
    assert ("dataset", "uri") in facts


# ---------------------------------------------------------------------------
# the two build paths agree on more than names
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_all_and_the_migration_build_the_same_objects(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Build everything this migration owns BOTH ways in one Postgres and compare
    column types, nullability and defaults, index definitions (GIN vs btree shows up
    here) and the dataset table's constraints."""
    url = fresh_postgres_url
    await _create_all(url)
    by_create_all = (await _column_facts(url), await _index_defs(url), await _dataset_constraints(url))
    assert all(by_create_all), "create_all built nothing to compare -- the comparison would be vacuous"

    await _drop_provenance_objects(url)
    cfg = _cfg(url, monkeypatch)
    await asyncio.to_thread(command.stamp, cfg, PRE_REVISION)
    await asyncio.to_thread(command.upgrade, cfg, "head")
    by_migration = (await _column_facts(url), await _index_defs(url), await _dataset_constraints(url))

    _assert_same("columns", by_create_all[0], by_migration[0])
    _assert_same("indexes", by_create_all[1], by_migration[1])
    _assert_same("constraints", by_create_all[2], by_migration[2])


def _assert_same(name: str, by_create_all: dict[Any, Any], by_migration: dict[Any, Any]) -> None:
    only_create_all = {k: v for k, v in by_create_all.items() if by_migration.get(k) != v}
    only_migration = {k: v for k, v in by_migration.items() if by_create_all.get(k) != v}
    assert not only_create_all and not only_migration, (
        f"{name} differ between create_all and the migration:\n"
        f"  create_all: {only_create_all}\n  migration:  {only_migration}"
    )
