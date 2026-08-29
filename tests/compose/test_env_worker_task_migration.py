"""The `env_worker_task` migration must be safe against BOTH database shapes.

`env_worker_task` lives in the compose metadata, and compose tables are
bootstrapped at startup by `create_compose_db` (`create_all`). So on any database
the app has already touched, the table ALREADY EXISTS by the time Alembic runs —
a plain `op.create_table` would fail re-creating it, which is exactly the class of
failure `db_reconcile` exists to diagnose.

These run the real SQL against a real Postgres rather than asserting on the text
of the migration, because "is this idempotent" is a question only the database can
answer.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from viva_api.compose.tables_orm import ComposeBase, create_compose_db

MIGRATION = (
    """
    CREATE TABLE IF NOT EXISTS env_worker_task (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
        job_name VARCHAR NOT NULL,
        method VARCHAR NOT NULL,
        params JSONB,
        status VARCHAR NOT NULL,
        result JSONB,
        error_message VARCHAR,
        started_at TIMESTAMP WITHOUT TIME ZONE,
        ended_at TIMESTAMP WITHOUT TIME ZONE,
        created_by VARCHAR,
        correlation_id VARCHAR NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_env_worker_task_job_name ON env_worker_task (job_name)",
    "CREATE INDEX IF NOT EXISTS ix_env_worker_task_created_by ON env_worker_task (created_by)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_env_worker_task_correlation_id ON env_worker_task (correlation_id)",
)


async def _fresh(engine: AsyncEngine) -> None:
    """Drop the table so each test starts from a known-absent state.

    The fixture hands out ONE database, so isolation is per-table rather than
    per-database — inventing a database name in the URL only produces
    InvalidCatalogNameError, since nothing ever created it.
    """
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS env_worker_task CASCADE"))


async def _apply(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        for stmt in MIGRATION:
            await conn.execute(text(stmt))


async def _columns(engine: AsyncEngine) -> set[str]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_name = 'env_worker_task'")
        )
        return {r[0] for r in rows}


@pytest.mark.asyncio
async def test_migration_creates_the_table_on_an_alembic_owned_database(postgres_url: str) -> None:
    """The FRESH case: Alembic owns the schema, nothing has run create_all."""
    engine = create_async_engine(postgres_url, echo=False)
    try:
        await _fresh(engine)
        await _apply(engine)
        cols = await _columns(engine)
        assert "created_by" in cols
        assert {"job_name", "method", "params", "status", "correlation_id"} <= cols
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_migration_is_a_no_op_where_create_all_already_made_the_table(
    postgres_url: str,
) -> None:
    """THE case this migration's shape exists for. create_all bootstrapped the
    table at startup; the migration must not fail re-creating it."""
    engine = create_async_engine(postgres_url, echo=False)
    try:
        await _fresh(engine)
        await create_compose_db(engine)  # what dependencies.py does at boot
        before = await _columns(engine)
        assert before, "precondition: create_all made the table"
        await _apply(engine)  # must not raise
        assert await _columns(engine) == before, "the migration altered a create_all table"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_migration_is_idempotent_when_applied_twice(postgres_url: str) -> None:
    """A re-run of the migration Job is normal (Jobs are immutable and get
    deleted/reapplied), so applying twice must be uneventful."""
    engine = create_async_engine(postgres_url, echo=False)
    try:
        await _fresh(engine)
        await _apply(engine)
        await _apply(engine)
        assert "created_by" in await _columns(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_the_orm_and_the_migration_agree_on_the_columns(postgres_url: str) -> None:
    """Two definitions of one table drift silently; this is the check that they
    have not. create_all builds from the ORM, the migration from hand-written
    SQL, and a column present in one and absent from the other is a bug that only
    shows up on whichever database shape the author did not test."""
    engine = create_async_engine(postgres_url, echo=False)
    try:
        await _fresh(engine)
        await create_compose_db(engine)  # the ORM's idea of the table
        from_orm = await _columns(engine)
        await _fresh(engine)
        await _apply(engine)  # the migration's idea of it
        from_sql = await _columns(engine)
        assert from_orm == from_sql, (
            f"ORM-only: {sorted(from_orm - from_sql)}  migration-only: {sorted(from_sql - from_orm)}"
        )
    finally:
        await engine.dispose()


def test_the_orm_table_is_registered_in_the_compose_metadata() -> None:
    """If it is not in ComposeBase, create_all never makes it and the whole
    idempotency argument above is moot."""
    assert "env_worker_task" in ComposeBase.metadata.tables
