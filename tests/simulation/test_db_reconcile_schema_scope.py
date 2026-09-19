"""The reconciler's schema probes answer for ``current_schema()`` only.

Unscoped, ``information_schema`` / ``pg_type`` answer for EVERY schema, so a same-named
table, column or enum elsewhere in the database reads as this chain's progress. That is
harmless while everything lives in ``public`` and wrong the moment the core split
(docs/plan-core.md) gives ``core`` its own ``task`` / ``dataset`` tables.
"""

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from viva_api.simulation.db_reconcile import _column_exists, _enum_has_value, _table_exists


@pytest_asyncio.fixture
async def conn_with_decoy_schema(postgres_url: str) -> AsyncGenerator[AsyncConnection]:
    """An empty ``public`` beside a ``decoy`` schema holding look-alike objects."""
    engine = create_async_engine(postgres_url)
    try:
        async with engine.begin() as setup:
            await setup.execute(text("DROP SCHEMA IF EXISTS decoy CASCADE"))
            await setup.execute(text("CREATE SCHEMA decoy"))
            await setup.execute(text("CREATE TYPE decoy.schema_scope_enum AS ENUM ('only_in_decoy')"))
            await setup.execute(text("CREATE TABLE decoy.schema_scope_probe (id int, only_in_decoy int)"))
        async with engine.connect() as conn:
            yield conn
        async with engine.begin() as teardown:
            await teardown.execute(text("DROP TABLE IF EXISTS public.schema_scope_probe"))
            await teardown.execute(text("DROP SCHEMA IF EXISTS decoy CASCADE"))
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_probes_ignore_same_named_objects_in_another_schema(conn_with_decoy_schema: AsyncConnection) -> None:
    conn = conn_with_decoy_schema
    assert not await _table_exists(conn, "schema_scope_probe")
    assert not await _column_exists(conn, "schema_scope_probe", "only_in_decoy")
    assert not await _enum_has_value(conn, "schema_scope_enum", "only_in_decoy")


@pytest.mark.asyncio
async def test_probes_see_the_current_schema(conn_with_decoy_schema: AsyncConnection) -> None:
    conn = conn_with_decoy_schema
    await conn.execute(text("CREATE TABLE public.schema_scope_probe (id int, in_public int)"))
    await conn.commit()

    assert await _table_exists(conn, "schema_scope_probe")
    assert await _column_exists(conn, "schema_scope_probe", "in_public")
    # the decoy's column still does not leak through the same-named public table
    assert not await _column_exists(conn, "schema_scope_probe", "only_in_decoy")


@pytest.mark.asyncio
async def test_probes_follow_the_search_path(conn_with_decoy_schema: AsyncConnection) -> None:
    """``current_schema()`` is the contract: a chain pointed at another schema fingerprints THAT schema."""
    conn = conn_with_decoy_schema
    await conn.execute(text("SET search_path TO decoy"))

    assert await _table_exists(conn, "schema_scope_probe")
    assert await _column_exists(conn, "schema_scope_probe", "only_in_decoy")
    assert await _enum_has_value(conn, "schema_scope_enum", "only_in_decoy")
