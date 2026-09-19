"""``DB_CREATE_ALL`` and the startup schema check (``viva_api/simulation/db_startup.py``)."""

import asyncio
import logging
import secrets
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from alembic import command
from tests.docker_utils import SKIP_DOCKER_REASON, SKIP_DOCKER_TESTS
from viva_api.simulation import db_startup
from viva_api.simulation.db_reconcile import _alembic_config
from viva_api.simulation.tables_orm import create_db

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def postgres_server() -> Iterator[str]:
    if SKIP_DOCKER_TESTS:
        pytest.skip(SKIP_DOCKER_REASON)
    with PostgresContainer("postgres:15") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql+asyncpg://")


@pytest_asyncio.fixture
async def empty_engine(postgres_server: str) -> AsyncIterator[AsyncEngine]:
    name = f"startup_{secrets.token_hex(4)}"
    admin = create_async_engine(postgres_server, isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        await conn.execute(sa.text(f"CREATE DATABASE {name}"))
    await admin.dispose()
    engine = create_async_engine(postgres_server.rsplit("/", 1)[0] + f"/{name}")
    yield engine
    await engine.dispose()


@pytest.fixture(autouse=True)
def _reset_state() -> Iterator[None]:
    saved = db_startup.get_schema_state()
    yield
    db_startup.set_schema_state(saved)


class _Records(logging.Handler):
    """Attached straight to the module's logger, and see ``_check`` below: running Alembic
    executes ``alembic/env.py``, whose ``logging.config.fileConfig`` DISABLES every logger that
    already exists -- this module's included. Any earlier test that ran a migration (or the
    ``command.upgrade`` one line above the check) therefore silenced it, which is why these
    tests passed alone and failed in the full suite. The API process never runs Alembic, so
    production logging is unaffected."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.messages: list[tuple[int, str]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append((record.levelno, record.getMessage()))

    def text(self, level: int) -> str:
        return "\n".join(message for lvl, message in self.messages if lvl >= level)


@pytest.fixture
def records() -> Iterator[_Records]:
    handler = _Records()
    previous = db_startup.logger.level
    db_startup.logger.addHandler(handler)
    db_startup.logger.setLevel(logging.DEBUG)
    yield handler
    db_startup.logger.removeHandler(handler)
    db_startup.logger.setLevel(previous)


async def _check(engine: AsyncEngine, *, create_all: bool) -> db_startup.SchemaState:
    db_startup.logger.disabled = False  # undo alembic env.py's fileConfig(disable_existing_loggers=True)
    return await db_startup.check_schema(engine, create_all=create_all)


async def _table_count(engine: AsyncEngine) -> int:
    async with engine.connect() as conn:
        return len(await conn.run_sync(lambda c: sa.inspect(c).get_table_names()))


@pytest.mark.asyncio
async def test_with_the_guard_off_the_app_creates_nothing(empty_engine: AsyncEngine) -> None:
    ran = await db_startup.create_tables_if_enabled(empty_engine, create_db, enabled=False, what="simulation")
    assert ran is False
    assert await _table_count(empty_engine) == 0


@pytest.mark.asyncio
async def test_with_the_guard_on_behaviour_is_unchanged(empty_engine: AsyncEngine) -> None:
    ran = await db_startup.create_tables_if_enabled(empty_engine, create_db, enabled=True, what="simulation")
    assert ran is True
    assert await _table_count(empty_engine) > 0


@pytest.mark.asyncio
async def test_a_create_all_database_is_reported_as_not_at_head_and_says_how_to_fix_it(
    empty_engine: AsyncEngine, records: _Records
) -> None:
    """Every table exists, nothing is stamped: exactly what ``create_all`` leaves behind."""
    await create_db(empty_engine)
    state = await _check(empty_engine, create_all=False)
    assert state.revision is None and not state.at_head
    errors = records.text(logging.ERROR)
    assert "NOT AT HEAD" in errors and "alembic-migrate" in errors
    assert "DB_CREATE_ALL is false" in errors
    assert db_startup.get_schema_state() is state


@pytest.mark.asyncio
async def test_a_migrated_database_is_at_head(
    empty_engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch, records: _Records
) -> None:
    url = empty_engine.url.render_as_string(hide_password=False)
    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", url)
    await asyncio.to_thread(command.upgrade, _alembic_config(url), "head")
    state = await _check(empty_engine, create_all=False)
    assert state.at_head and state.revision == state.head == db_startup.chain_head()
    assert "NOT AT HEAD" not in records.text(logging.DEBUG)
    assert "at the Alembic head" in records.text(logging.INFO)
    assert state.as_health_fields() == {
        "db_revision": state.head,
        "db_head": state.head,
        "db_at_head": "true",
        "db_create_all": "false",
    }


@pytest.mark.asyncio
async def test_a_database_behind_head_is_reported(empty_engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch) -> None:
    url = empty_engine.url.render_as_string(hide_password=False)
    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", url)
    await asyncio.to_thread(command.upgrade, _alembic_config(url), "e3a9c1d70b62")
    state = await db_startup.check_schema(empty_engine, create_all=False)
    assert state.revision == "e3a9c1d70b62" and not state.at_head
    assert state.as_health_fields()["db_at_head"] == "false"


def test_an_unreadable_head_is_reported_as_unknown_not_as_a_mismatch() -> None:
    state = db_startup.SchemaState(revision="abc", head=db_startup.UNKNOWN, create_all=True)
    assert state.as_health_fields()["db_at_head"] == "unknown"


def test_health_reports_the_schema_state_and_omits_it_without_a_database() -> None:
    from fastapi.testclient import TestClient

    from viva_api.api.main import app

    # No lifespan (no `with`): nothing is initialised, exactly the "no database" case.
    client = TestClient(app)
    db_startup.set_schema_state(None)
    assert "db_at_head" not in client.get("/health").json()

    db_startup.set_schema_state(db_startup.SchemaState(revision="r1", head="r2", create_all=False))
    body = client.get("/health").json()
    assert (body["db_revision"], body["db_head"], body["db_at_head"], body["db_create_all"]) == (
        "r1",
        "r2",
        "false",
        "false",
    )


@pytest.mark.parametrize("namespace", ["sms-api-stanford-test", "sms-api-stanford"])
def test_deployed_sites_turn_create_all_off(namespace: str) -> None:
    """On a deployed site the schema belongs to the migration Job alone. If this setting is
    dropped, ``create_all`` quietly resumes hiding model changes that have no migration."""
    lines = (REPO_ROOT / "kustomize" / "config" / namespace / "api.env").read_text(encoding="utf-8").splitlines()
    assert "DB_CREATE_ALL=false" in [line.strip() for line in lines]
