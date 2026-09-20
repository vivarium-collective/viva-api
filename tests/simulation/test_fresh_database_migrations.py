"""``alembic upgrade head`` from a genuinely EMPTY database -- and what it builds (viva-api#637).

Two claims are held to account here, against a real Postgres:

1. **It runs.** For a long time it did not: nine tables (``analysis``, the ``compose_*`` family)
   were only ever created by ``create_all``, so the chain died at the first revision that
   assumed one. ``b9e1d5a3c7f2`` supplies them.
2. **What it builds is what the application uses.** Running is not enough: the baseline had
   captured an old shape of ``hpcrun`` / ``simulation`` / ``worker_event``, so a chain-built
   database lacked ``hpcrun.correlation_id`` and still had a NOT NULL
   ``simulation.variant_config`` the models do not know. ``c3f7a1e5b9d4`` reconciles them. The
   parity test below is the guard for every future revision too: a schema change made in the
   models and forgotten in a migration shows up here as a named difference.

Both inserted revisions are guarded, because a LEGACY database is stamped below them and
upgraded through them with all of this already in place -- that path is tested as well.
"""

import asyncio
import re
import secrets
from collections.abc import AsyncIterator, Iterator
from typing import Any, cast

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql.base import PGInspector
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from alembic import command
from tests.docker_utils import SKIP_DOCKER_REASON, SKIP_DOCKER_TESTS
from viva_api.compose.tables_orm import ComposeBase
from viva_api.simulation.db_reconcile import _alembic_config
from viva_api.simulation.tables_orm import Base

BEFORE_THE_FIX = "c1a2b3d4e5f6"
CREATES_THE_TABLES = "b9e1d5a3c7f2"
RECONCILES_THE_BASELINE = "c3f7a1e5b9d4"

NEVER_CREATED_BEFORE = (
    "analysis",
    "compose_allow_list",
    "compose_bigraph_compute",
    "compose_hpcrun",
    "compose_packages",
    "compose_simulation",
    "compose_simulator",
    "compose_simulator_to_package",
    "compose_worker_event",
)

#: The ONE accepted difference. ``a1c3e5f7b9d2`` added a lower-case 'cancelled' to the enum by
#: literal DDL; ``create_all`` derives labels from member NAMES and never has it; Postgres cannot
#: drop an enum label, so the chain keeps it for good. Unused, harmless, and named here so that
#: nothing else can hide behind it.
ACCEPTED_DIFFERENCES = {"enum jobstatusdb: only the chain has ['cancelled']"}


@pytest.fixture(scope="module")
def postgres_server() -> Iterator[str]:
    if SKIP_DOCKER_TESTS:
        pytest.skip(SKIP_DOCKER_REASON)
    with PostgresContainer("postgres:15") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql+asyncpg://")


@pytest_asyncio.fixture
async def empty_database(postgres_server: str) -> AsyncIterator[str]:
    """A brand-new, genuinely empty database on the shared server."""
    name = f"fresh_{secrets.token_hex(4)}"
    admin = create_async_engine(postgres_server, isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        await conn.execute(sa.text(f"CREATE DATABASE {name}"))
    await admin.dispose()
    yield postgres_server.rsplit("/", 1)[0] + f"/{name}"


async def _upgrade(url: str, revision: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", url)
    await asyncio.to_thread(command.upgrade, _alembic_config(url), revision)


async def _stamp(url: str, revision: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", url)
    await asyncio.to_thread(command.stamp, _alembic_config(url), revision)


async def _create_all(url: str) -> None:
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(ComposeBase.metadata.create_all)
    await engine.dispose()


def _snapshot(sync_conn: Connection) -> dict[str, Any]:
    """Everything that makes two schemas the same: columns (type, nullability, default), keys,
    unique constraints, indexes, enum labels. ``alembic_version`` is bookkeeping, not schema."""
    inspector = sa.inspect(sync_conn)
    tables: dict[str, Any] = {}
    for table in sorted(inspector.get_table_names()):
        if table == "alembic_version":
            continue
        tables[table] = {
            "columns": {
                c["name"]: (str(c["type"]), c["nullable"], re.sub(r"::[a-z ]+", "", str(c.get("default") or "")))
                for c in inspector.get_columns(table)
            },
            "primary key": tuple(inspector.get_pk_constraint(table)["constrained_columns"]),
            "foreign keys": sorted(
                (
                    tuple(fk["constrained_columns"]),
                    fk["referred_table"],
                    tuple(fk["referred_columns"]),
                    (fk.get("options") or {}).get("ondelete"),
                )
                for fk in inspector.get_foreign_keys(table)
            ),
            "unique": sorted(tuple(u["column_names"]) for u in inspector.get_unique_constraints(table)),
            "indexes": {i["name"]: (tuple(i["column_names"]), bool(i["unique"])) for i in inspector.get_indexes(table)},
        }
    enums = cast(PGInspector, inspector).get_enums()  # Postgres-only, so not on the generic Inspector type
    return {"tables": tables, "enums": {e["name"]: sorted(e["labels"]) for e in enums}}


async def _schema(url: str) -> dict[str, Any]:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            return await conn.run_sync(_snapshot)
    finally:
        await engine.dispose()


def _table_differences(table: str, a: dict[str, Any], b: dict[str, Any]) -> set[str]:
    found: set[str] = set()
    for part in a:
        if a[part] == b[part]:
            continue
        if isinstance(a[part], dict):
            for key in sorted(set(a[part]) | set(b[part])):
                if a[part].get(key) != b[part].get(key):
                    found.add(f"{table} {part} [{key}]: chain={a[part].get(key)} models={b[part].get(key)}")
        else:
            found.add(f"{table} {part}: chain={a[part]} models={b[part]}")
    return found


def _enum_differences(chain: dict[str, list[str]], models: dict[str, list[str]]) -> set[str]:
    found: set[str] = set()
    for enum in sorted(set(chain) | set(models)):
        only_chain = sorted(set(chain.get(enum, [])) - set(models.get(enum, [])))
        only_models = sorted(set(models.get(enum, [])) - set(chain.get(enum, [])))
        if only_chain:
            found.add(f"enum {enum}: only the chain has {only_chain}")
        if only_models:
            found.add(f"enum {enum}: only the models have {only_models}")
    return found


def _differences(chain: dict[str, Any], models: dict[str, Any]) -> set[str]:
    found = _enum_differences(chain["enums"], models["enums"])
    for table in sorted(set(chain["tables"]) | set(models["tables"])):
        a, b = chain["tables"].get(table), models["tables"].get(table)
        if a is None or b is None:
            found.add(f"table {table}: only in {'the models' if a is None else 'the chain'}")
        else:
            found |= _table_differences(table, a, b)
    return found


@pytest.mark.asyncio
async def test_upgrade_head_runs_from_an_empty_database(empty_database: str, monkeypatch: pytest.MonkeyPatch) -> None:
    await _upgrade(empty_database, "head", monkeypatch)
    schema = await _schema(empty_database)
    assert set(NEVER_CREATED_BEFORE) <= set(schema["tables"])


@pytest.mark.asyncio
async def test_negative_control_the_inserted_revision_is_what_supplies_the_tables(
    empty_database: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Just below the fix none of the nine exist -- which is why ``d3f9a1c72b84`` used to die
    on ``relation "analysis" does not exist``. One revision later, all of them do."""
    await _upgrade(empty_database, BEFORE_THE_FIX, monkeypatch)
    assert not set(NEVER_CREATED_BEFORE) & set((await _schema(empty_database))["tables"])

    await _upgrade(empty_database, CREATES_THE_TABLES, monkeypatch)
    assert set(NEVER_CREATED_BEFORE) <= set((await _schema(empty_database))["tables"])


@pytest.mark.asyncio
async def test_the_chain_builds_the_schema_the_models_describe(
    empty_database: str, postgres_server: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE parity test. A database built by the migrations and one built by ``create_all`` must
    be the same schema. It fails, naming the difference, whenever a model changes without a
    migration -- which is how the baseline drifted unnoticed in the first place."""
    await _upgrade(empty_database, "head", monkeypatch)

    models_db = f"models_{secrets.token_hex(4)}"
    admin = create_async_engine(postgres_server, isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        await conn.execute(sa.text(f"CREATE DATABASE {models_db}"))
    await admin.dispose()
    models_url = postgres_server.rsplit("/", 1)[0] + f"/{models_db}"
    await _create_all(models_url)

    differences = _differences(await _schema(empty_database), await _schema(models_url))
    unexpected = sorted(differences - ACCEPTED_DIFFERENCES)
    assert not unexpected, "the migration chain and the models disagree:\n  " + "\n  ".join(unexpected)
    assert differences >= ACCEPTED_DIFFERENCES, "an accepted difference is gone -- remove it from the allow-list"


@pytest.mark.asyncio
async def test_a_create_all_database_passes_through_both_revisions_untouched(
    empty_database: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The LEGACY shape: every table already exists, in the models' shape, and the reconciler
    has stamped the database BELOW the inserted revisions. They must be no-ops, not
    DuplicateTable errors -- the ``d7e2f4a6c8b0`` double-create all over again."""
    await _create_all(empty_database)
    before = await _schema(empty_database)
    await _stamp(empty_database, BEFORE_THE_FIX, monkeypatch)

    await _upgrade(empty_database, RECONCILES_THE_BASELINE, monkeypatch)

    assert await _schema(empty_database) == before


@pytest.mark.asyncio
async def test_a_populated_baseline_table_stops_the_reconciliation(
    empty_database: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A populated table that still has the baseline's columns was not written by this
    application. The revision refuses rather than dropping a NOT NULL column full of data."""
    await _upgrade(empty_database, CREATES_THE_TABLES, monkeypatch)
    engine = create_async_engine(empty_database)
    async with engine.begin() as conn:
        await conn.execute(
            sa.text("INSERT INTO simulator (git_repo_url, git_branch, git_commit_hash) VALUES ('r','b','c')")
        )
        await conn.execute(
            sa.text("INSERT INTO parca_dataset (simulator_id, parca_config, parca_config_hash) VALUES (1, '{}', 'h')")
        )
        await conn.execute(
            sa.text(
                "INSERT INTO simulation (simulator_id, parca_dataset_id, variant_config, variant_config_hash) "
                "VALUES (1, 1, '{}', 'h')"
            )
        )
    await engine.dispose()

    with pytest.raises(RuntimeError, match="'simulation' holds rows"):
        await _upgrade(empty_database, RECONCILES_THE_BASELINE, monkeypatch)


@pytest.mark.asyncio
async def test_owner_instance_revision_round_trips(empty_database: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """``docs/plan-core.md`` section 7a: every revision in the core split ships a real downgrade,
    proven by upgrade -> downgrade -> upgrade against a real Postgres."""

    async def owner_column() -> bool:
        columns = (await _schema(empty_database))["tables"]["env_worker_task"]["columns"]
        return "owner_instance" in columns

    await _upgrade(empty_database, "e7b3c9a1d5f2", monkeypatch)
    assert await owner_column()
    at_head = await _schema(empty_database)

    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", empty_database)
    await asyncio.to_thread(command.downgrade, _alembic_config(empty_database), "c9a1e3f5b7d2")
    assert not await owner_column()
    assert (
        "ix_env_worker_task_owner_instance"
        not in (await _schema(empty_database))["tables"]["env_worker_task"]["indexes"]
    )

    await _upgrade(empty_database, "e7b3c9a1d5f2", monkeypatch)
    assert await _schema(empty_database) == at_head


@pytest.mark.asyncio
async def test_simulator_temporary_marker_revision_round_trips(
    empty_database: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """f4c8a2e6d0b3, the write-once marker (``docs/plan-core.md`` D11 and section 7a): a real
    downgrade, proven by upgrade -> downgrade -> upgrade against a real Postgres."""
    marker_columns = {"temporary", "label", "image_tag"}

    async def simulator_columns() -> set[str]:
        return set((await _schema(empty_database))["tables"]["simulator"]["columns"])

    await _upgrade(empty_database, "f4c8a2e6d0b3", monkeypatch)
    assert marker_columns <= await simulator_columns()
    at_head = await _schema(empty_database)

    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", empty_database)
    await asyncio.to_thread(command.downgrade, _alembic_config(empty_database), "e7b3c9a1d5f2")
    assert not (marker_columns & await simulator_columns())

    await _upgrade(empty_database, "f4c8a2e6d0b3", monkeypatch)
    assert await _schema(empty_database) == at_head
