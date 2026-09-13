"""The observability migration, exercised against the REAL Alembic chain.

Every other test of this migration builds its schema with
``Base.metadata.create_all`` (``tests/fixtures/postgres_fixtures.py``). As
``tests/simulation/test_jobstatusdb_cancel_migration.py`` spells out at length,
``create_all`` always reflects the CURRENT ORM model, so it is *structurally
incapable* of catching a defect that lives in the gap between the migration
history and that model. That is exactly the gap that produced the real
production failure::

    InvalidTextRepresentationError: invalid input value for enum jobstatusdb: "CANCELLED"

So this migration gets the same shape of test as that one: schema built by
``alembic upgrade`` from empty, never ``create_all``.

Covered here:

* the pre-observability schema genuinely lacks the new columns and tables --
  the baseline the migration has to move;
* **the migration does not touch the status enum at all.** An earlier draft
  added a ``PARTIAL`` label; dropping it is what keeps this migration pure
  addition, and therefore free of the deploy-ordering constraint an
  ``ALTER TYPE ... ADD VALUE`` imposes. Pinned by a test rather than a comment;
* a real terminal write round-trips against the migrated schema and drops out
  of the active-run queries that bind this same column;
* the migrated schema agrees with the ORM for everything this migration owns
  (checked with the shipped ``schema_diff`` machinery, not a hand-written list);
* the migration is idempotent -- a no-op on a ``create_all`` database that
  already has every object, and safe to re-apply after a downgrade.
"""

import asyncio
import datetime
from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from sqlalchemy import insert, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from alembic import command
from tests.docker_utils import SKIP_DOCKER_REASON, SKIP_DOCKER_TESTS
from tests.simulation.test_jobstatusdb_cancel_migration import (  # reuse the precedent's machinery verbatim
    _migrate_to,
    _real_jobstatusdb_labels,
    _shim_missing_hpcrun_columns,
)
from viva_api.common.hpc.job_service import JobStatusUpdate
from viva_api.common.models import JobId, JobStatus
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.db_reconcile import _alembic_config
from viva_api.simulation.models import JobType
from viva_api.simulation.schema_diff import DbSchema, _collect_db_schema, _expected_schema_from_orm, diff_schemas
from viva_api.simulation.tables_orm import Base, ORMHpcRunEvent, ORMHpcRunSpan

# The last revision before observability: the shape a currently-deployed
# database is at.
PRE_OBS_REVISION = "f76e43d01841"
# The observability migration under test.
OBS_REVISION = "a3b5c7d9e1f2"

# The tables this migration owns. Anything else in the ORM predates it and may
# have its own (pre-existing, unrelated) migration gaps -- see
# ``_shim_missing_hpcrun_columns``.
_OWNED_TABLES = ("hpcrun_event", "hpcrun_span")
# The columns it adds to an existing table.
_NEW_HPCRUN_COLUMNS = frozenset({
    "exit_code",
    "attempt",
    "error_source",
    "trace_id",
    "campaign_span_id",
    "events_s3_prefix",
    "events_cursor",
    "stage",
    "generation",
    "last_event_at",
})


@pytest.fixture(scope="function")
def fresh_postgres_url() -> Generator[str]:
    """A dedicated, empty Postgres per test -- each test here migrates from
    true empty to a specific, explicit revision and must not observe another
    test's schema. Same rationale (and shape) as the fixture of the same name
    in ``test_jobstatusdb_cancel_migration``; defined locally rather than
    imported so pytest sees exactly one definition."""
    if SKIP_DOCKER_TESTS:
        pytest.skip(SKIP_DOCKER_REASON)
    with PostgresContainer("postgres:15") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql+asyncpg://")


async def _reflect(asyncpg_url: str) -> DbSchema:
    engine = create_async_engine(asyncpg_url)
    try:
        async with engine.connect() as conn:
            return await conn.run_sync(_collect_db_schema)
    finally:
        await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def engine_pre_obs(fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[AsyncEngine]:
    """Real migrations up to the last pre-observability revision, plus the
    generic column shim the precedent file documents (pre-existing gaps,
    unrelated to this migration)."""
    await _migrate_to(fresh_postgres_url, PRE_OBS_REVISION, monkeypatch)
    engine = create_async_engine(fresh_postgres_url)
    await _shim_missing_hpcrun_columns(engine)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def engine_post_obs(fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[AsyncEngine]:
    """Same, migrated all the way to the observability revision."""
    await _migrate_to(fresh_postgres_url, OBS_REVISION, monkeypatch)
    engine = create_async_engine(fresh_postgres_url)
    await _shim_missing_hpcrun_columns(engine)
    yield engine
    await engine.dispose()


async def _a_run(db: DatabaseServiceSQL) -> tuple[int, int]:
    """Insert a simulator and one hpcrun against it; return (ref_id, hpcrun_id).

    ``DatabaseServiceSQL`` has no get-by-id, so the ref is needed to read the
    row back through ``get_hpcrun_by_ref`` (as the precedent file does).
    """
    simulator = await db.insert_simulator(
        git_repo_url="https://example.com/repo", git_branch="main", git_commit_hash="deadbee"
    )
    hpcrun = await db.insert_hpcrun(
        job_id=JobId.slurm(1), job_type=JobType.BUILD_IMAGE, ref_id=simulator.database_id, correlation_id="t"
    )
    return int(simulator.database_id), int(hpcrun.database_id)


@pytest.mark.asyncio
async def test_pre_observability_schema_lacks_the_new_objects(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The real, currently-deployed shape: no event tables and no new ``hpcrun``
    columns -- the baseline the migration has to move."""
    await _migrate_to(fresh_postgres_url, PRE_OBS_REVISION, monkeypatch)

    actual = await _reflect(fresh_postgres_url)
    for table in _OWNED_TABLES:
        assert table not in actual.tables
    assert not (_NEW_HPCRUN_COLUMNS & actual.tables["hpcrun"])


@pytest.mark.asyncio
async def test_the_migration_does_not_touch_the_status_enum(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The enum is left exactly as it was found -- and that is the point.

    An earlier draft of this migration added a ``PARTIAL`` label. Dropping it
    removed three costs at once: Postgres cannot remove an enum label once
    added, so the change was permanent; the ``ADD VALUE`` forced the migration
    Job to complete before any new app pod could serve traffic, or a write would
    raise ``InvalidTextRepresentationError``; and any client that had not
    learned the label would treat the run as non-terminal and poll forever.

    Because the enum is untouched, this migration is pure addition -- new
    nullable columns and new tables -- and imposes no deploy ordering at all.
    A test rather than a comment, so re-adding a label is a deliberate act.
    """
    await _migrate_to(fresh_postgres_url, PRE_OBS_REVISION, monkeypatch)
    before = await _real_jobstatusdb_labels(fresh_postgres_url)

    cfg = _alembic_config(fresh_postgres_url)
    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", fresh_postgres_url)
    await asyncio.to_thread(command.upgrade, cfg, OBS_REVISION)

    after = await _real_jobstatusdb_labels(fresh_postgres_url)
    assert after == before, f"the migration changed the status enum: {sorted(after ^ before)}"


@pytest.mark.asyncio
async def test_a_terminal_write_round_trips_against_the_migrated_schema(
    engine_post_obs: AsyncEngine,
) -> None:
    """The real application write path, against a migration-built database.

    ``FAILED`` is what a run with some tasks missing now records; it must
    round-trip and be terminal, so the pollers stop on it rather than looping.
    """
    db = DatabaseServiceSQL(async_engine=engine_post_obs)
    ref_id, hpcrun_id = await _a_run(db)

    await db.update_hpcrun_status(
        hpcrun_id=hpcrun_id, update=JobStatusUpdate(job_id=JobId.slurm(1), status=JobStatus.FAILED)
    )

    run = await db.get_hpcrun_by_ref(ref_id=ref_id, job_type=JobType.BUILD_IMAGE)
    assert run is not None
    assert run.status == JobStatus.FAILED
    assert run.status.is_terminal

    # The active-run queries bind JobStatusDB members against this same column
    # (that is how backlog item 53 surfaced); a terminal row must drop out of
    # them rather than raise.
    active = await db.list_active_hpcruns()
    assert hpcrun_id not in [r.database_id for r in active]
    await db.list_active_chain_campaigns()


@pytest.mark.asyncio
async def test_migrated_schema_agrees_with_the_orm_for_what_this_migration_owns(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The hand-written DDL in the migration and the ORM models must not drift.

    Checked with the shipped ``schema_diff`` comparison rather than a
    hand-copied column list, and scoped to this migration's own objects: other
    tables carry pre-existing, unrelated migration gaps (the reason
    ``_shim_missing_hpcrun_columns`` exists), which are deliberately NOT
    asserted on here.
    """
    await _migrate_to(fresh_postgres_url, OBS_REVISION, monkeypatch)
    actual = await _reflect(fresh_postgres_url)
    diff = diff_schemas(_expected_schema_from_orm(), actual)

    for table in _OWNED_TABLES:
        assert table not in diff.missing_tables, f"{table} was not created by the migration"
        assert table not in diff.missing_columns, f"{table} columns drifted: {diff.missing_columns.get(table)}"
        assert table not in diff.extra_columns, f"{table} has DDL columns the ORM lost: {diff.extra_columns.get(table)}"

    assert actual.tables["hpcrun"] >= _NEW_HPCRUN_COLUMNS
    assert not (_NEW_HPCRUN_COLUMNS & set(diff.missing_columns.get("hpcrun", [])))
    assert "jobstatusdb" not in diff.missing_enum_values

    # The models are usable against the migrated tables, not merely
    # column-compatible on paper.
    engine = create_async_engine(fresh_postgres_url)
    try:
        db = DatabaseServiceSQL(async_engine=engine)
        await _shim_missing_hpcrun_columns(engine)
        _ref_id, hpcrun_id = await _a_run(db)
        async with engine.begin() as conn:
            await conn.execute(
                insert(ORMHpcRunSpan).values(
                    hpcrun_id=hpcrun_id, trace_id="tr", span_id="s0", name="campaign", status="ok"
                )
            )
            await conn.execute(
                insert(ORMHpcRunEvent).values(
                    hpcrun_id=hpcrun_id,
                    trace_id="tr",
                    source="api",
                    seq=1,
                    ts=datetime.datetime(2026, 9, 10, 12, 0, 0),
                    layer="dispatcher",
                    event="submitted",
                    span_id="s0",
                )
            )
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM hpcrun_event"))).scalar_one() == 1
            assert (await conn.execute(text("SELECT count(*) FROM hpcrun_span"))).scalar_one() == 1
            # The uniqueness that makes re-ingest idempotent is real DDL, not
            # only an ORM declaration.
            with pytest.raises(DBAPIError):
                async with engine.begin() as dup:
                    await dup.execute(
                        text(
                            "INSERT INTO hpcrun_event (hpcrun_id, trace_id, source, seq, ts, layer, event) "
                            "VALUES (:h, 'tr', 'api', 1, '2026-09-10 12:00:00', 'dispatcher', 'submitted')"
                        ),
                        {"h": hpcrun_id},
                    )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_migration_is_a_noop_on_a_create_all_database(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The real deployment case the migration's ``IF NOT EXISTS`` clauses are
    for: the app bootstrapped this database with ``create_all`` (so every
    object already exists), it was stamped at the
    pre-observability revision by the reconciler, and the migration then runs.
    It must not fail on a duplicate object or a duplicate enum label."""
    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", fresh_postgres_url)
    engine = create_async_engine(fresh_postgres_url)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    finally:
        await engine.dispose()

    cfg = _alembic_config(fresh_postgres_url)
    await asyncio.to_thread(command.stamp, cfg, PRE_OBS_REVISION)
    await asyncio.to_thread(command.upgrade, cfg, OBS_REVISION)

    actual = await _reflect(fresh_postgres_url)
    for table in _OWNED_TABLES:
        assert table in actual.tables


@pytest.mark.asyncio
async def test_migration_can_be_re_applied_after_a_downgrade(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """down/up round trip. The enum label deliberately survives the downgrade
    (Postgres has no ``DROP VALUE``), so the second upgrade re-runs
    ``ADD VALUE IF NOT EXISTS`` against a label that is already there."""
    await _migrate_to(fresh_postgres_url, OBS_REVISION, monkeypatch)
    cfg = _alembic_config(fresh_postgres_url)
    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", fresh_postgres_url)

    await asyncio.to_thread(command.downgrade, cfg, PRE_OBS_REVISION)
    dropped = await _reflect(fresh_postgres_url)
    for table in _OWNED_TABLES:
        assert table not in dropped.tables
    assert not (_NEW_HPCRUN_COLUMNS & dropped.tables["hpcrun"])

    await asyncio.to_thread(command.upgrade, cfg, OBS_REVISION)
    restored = await _reflect(fresh_postgres_url)
    for table in _OWNED_TABLES:
        assert table in restored.tables
    assert restored.tables["hpcrun"] >= _NEW_HPCRUN_COLUMNS
