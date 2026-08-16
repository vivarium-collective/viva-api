"""Regression tests for backlog item 40.

``POST /api/v1/simulations/{id}/cancel`` raised a real Postgres error writing
the terminal job status::

    InvalidTextRepresentationError: invalid input value for enum jobstatusdb: "CANCELLED"

Root cause: ``ORMHpcRun.status: Mapped[JobStatusDB]`` has no
``values_callable``, so SQLAlchemy's default ``Enum`` type binds a plain
Python ``enum.Enum`` member by its **name** (upper-case: ``WAITING``,
``PENDING``, ``QUEUED``, ``RUNNING``, ``COMPLETED``, ``CANCELLED``,
``FAILED``), not its lower-case ``.value``. The real, migration-produced
Postgres enum never contained upper-case ``'CANCELLED'`` (the prior migration,
``a1c3e5f7b9d2``, added the wrong case: lower-case ``'cancelled'``, never
written by the app) nor ``'PENDING'`` at all (no migration ever added it,
despite ``DatabaseServiceSQL.list_active_hpcruns``/``list_active_chain_campaigns``
both binding it into a query against this same column). See
``alembic/versions/44335812e447_add_pending_and_uppercase_cancelled_to_.py``
for the fix and full root-cause writeup.

These tests build their schema via the REAL Alembic migration chain
(``alembic upgrade``), never ``Base.metadata.create_all``. ``create_all``
always reflects the CURRENT ORM model (including ``JobStatusDB``'s current
members), so it is structurally incapable of catching a defect that lives
specifically in the gap between the migration history and that model --
concretely confirmed by this repo's own pre-existing
``tests/api/ecoli/test_cancel.py``, which exercises the identical
``update_hpcrun_status(..., JobStatus.CANCELLED)`` write, passes today, and
never caught this bug: it runs against the ``database_service`` fixture
(``tests/fixtures/postgres_fixtures.py``), which bootstraps via
``create_all``.
"""

import asyncio
from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
import sqlalchemy as sa
from alembic import command
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from tests.docker_utils import SKIP_DOCKER_REASON, SKIP_DOCKER_TESTS
from viva_api.common.hpc.job_service import JobStatusUpdate
from viva_api.common.models import JobId, JobStatus
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.db_reconcile import _alembic_config
from viva_api.simulation.models import JobType
from viva_api.compose.tables_orm import ComposeBase
from viva_api.simulation.tables_orm import ORMHpcRun

# The buggy migration itself: real, currently-deployed enum shape.
PRE_FIX_REVISION = "a1c3e5f7b9d2"
# backlog item 40's fix.
FIX_REVISION = "44335812e447"


@pytest.fixture(scope="function")
def fresh_postgres_url() -> Generator[str]:
    """A dedicated, empty Postgres per test (not the shared module-scoped
    ``postgres_url`` fixture) -- each test here migrates from true empty to a
    specific, explicit revision, and must not observe another test's schema."""
    if SKIP_DOCKER_TESTS:
        pytest.skip(SKIP_DOCKER_REASON)
    with PostgresContainer("postgres:15") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql+asyncpg://")


async def _migrate_to(asyncpg_url: str, revision: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the REAL alembic migration chain from empty up to ``revision``.

    ``a1c3e5f7b9d2`` (``PRE_FIX_REVISION``) sits BEFORE ``d3f9a1c72b84`` in
    the chain and is reachable directly from empty. Any LATER target
    (including ``FIX_REVISION``) crosses ``d3f9a1c72b84``, which ALTERs an
    ``analysis`` table that no migration in this repo ever CREATEs (it only
    ever existed via the app's own ``Base.metadata.create_all``, which every
    real deployment has run at least once) -- for those, this migrates to
    ``c1a2b3d4e5f6`` first (creates ``simulation``, which ``analysis``'s own
    FK references), pre-creates ONLY ``analysis``'s pre-``d3f9a1c72b84``
    columns (id/name/config/last_updated/job_name/job_id -- exactly what that
    migration's own ``add_column`` calls do NOT already cover, so it can add
    its query columns on top without a duplicate-column error), then
    continues. This mirrors the real path for exactly the one unrelated table
    needed to walk the chain, without pulling in the FULL ``create_all``
    (which would give ``jobstatusdb`` its current-model shape for free and
    defeat these tests' purpose). A separate, pre-existing gap, not caused by
    and not fixed by backlog item 40 -- flagged in the PR description, not
    fixed here.
    """
    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", asyncpg_url)
    cfg = _alembic_config(asyncpg_url)
    # alembic's env.py calls asyncio.run() internally (run_migrations_online),
    # which raises "cannot be called from a running event loop" from inside
    # this already-running (pytest-asyncio) loop. A worker thread gives it a
    # loop-free thread to call that from.
    if revision == PRE_FIX_REVISION:
        await asyncio.to_thread(command.upgrade, cfg, revision)
        return

    await asyncio.to_thread(command.upgrade, cfg, "c1a2b3d4e5f6")
    engine = create_async_engine(asyncpg_url)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "CREATE TABLE analysis ("
                "id SERIAL PRIMARY KEY, name VARCHAR NOT NULL, config JSONB NOT NULL, "
                "last_updated VARCHAR NOT NULL, job_name VARCHAR, job_id INTEGER)"
            )
        )
        # compose_hpcrun (and the rest of the compose subsystem's own tables,
        # a SEPARATE ComposeBase.metadata) is likewise never CREATEd by any
        # migration, only by ComposeBase.metadata.create_all at app startup --
        # e5a7c9d10f21 (below) ALTERs it the same way d3f9a1c72b84 ALTERs
        # analysis. Unlike that one, e5a7c9d10f21 uses `ADD COLUMN IF NOT
        # EXISTS` (its own docstring: "a no-op on a fresh create_all DB that
        # already has the columns"), so the FULL current compose shape can be
        # pre-created here safely, no column-subsetting needed.
        await conn.run_sync(ComposeBase.metadata.create_all)
    await engine.dispose()
    await asyncio.to_thread(command.upgrade, cfg, revision)


async def _real_jobstatusdb_labels(asyncpg_url: str) -> set[str]:
    engine = create_async_engine(asyncpg_url)
    try:
        async with engine.connect() as conn:
            rows = await conn.execute(
                text(
                    "SELECT e.enumlabel FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid "
                    "WHERE t.typname = 'jobstatusdb'"
                )
            )
            return {r[0] for r in rows.fetchall()}
    finally:
        await engine.dispose()


async def _shim_missing_hpcrun_columns(engine: AsyncEngine) -> None:
    """Add any ``hpcrun`` column present in the CURRENT ORM model but absent
    from the real (migration-only) table -- e.g. ``correlation_id``,
    ``chain_n_generations``, ``chain_final_job_ids``: like the ``analysis``
    table itself, none of these has ever had its own migration, only ever
    existing via ``create_all`` in every real deployment. Unrelated to
    backlog item 40; a generic shim (nullable, no constraints -- irrelevant
    to what's under test) so ``DatabaseServiceSQL``'s CURRENT-model-shaped
    queries can run end to end against a migration-only schema without
    chasing each pre-existing gap by hand.
    """
    async with engine.connect() as conn:
        existing = set(await conn.run_sync(lambda c: [col["name"] for col in sa.inspect(c).get_columns("hpcrun")]))
    missing = [col for col in ORMHpcRun.__table__.columns if col.name not in existing]
    if not missing:
        return
    async with engine.begin() as conn:
        for col in missing:
            coltype = col.type.compile(dialect=conn.dialect)
            await conn.execute(text(f'ALTER TABLE hpcrun ADD COLUMN "{col.name}" {coltype}'))


@pytest_asyncio.fixture(scope="function")
async def hpcrun_capable_engine_pre_fix(fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[AsyncEngine]:
    """Real migrations up to the buggy revision, plus the column shim needed
    for ``DatabaseServiceSQL`` to operate (see ``_shim_missing_hpcrun_columns``).
    Unrelated to backlog item 40; not fixed here."""
    await _migrate_to(fresh_postgres_url, PRE_FIX_REVISION, monkeypatch)
    engine = create_async_engine(fresh_postgres_url)
    await _shim_missing_hpcrun_columns(engine)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def hpcrun_capable_engine_fixed(fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[AsyncEngine]:
    """Same as above, migrated all the way to backlog item 40's fix."""
    await _migrate_to(fresh_postgres_url, FIX_REVISION, monkeypatch)
    engine = create_async_engine(fresh_postgres_url)
    await _shim_missing_hpcrun_columns(engine)
    yield engine
    await engine.dispose()


@pytest.mark.asyncio
async def test_pre_fix_migration_enum_is_missing_pending_and_cancelled(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Documents the real, currently-deployed defect (no fix applied)."""
    await _migrate_to(fresh_postgres_url, PRE_FIX_REVISION, monkeypatch)
    labels = await _real_jobstatusdb_labels(fresh_postgres_url)
    assert labels == {"WAITING", "QUEUED", "RUNNING", "COMPLETED", "FAILED", "cancelled"}
    assert "PENDING" not in labels
    assert "CANCELLED" not in labels


@pytest.mark.asyncio
async def test_fix_migration_adds_pending_and_uppercase_cancelled(
    fresh_postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _migrate_to(fresh_postgres_url, FIX_REVISION, monkeypatch)
    labels = await _real_jobstatusdb_labels(fresh_postgres_url)
    assert {"PENDING", "CANCELLED"} <= labels


@pytest.mark.asyncio
async def test_cancel_write_fails_against_real_pre_fix_schema(hpcrun_capable_engine_pre_fix: AsyncEngine) -> None:
    """The exact real production failure (item 40's own evidence), reproduced
    end to end through the actual application code, against a database built
    the same way a real deployment's is -- through the real migration chain,
    never ``create_all``."""
    db = DatabaseServiceSQL(async_engine=hpcrun_capable_engine_pre_fix)
    simulator = await db.insert_simulator(
        git_repo_url="https://example.com/repo", git_branch="main", git_commit_hash="deadbee"
    )
    hpcrun = await db.insert_hpcrun(
        job_id=JobId.slurm(1), job_type=JobType.BUILD_IMAGE, ref_id=simulator.database_id, correlation_id="t"
    )

    with pytest.raises(DBAPIError, match="jobstatusdb"):
        await db.update_hpcrun_status(
            hpcrun_id=hpcrun.database_id, update=JobStatusUpdate(job_id=JobId.slurm(1), status=JobStatus.CANCELLED)
        )


@pytest.mark.asyncio
async def test_cancel_write_succeeds_against_real_fixed_schema(hpcrun_capable_engine_fixed: AsyncEngine) -> None:
    """backlog item 40's fix, proven against the REAL migrated schema: the
    identical write that fails above now succeeds."""
    db = DatabaseServiceSQL(async_engine=hpcrun_capable_engine_fixed)
    simulator = await db.insert_simulator(
        git_repo_url="https://example.com/repo", git_branch="main", git_commit_hash="deadbee"
    )
    hpcrun = await db.insert_hpcrun(
        job_id=JobId.slurm(1), job_type=JobType.BUILD_IMAGE, ref_id=simulator.database_id, correlation_id="t"
    )

    await db.update_hpcrun_status(
        hpcrun_id=hpcrun.database_id, update=JobStatusUpdate(job_id=JobId.slurm(1), status=JobStatus.CANCELLED)
    )
    updated = await db.get_hpcrun_by_ref(ref_id=simulator.database_id, job_type=JobType.BUILD_IMAGE)
    assert updated is not None
    assert updated.status == JobStatus.CANCELLED

    # backlog item 53's own evidence: list_active_hpcruns/list_active_chain_campaigns
    # ALSO bind JobStatusDB.PENDING against this same column (viva_api/simulation/
    # database_service.py) -- confirm the fix covers them too, not just the cancel
    # write. Both raised the identical InvalidTextRepresentationError pre-fix.
    await db.list_active_hpcruns()
    await db.list_active_chain_campaigns()
