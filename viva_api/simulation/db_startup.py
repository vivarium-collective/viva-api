"""What the app does to the schema at startup -- and what it checks (``docs/plan-core.md`` P0).

Historically the app ran ``create_all`` on every boot. That is convenient on a laptop and
corrosive in production: it creates a missing TABLE silently, so a model change that never got
a migration works on every site that boots the app first -- and fails on the first new site,
or on the first column (``create_all`` never alters an existing table). Nine tables reached
production that way before anyone noticed (viva-api#637).

``DB_CREATE_ALL`` makes it a choice:

* ``true`` (the default -- laptops, tests, anything without a migration Job): unchanged.
* ``false`` (deployed sites): the schema belongs to the ``alembic-migrate`` Job and nothing else.
  The app creates nothing. This is also what finally FREEZES the reconciler's fingerprint list:
  with no ``create_all`` there are no new un-stamped databases to adopt.

Turning the safety net off means a site whose migration Job did not run would fail later, at
query time, with an obscure ``UndefinedColumn``. So with or without ``create_all`` the app reads
the database's Alembic revision at startup and compares it with the chain's head. A mismatch is
logged as an ERROR that names the remedy, and is reported by ``/health`` -- it does NOT stop the
pod: a rolling deploy can start a pod moments before the Job finishes, and that must not become
an outage. ``atlantis smoke`` reads ``/health`` and fails the deploy check instead.
"""

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

UNKNOWN = "unknown"


@dataclass(frozen=True)
class SchemaState:
    """The database's Alembic revision against the chain's head, as of startup."""

    revision: str | None  # None: no alembic_version row -- an un-stamped or empty database
    head: str
    create_all: bool

    @property
    def at_head(self) -> bool:
        return self.revision is not None and self.revision == self.head

    def as_health_fields(self) -> dict[str, str]:
        """Strings, because ``/health`` is a ``dict[str, str]`` and its shape is in the spec."""
        return {
            "db_revision": self.revision or "none",
            "db_head": self.head,
            "db_at_head": UNKNOWN if self.head == UNKNOWN else str(self.at_head).lower(),
            "db_create_all": str(self.create_all).lower(),
        }


_state: SchemaState | None = None


def get_schema_state() -> SchemaState | None:
    return _state


def set_schema_state(state: SchemaState | None) -> None:
    global _state
    _state = state


async def create_tables_if_enabled(
    engine: AsyncEngine, create: Callable[[AsyncEngine], Awaitable[None]], *, enabled: bool, what: str
) -> bool:
    """The ONE place ``create_all`` is allowed to run from. Returns whether it ran."""
    if not enabled:
        logger.info("DB_CREATE_ALL=false: not creating %s tables; the schema belongs to the alembic-migrate Job", what)
        return False
    logger.info("Initializing %s tables (DB_CREATE_ALL=true)...", what)
    await create(engine)
    return True


def chain_head() -> str:
    """The head of the Alembic chain shipped in this image. ``unknown`` if it cannot be read --
    the check then reports rather than guesses."""
    try:
        from viva_api.simulation.db_reconcile import _alembic_config, _head_revision

        return _head_revision(_alembic_config("postgresql+asyncpg://unused/unused")) or UNKNOWN
    except Exception:
        logger.warning("could not read the Alembic head from this image", exc_info=True)
        return UNKNOWN


async def read_revision(engine: AsyncEngine) -> str | None:
    """``alembic_version.version_num`` in the current schema, or ``None`` if there is none."""
    async with engine.connect() as conn:
        exists = await conn.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = current_schema() AND table_name = 'alembic_version')"
            )
        )
        if not exists.scalar():
            return None
        value = (await conn.execute(text("SELECT version_num FROM alembic_version LIMIT 1"))).scalar()
        return str(value) if value is not None else None


async def check_schema(engine: AsyncEngine, *, create_all: bool) -> SchemaState:
    """Read-only. Records the state for ``/health`` and logs it -- loudly when it is wrong."""
    state = SchemaState(revision=await read_revision(engine), head=chain_head(), create_all=create_all)
    set_schema_state(state)
    if state.head == UNKNOWN:
        logger.warning("database revision %s; the chain head could not be read, so it was not checked", state.revision)
    elif state.at_head:
        logger.info("✓ database schema is at the Alembic head (%s)", state.head)
    else:
        logger.error(
            "DATABASE SCHEMA IS NOT AT HEAD: database revision is %s, this image expects %s. "
            "The app will fail at query time on whatever changed. Run the alembic-migrate Job for this "
            "namespace (kubectl delete job alembic-migrate; kubectl apply -k kustomize/overlays/<ns>-db-migration)%s",
            state.revision or "none (un-stamped)",
            state.head,
            "" if create_all else " -- DB_CREATE_ALL is false, so nothing here will create what is missing.",
        )
    return state
