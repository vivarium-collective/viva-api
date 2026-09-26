"""Core's database connection, from its settings (U2e; decision D19).

One engine per process, built from the ``postgres_*`` fields. A site that never set them has no
database -- ``postgres_configured`` says so, and the lifespan runs without one rather than dialling
the placeholder. ``enable_ssl=False`` is for a laptop or a test container speaking plain TCP; a
deployed site's server decides.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from viva_core.settings import CoreSettings

UNSET_USER = "<USER>"  # the placeholder default: "never configured", not a user


def postgres_configured(settings: CoreSettings) -> bool:
    return settings.postgres_user != UNSET_USER and bool(settings.postgres_host) and bool(settings.postgres_database)


def postgres_url(settings: CoreSettings) -> str:
    return (
        f"postgresql+asyncpg://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_database}"
    )


def async_engine_from_settings(settings: CoreSettings, *, enable_ssl: bool = True) -> AsyncEngine:
    connect_args: dict[str, str] = {} if enable_ssl else {"ssl": "disable"}
    return create_async_engine(
        postgres_url(settings),
        echo=False,
        pool_size=settings.postgres_pool_size,
        max_overflow=settings.postgres_max_overflow,
        pool_timeout=settings.postgres_pool_timeout,
        pool_recycle=settings.postgres_pool_recycle,
        connect_args=connect_args,
    )
