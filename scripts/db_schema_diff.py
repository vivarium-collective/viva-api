#!/usr/bin/env python
"""Read-only: compare the live database schema against the current ORM. Changes nothing.

    uv run python scripts/db_schema_diff.py

Surfaces schema drift that neither `create_all` nor Alembic will fix on an
existing database — most importantly, columns/enum-values the ORM expects that
are missing from tables/types that already exist. Run it before deploying to a
database that has jumped many app versions (e.g. prod 0.7.2 -> 0.9.20).

In-cluster the same check is available as
``uv run python -m viva_api.simulation.schema_diff``.

Connection: uses SQLALCHEMY_DATABASE_URL if set, else POSTGRES_HOST/PORT/USER/
PASSWORD/DATABASE. Exit code 0 = no blocking drift; 2 = blocking drift found.
"""

from viva_api.simulation.schema_diff import main

if __name__ == "__main__":
    raise SystemExit(main())
