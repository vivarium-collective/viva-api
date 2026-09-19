#!/usr/bin/env python
"""Load a CSV export of a site's tables into a LOCAL Postgres, for offline work.

Pairs with the `\\copy` export in `docs/runbook-dataset-walk.md`: that writes one CSV per table
from a running deployment (read-only), and this builds a local copy from them, parents first.

    SQLALCHEMY_DATABASE_URL=postgresql+asyncpg://postgres:pw@localhost:5433/postgres \\
        uv run python scripts/load_db_export.py /path/to/export

Two deliberate choices:

* **The schema comes from the ORM** (`tables_orm.create_db`), the way the test fixtures build
  one -- not from `alembic upgrade head`, which still fails on a truly empty database (#637).
  The copy therefore has the current schema, including columns the source deployment predates.
* **COPY, not INSERT.** The CSVs were written by `\\copy`, so Postgres parses `jsonb` columns
  (`simulation.config`, `analysis.config`) itself instead of us pushing text at typed columns.

Refuses any URL that is not local: this is for a throwaway copy, never a hosted database.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import pathlib

#: Load order: a child's parents must exist first. Also the allow-list every identifier below
#: is checked against, so no caller-supplied text reaches SQL.
TABLES: tuple[str, ...] = ("simulator", "parca_dataset", "simulation", "analysis")

#: The statements each table needs, built once from the allow-list above: a literal per table,
#: never an f-string over a name from outside it.
_COUNT_SQL = {table: f"SELECT count(*) FROM public.{table}" for table in TABLES}  # noqa: S608
_SEQUENCE_SQL = {table: f"SELECT pg_get_serial_sequence('public.{table}', 'id')" for table in TABLES}
_SETVAL_SQL = {table: f"SELECT setval($1, (SELECT max(id) FROM public.{table}))" for table in TABLES}  # noqa: S608

_LOCAL_HOSTS = ("localhost", "127.0.0.1")


async def load(directory: pathlib.Path, url: str) -> int:
    import asyncpg
    from sqlalchemy.ext.asyncio import create_async_engine

    from viva_api.simulation.tables_orm import create_db

    if not any(host in url for host in _LOCAL_HOSTS):
        print(f"refusing to load into {url!r}: this must be a local copy")
        return 2
    missing = [table for table in TABLES if not (directory / f"{table}.csv").is_file()]
    if missing:
        print(f"no CSV for {', '.join(missing)} in {directory}")
        return 2

    engine = create_async_engine(url)
    try:
        await create_db(engine)
    finally:
        await engine.dispose()
    print("schema created from the ORM")

    connection = await asyncpg.connect(url.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        for table in TABLES:
            path = directory / f"{table}.csv"
            with path.open(encoding="utf-8", newline="") as handle:
                columns = next(csv.reader(handle))
            with path.open("rb") as source:
                await connection.copy_to_table(
                    table, source=source, columns=columns, format="csv", header=True, schema_name="public"
                )
            count = await connection.fetchval(_COUNT_SQL[table])
            sequence = await connection.fetchval(_SEQUENCE_SQL[table])
            if sequence:
                await connection.execute(_SETVAL_SQL[table], sequence)
            print(f"{table}: {count} row(s)")
    finally:
        await connection.close()
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load a CSV export into a local Postgres copy.")
    parser.add_argument("directory", type=pathlib.Path, help="Directory holding <table>.csv files.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    url = os.environ.get("SQLALCHEMY_DATABASE_URL", "")
    if not url:
        print("set SQLALCHEMY_DATABASE_URL to the local copy")
        return 2
    return asyncio.run(load(args.directory, url))


if __name__ == "__main__":
    raise SystemExit(main())
