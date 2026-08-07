#!/usr/bin/env python
"""Diagnose and reconcile the database's Alembic state (idempotent, mutating).

    uv run python scripts/db_reconcile.py          # apply
    uv run python scripts/db_analyze.py            # read-only preview first

Handles every state on one code path: fresh installs (upgrade from base),
already-managed databases (upgrade head, no-op if current), and legacy
create_all-bootstrapped databases (stamp the matched revision, then upgrade).
Refuses and prints manual instructions if the schema is inconsistent.

Connection: uses SQLALCHEMY_DATABASE_URL if set, else POSTGRES_HOST/PORT/USER/
PASSWORD/DATABASE. This is the same entrypoint the migration Job runs as
``python -m viva_api.simulation.db_reconcile --apply``.
"""

from viva_api.simulation.db_reconcile import main

if __name__ == "__main__":
    raise SystemExit(main(["--apply"]))
