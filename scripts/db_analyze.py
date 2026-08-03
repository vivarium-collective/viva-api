#!/usr/bin/env python
"""Read-only: report the database's Alembic reconciliation state. Changes nothing.

    uv run python scripts/db_analyze.py

Connection: uses SQLALCHEMY_DATABASE_URL if set, else POSTGRES_HOST/PORT/USER/
PASSWORD/DATABASE. In-cluster the same check is available as
``uv run python -m viva_api.simulation.db_reconcile --analyze``.

Exit code 0 = safe to reconcile; 2 = INCONSISTENT, manual reconciliation needed.
"""

from viva_api.simulation.db_reconcile import main

if __name__ == "__main__":
    raise SystemExit(main(["--analyze"]))
