#!/usr/bin/env python
"""Run the dataset walk once, by hand, against whatever database you point it at.

The walk normally lives in the scheduler (`JobScheduler.reconcile_datasets`, 25 simulations
every 60 s), which makes it awkward to exercise: you cannot see what it *would* do before it
does it. This runs the same code path (`dataset_walk.reconcile_simulation`) over a chosen set
of simulations, read-only by default.

    uv run python scripts/walk_datasets.py --limit 25              # analyze: nothing written
    uv run python scripts/walk_datasets.py --simulation 1319 -v    # one simulation, per-file
    uv run python scripts/walk_datasets.py --limit 25 --apply      # register rows

**S3 is only ever read** -- one LIST per simulation and a ranged GET of a TSV's first bytes
(`FileServiceS3.get_file_head`). The walk's only writes are `dataset` rows, so `--apply`
writes exactly to the database in `SQLALCHEMY_DATABASE_URL` / `POSTGRES_*` and nowhere else.
Point that at a restored dump, not at a hosted database: see
`docs/runbook-dataset-walk.md`.

`--analyze` wraps the database service in a proxy that answers every read and intercepts the
two writes the walk makes (`upsert_dataset`, `set_dataset_available`), so the report is what
the same code would do, not a re-implementation of it.
"""

from __future__ import annotations

import argparse
import asyncio
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from viva_api.analysis.models import DatasetDTO
    from viva_api.simulation.database_service import DatabaseService
    from viva_api.simulation.models import Simulation

#: Sampled into the report; the counts are always complete.
_MAX_SAMPLES = 20


class _DryRunDatabase:
    """Every read reaches the real service; the walk's two writes are recorded instead.

    Not a `DatabaseService` subclass on purpose: that is a wide ABC, and a proxy that
    forwards everything cannot drift out of date with it.
    """

    def __init__(self, db: DatabaseService) -> None:
        self._db = db
        self.would_register: list[str] = []
        self.would_update: list[str] = []
        self.would_flip: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._db, name)

    async def _existing(self, uri: str) -> DatasetDTO | None:
        rows = await self._db.list_datasets(uri_prefix=uri, available=None, limit=2)
        return next((row for row in rows if row.uri == uri), None)

    async def upsert_dataset(self, **fields: Any) -> tuple[DatasetDTO, str]:
        from viva_api.analysis.models import DatasetDTO

        uri = str(fields["uri"])
        origin = str(fields.get("origin") or "")
        existing = await self._existing(uri)
        if existing is None:
            if not any(fields.get(key) for key in ("simulation_id", "parca_dataset_id", "analysis_id")):
                raise ValueError("a new dataset row needs a producer id")
            self.would_register.append(uri)
            attributes = dict(fields.get("attributes") or {})
            attributes.setdefault("origin", origin)
            return (
                DatasetDTO(
                    database_id=0,
                    kind=str(fields["kind"]),
                    uri=uri,
                    simulation_id=fields.get("simulation_id"),
                    parca_dataset_id=fields.get("parca_dataset_id"),
                    analysis_id=fields.get("analysis_id"),
                    view=fields.get("view"),
                    display_name=fields.get("display_name"),
                    size_bytes=fields.get("size_bytes"),
                    sha256=fields.get("sha256"),
                    attributes=attributes,
                    tags=list(fields.get("tags") or []),
                    source=fields.get("source"),
                    available=bool(fields.get("available", True)),
                ),
                "inserted",
            )
        if origin == "walk" and existing.origin == "event":
            return existing, "skipped"  # the walk may not rewrite an event-sourced row
        changed = existing.size_bytes != fields.get("size_bytes") or existing.available != bool(
            fields.get("available", True)
        )
        if changed:
            self.would_update.append(uri)
            return existing, "updated"
        return existing, "skipped"

    async def set_dataset_available(self, dataset_id: int, available: bool) -> DatasetDTO | None:
        self.would_flip.append(f"dataset {dataset_id} -> available={str(available).lower()}")
        return None


def _print_dry_run(dry_run: _DryRunDatabase, *, verbose: bool) -> None:
    """What the same walk would have written, had it been ``--apply``."""
    print(
        f"analyze (nothing written): would register {len(dry_run.would_register)}, "
        f"update {len(dry_run.would_update)}, flip availability on {len(dry_run.would_flip)}"
    )
    if not verbose:
        return
    for label, items in (
        ("register", dry_run.would_register),
        ("update", dry_run.would_update),
        ("flip", dry_run.would_flip),
    ):
        for item in items[:_MAX_SAMPLES]:
            print(f"  would {label}: {item}")
        if len(items) > _MAX_SAMPLES:
            print(f"  ... and {len(items) - _MAX_SAMPLES} more to {label}")


async def _simulations(db: DatabaseService, args: argparse.Namespace) -> list[Simulation]:
    if args.simulation:
        found = [await db.get_simulation(sid) for sid in args.simulation]
        missing = [sid for sid, sim in zip(args.simulation, found, strict=True) if sim is None]
        if missing:
            print(f"no simulation row for id(s): {', '.join(str(sid) for sid in missing)}")
        return [sim for sim in found if sim is not None]
    return await db.list_simulations_after(args.after, limit=args.limit)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Walk simulations' analyses/ prefixes and register datasets.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--analyze", action="store_true", help="Report what would change (default; no writes).")
    mode.add_argument("--apply", action="store_true", help="Register rows and flip availability.")
    parser.add_argument("--simulation", action="append", type=int, default=[], help="Simulation id (repeatable).")
    parser.add_argument("--limit", type=int, default=25, help="Simulations to walk when none is named (default 25).")
    parser.add_argument("--after", type=int, default=0, help="Start after this simulation id (the tick's cursor).")
    parser.add_argument("-v", "--verbose", action="store_true", help="List the sampled uris, not just the counts.")
    return parser.parse_args(argv)


async def run(args: argparse.Namespace) -> int:
    from sqlalchemy.ext.asyncio import create_async_engine

    from viva_api.common.storage.file_service_s3 import FileServiceS3
    from viva_api.config import get_settings
    from viva_api.simulation import dataset_walk
    from viva_api.simulation.database_service import DatabaseServiceSQL
    from viva_api.simulation.db_reconcile import resolve_database_url

    bucket = get_settings().storage_s3_bucket or None
    if not bucket:
        print("STORAGE_S3_BUCKET is not set: the walk would skip every simulation")
        return 2
    engine = create_async_engine(resolve_database_url())
    real_db = DatabaseServiceSQL(async_engine=engine)
    dry_run = None if args.apply else _DryRunDatabase(real_db)
    db = cast("DatabaseService", dry_run) if dry_run is not None else real_db
    file_service = FileServiceS3()
    total = dataset_walk.WalkResult()
    try:
        simulations = await _simulations(real_db, args)
        print(f"{'applying to' if args.apply else 'analyzing'} {len(simulations)} simulation(s), bucket {bucket}")
        for simulation in simulations:
            try:
                result = await dataset_walk.reconcile_simulation(
                    simulation, db=db, file_service=file_service, storage_bucket=bucket
                )
            except Exception as error:  # one bad simulation must not end the run
                print(f"  simulation {simulation.database_id}: FAILED {error}")
                continue
            total.merge(result)
            if result.bundles or result.skipped:
                print(
                    f"  simulation {simulation.database_id} ({simulation.experiment_id}): "
                    f"{result.bundles} bundle(s), {result.unclaimed} unclaimed, {result.registered} registered, "
                    f"{result.unchanged} unchanged, {result.unavailable} unavailable, {result.skipped} skipped"
                )
    finally:
        await file_service.close()
        await engine.dispose()

    for reason in total.reasons:
        print(f"skipped: {reason}")
    if dry_run is not None:
        _print_dry_run(dry_run, verbose=bool(args.verbose))
    print(
        f"totals: {total.bundles} bundle(s), {total.unclaimed} unclaimed, {total.registered} registered, "
        f"{total.unchanged} unchanged, {total.unavailable} marked unavailable, {total.skipped} skipped"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run(parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
