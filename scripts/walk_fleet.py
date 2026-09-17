#!/usr/bin/env python
"""Walk EVERY simulation's ``analyses/`` prefix once, newest first, and register what it finds.

``scripts/walk_datasets.py`` walks a batch and is the tool for inspecting the walk's behaviour
(it is read-only by default). This is the other shape: a full pass over the fleet, meant to be
run by hand after a deploy, or as a one-shot Kubernetes Job (viva-api#673), where it must
survive hours, credential expiry and eviction.

    uv run python scripts/walk_fleet.py --analyze              # report only, no writes
    uv run python scripts/walk_fleet.py --apply                # register rows
    uv run python scripts/walk_fleet.py --apply --resume       # continue after an interruption

It runs the same code path as the scheduler's tick (``dataset_walk.reconcile_simulation``), so
the result is the walk's real behaviour rather than a re-implementation of it.

**S3 is only ever read, and only LISTED** -- one `list_objects_v2` per simulation, never an
object's contents (viva-api#675).
The only writes are ``dataset`` rows in ``SQLALCHEMY_DATABASE_URL`` / ``POSTGRES_*``.

Three properties a full pass needs that a batch does not, each learned from the 2026-09-16 run
over 1,319 simulations (see ``docs/runbook-dataset-walk.md``):

* **A cursor after every simulation.** That run was killed three times by host memory pressure.
  Resuming costs at most one simulation, and because the upsert is keyed on ``uri`` a re-walk
  reports ``unchanged`` rather than duplicating -- which is what makes interruption safe.
* **Credential tolerance.** A multi-hour pass outlives an SSO access token. The walk waits and
  retries rather than exiting, so re-authenticating at any point resumes it.
* **Newest first.** Simulations are taken in ``id`` DESC order via ``list_simulations``, which
  is also the LENIENT read path: ``_build_simulations`` strips keys that a stored config carries
  but today's model forbids and re-parses, so rows like dev ids 61-72 (``parca_options.rnaseq_*``,
  made ``extra="forbid"`` after they were written) walk normally. Resolving ids one at a time
  through ``get_simulation`` instead would raise ``ValidationError`` on exactly those rows.

**There is deliberately no ``--wipe``.** The walk can only recreate rows it can see in S3, which
means ``origin = walk`` rows. A row registered from an ``artifact.written`` trace event cannot be
rebuilt -- the events that produced it are long since consumed -- so truncating the table would
be silent, permanent data loss as soon as the emit side ships. The walk is idempotent; a rebuild
does not need a wipe.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import pathlib
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from viva_api.simulation.models import Simulation

#: Wait this long for credentials to come back before trying the same simulation again.
CREDENTIAL_WAIT_SECONDS = 300

#: Give up once credentials have been unusable for this long (a whole night, then stop).
CREDENTIAL_MAX_WAIT_HOURS = 10

#: Simulations fetched per query. Each one parses its stored config, so this is a real cost;
#: ``list_simulations`` without a limit validates the whole table in one go (viva-api#653).
PAGE_SIZE = 100

#: Substrings that mean "the session is gone", not "this simulation is broken".
CREDENTIAL_MARKERS = (
    "expiredtoken",
    "tokenexpired",
    "unauthorizedssotoken",
    "invalidgrant",
    "credential",
    "accessdenied",
    "unable to locate credentials",
    "sso session",
)


def _now() -> str:
    return dt.datetime.now(dt.UTC).strftime("%H:%M:%S")


def _looks_like_credentials(error: BaseException) -> bool:
    text = f"{type(error).__name__} {error}".lower()
    return any(marker in text for marker in CREDENTIAL_MARKERS)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Walk every simulation's analyses/ prefix, newest first.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--analyze", action="store_true", help="Report what would change (default; no writes).")
    mode.add_argument("--apply", action="store_true", help="Register rows and flip availability.")
    parser.add_argument(
        "--cursor",
        default="walk_fleet_cursor.json",
        help="Progress file, rewritten after every simulation (default: ./walk_fleet_cursor.json). "
        "In a Job, point this at a volume that outlives the pod, or re-run from the start.",
    )
    parser.add_argument("--resume", action="store_true", help="Continue below the cursor's last simulation id.")
    parser.add_argument("--limit", type=int, default=0, help="Stop after N simulations (0 = the whole fleet).")
    parser.add_argument("-v", "--verbose", action="store_true", help="With --analyze, list the sampled uris.")
    return parser.parse_args(argv)


async def _walk_one(
    simulation: Simulation,
    *,
    db: Any,
    file_service: Any,
    bucket: str,
    index: int,
    total: int,
) -> tuple[Any, Any]:
    """Walk one simulation, waiting out a credential outage rather than failing the run.

    Returns ``(result, file_service)`` -- the file service is rebuilt after an outage, so the
    caller must take the one handed back.
    """
    from viva_api.common.storage.file_service_s3 import FileServiceS3
    from viva_api.simulation import dataset_walk

    waited = 0.0
    while True:
        try:
            result = await dataset_walk.reconcile_simulation(
                simulation, db=db, file_service=file_service, storage_bucket=bucket
            )
        except Exception as error:
            if _looks_like_credentials(error) and waited < CREDENTIAL_MAX_WAIT_HOURS * 3600:
                print(
                    f"{_now()} [{index}/{total}] sim {simulation.database_id}: credentials unusable "
                    f"({type(error).__name__}); waiting {CREDENTIAL_WAIT_SECONDS}s",
                    flush=True,
                )
                await file_service.close()
                await asyncio.sleep(CREDENTIAL_WAIT_SECONDS)
                waited += CREDENTIAL_WAIT_SECONDS
                file_service = FileServiceS3()
                continue
            print(
                f"{_now()} [{index}/{total}] sim {simulation.database_id}: FAILED "
                f"{type(error).__name__}: {str(error)[:160]}",
                flush=True,
            )
            return None, file_service
        return result, file_service


def _record_progress(
    result: Any,
    simulation: Simulation,
    *,
    cursor_path: pathlib.Path,
    index: int,
    count: int,
    walked: int,
    failed: int,
    total: Any,
    started: float,
) -> None:
    """Log what one simulation did, then rewrite the cursor.

    The cursor is written after EVERY simulation, which is what makes an interrupted pass cost
    at most one simulation to resume -- the 2026-09-16 run was killed three times.
    """
    if result.bundles or result.skipped:
        print(
            f"{_now()} [{index}/{count}] sim {simulation.database_id} ({simulation.experiment_id}): "
            f"{result.bundles} bundle(s), {result.unclaimed} unclaimed, {result.registered} registered, "
            f"{result.unchanged} unchanged, {result.unavailable} unavailable, {result.skipped} skipped",
            flush=True,
        )
    cursor_path.write_text(
        json.dumps(
            {
                "last_id": simulation.database_id,
                "index": index,
                "of": count,
                "walked": walked,
                "failed": failed,
                "registered": total.registered,
                "bundles": total.bundles,
                "elapsed_s": round(time.monotonic() - started, 1),
                "at": dt.datetime.now(dt.UTC).isoformat(),
            },
            indent=2,
        )
    )
    if index % 50 == 0:
        rate = (time.monotonic() - started) / index
        print(
            f"{_now()} --- {index}/{count} done, {total.registered} registered, "
            f"{total.bundles} bundles, {failed} failed, ~{rate * (count - index) / 60:.0f} min left",
            flush=True,
        )


async def _simulations_newest_first(db: Any, below_id: int | None) -> list[Simulation]:
    """Every simulation, id descending, page by page.

    ``list_simulations`` is the lenient read path (``_build_simulations`` drops stored-config keys
    the current model forbids), which is why a full pass uses it rather than resolving ids one at
    a time through the strict ``get_simulation``.
    """
    found: list[Simulation] = []
    offset = 0
    while True:
        page = await db.list_simulations(limit=PAGE_SIZE, offset=offset)
        if not page:
            break
        found.extend(sim for sim in page if below_id is None or sim.database_id < below_id)
        offset += len(page)
        if len(page) < PAGE_SIZE:
            break
    return found


async def run(args: argparse.Namespace) -> int:
    from sqlalchemy.ext.asyncio import create_async_engine

    from viva_api.common.storage.file_service_s3 import FileServiceS3
    from viva_api.config import get_settings
    from viva_api.simulation import dataset_walk
    from viva_api.simulation.database_service import DatabaseServiceSQL
    from viva_api.simulation.db_reconcile import resolve_database_url

    bucket = get_settings().storage_s3_bucket or None
    if not bucket:
        print("STORAGE_S3_BUCKET is not set: the walk would skip every simulation", flush=True)
        return 2

    cursor_path = pathlib.Path(args.cursor)
    below_id: int | None = None
    if args.resume and cursor_path.exists():
        below_id = int(json.loads(cursor_path.read_text())["last_id"])
        print(f"resuming below simulation {below_id}", flush=True)

    engine = create_async_engine(resolve_database_url())
    real_db = DatabaseServiceSQL(async_engine=engine)
    # --analyze reuses the dry-run proxy `walk_datasets.py` already ships rather than a second,
    # weaker copy: it answers every read from the real service and decides "would register" vs
    # "would update" vs "unchanged" the way `upsert_dataset` would, including refusing to rewrite
    # an event-sourced row. One instance for the whole pass, so the totals span every simulation.
    walk_datasets = _walk_datasets_module()
    dry_run = None if args.apply else walk_datasets._DryRunDatabase(real_db)
    db = dry_run if dry_run is not None else real_db
    file_service = FileServiceS3()
    total = dataset_walk.WalkResult()
    walked = 0
    failed = 0
    started = time.monotonic()

    try:
        simulations = await _simulations_newest_first(real_db, below_id)
        if args.limit:
            simulations = simulations[: args.limit]
        count = len(simulations)
        print(
            f"{'applying to' if args.apply else 'analyzing'} {count} simulation(s), newest first, bucket {bucket}",
            flush=True,
        )
        if not args.apply:
            print("(--analyze: nothing is written; pass --apply to register rows)", flush=True)

        for index, simulation in enumerate(simulations, start=1):
            result, file_service = await _walk_one(
                simulation, db=db, file_service=file_service, bucket=bucket, index=index, total=count
            )
            if result is None:
                failed += 1
                continue
            total.merge(result)
            walked += 1
            _record_progress(
                result,
                simulation,
                cursor_path=cursor_path,
                index=index,
                count=count,
                walked=walked,
                failed=failed,
                total=total,
                started=started,
            )
    finally:
        await file_service.close()
        await engine.dispose()

    for reason in sorted(set(total.reasons))[:20]:
        print(f"skipped: {reason}", flush=True)
    if dry_run is not None:
        walk_datasets._print_dry_run(dry_run, verbose=bool(args.verbose))
    print(
        f"\nFLEET WALK COMPLETE in {(time.monotonic() - started) / 60:.1f} min: {walked} simulation(s) walked, "
        f"{failed} failed, {total.bundles} bundle(s), {total.unclaimed} unclaimed, {total.registered} registered, "
        f"{total.unchanged} unchanged, {total.unavailable} marked unavailable, {total.skipped} skipped",
        flush=True,
    )
    return 0


def _walk_datasets_module() -> Any:
    """Load ``scripts/walk_datasets.py`` as a module, by path.

    ``scripts/`` is not a package, so a plain ``import walk_datasets`` would only resolve when
    this file is launched as ``python scripts/walk_fleet.py`` (its directory becoming
    ``sys.path[0]``). Loading by a path derived from ``__file__`` works however it is invoked --
    the same approach ``tests/scripts/`` uses to reach these scripts.
    """
    import importlib.util

    path = pathlib.Path(__file__).with_name("walk_datasets.py")
    spec = importlib.util.spec_from_file_location("walk_datasets", path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise ImportError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run(parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
