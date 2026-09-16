#!/usr/bin/env python
"""Register and tag the CD2 ptools fill bundles in the dataset registry (docs/plan-data-provenance.md §8).

Reads the CD2 manifest (``cd2_ptools_manifest.json``: every store, with the fill bundles
present in S3) and registers each bundle's ``ptools/*.tsv``, ``viz/`` figures and
``analysis.json`` through the reconciliation walk's own ``register_bundle``, tagged ``cd2`` and
``cd2-<family>``, plus ``cd2-fill`` when a dedicated fill job wrote the whole bundle.

It creates no run rows. A bundle belongs to the analysis run whose ``result_uri`` is the bundle
directory when one exists (the sim-time ``analysis-mnp-*`` gathers record theirs), otherwise
to the store's simulation, exactly as the walk attributes it. The fill jobs were not analysis
dispatches: slice 2 records them as task runs (``cd2_tool_runs.json``) and moves their files
to those rows, including the metabolite TSVs ``append-metabolites`` fills wrote into sim-time
bundles. Over waiting for the walk, this adds the CD2 tags and registers the bundles now.
Idempotent on dataset ``uri``.

    uv run python scripts/import_cd2_datasets.py --manifest PATH             # analyze: read-only report
    uv run python scripts/import_cd2_datasets.py --manifest PATH --apply     # register and tag

``--analyze`` (the default) reads only the database. ``--apply`` also lists each bundle's S3
prefix (read-only) and writes dataset rows. Narrow either with ``--family`` / ``--store``
(repeatable). Connection: SQLALCHEMY_DATABASE_URL or POSTGRES_* (as db_reconcile); S3 through
the app's storage settings. The manifest lives with the CD2 campaign notes, not in the repo.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from viva_api.analysis.models import DATASET_LIST_MAX_LIMIT, ProducerRef
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.simulation import dataset_walk

if TYPE_CHECKING:
    from viva_api.common.storage.file_service import FileService
    from viva_api.simulation.database_service import DatabaseService
    from viva_api.simulation.models import Simulation

DEDICATED_FILL = "dedicated-fill"


@dataclass(frozen=True)
class Bundle:
    """One fill bundle directory from the manifest."""

    store: str
    experiment_id: str
    family: str
    name: str
    uri: str  # s3://bucket/<store>/analyses/<name>, no trailing slash
    mode: str  # dedicated-fill | append-metabolites
    store_uri: str
    fill_jobs: tuple[str, ...]
    view_types: tuple[str, ...]
    ptools_tsv: int
    viz: int

    @property
    def tags(self) -> list[str]:
        # Every file of a dedicated fill came from the fill job; an append-metabolites bundle also
        # holds the sim-time gather's own files, so it is not tagged as a fill.
        return ["cd2", f"cd2-{self.family}", *(["cd2-fill"] if self.mode == DEDICATED_FILL else [])]


def fill_mode(kind: str) -> str:
    """The manifest's bundle ``kind`` without its note: ``append-metabolites(into sim-time analysis)``
    -> ``append-metabolites``."""
    return kind.split("(", 1)[0].strip() or "fill"


def _bundle(store: dict[str, Any], name: str, info: dict[str, Any]) -> Bundle:
    return Bundle(
        store=str(store["store"]),
        experiment_id=str(store.get("experiment_id") or store["store"]),
        family=str(store.get("family") or "unknown"),
        name=name,
        uri=str(info["s3_uri"]).rstrip("/"),
        mode=fill_mode(str(info.get("kind", ""))),
        store_uri=str(store.get("s3_uri", "")).rstrip("/"),
        fill_jobs=tuple(str(job) for job in store.get("fill_jobs") or ()),
        view_types=tuple(str(view) for view in info.get("view_types") or ()),
        ptools_tsv=int(info.get("ptools_tsv") or 0),
        viz=int(info.get("viz") or 0),
    )


def load_bundles(
    manifest: dict[str, Any], *, families: set[str] | None = None, stores: set[str] | None = None
) -> tuple[list[Bundle], list[str]]:
    """The bundles to import, in store order, and the selected stores that have none."""
    bundles: list[Bundle] = []
    empty: list[str] = []
    for store in manifest.get("stores", []):
        selected = (not families or store.get("family") in families) and (not stores or store["store"] in stores)
        if not selected:
            continue
        if not store.get("fill_bundles"):
            empty.append(str(store["store"]))
            continue
        bundles.extend(_bundle(store, name, info) for name, info in sorted(store["fill_bundles"].items()))
    return bundles, empty


@dataclass
class ImportReport:
    bundles: int = 0
    claimed: int = 0  # bundles an analysis run claims
    unclaimed: int = 0  # bundles attributed to their simulation
    registered: int = 0
    unchanged: int = 0
    unavailable: int = 0
    skipped: list[str] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)

    def summary(self, *, applied: bool) -> str:
        mode = "applied" if applied else "analyze (nothing written)"
        return (
            f"{mode}: {self.bundles} bundle(s), {self.claimed} claimed by an analysis run, "
            f"{self.unclaimed} attributed to their simulation; datasets {self.registered} registered, "
            f"{self.unchanged} unchanged, {self.unavailable} marked unavailable; {len(self.skipped)} skipped"
        )


async def _producer(
    db: DatabaseService, bundle: Bundle, simulation: Simulation, report: ImportReport
) -> tuple[ProducerRef, str]:
    """The producer the walk would give the bundle, and how to say so."""
    claimed = await db.get_analysis_by_result_uri(bundle.uri)
    if claimed is not None:
        report.claimed += 1
        return {"analysis_id": claimed.database_id}, f"analysis run {claimed.database_id} ({claimed.backend})"
    report.unclaimed += 1
    return {"simulation_id": simulation.database_id}, f"simulation {simulation.database_id} (no analysis run claims it)"


async def analyze_bundle(db: DatabaseService, bundle: Bundle, simulation: Simulation, report: ImportReport) -> None:
    _producer_ids, producer = await _producer(db, bundle, simulation, report)
    rows = await db.list_datasets(uri_prefix=f"{bundle.uri}/", available=None, limit=DATASET_LIST_MAX_LIMIT)
    count = f"{len(rows)}+" if len(rows) == DATASET_LIST_MAX_LIMIT else str(len(rows))
    report.lines.append(
        f"  {bundle.name}: {producer}; {count} dataset(s) registered; "
        f"manifest lists {bundle.ptools_tsv} TSV, {bundle.viz} figure(s)"
    )


async def apply_bundle(
    db: DatabaseService, file_service: FileService, bundle: Bundle, simulation: Simulation, report: ImportReport
) -> None:
    producer_ids, producer = await _producer(db, bundle, simulation, report)
    bucket, key = dataset_walk.split_s3_uri(bundle.uri)
    items = await file_service.get_listing(S3FilePath(s3_path=Path(key)))
    walked = await dataset_walk.register_bundle(
        items,
        bucket=bucket,
        bundle_key=key,
        producer=producer_ids,
        simulation=simulation,
        db=db,
        file_service=file_service,
        tags=bundle.tags,
    )
    report.registered += walked.registered
    report.unchanged += walked.unchanged
    report.unavailable += walked.unavailable
    report.skipped.extend(walked.reasons)
    report.lines.append(
        f"  {bundle.name}: {producer}; {walked.registered} registered, {walked.unchanged} unchanged, "
        f"{walked.unavailable} marked unavailable"
    )


async def import_bundles(
    bundles: list[Bundle],
    *,
    db: DatabaseService,
    file_service: FileService | None,
    storage_bucket: str | None,
    apply: bool,
) -> ImportReport:
    """Analyze or apply every bundle. A bundle outside the storage bucket, or whose store has
    no simulation row, is skipped with a reason; nothing else stops the run."""
    if apply and file_service is None:
        raise ValueError("--apply needs a file service to list the bundles")
    report = ImportReport()
    simulations: dict[str, Simulation | None] = {}
    for bundle in bundles:
        if not report.lines or not report.lines[-1].startswith(("  ", bundle.store)):
            report.lines.append(bundle.store)
        bucket, _key = dataset_walk.split_s3_uri(bundle.uri)
        if storage_bucket and bucket != storage_bucket:
            report.skipped.append(f"{bundle.uri}: not in the storage bucket {storage_bucket!r}")
            continue
        if bundle.experiment_id not in simulations:
            simulations[bundle.experiment_id] = await db.get_simulation_by_experiment_id(bundle.experiment_id)
        simulation = simulations[bundle.experiment_id]
        if simulation is None:
            report.skipped.append(
                f"{bundle.store}/{bundle.name}: no simulation with experiment_id {bundle.experiment_id!r}"
            )
            continue
        report.bundles += 1
        if apply and file_service is not None:
            await apply_bundle(db, file_service, bundle, simulation, report)
        else:
            await analyze_bundle(db, bundle, simulation, report)
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register and tag the CD2 ptools fill bundles as datasets.")
    parser.add_argument("--manifest", required=True, type=Path, help="Path to cd2_ptools_manifest.json.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--analyze", action="store_true", help="Report what would be registered (default; no writes).")
    mode.add_argument("--apply", action="store_true", help="Register and tag dataset rows.")
    parser.add_argument("--family", action="append", default=[], help="Only this family (repeatable), e.g. run3.")
    parser.add_argument("--store", action="append", default=[], help="Only this store (repeatable).")
    return parser.parse_args(argv)


async def run(args: argparse.Namespace) -> int:
    from sqlalchemy.ext.asyncio import create_async_engine

    from viva_api.common.storage.file_service_s3 import FileServiceS3
    from viva_api.config import get_settings
    from viva_api.simulation.database_service import DatabaseServiceSQL
    from viva_api.simulation.db_reconcile import resolve_database_url

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    bundles, empty = load_bundles(manifest, families=set(args.family) or None, stores=set(args.store) or None)
    engine = create_async_engine(resolve_database_url())
    file_service = FileServiceS3() if args.apply else None
    try:
        report = await import_bundles(
            bundles,
            db=DatabaseServiceSQL(async_engine=engine),
            file_service=file_service,
            storage_bucket=get_settings().storage_s3_bucket or None,
            apply=bool(args.apply),
        )
    finally:
        if file_service is not None:
            await file_service.close()
        await engine.dispose()
    for line in report.lines:
        print(line)
    for reason in report.skipped:
        print(f"skipped: {reason}")
    print(f"{len(empty)} selected store(s) have no fill bundles")
    print(report.summary(applied=bool(args.apply)))
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run(parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
