#!/usr/bin/env python
"""Backfill `analysis` rows for existing S3 analysis result sets.

For each experiment under the output prefix that has an ``analyses/`` directory
with output files, records a READY `analysis` row pointing at it (``result_uri``),
so the analysis-results endpoints can list and serve pre-run results (see
docs/source/architecture/analysis-results-design.md).

Handles both S3 nestings (single ``{prefix}/{exp}/analyses`` and double
``{prefix}/{exp}/{exp}/analyses``). ``n_tp`` is inferred from a representative TSV
(count of ``t*`` header columns) and left NULL when not time-sampled (e.g. the
cd1_* demo data). Idempotent: skips an experiment that already has a row with the
same ``result_uri``.

Connection: SQLALCHEMY_DATABASE_URL or POSTGRES_* (same as db_reconcile). S3:
the app's storage settings (IAM role / env creds). Read-only against S3; only
writes `analysis` rows. Pass --dry-run to log without writing.

    uv run python scripts/backfill_analysis_results.py [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio

import aioboto3
from sqlalchemy.ext.asyncio import create_async_engine

from viva_api.analysis.models import infer_n_tp_from_tsv
from viva_api.config import get_settings
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.db_reconcile import resolve_database_url
from viva_api.simulation.tables_orm import AnalysisStatusDB

_OUTPUT_EXTENSIONS = (".tsv", ".csv", ".txt", ".html")


async def _analyses_prefix_with_files(client, bucket: str, prefix: str, exp: str) -> str | None:  # type: ignore[no-untyped-def]
    """Return the bucket-relative analyses prefix (single- or double-nested) that has output files."""
    base = prefix.rstrip("/")
    for candidate in (f"{base}/{exp}/{exp}/analyses", f"{base}/{exp}/analyses"):
        resp = await client.list_objects_v2(Bucket=bucket, Prefix=candidate + "/", MaxKeys=200)
        for obj in resp.get("Contents", []):
            if obj["Key"].lower().endswith(_OUTPUT_EXTENSIONS):
                return candidate
    return None


async def _infer_n_tp(client, bucket: str, analyses_prefix: str) -> int | None:  # type: ignore[no-untyped-def]
    """Best-effort n_tp from the first .tsv under the prefix (None if not time-sampled)."""
    paginator = client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=analyses_prefix + "/"):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".tsv"):
                body = await (await client.get_object(Bucket=bucket, Key=obj["Key"]))["Body"].read()
                n_tp = infer_n_tp_from_tsv(body.decode(errors="replace"))
                return n_tp or None
    return None


async def backfill(dry_run: bool) -> int:
    settings = get_settings()
    bucket, prefix, region = settings.s3_work_bucket, settings.s3_output_prefix, settings.storage_s3_region
    engine = create_async_engine(resolve_database_url())
    db = DatabaseServiceSQL(async_engine=engine)
    recorded = 0
    try:
        session = aioboto3.Session(region_name=region)
        async with session.client("s3") as client:
            resp = await client.list_objects_v2(Bucket=bucket, Prefix=prefix.rstrip("/") + "/", Delimiter="/")
            experiments = [cp["Prefix"].rstrip("/").split("/")[-1] for cp in resp.get("CommonPrefixes", [])]
            print(f"scanning {len(experiments)} experiment dirs under {bucket}/{prefix}")
            for exp in experiments:
                analyses_prefix = await _analyses_prefix_with_files(client, bucket, prefix, exp)
                if analyses_prefix is None:
                    continue
                existing = await db.list_analyses(experiment_id=exp)
                if any(a.result_uri == analyses_prefix for a in existing):
                    print(f"  {exp}: already recorded, skipping")
                    continue
                n_tp = await _infer_n_tp(client, bucket, analyses_prefix)
                sim = await db.get_simulation_by_experiment_id(exp)
                sim_id = sim.database_id if sim is not None else None
                print(f"  {exp}: record READY n_tp={n_tp} sim_id={sim_id} result_uri={analyses_prefix}")
                if not dry_run:
                    await db.record_analysis(
                        experiment_id=exp,
                        n_tp=n_tp,
                        status=AnalysisStatusDB.READY,
                        config={"analysis_options": {"experiment_id": [exp]}},
                        name=f"backfill-{exp}"[:200],
                        simulation_id=sim_id,
                        backend="batch",
                        result_uri=analyses_prefix,
                    )
                recorded += 1
    finally:
        await engine.dispose()
    print(f"{'(dry-run) would record' if dry_run else 'recorded'} {recorded} analysis rows")
    return recorded


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill analysis rows for existing S3 result sets.")
    parser.add_argument("--dry-run", action="store_true", help="Log what would be recorded without writing.")
    args = parser.parse_args()
    asyncio.run(backfill(dry_run=args.dry_run))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
