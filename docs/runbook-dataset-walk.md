# Runbook: exercising the dataset walk against real S3 and a copy of the database

**What this is for.** Slice 1's walk feeder (`docs/plan-data-provenance.md` §5) is the only
feeder that works on historical jobs: no producer emits `artifact.written` yet, so every row a
live system would grow today comes from the walk. This runs it against the **real** objects
with a **copy** of the database, so you can see what it would register before anything hosted
is touched.

## Why this is safe

| what | reads | writes |
|---|---|---|
| `dataset_walk.reconcile_simulation` | one S3 `LIST` per simulation; a ranged `GET` of the first 8 KB of a TSV for `n_tp` | `dataset` rows only |
| `scripts/walk_datasets.py` (default `--analyze`) | the above | **nothing** — the writes are intercepted and counted |
| `scripts/walk_datasets.py --apply` | the above | `dataset` rows in `SQLALCHEMY_DATABASE_URL` |
| `scripts/import_cd2_datasets.py --analyze` | the database only | nothing |

The walk never writes to S3, never deletes a row, and never creates a run row. `--apply`
writes exactly to the database you point at — which is why step 2 restores a dump instead of
using a hosted one. **Never point `--apply` at a hosted database**; the app's own scheduler
does that, once slice 1 deploys.

## 1. Dump the dev database (needs Jim's go-ahead — it reads the hosted DB)

The api pod already holds the credentials, so dump from inside it:

```bash
CTX=arn:aws-us-gov:eks:us-gov-west-1:476270107793:cluster/smsvpctest-eks-blueprint
NS=sms-api-stanford-test
kubectl --context "$CTX" -n "$NS" exec deploy/api -c api -- \
  sh -lc 'PGPASSWORD="$POSTGRES_PASSWORD" pg_dump -h "$POSTGRES_HOST" -U "$POSTGRES_USER" \
          -d "$POSTGRES_DATABASE" -Fc --no-owner --no-privileges' > /tmp/viva-dev.dump
```

Smaller alternative when you only need the walk's inputs — `simulation`, `analysis`,
`dataset`, `hpcrun` and `alembic_version` — add `-t simulation -t analysis -t dataset -t hpcrun
-t alembic_version`. The dump carries real experiment ids and config; delete it when finished.

## 2. Restore into a local Postgres

```bash
docker run -d --name viva-dump -e POSTGRES_PASSWORD=pw -p 5433:5432 postgres:16
pg_restore -h localhost -p 5433 -U postgres -d postgres --no-owner /tmp/viva-dev.dump
export SQLALCHEMY_DATABASE_URL="postgresql+asyncpg://postgres:pw@localhost:5433/postgres"
```

## 3. Bring the copy up to slice 1's schema

```bash
uv run python scripts/db_analyze.py                       # read-only: classification + head
uv run python -m viva_api.simulation.db_reconcile         # applies migrations to the COPY
```

`db_analyze` prints `MANAGED` / `LEGACY` and what it would do; the reconciler then adds
`dataset`, `analysis.source`/`tags`, `hpcrun.jobref_analysis_id` and the `ANALYSIS` job type.

## 4. Point storage at the real bucket, read-only

```bash
export AWS_PROFILE=stanford-sso AWS_DEFAULT_REGION=us-gov-west-1
export STORAGE_S3_BUCKET=smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91
export STORAGE_S3_REGION=us-gov-west-1
aws sts get-caller-identity --profile stanford-sso   # confirm the session is live
```

The walk only reaches `<out_uri>/analyses/` under each simulation's output prefix. It never
touches `s3://…/tools/`.

## 5. Walk

```bash
uv run python scripts/walk_datasets.py --limit 25                 # the tick's first batch
uv run python scripts/walk_datasets.py --simulation 1319 -v       # one simulation, sampled uris
uv run python scripts/walk_datasets.py --limit 25 --apply         # write rows into the COPY
```

`--after <id>` continues where a previous batch stopped, exactly as the scheduler's cursor
does. Re-running `--apply` must report everything `unchanged`: the walk is idempotent on
`uri`, and that is the cheapest check that it is.

## 6. Read the result

```sql
-- what landed, by kind and how it was found
SELECT kind, attributes->>'origin' AS origin, count(*), sum((NOT available)::int) AS gone
FROM dataset GROUP BY 1, 2 ORDER BY 3 DESC;

-- who owns it: an analysis run, or the simulation the walk found it under
SELECT CASE WHEN analysis_id IS NOT NULL THEN 'analysis'
            WHEN parca_dataset_id IS NOT NULL THEN 'parca' ELSE 'simulation' END AS producer,
       count(*) FROM dataset GROUP BY 1;
```

Cross-check a bundle against the objects themselves:

```bash
aws s3 ls --recursive --profile stanford-sso \
  "s3://$STORAGE_S3_BUCKET/vecoli-output/<experiment_id>/analyses/" | grep -c -E '\.(tsv|html)$'
```

The counts should agree for `ptools/*.tsv` and `viz/*` — one row per consumable file.

## What this exercise does NOT cover

- **The trace feeder.** No producer emits `artifact.written` yet, so no row here will carry
  `origin = event`, `hpcrun_id` or `span_id`. That path is tested with the recorded event
  streams in `tests/simulation/test_real_trace_fixture.py`.
- **`parquet` and `parca-cache` rows.** The walk only classifies `ptools/*.tsv`, `viz/*` and
  `analysis.json` under a bundle. Those kinds arrive only by event.
- **Availability flips over time.** You will see rows marked unavailable only for objects that
  are already gone.
- **The API and clients.** They read the same rows, but nothing here starts a server.

## Cleanup

```bash
docker rm -f viva-dump && rm -f /tmp/viva-dev.dump
```
