# Runbook: exercising the dataset walk against real S3 and a copy of the database

**What this is for.** Slice 1's walk feeder (`docs/plan-data-provenance.md` §5) is the only feeder
that works on historical jobs: no producer emits `artifact.written` yet, so every row a live
system would grow today comes from the walk. This runs it against the **real** objects with a
**copy** of the database, so you can see what it would register before anything hosted is
touched. Run end to end on 2026-09-15 against `sms-api-stanford-test`.

## Why this is safe

| what | reads | writes |
|---|---|---|
| `dataset_walk.reconcile_simulation` | one S3 `LIST` per simulation; a ranged `GET` of the first 8 KB of a TSV for `n_tp` | `dataset` rows only |
| `scripts/walk_datasets.py` (default `--analyze`) | the above | **nothing** — the writes are intercepted and counted |
| `scripts/walk_datasets.py --apply` | the above | `dataset` rows in `SQLALCHEMY_DATABASE_URL` |
| the export in step 1 | the deployment's database | nothing |

The walk never writes to S3, never deletes a row, and never creates a run row. `--apply` writes
exactly to the database you point at — which is why step 2 builds a local copy. **Never point
`--apply` at a hosted database**; the app's own scheduler does that once slice 1 deploys.

## 1. Export the tables, as CSV, through psql

> **Why not `pg_dump`?** The api image ships **postgresql-client 15** and the RDS instances run
> **Postgres 17.9**, so `pg_dump` refuses: *"aborting because of server version mismatch"*.
> #668 upgrades the image to client 17; once an image built from it is deployed, a plain
> `kubectl exec deploy/api -c api -- pg_dump …` replaces this whole step. `psql` is unaffected by
> the version gap, so until then the export below is the way.
>
> **`aws rds` has no dump operation at all** — a snapshot cannot be downloaded, an export task
> writes Parquet (and wants a customer-managed KMS key), and restoring a snapshot builds a new
> instance. Don't go looking for one.

The walk reads simulations and the analysis rows that claim a bundle, so four tables suffice,
parents first: `simulator → parca_dataset → simulation → analysis`.

```bash
CTX=arn:aws-us-gov:eks:us-gov-west-1:476270107793:cluster/smsvpctest-eks-blueprint
NS=sms-api-stanford-test
kubectl --context "$CTX" -n "$NS" exec deploy/api -c api -- sh -lc '
  mkdir -p /tmp/vivadump
  for t in simulator parca_dataset simulation analysis; do
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
      -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" -v ON_ERROR_STOP=1 \
      -c "\copy (SELECT * FROM public.$t) TO /tmp/vivadump/$t.csv WITH (FORMAT csv, HEADER true)"
  done'
kubectl --context "$CTX" cp "$NS"/<api-pod>:/tmp/vivadump -c api ./vivadump
kubectl --context "$CTX" -n "$NS" exec deploy/api -c api -- rm -rf /tmp/vivadump
```

For scale: on 2026-09-15 that was 204 simulators, 258 parca datasets, 1319 simulations and 765
analyses — 12 MB of CSV from a 24 MB database. **The CSVs carry real experiment ids and
configs**; keep them out of the repo and delete them when you are done.

*Alternative, if you want a true `pg_dump` before #668 ships:* `AWS-StartPortForwardingSessionToRemoteHost`
is available in the GovCloud account, so SSM can forward RDS:5432 to your laptop (the same
mechanism `scripts/sms-tunnel.sh` uses for the ALB) and a local `docker run --rm postgres:17
pg_dump` then works. The RDS security group admits 5432 only from the security group its VPC
peers carry, so the session must target an instance in that group.

## 2. Build the local copy

```bash
docker run -d --name viva-dump -e POSTGRES_PASSWORD=pw -p 5433:5432 postgres:15
export SQLALCHEMY_DATABASE_URL="postgresql+asyncpg://postgres:pw@localhost:5433/postgres"
uv run python scripts/load_db_export.py ./vivadump
```

`load_db_export.py` creates the schema from the **ORM** (`tables_orm.create_db`) and then COPYs
the CSVs in, parents first. Two reasons it works that way: `alembic upgrade head` still fails on
a truly empty database (#637), and the ORM schema gives the copy slice 1's `dataset` table and
`analysis.source`/`tags` even though the source deployment predates them.

## 3. Point storage at the real bucket, read-only

```bash
export AWS_PROFILE=stanford-sso AWS_DEFAULT_REGION=us-gov-west-1
export STORAGE_S3_BUCKET=smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91
export STORAGE_S3_REGION=us-gov-west-1
aws sts get-caller-identity --profile stanford-sso   # confirm the session is live
```

The walk only reaches `<out_uri>/analyses/` under each simulation's output prefix. It never
touches `s3://…/tools/`.

## 4. Walk

```bash
uv run python scripts/walk_datasets.py --limit 25                 # the tick's first batch
uv run python scripts/walk_datasets.py --simulation 1131 -v       # one simulation, sampled uris
uv run python scripts/walk_datasets.py --simulation 1289 --apply  # write rows into the COPY
```

`--after <id>` continues where a previous batch stopped, exactly as the scheduler's cursor does.
Re-running `--apply` must report everything `unchanged`: the walk is idempotent on `uri`, and
that is the cheapest check that it is.

What this looked like on 2026-09-15, against the copy and the real bucket:

| simulation | bundles | what the walk found |
|---|---|---|
| 1289 (`run4-native-design9`) | 1 | 449 rows: 288 figures, 160 ptools TSVs, 1 report — matching the objects under that prefix exactly |
| 1002 (`run3-sweep-combo00`) | 3 | 815 rows, all three bundles unclaimed → "found under" the simulation |
| 1131 (`run2-j3-refire3`) | 4 | 1318 rows, three of four bundles unclaimed |
| ids 1301–1320 | 13 | 4649 rows, nothing to update or flip |

Applying simulation 1289 and re-analyzing it reported `0 registered, 449 unchanged`.

## 5. Read the result

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

The counts should agree for `ptools/*.tsv` and `viz/*`, plus one `report` row per bundle that
has an `analysis.json`.

## What this exercise does NOT cover

- **The trace feeder.** No producer emits `artifact.written` yet, so no row here carries
  `origin = event`, `hpcrun_id` or `span_id`. That path is tested with the recorded event streams
  in `tests/simulation/test_real_trace_fixture.py`.
- **`parquet` and `parca-cache` rows.** The walk only classifies `ptools/*.tsv`, `viz/*` and
  `analysis.json` under a bundle. Those kinds arrive only by event.
- **Availability flips over time.** You will see rows marked unavailable only for objects that
  are already gone.
- **The API and clients.** They read the same rows, but nothing here starts a server.

## Cleanup

```bash
docker rm -f viva-dump && rm -rf ./vivadump
```
