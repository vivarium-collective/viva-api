# Design: sampling-aware analysis-result endpoints (`n_tp`)

- **Status:** Proposed
- **Date:** 2026-07-14
- **Scope:** viva-api endpoints only (Stanford K8s/Batch). Workbench UI is a follow-up.

## Context & motivation

ptools wants to consume pre-computed analysis results from viva-api. Analysis is **very expensive**, so
submission and retrieval must be **fully decoupled** — no blocking call may ever compute. Concretely
ptools needs to:

1. **List** existing / pre-run analyses for a simulation (with their sampling `n_tp` and status).
2. **Retrieve** an existing analysis data file **by its analysis database id** — a pure, non-computing
   fetch.
3. **Request** an analysis in a **nonblocking** way that **only submits a new job if one doesn't
   already exist** and is **idempotent** for an existing job (returns the existing record, never
   resubmits, never waits).
4. **Poll status** by analysis id.

`n_tp` ("number of timepoints" = the count of time-columns in the output TSV) is the sampling attribute
for this use case. It is an *input-only* knob today — it shapes the TSV column count but appears in no
S3 path or result DTO.

### Current state

The Stanford **K8s/Batch** analysis path (`run_standalone_analysis` → `submit_standalone_analysis`,
`viva_api/common/handlers/simulations.py:1459`, `viva_api/simulation/simulation_service_k8s.py:333`) fires
a fire-and-forget K8s Job, **persists nothing**, has **no list/status/fetch endpoint and no
idempotency**, and its K8s Job name (`ana-{safe_id}`) is not unique per sampling (collision across
`n_tp`). The legacy `POST /analyses` path is SLURM-only and **synchronous** (blocks polling the job to
completion).

### Explicit non-goal

**NOT** a blocking getter that returns cached results or computes-then-returns. The legacy synchronous
`POST /analyses` (blocks until the job finishes) is exactly what we are **not** building. Submit and
retrieve are separate operations; retrieval never triggers computation.

### Confirmed decisions

- Target **Stanford K8s/Batch (S3)** only; the SLURM path may 501 for now.
- `n_tp` is a **predetermined set**: `AVAILABLE_NTP = [10, 50, 100]`.
- Persist in a DB table that supports **backfilling** existing results.
- **viva-api endpoints only** (no workbench UI in this task).

## Design

### 1. Data model — generalize the existing `analysis` table (do **not** fork a new table)

Keep `ORMAnalysis` (table `analysis`, `viva_api/simulation/tables_orm.py:209`) as the single, general
record of **all** analysis types, with `config` (JSONB) remaining the authoritative store of arbitrary
analysis config. `n_tp` is merely the first attribute we need to *query* on; we denormalize/index only
what we must, extensible later. Add these **nullable** columns (nullable so legacy rows and the existing
`/ecoli` router / `to_dto()` are untouched; backfill fills them for old rows):

| column | type | notes |
|---|---|---|
| `experiment_id` | `str \| None`, indexed | the lookup key (also extractable from `config`) |
| `n_tp` | `int \| None`, indexed | first indexed attribute; denormalized from `config` |
| `status` | `AnalysisStatusDB \| None` enum (COMPUTING / READY / FAILED) | live status of the result |
| `result_uri` | `str \| None` | S3 dir of the outputs (when READY) |
| `backend` | `str \| None` (default `"batch"`) | ray / batch / slurm |
| `simulation_id` | `int \| None` FK→`simulation.id`, indexed | convenience link (nullable: S3 is shared) |
| `job_id_ext` | `str \| None` | K8s Job name / batch id for status polling (legacy `job_id:int` stays for SLURM) |
| `created_at` / `updated_at` | `datetime` server_default now (updated_at onupdate) | proper timestamps alongside legacy `last_updated:str` |

- Legacy columns (`name`, `config`, `last_updated`, `job_name`, `job_id`) and `to_dto()` are unchanged
  — additive only. **The K8s standalone path, which persists nothing today, starts INSERTing here.**
- **Idempotency** is handler-level (query-then-insert, like the parca dedup at
  `viva_api/simulation/database_service.py:430`), **not** a DB unique constraint — the table is general
  and other analysis kinds could legitimately share `(experiment_id, n_tp)`. For this use case, look up
  the latest row matching `(experiment_id, n_tp)` for the ptools bundle. (A partial unique index can be
  added later if we formalize an analysis-kind.)
- `AnalysisStatusDB` maps to the existing `JobStatus` (`viva_api/common/models.py:91`):
  COMPUTING ↔ PENDING/RUNNING/QUEUED/WAITING/UNKNOWN, READY ↔ COMPLETED, FAILED ↔ FAILED/CANCELLED.
- **Alembic migration** `alembic/versions/<rev>_add_analysis_query_columns.py`
  (`down_revision="c1a2b3d4e5f6"`), mirroring `c1a2b3d4e5f6_add_tags_to_simulation.py`: `add_column`
  each nullable column + the enum type + indexes on `experiment_id`/`n_tp`/`simulation_id`; `downgrade`
  drops them + the enum.
- **Reconciler fingerprint** (`viva_api/simulation/db_reconcile.py`): a `_column_exists(conn, "analysis",
  "n_tp")` predicate; append `("<rev>", "analysis.n_tp column exists")` to `LEGACY_FINGERPRINTS` and
  `_LEGACY_PREDICATES` (after the tags entry). See the "Database migrations" section in `CLAUDE.md` for
  why every new migration also needs a fingerprint marker while `create_all` still bootstraps prod DBs.

### 2. DatabaseService methods — extend the existing analysis methods

Build on `insert_analysis` / `get_analysis` / `list_analyses` (`database_service.py:259`):

- `record_analysis(experiment_id, n_tp, status, config, simulation_id=None, backend="batch", name=None, job_name=None, job_id_ext=None, result_uri=None, error_message=None) -> ExperimentAnalysisDTO` — query-then-insert/update on `(experiment_id, n_tp)` (parca dedup pattern).
- `get_analysis_by_experiment_ntp(experiment_id, n_tp) -> ExperimentAnalysisDTO | None`
- extend `list_analyses(*, experiment_id=None, simulation_id=None)` with optional filters (keep the no-arg behavior).
- `update_analysis_status(analysis_id, status, result_uri=None, error_message=None) -> ExperimentAnalysisDTO`
- Extend `ExperimentAnalysisDTO` (`viva_api/analysis/models.py:254`) with the new optional fields
  (`experiment_id`, `n_tp`, `status`, `result_uri`, `simulation_id`, `backend`, timestamps) so one DTO
  serves both `to_dto()` and the new endpoints; `ORMAnalysis.to_dto()` populates them when present.

### 3. Submit + idempotency (nonblocking)

Add `AVAILABLE_NTP = [10, 50, 100]` to `viva_api/analysis/models.py` (single source of truth). New
handler `request_analysis_sampling(db, simulation_id, n_tp)`:

1. Validate `n_tp in AVAILABLE_NTP` (else 400/422); `get_simulation(id)` (404 if none) → `experiment_id`.
2. `get_job_backend() != BATCH` → 501.
3. **Idempotency (nonblocking):** `get_analysis_by_experiment_ntp(experiment_id, n_tp)` — if a row
   exists (READY or COMPUTING) return it as-is and **do not resubmit / do not wait**; if a FAILED row,
   resubmit (→ COMPUTING) since no valid result exists. If no row but the S3 probe (§4) already finds
   results, record a READY row and return. In all cases return immediately.
4. **Submit:** build the analysis_config like `run_standalone_analysis` but with the fixed ptools bundle
   at a uniform `n_tp` (`{"multiseed": {"ptools_rna": {"n_tp": n}, "ptools_rxns": {"n_tp": n},
   "ptools_proteins": {"n_tp": n}}}`), `analysis_name = f"analysis-{experiment_id[:20]}-ntp{n}-{uuid4()[:4]}"`.
   Refactor `run_standalone_analysis` to share this config builder so the existing
   `POST /simulations/{id}/analysis` route stays consistent.
5. **Unique K8s job per (experiment, n_tp):** change `submit_standalone_analysis`
   (`simulation_service_k8s.py:333`) to derive `job_name = f"ana-{safe_id}-ntp{n}"[:63]` (+ matching
   `-config` ConfigMap). Fixes today's `ana-{safe_id}` collision across samplings.
6. **Record a COMPUTING row first, then submit**, then store `job_id_ext`. Return the analysis DTO.
   Never poll.

### 4. Status resolution (status on the analysis row; do **not** add `JobTypeDB.ANALYSIS`)

Adding `JobTypeDB.ANALYSIS` + `jobref_analysis_id` to `hpcrun` is unnecessary churn; the `analysis` row
now carries `job_id_ext`/`status`/`result_uri`. Handler `resolve_analysis_status(db, analysis_id)` (by
analysis id; used by both `GET /analyses/{id}/status` and `GET /analyses/{id}/data` before serving). It
only reads/persists status — it never submits or waits:

- Terminal (READY/FAILED) → return persisted (no backend hit).
- COMPUTING → **S3-exists is the authoritative READY signal** (the K8s Job has
  `ttl_seconds_after_finished=86400`, so `get_job_status` returns None after 24h — S3 is the only
  durable truth): probe the analysis prefix via `FileService.get_listing`, and if a TSV whose `t*`
  column count == `n_tp` exists → persist READY + `result_uri`. Else consult K8s `get_job_status(JobId)`
  → FAILED (persist + error) / still RUNNING (stay COMPUTING) / None-with-no-S3 after a grace window →
  FAILED.

### 5. Endpoints (`viva_api/api/routers/sms.py`, tag `["Analyses"]`, new unique `operation_id`s)

Retrieval and status are keyed by **analysis database id** (reusing the existing `/analyses/{id}`
resource family: `get_analysis_spec` `sms.py:675`, `/analyses/{id}/status` `:693`, `/log` `:720`,
`/plots` `:742`). Submission is on the simulation. Return DTO is the extended `ExperimentAnalysisDTO`
(now with `database_id`, `experiment_id`, `n_tp`, `status`, `result_uri`, …); add an `AnalysisStatus`
enum.

| Method | Path | Returns | Behavior |
|---|---|---|---|
| GET | `/simulations/{id}/analyses` | `list[ExperimentAnalysisDTO]` | List the sim's existing / pre-run analyses via `list_analyses(experiment_id=...)`. Optional `?n_tp=` / `?status=` filters. Pure read. |
| POST | `/simulations/{id}/analyses` (`n_tp` param) | `ExperimentAnalysisDTO` | **Nonblocking, idempotent submit-or-reuse** (§3): existing analysis for `(experiment_id, n_tp)` → return it, DO NOT resubmit; else submit a new K8s Job + record a COMPUTING row; return immediately with `database_id` + status. **Never waits for the job.** |
| GET | `/analyses/{id}/status` | `ExperimentAnalysisDTO` | **Extend** the existing (SLURM-only 501) endpoint to also resolve Batch analyses via §4. Pure status read; never computes. |
| GET | `/analyses/{id}/data` | `list[TsvOutputFile]` if READY, else **409** | **New.** Pure retrieval of an existing analysis's TSVs by analysis id — reads S3 under the row's `result_uri` (`_download_outputs_from_s3`-style, `simulations.py:1153-1285`). If not READY → 409 with the status DTO. **Never computes/blocks.** |

`AVAILABLE_NTP` governs only what **POST** accepts. Do **not** reuse the pre-existing duplicate
`operation_id="run-ecoli-simulation-analysis"` (`sms.py:409` & `:627`).

#### ptools transition (response-shape parity)

The legacy blocking `POST /analyses` (`sms.py:625`) returns **`Sequence[TsvOutputFile | OutputFileMetadata]`**
— a buffered JSON array where each element is `{filename, variant, lineage_seed, generation, agent_id,
content}` and `content` is the raw TSV string. It does **not** stream and does **not** return an archive.

**Requirement (confirmed):** `GET /analyses/{id}/data` MUST return the **identical shape**
(`list[TsvOutputFile]`) so ptools reuses its existing response-parsing verbatim. We **retain** the legacy
`POST /analyses` (SLURM, blocking) unchanged; ptools' only change is to replace its single blocking call
with **submit → poll status → fetch data**, where the fetched data shape is byte-for-byte the same. This
is why the fetch shape is `list[TsvOutputFile]` (not a tar.gz stream).

### 6. Backfill (`scripts/backfill_analysis_results.py`, no admin endpoint)

Config-first, two tiers:

- **Tier A (existing `analysis` DB rows):** read `n_tp` straight from `ORMAnalysis.config` JSONB at
  `config["analysis_options"][<domain>][<module>]["n_tp"]` (best-effort; legacy rows are SLURM-origin —
  gate on the experiment also existing in S3).
- **Tier B (S3 result sets with no DB row — primary Batch case):** list the analyses prefix via
  `FileService.get_listing`, and for each analysis dir infer `n_tp` from a TSV by counting `t*` columns
  (extract `AnalysisServiceSlurm._verify_result`'s rule, `analysis_service.py:266`, into a pure
  `infer_n_tp_from_tsv(text) -> int`). Upsert `(experiment_id, n_tp, status=READY, result_uri, simulation_id)`.

## Key risk — verify FIRST

**S3 write/read double-nesting asymmetry.** `run_standalone_analysis` writes `outdir` under the
*single-nested* `NextflowLayout.output_uri` (`…/{exp}/analyses/…`), while `_download_outputs_from_s3`
reads the *double-nested* `NextflowLayout.experiment_prefix` (`…/{exp}/{exp}/analyses/…`)
(`viva_api/common/storage/data_layout.py:94`). Before wiring the readiness probe / `result_uri`, **list a
real Stanford `sim*` experiment's `analyses/` prefix in S3** to confirm which nesting the standalone K8s
analysis job actually writes — the probe and `result_uri` must match reality.

## Implementation plan

### Files

**Modify:** `viva_api/simulation/tables_orm.py` (nullable columns + `AnalysisStatusDB` enum; extend
`to_dto()`) · `viva_api/analysis/models.py` (`AVAILABLE_NTP` + extend `ExperimentAnalysisDTO`) ·
`viva_api/simulation/database_service.py` (extend analysis methods) · `viva_api/common/handlers/simulations.py`
(new handlers + shared config builder + `infer_n_tp_from_tsv`) · `viva_api/simulation/simulation_service_k8s.py`
(unique job name) · `viva_api/api/routers/sms.py` (routes) · `viva_api/simulation/db_reconcile.py`
(fingerprint) · `viva_api/analysis/analysis_service.py` (extract `n_tp` helper).

**Create:** `alembic/versions/<rev>_add_analysis_query_columns.py` · `scripts/backfill_analysis_results.py`
· tests below.

### Tests

- **DB layer** (testcontainer Postgres, mirror `tests/simulation/test_simulation_tags_db.py`): record/get
  round-trip, record-twice dedup on `(experiment_id, n_tp)`, list filters, COMPUTING→READY status update,
  enum mapping.
- **Pure logic**: `infer_n_tp_from_tsv` counts `t*` columns (10/50/100 fixtures); config-first extraction
  incl. non-uniform-bundle handling.
- **Endpoints** (mirror `tests/api/ecoli/test_simulations.py`, mock K8s + FileService): POST submits a
  COMPUTING row with a `ntp{n}`-unique job name (nonblocking, no poll); POST bad `n_tp` → 400; POST when a
  row exists → returns it, **no resubmit**; GET `/simulations/{id}/analyses` lists; `GET /analyses/{id}/status`
  COMPUTING→READY on S3 probe; `GET /analyses/{id}/data` → 409 when not READY, TSVs when READY (never
  computes); non-BATCH → 501.
- **Gate:** `make check` (mypy strict, ruff, deptry, lock), `uv run pytest`.

### End-to-end (Stanford)

After deploy + the `alembic-migrate` reconciler applies the new columns, run
`scripts/backfill_analysis_results.py`, then `GET /simulations/{id}/analyses` (list, pick an
`analysis_id`), `POST …/analyses?n_tp=10` (nonblocking submit), poll `GET /analyses/{id}/status` → READY,
`GET /analyses/{id}/data` returns TSVs.

## Resolved decisions

- **Fetch response shape** — DECIDED: `GET /analyses/{id}/data` returns `list[TsvOutputFile]`, the
  identical shape the legacy `POST /analyses` already returns, so ptools reuses its response parsing.
  The legacy `POST /analyses` is retained unchanged (see "ptools transition" above).

## Open decisions

- **Release:** this adds a migration → same reconciler / migration-Job flow as 0.9.20; version bump TBD.

## Alternatives considered

- **New `analysis_result` table** (rejected): forks a near-duplicate of `analysis`; the existing table
  with `config` JSONB is the general home for all analysis types, and `n_tp` is just one attribute to
  index. Generalizing the existing table keeps one lifecycle and lets backfill index old + new results
  uniformly.
- **Unify with `hpcrun` tracking** (`JobTypeDB.ANALYSIS` + `jobref_analysis_id`) (rejected): unnecessary
  schema churn — the analysis row already carries status/job/result fields, and S3-exists is the
  authoritative readiness signal regardless.
- **Blocking getter** (rejected per the requirement): analysis is too expensive to compute inside a
  request; submit and retrieve are decoupled.
