# Data provenance across composites, analysis jobs and task jobs — slice 1

**Status (2026-09-14): plan, not started.** This is **slice 1 of two**. It delivers the
provenance model and the `dataset` registry for every producer that is *already* a traced
run — process-bigraph composites (simulation runs), ParCa, and analysis jobs — **without
touching the task endpoints**. Slice 2, [`plan-task-provenance.md`](plan-task-provenance.md)
(viva-api PR #656, issue #655), then makes a task a first-class traced run and *inherits*
this registry with no new dataset code. The ptools consumer is
[`plan-ptools-datasets.md`](plan-ptools-datasets.md).

Sequencing decided by Jim on 2026-09-14: data provenance first, task provenance second.

Open for refinement with input from others before any code is written.

---

## 1. The problem, in one paragraph

Today "what produced this file, from what, with which code" is answerable only by hand:
simulation output is keyed to a store prefix a human names; analysis bundles are found by
walking `<store>/analyses/*` (viva-api#650); hand-dispatched task jobs leave no record at
all past Batch's 7-day retention (viva-api#655 §1–5); and the ptools consumer can only see
an analysis if someone registered a row for it. The observability stack (0.9.142,
`docs/OBSERVABILITY.md`) already gives every viva-api-dispatched run a **trace**, **spans**
with attributes, and an **opaque baggage map** carrying the run's identity — but nothing in
that stream says *which files were written*. This plan makes artifacts first-class in the
trace, and makes the trace the primary source of the dataset registry.

## 2. The model

```
producer run  ──(trace_id, baggage)──▶  spans  ──▶  artifact.written events
    │                                                       │
    │ HpcRun row (simulation | parca | analysis-on-sim | TASK*)│  ingest (viva-api)
    ▼                                                       ▼
 task.inputs* / analysis.source  (ProvenanceRef, declared)  dataset rows
                                                            │  one per consumable file set
                                                            │  producer FK, attributes, tags
                                                            ▼
                                             consumers: ptools page, workbench, CLI, next runs
                                                                 (* = slice 2)
```

| concept | record | identity |
|---|---|---|
| **run** (a composite execution, an analysis job, a task job) | `hpcrun` (+ `simulation` / `parca_dataset` / `analysis` / `task`) | `hpcrun.trace_id`; `PBG_TRACE_BAGGAGE` carries `sim_id`, `experiment_id`, and now `analysis_id` (slice 1) / `task_id` (slice 2) |
| **what a run read** | `analysis.source` (slice 1), `task.inputs` (slice 2) — `ProvenanceRef`, declared at dispatch, best-effort, never enforced; optional `artifact.read` events observed at run time | `{kind, ref, resolved_id, uri, coordinate}` |
| **what a run wrote** | `dataset` — one row per consumable file set | `uri` (unique), exactly one producer FK, `kind`, `attributes`, `tags` |
| **how we know** | `hpcrun_event` / `hpcrun_span` (the trace) | `origin = event` (from the trace) vs `origin = walk` (from listing S3) |

### 2a. Two records, two birth times (Jim, 2026-09-14)

- **The run record** (`analysis`, `task`, `simulation`) is created **at submit**. Its only
  reason to exist before the job starts is its id: the id rides in the baggage so every
  event the job emits is attributable. It records what was *asked for*; its `result_uri` /
  `dest_prefixes` are hints for the walk, not dataset identity.
- **The dataset record is never pre-created.** It is born when the ingester scrapes an
  `artifact.written` event out of the trace, or — the fallback — when the reconciliation
  walk finds an object with no row. It records what *actually happened*. A dataset without
  an object behind it must not exist; "expected but missing" is expressed by the run's
  status and by `error` events (`available = false`), never by a placeholder row.
- Consequence for the API: `GET /analyses/{id}` (and later `/tasks/{id}`) reports the run
  status and the dataset count **separately**, so "running, 0 datasets" and "done, 0
  datasets" read differently. Consumers that want "everything this run produced" query
  `dataset` by producer FK and accept that it lags ingestion.

The engine stays domain-free (the process-bigraph rule: domain ids only ride in opaque
baggage; the emitter never names `sim_id` or `variant`). `artifact.written` is therefore
an **event-name convention plus a payload schema**, not an engine feature: the emit helper
lives in v2ecoli's `workflow/events.py` (`emit(name, level, **payload)` already exists),
and the engine's `EventSink`s carry it like any other event.

## 3. The `artifact.written` contract

Emitted **inside the span that produced the file**, at `info` level (viva-api's ingest
drops `debug` and `tick`; see OBSERVABILITY.md "Debug-level events never reach /events").

```
name:      artifact.written
component: v2ecoli.emitter | v2ecoli.analysis | v2ecoli.parca | task | <other producer>
payload:
  uri:         s3://bucket/key            # a single object, or a prefix for multi-file kinds
  kind:        parquet | parca-cache | ptools-analysis | analysis | figure | report | other
  name:        basename (or last prefix segment)
  view:        optional — the analysis/view name (ptools_rna, mass_fraction, …)
  bytes:       optional
  sha256:      optional — only when the producer already has it cheaply
  attributes:  {}                          # extensible; see §4 per producer
  error:       optional — the file was NOT produced; why (registered as available=false)
baggage (already promoted by viva-api): sim_id, experiment_id, variant, lineage_seed,
           + analysis_id (slice 1), task_id (slice 2)   → producer resolution
```

**Granularity rule:** one event per thing a consumer would fetch or mount as a unit. For
parquet that is a **partition prefix** (a generation's `history/…` partition, a
`daughter-state/` checkpoint), never a shard. For ptools it is one TSV. For a figure one
HTML/SVG. For a ParCa cache the bundle prefix.

**Optional companion, `artifact.read`:** `{uri, kind?}` when a run opens an upstream object
it did not write (a sim_data pickle, a store it aggregates). Best-effort; it lets the
registry derive *consumers* of a dataset without anyone declaring them. Not required for v1.

**Sidecar fallback for non-Python producers** (used by slice 2's bash toolkits, defined
here so the schema is one): the same JSON objects, one per line, in
`$CONTAINER_OUT_DIR/artifacts.jsonl`; the container entrypoint's existing output sync ships
it, and viva-api ingests it exactly like an events object (`origin = event`).

## 4. Per producer

### 4a. process-bigraph composites (simulation runs)

- **Where:** v2ecoli `library/parquet_emitter.py` (already wrapped by
  `workflow/events.py::_ObservedEmitter`, which knows the chunk index), at partition
  finalize; plus `write_run_identity` in `workflow/run.py` for `run_identity.json`, and the
  lineage's `daughter-state/` checkpoint write.
- **Kinds/attributes:** `parquet` with `{store: <experiment_id>, table: history|config|…,
  variant, seed, generation, chunk_range}`; `report` for `run_identity.json`,
  `summary.json`, `final_state.json`; `other` for `daughter-state/<gen>/` checkpoints.
- **Producer FK:** `simulation_id` (from baggage `sim_id`).
- **ParCa:** its own HpcRun (`job_type = PARCA`); the cache bundle is a `parca-cache`
  dataset with `attributes = {commit, inputs_hash, cache_variant}` and producer
  `parca_dataset_id` (§6 schema; open question 1 resolved in favour of the FK).
- **Multi-node Ray:** `AWS_BATCH_JOB_ID` is per node, so the emitter keys its sink object
  per process (already the rule in OBSERVABILITY "the Ray path").

### 4b. analysis jobs (sim-time gather, standalone `POST /simulations/{id}/analysis`, fills)

- **Where:** v2ecoli `workflow/analysis_runner.py:1015-1047` — the TSV/HTML writes already
  sit inside the `analysis.group` span whose `group` attribute *is* the coordinate string
  (`variant=0/seed=3/gen=12/agent=…`). Emit one `artifact.written` per written file there,
  and one with `error` when `step.update` raises (today that error is only kept in the
  runner's `per_group` summary).
- **Kinds/attributes:** `ptools-analysis` for `ptools/*.tsv` with `{protocol: single|
  multiseed|multigeneration, variant, seed, generation, agent, n_tp}`; `figure` for
  `viz/*.html`; `analysis` for any other table; `report` for `analysis.json`.
- **Producer FK:** `analysis_id` when baggage carries it. For that, viva-api's standalone
  path must **record the `analysis` row before submitting and inject `analysis_id`**
  (today `_run_standalone_analysis_ray_native` records after submit — flip the order; this
  is the run record, §2a, not a dataset). The sim-time gather inside a simulation run
  carries only `sim_id` → producer `simulation_id`; the registration hook still creates /
  attaches an `analysis` run row named from the bundle directory (`<store>/analyses/<name>/`).
- **Cross-process gap:** analysis spans from worker processes can attach to the trace root
  rather than under `task` (OBSERVABILITY §"What the tree actually looks like"). Producer
  resolution therefore uses **baggage, never span parentage**.

### 4c. task jobs — slice 2

Everything about tasks (their HpcRun row, `task_id` in baggage, `task.inputs`,
`dest_prefixes`, the helper call in `ptools_flush.py`, the sidecar for bash toolkits) is in
`plan-task-provenance.md`. Nothing in this slice special-cases tasks; the `ProvenanceRef`
vocabulary already has `kind = task`, and producer resolution already falls through to the
HpcRun job reference, so slice 2 adds the `dataset.task_id` FK and the baggage key and is
done.

## 5. Feeders — tracing is primary

`register_datasets` is one seam with two feeders, both idempotent on `uri`, each stamping
`attributes.origin`; `origin = event` wins over `origin = walk` on conflict.

*Feeder 1 — the trace (primary, steady state).* In `event_ingest._ingest_object`, collect
`artifact.written` / `artifact.read` events per pass; after the pass, `register_datasets`
upserts `dataset` rows. Producer resolution order: baggage `task_id` (slice 2) → baggage
`analysis_id` → the HpcRun's job reference (`jobref_simulation_id`, `jobref_parca_dataset_id`,
`jobref_task_id` in slice 2). `kind`, `view`, `attributes`, `tags` (source simulation tags
∪ event `attributes.tags`), `source` = simulation + coordinate. Tested with synthetic
`.jsonl` objects; the live feed lights up when the emit-side PRs ship (§9).

Why it is the design center: the runner already scopes each write inside an
`analysis.group` span whose `group` attr *is* the coordinate, so the event is ground truth
— no filename parsing — and it can record a coordinate whose view **failed**
(`payload.error`, row `available = false`), which a walk can only infer by absence.

*Feeder 2 — the S3 walk (backfill + reconciliation).* The importer (§8) registers
everything historical with `origin = walk`, attributes parsed from filenames. A periodic
`reconcile_datasets` scheduler tick (hourly, cheap) lists the dest prefixes of READY runs —
`analysis.result_uri`, and each simulation's `<out_uri>/analyses/*` (the #650 walk) — and
(a) registers files with no row yet (events lost or emit side not deployed), (b) flips
`available` for rows whose object is gone. Events are best-effort by design (opt-in sink,
size-skip, never raised), so the walk is the safety net, never the source of truth when an
event exists.

**Filename → attributes (walk feeder only).** Fill files carry their coordinates in the
filename (`ptools_rna__variant=0_seed=3_gen=12_agent=….tsv`,
`ptools_rna_multiseed__variant=0.tsv`); vEcoli's own output carries them in partition
directories. Extend `parse_partition_metadata` (`viva_api/analysis/analysis_service.py:377`,
currently an anchored `match` on path parts, so it sees nothing in a fill filename) to also
scan the stem for `key=value` pairs, mapping `seed`→`lineage_seed`, `gen`→`generation`,
`agent`→`agent_id`, and derive `protocol` from the view suffix (`_multiseed`,
`_multigeneration`, else `single`). `n_tp` comes from the file's header (one small ranged
GET). `display_name` = `<experiment_id> · <view> · <protocol>[ · s3 g12]`.

## 6. Schema (one Alembic revision, `down_revision = "e3a9c1d70b62"`, current head)

**New table `dataset`** (renamed from the earlier `analysis_result`; the API already said
`datasets`, and a parquet partition or a ParCa cache is not an "analysis result"):

| column | type | what |
|---|---|---|
| `id` | PK | the stable id consumers fetch by |
| `simulation_id` | FK → `simulation.id`, nullable, indexed | producer: a simulation run |
| `parca_dataset_id` | FK → `parca_dataset.id`, nullable, indexed | producer: a ParCa run |
| `analysis_id` | FK → `analysis.id`, nullable, indexed | producer: an analysis run |
| *(slice 2 adds `task_id`)* | | |
| | CHECK `ck_dataset_producer` | at least one producer FK non-null |
| `kind` | TEXT NOT NULL | `parquet` \| `parca-cache` \| `ptools-analysis` \| `analysis` \| `figure` \| `report` \| `other` |
| `view` | TEXT | `ptools_rna`, `ptools_rxns`, … (analysis kinds) |
| `display_name` | TEXT | what a picker shows |
| `uri` | TEXT NOT NULL, unique | the object, or the prefix for multi-file kinds |
| `size_bytes`, `sha256` | INTEGER / TEXT NULL | sha only when cheap |
| `attributes` | JSONB NOT NULL default `{}`, GIN | extensible: `variant`, `seed`, `generation`, `agent`, `protocol`, `n_tp`, `experiment_id`, `family`, `origin`, … — any key, filterable |
| `tags` | JSONB NOT NULL default `[]`, GIN | selection tags (`cd2`, `run3`, …) |
| `source` | JSONB | `ProvenanceRef` with coordinate — what it is *of* (kept even when `simulation_id` is set, for sub-stores) |
| `available` | BOOLEAN NOT NULL default true | flipped by the walk when the object is gone; rows are never deleted |
| `created_at`, `updated_at` | DateTime (naive UTC, matching the other tables) | |

Indexes: each FK; `(kind, view)`; GIN on `attributes` and `tags`; unique on `uri`.

**New columns on `analysis`** (nullable; legacy rows unaffected): `source` JSONB (a
`ProvenanceRef`), `tags` JSONB NOT NULL default `[]` with GIN `ix_analysis_tags` (copy
`simulation.tags`'s declaration, `tables_orm.py:351`). `n_tp` (existing) is now populated at
registration from the TSV header. *(slice 2 adds `analysis.task_id`.)*

**`ProvenanceRef`** (`viva_api/simulation/models.py`): `{kind: simulation|task|analysis|s3,
ref: str, coordinate?: {variant, seed, generation, agent, protocol}}`, stored resolved as
`{…, resolved_id, uri, resolved_at, verified_at}`. Shared by `analysis.source`,
`dataset.source`, and slice 2's `task.inputs`.

**Migration guards.** The `analysis` table has no creating migration (#637), so this
revision does `inspector.has_table("analysis")` and, when absent, creates it from the ORM
(`ORMAnalysis.__table__.create(bind, checkfirst=True)` after an explicit `analysisstatusdb`
`create(checkfirst=True)` — the `d3f9a1c72b84` enum pattern); then guarded `add_column`
for `source`/`tags`, guarded `create_table("dataset")`, every index guarded by name.
**This does not fix #637**: an empty DB still fails earlier at `d3f9a1c72b84`'s unguarded
`add_column("analysis", …)`; that fix is its own small PR. Column types must match the ORM
exactly (naive `sa.DateTime()`), or the types-parity test fails.

**Fingerprint contract** (`viva_api/simulation/db_reconcile.py`): one marker
`("<id1>", "table 'dataset' exists")` with `_table_exists(conn, "dataset")`, appended to
`LEGACY_FINGERPRINTS` and `_LEGACY_PREDICATES`. `tests/simulation/test_db_reconcile.py`:
`HEAD` → `<id1>`; `REVS` is 15 entries and is missing `e3a9c1d70b62` — append it and
`<id1>`; every vector 16 → 17. Types-parity: a new
`tests/simulation/test_dataset_migration.py` (real `upgrade head` from empty; create_all vs
migration `(data_type, is_nullable, column_default)` for `dataset` and the new `analysis`
columns; a create_all DB matches the marker and stamps at head). Do **not** widen
`test_observability_migration.py`'s `_OWNED_TABLES` — that harness drops the owned tables
and stamps at `d7e2f4a6c8b0`.

## 7. Queries the model has to answer (API + CLI)

| question | route |
|---|---|
| what datasets exist with these attributes / tags? | `GET /api/v1/datasets?kind=&tag=&view=&attr.<k>=&simulation_id=&analysis_id=&parca_dataset_id=&available=true&since=&limit=&offset=` (`attr.<k>=<v>` → JSONB containment; ordered `updated_at` desc; `limit` ≤ 200) |
| one dataset / its bytes | `GET /api/v1/datasets/{id}`; `GET /api/v1/datasets/{id}/content` (streamed, content-type by kind) |
| picker helpers | `GET /api/v1/datasets/attributes` (distinct keys/values present), `GET /api/v1/datasets/tags`, `POST /api/v1/datasets/{id}/tags` (union-merge, mirror of `/simulations/{id}/tags`) |
| what produced this dataset, from what, with which code? | `GET /api/v1/datasets/{id}/provenance` → producer run (+ `task.command`, `script_sha256`, `image`, `commit` in slice 2), its `source`/`inputs`, trace/span ids, upstream datasets those inputs resolve to (one hop; `?depth=`) |
| what did this run write? | `GET /api/v1/simulations/{id}/datasets`, `/analyses/{id}/datasets` (slice 2: `/tasks/{id}/datasets`); `GET /analyses/{id}` carries `status` and `n_datasets` separately (§2a) |
| who consumed this dataset? | `GET /api/v1/datasets/{id}/consumers` (from `analysis.source` / `task.inputs` / `artifact.read`) |
| the ptools consumer's picker unit | `GET /api/v1/datasets/groups` — see `plan-ptools-datasets.md` §3 |
| analyses, listed directly | `GET /api/v1/analyses` gains `status`, `backend`, `source` (`sim:1002`), `since`, `limit`/`offset`, ordered `updated_at` desc (replaces "exhaustive; filtering/paging to come") |
| compatibility for the unpatched ptools page | `GET /api/v1/analyses/{id}/data` gains coordinate filters (`view`, `protocol`, `variant`, `seed`, `generation`); when the selection yields one file per view the response `filename` is aliased to `<view>.tsv` with the real key in a new `path` field |

CLI: `atlantis dataset list|get|fetch|provenance|consumers`, `atlantis analysis list`,
`atlantis simulation datasets <id>`.

## 8. Backfill (one-shot importer, `scripts/import_cd2_datasets.py`)

Reads `assets/ptools/cd2_ptools_manifest.json` (183 stores, `fill_bundles` with URIs and
view types; every CD2 store is its own `simulation` row — verified) and each store's
`run_identity.json` where present. Writes, through `DatabaseServiceSQL`: `analysis` run rows
per bundle directory (`backend = "fill"`, `source = {kind: simulation, ref: <store>}`
resolved, `tags` = `cd2` + family + kind), then `dataset` rows per `ptools/*.tsv` and
`viz/*.html` (`origin = walk`, producer `analysis_id`; sim-time `analysis-mnp-*` bundles get
producer `simulation_id`). `--analyze` / `--apply`; idempotent on `uri` and `result_uri`.
Run by hand per site after the migration Job. **Slice 2's importer** adds the 46 task rows
from `cd2_tool_runs.json` and, idempotent on `uri`, back-fills `dataset.task_id` on the
datasets those jobs produced.

## 9. PR map across repos (order matters)

| # | repo | change | depends on |
|---|---|---|---|
| 1 | viva-api | **this slice:** `dataset` + `analysis.source/tags` + `ProvenanceRef`; the ingest hook (synthetic-event tested); `/datasets` API + list filters; analysis-id-in-baggage order flip; walk reconciliation tick; importer; `docs/OBSERVABILITY.md` contract section | — |
| 2 | viva-api | post the §3 contract on #655 | 1 |
| 3 | v2ecoli | `workflow/events.py`: `artifact(uri, kind, **attrs)` helper (thin over `emit`); `analysis_runner.py` per-file + error events; `parquet_emitter.py` per-partition events at finalize; `run.py` reports; ParCa cache bundle event | 2 |
| 4 | sms-ecoli | pin v2ecoli; simulator rebuild; deploy dev — the live feed lights up for composites and analysis jobs | 3 |
| 5 | viva-api | **slice 2** (`plan-task-provenance.md`): task provenance + snapshot mode; tasks as HpcRuns; `dataset.task_id`; task importer; toolkit/sidecar call sites in sms-ecoli | 1 (can run in parallel with 3–4) |
| 6 | viva-api + SRI | the ptools consumer — `plan-ptools-datasets.md` | 1 (API), 4 (live feed) |

## 10. Retention and durability

- **The DB rows are the durable record**; events objects in S3 are transport. If
  `events_s3_prefix` resolves under `ray-logs/` it expires in 30 days (bucket lifecycle),
  so ingest must run continuously (it does: every 5 s tick) and the reconciliation walk
  covers any gap. **Verify per site where the events prefix actually lands before this
  slice deploys** (open question 4).
- Dataset objects themselves: whatever the store's lifecycle is; the row outlives the
  object with `available = false`, so provenance never dangles.
- Script bytes (slice 2): content-addressed in `task_script`, permanent regardless of S3.

## 11. Verification

1. Offline: `uv run pytest tests/simulation/test_event_ingest_artifacts.py
   tests/api/test_datasets_router.py tests/common/handlers/test_datasets_registration.py
   tests/scripts/test_import_cd2_datasets.py -v` — synthetic `artifact.written` objects
   for a simulation HpcRun and an analysis HpcRun (producer from baggage `analysis_id`,
   from `jobref_simulation_id`; a failed group → `available = false`); re-ingest is a no-op;
   a later walk over the same prefix adds nothing and never downgrades `origin`; filename
   parser for both naming conventions; list filters; content streams; tags merge.
   Confirm the tunnel is **down** first (`tests/api/app/test_cli_e2e.py` runs live when it
   is up).
2. Migration (Docker): `test_db_reconcile.py` (17 vectors, single head) +
   `test_dataset_migration.py`.
3. `make check` twice; `uv run pytest` (full, minus `test_cli_e2e.py`).
4. On dev after deploy + importer, through the CLI: `atlantis dataset list --kind
   ptools-analysis --tag cd2 --view ptools_rna --attr protocol=multiseed` → one row per
   filled store (20 Run-3 + 1 Run-2); `atlantis dataset fetch <id>` byte-equals the S3
   object; `atlantis dataset provenance <id>` names the simulation row and its
   `run_identity.json` commit. `GET /analyses/{id}` for the Run-3 combo00 fill shows
   `status = ready`, `n_datasets = 405`.
5. After step 4 of the PR map (emit side deployed): a fresh standalone analysis on dev
   yields `origin = event` rows with `n_tp` from the payload, before any walk runs.

## 12. Open questions for reviewers

1. ~~Fourth producer FK for ParCa~~ — taken: `parca_dataset_id` (a cache is reused by many runs).
2. **`artifact.read` in v1?** Cheap on the emit side, and the only way to learn consumers
   nobody declared. Leaning yes, `info` level, optional.
3. **Parquet granularity**: partition prefix per generation (proposed) vs per store.
4. **Where do events objects live per site** — under the run's out_uri, or `ray-logs/`?
   Determines whether the 30-day lifecycle can eat un-ingested artifacts.
5. **Per-`kind` attribute schema** (validated) or free-form + conventions? Proposed:
   free-form + conventions + `GET /datasets/attributes` so pickers are built from what exists.
6. ~~Table name~~ — taken: `dataset`.
7. **Backend label for fills** on the `analysis` run row: `fill` (proposed) vs `task`
   (only once slice 2 links them).
