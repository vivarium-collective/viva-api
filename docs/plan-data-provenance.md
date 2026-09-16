# Data provenance across composites, analysis jobs and task jobs — slice 1

**Status (2026-09-15): implemented in draft viva-api#661, not merged, not deployed.** This
is **slice 1 of two**. It delivers the provenance model and the `dataset` registry for every
producer that is *already* a traced run — process-bigraph composites (simulation runs),
ParCa, and analysis jobs — **without touching the task endpoints**. Slice 2,
[`plan-task-provenance.md`](plan-task-provenance.md) (viva-api PR #656, issue #655), then
makes a task a first-class traced run and *inherits* this registry with no new dataset code.
The ptools consumer is [`plan-ptools-datasets.md`](plan-ptools-datasets.md).

Sequencing decided by Jim on 2026-09-14: data provenance first, task provenance second.

**Reconciled with the implementation on 2026-09-15**, folding in the two #657 comments
([changes found while implementing](https://github.com/vivarium-collective/viva-api/pull/657#issuecomment-5674760129),
[no fabricated analysis rows](https://github.com/vivarium-collective/viva-api/pull/657#issuecomment-5679825098)).
Where the implementation departed from the earlier text, the section says so. Still open for
refinement with input from others; §12 lists what is undecided.

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
    │ HpcRun row (simulation | parca | analysis | TASK*)    │  ingest (viva-api)
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
- **A run record is never invented either (Jim, 2026-09-15).** Files no dispatched run
  claims are attributed to the simulation they sit under (§5), not to a made-up `analysis`
  row. An `analysis` row means an analysis was dispatched.
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
  kind:        store | parca-cache | ptools-analysis | analysis | figure | report | other
  name:        basename (or last prefix segment)
  view:        optional — the analysis/view name (ptools_rna, mass_fraction, …)
  bytes:       optional
  sha256:      optional — only when the producer already has it cheaply
  attributes:  {}                          # extensible; see §4 per producer
               #   "tags": [...] adds tags; "display_name" overrides the generated one
  error:       optional — the file was NOT produced; why (registered as available=false)
baggage (already promoted by viva-api): sim_id, experiment_id, variant, lineage_seed,
           + analysis_id (slice 1), task_id (slice 2)   → producer resolution
```

As implemented, an event whose `uri` is not `s3://` or whose `kind` is outside the
vocabulary is **skipped and counted**, never raised, so one malformed event cannot stop a
run's other files from registering. Each row also records the `hpcrun_id` and `span_id` it
came from, so provenance can walk back to the span.

**Granularity rule:** one event per thing a consumer would fetch or mount as a unit. For a
`store` that is the store prefix itself (`…/v2ecoli_seed00.zarr`, or a generation's
`history/…` parquet partition), never a chunk or a shard. For ptools it is one TSV. For a
figure one HTML/SVG. For a ParCa cache the bundle prefix.

**One `store` kind covers both formats (Jim, 2026-09-15; viva-api#672).** A run's trajectory
output is a single kind with the format in attributes -- `kind = store`, `attributes.format =
zarr | parquet` -- mirroring `observable_reader.StoreIndex.store`, so a consumer asks "where is
the trajectory?" once and filters on format afterwards. *Superseded:* the earlier `parquet`
kind, which named one format as though it were the concept. Slice 1 ships the old vocabulary
(`DATASET_KINDS` still reads `parquet`); #672 makes the change, and registers stores by LISTing
`<out_uri>/` rather than deriving a URI -- the flat `v2ecoli_seed{NN}.zarr` name holds only for
phase0 and comparison-engine dispatch shapes.

**Optional companion, `artifact.read`:** `{uri, kind?}` when a run opens an upstream object
it did not write (a sim_data pickle, a store it aggregates). Best-effort; it lets the
registry derive *consumers* of a dataset without anyone declaring them. **Slice 1 stores it as
an ordinary `hpcrun_event` and does not consume it**; consumer derivation is a later step.

**Sidecar fallback for non-Python producers** (used by slice 2's bash toolkits, defined
here so the schema is one): the same JSON objects, one per line, in
`$CONTAINER_OUT_DIR/artifacts.jsonl`; the container entrypoint's existing output sync ships
it, and viva-api ingests it exactly like an events object (`origin = event`). Not
implemented in slice 1.

## 4. Per producer

### 4a. process-bigraph composites (simulation runs)

- **Where:** v2ecoli `library/parquet_emitter.py` (already wrapped by
  `workflow/events.py::_ObservedEmitter`, which knows the chunk index), at partition
  finalize; plus `write_run_identity` in `workflow/run.py` for `run_identity.json`, and the
  lineage's `daughter-state/` checkpoint write.
- **Kinds/attributes:** `store` with `{format: zarr|parquet, store: <experiment_id>,
  table: history|config|…, variant, seed, generation, chunk_range}`; `report` for
  `run_identity.json`, `summary.json`, `final_state.json`; `other` for
  `daughter-state/<gen>/` checkpoints.
- **Producer FK:** `simulation_id` (the run's own reference).
- **ParCa:** its own HpcRun (`job_type = PARCA`); the cache bundle is a `parca-cache`
  dataset with `attributes = {commit, inputs_hash, cache_variant}` and producer
  `parca_dataset_id` (§6 schema; open question 1 resolved in favour of the FK). As
  implemented, the producer is the emitting run's simulation's `parca_dataset_id`.
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
- **Standalone analyses are traced runs with their own HpcRun (Jim, 2026-09-14;
  implemented).** `POST /simulations/{id}/analysis` records the `analysis` row **before**
  submit (`source` = the simulation, `tags` from the simulation), seeds the job's trace from
  correlation id `analysis-<id>`, injects `analysis_id` into the baggage, and inserts a
  `JobTypeDB.ANALYSIS` HpcRun **after** submit with the real job id (no placeholder row;
  `hpcrun.jobref_analysis_id`, §6). Slice 2 then only adds the `TASK` label.
- **Finding: the Ray-native standalone path emitted no events at all.**
  `submit_ray_native_analysis` (the path sms-ecoli/v2ecoli standalone analyses actually take)
  built its K8s Job env with no `PBG_*` identity; the #646 fix landed only on the legacy
  `submit_standalone_analysis`, and the AST guard in `test_dispatch_events_identity.py` did
  not cover it. Fixed in #661 (identity injected, guard extended). This also explains why dev
  analyses 421/423 have no events objects.
- **Finding: the ingester only read `SIMULATION` rows.** `list_hpcruns_for_event_ingest`
  admitted only simulation runs on the Ray and Nextflow backends, and the ingest tick
  resolved a simulation from `ref_id`. It now also admits `ANALYSIS` rows (K8s or Ray
  backend), resolves their simulation through `analysis.simulation_id`, and a small
  scheduler tick (`update_analysis_runs`) ends an analysis run's HpcRun when its analysis
  record resolves, so the ingest grace window keys on a real `end_time`.
- **Producer FK:** baggage `analysis_id` naming a real analysis row, else the `ANALYSIS`
  run's own reference. **The in-run gather is already fine:** `_submit_analysis_job` threads
  the simulation's correlation id and `sim_id`, so its events are on the simulation's trace
  (verified on a live object for analysis 424) and its datasets resolve to the simulation.
  The multi-node path (`analysis-mnp-*`) records its own `analysis` row whose `result_uri` is
  the bundle directory, so the walk attributes that bundle to it (§5). *Superseded:* the
  earlier text had the registration hook create or attach an `analysis` run row named from
  the bundle directory; no run rows are created (§2a).
- **Cross-process gap:** analysis spans from worker processes can attach to the trace root
  rather than under `task` (OBSERVABILITY §"What the tree actually looks like"). Producer
  resolution therefore uses **baggage and the run row, never span parentage**.

### 4c. task jobs — slice 2

Everything about tasks (their HpcRun row, `task_id` in baggage, `task.inputs`,
`dest_prefixes`, the helper call in `ptools_flush.py`, the sidecar for bash toolkits) is in
`plan-task-provenance.md`. Nothing in this slice special-cases tasks; the `ProvenanceRef`
vocabulary already has `kind = task`, and producer resolution already falls through to the
HpcRun job reference, so slice 2 adds the `dataset.task_id` FK and the baggage key and is
done. The hand-dispatched CD2 fill jobs are task runs in this model: slice 2 records them and
moves their files to them (§8).

## 5. Feeders — tracing is primary

One registry with two feeders, both idempotent on `uri`, each stamping `attributes.origin`;
`origin = event` wins over `origin = walk` on conflict.

*Feeder 1 — the trace (primary, steady state).* `event_ingest` collects `artifact.written`
events per pass; after the pass, `dataset_registry.register_datasets` upserts `dataset` rows.
As implemented, producer resolution is:

| the event | producer |
|---|---|
| `kind = parca-cache` | the run's simulation's `parca_dataset_id` (skipped when there is none) |
| baggage `analysis_id` naming a real analysis row | that analysis |
| on an `ANALYSIS` run | the run's own analysis |
| on a `SIMULATION` run | that simulation |
| otherwise | skipped and counted |

Slice 2 puts baggage `task_id` first. `kind`, `view`, `attributes`, `tags` (source simulation
tags ∪ event `attributes.tags`), `source` = simulation + coordinate (`variant` / `seed` /
`generation` fall back to the baggage). A database failure during registration **withholds
the events cursor**, so the next tick re-reads the same objects; event inserts are idempotent
and registration is an upsert, so the retry is safe. Tested with synthetic `.jsonl` objects
and with thinned real dev event streams from sim 1319; the live feed lights up when the
emit-side PRs ship (§9).

Why it is the design center: the runner already scopes each write inside an
`analysis.group` span whose `group` attr *is* the coordinate, so the event is ground truth
— no filename parsing — and it can record a coordinate whose view **failed**
(`payload.error`, row `available = false`), which a walk can only infer by absence.

*Feeder 2 — the S3 walk (backfill + reconciliation).* Events are best-effort by design
(opt-in sink, size-skip, never raised), so the walk is the safety net, never the source of
truth when an event exists. As implemented (`dataset_walk.py`), a `reconcile_datasets`
scheduler tick walks the next `datasets_reconcile_batch_size` (25) simulations by id every
`datasets_reconcile_batch_interval_seconds` (60 s), round robin; `datasets_reconcile_enabled`
turns it off. *Changed from the earlier "hourly" and "READY runs' `result_uri`":* one LIST of
each simulation's `<out_uri>/analyses/` covers both. Per bundle directory it registers
`ptools/*.tsv` (`ptools-analysis`), `viz/*.html|svg|png` (`figure`) and `analysis.json`
(`report`, listed, never read — 240 MB seen); `driver.log` and anything else are not datasets.

**Attribution: the walk never creates run rows (Jim, 2026-09-15).** A bundle belongs to the
analysis run whose `result_uri` is the bundle directory (the standalone, in-run and
multi-node dispatch paths record one). A bundle no run claims — a hand-dispatched fill, say —
is attributed to the **simulation** whose output it sits under; `origin = walk` says nothing
claimed it. When a real producer is recorded later (slice 2's task rows, or a run recorded
after the walk), the next walk moves the rows: **a write that names a producer replaces the
row's producer**, so a row never carries two. *Superseded:* the walk and importer briefly
created `analysis` rows (`backend = "walk"` / `"fill"`) for unclaimed bundles; that made
`GET /analyses` list analyses that were never dispatched.

**Availability follows the object.** Any row under a walked bundle whose object is missing
from the listing — or whose whole bundle directory is gone — is marked `available = false`,
whoever registered it, and back when the object returns. The walk never rewrites an
event-sourced row otherwise.

**Upsert rules** (`DatabaseService.upsert_dataset`): a `walk` write never overwrites an
`event`-sourced row; an identical rewrite changes nothing, including `updated_at` (so `since`
means changed, not seen); a lost insert race on the same `uri` is retried as an update.
Attribute filters are JSONB containment and **typed** (`variant: 0` does not match `"0"`), so
the API coerces query-string values: a JSON scalar keeps its type, anything else is a string.

**Filename → attributes (walk feeder only).** Fill files carry their coordinates in the
filename. *Changed from extending `parse_partition_metadata`:* the walk has its own parser,
`dataset_walk.parse_artifact_name`, so the existing endpoints' behaviour is untouched. v2ecoli's
`analysis_runner` names files `f"{name}__{group}"` with the group key's `/` turned into `_`;
the group's shape gives the protocol (`variant` = multiseed; `variant, seed` =
multigeneration; `variant, seed, gen, agent` = single; `…, parent` = multidaughter; `all`),
and a `_<scale>` suffix on the name (`ptools_rna_multiseed`) wins. `n_tp` comes from a ranged
read of the header, re-read only when the object's size changes. Real ptools headers are
`$  0m  124m … 870m  0m_sd  124m_sd …`: the timepoints are the columns after the first that
do not end in `_sd`. `display_name` = `<experiment_id> · <view> · <protocol>[ · s3 g12]`.

## 6. Schema (two Alembic revisions on `e3a9c1d70b62`)

*Changed from one revision:* adding an enum value needs its own autocommit block, so there are
two. **`b2f6d8e0a4c7`**: `ALTER TYPE jobtypedb ADD VALUE IF NOT EXISTS 'ANALYSIS'`, written
like `44335812e447`. **`c9a1e3f5b7d2`**: the `dataset` table, `analysis.source`/`tags` and
`hpcrun.jobref_analysis_id`; guarded, typed to match the ORM, and a no-op on a `create_all`
database.

**New table `dataset`** (renamed from the earlier `analysis_result`, open question 6 closed;
the API already said `datasets`, and a parquet partition or a ParCa cache is not an "analysis
result"):

| column | type | what |
|---|---|---|
| `id` | PK | the stable id consumers fetch by |
| `simulation_id` | FK → `simulation.id`, nullable, indexed | producer: a simulation run, or the simulation an unclaimed bundle sits under |
| `parca_dataset_id` | FK → `parca_dataset.id`, nullable, indexed | producer: a ParCa run |
| `analysis_id` | FK → `analysis.id`, nullable, indexed | producer: an analysis run |
| *(slice 2 adds `task_id`)* | | |
| | CHECK `ck_dataset_producer` | at least one producer FK non-null (writes keep exactly one) |
| `kind` | TEXT NOT NULL | `store` \| `parca-cache` \| `ptools-analysis` \| `analysis` \| `figure` \| `report` \| `other` — free text, validated against `DATASET_KINDS`, which still reads `parquet` until #672 |
| `view` | TEXT | `ptools_rna`, `ptools_rxns`, … (analysis kinds) |
| `display_name` | TEXT | what a picker shows |
| `uri` | TEXT NOT NULL, unique | the object, or the prefix for multi-file kinds |
| `size_bytes`, `sha256` | BIGINT / TEXT NULL | sha only when cheap |
| `attributes` | JSONB NOT NULL default `{}`, GIN | extensible: `variant`, `seed`, `generation`, `agent`, `protocol`, `n_tp`, `origin`, `hpcrun_id`, `span_id`, `analysis_dir`, … — any key, filterable |
| `tags` | JSONB NOT NULL default `[]`, GIN | selection tags (`cd2`, `cd2-run3`, …) |
| `source` | JSONB | `ProvenanceRef` with coordinate — what it is *of* (kept even when `simulation_id` is set, for sub-stores) |
| `available` | BOOLEAN NOT NULL default true | flipped by the walk when the object is gone or returns; rows are never deleted |
| `created_at`, `updated_at` | DateTime (naive UTC, matching the other tables) | |

Indexes: each FK; `(kind, view)`; GIN on `attributes` and `tags`; unique on `uri`.

**New columns on `analysis`** (nullable; legacy rows unaffected): `source` JSONB (a
`ProvenanceRef`), `tags` JSONB NOT NULL default `[]` with GIN `ix_analysis_tags` (copy
`simulation.tags`'s declaration). *Changed:* `analysis.n_tp` is not populated at registration;
`n_tp` lives on each TSV dataset's `attributes`. *(slice 2 adds `analysis.task_id`.)*

**New on `hpcrun`:** `jobref_analysis_id` FK → `analysis.id`, indexed, and `JobTypeDB.ANALYSIS`
(§4b).

**`ProvenanceRef`** (`viva_api/simulation/models.py`): `{kind: simulation|task|analysis|s3,
ref: str, coordinate?: {variant, seed, generation, agent, protocol}}`, stored resolved as
`{…, resolved_id, uri, resolved_at, verified_at}`. Shared by `analysis.source`,
`dataset.source`, and slice 2's `task.inputs`.

**Migration guards.** The `analysis` table has no creating migration (#637), so
`c9a1e3f5b7d2` does `inspector.has_table("analysis")` and, when absent, creates it from the
ORM (`ORMAnalysis.__table__.create(bind, checkfirst=True)` after an explicit
`analysisstatusdb` `create(checkfirst=True)` — the `d3f9a1c72b84` enum pattern); then guarded
`add_column` for `source`/`tags`, guarded `create_table("dataset")`, every index guarded by
name. **This does not fix #637**: creating `analysis` when absent is defensive only; a
genuinely empty database still fails earlier, at `d3f9a1c72b84`'s unguarded
`add_column("analysis", …)`. That fix is its own small PR.

**Fingerprint contract** (`viva_api/simulation/db_reconcile.py`): *two* markers,
`("b2f6d8e0a4c7", "jobtypedb has value 'ANALYSIS'")` and `("c9a1e3f5b7d2", "table 'dataset'
exists")`, appended to `LEGACY_FINGERPRINTS` and `_LEGACY_PREDICATES`.
`tests/simulation/test_db_reconcile.py`: `HEAD` → `c9a1e3f5b7d2`; `REVS` gains `e3a9c1d70b62`
and both new revisions; every vector 16 → 18. Types-parity lives in
`tests/simulation/test_dataset_migration.py`. `test_observability_migration.py`'s
`_OWNED_TABLES` is not widened.

## 7. Queries the model has to answer (API + CLI)

As implemented in #661, except where marked *deferred*:

| question | route |
|---|---|
| what datasets exist with these attributes / tags? | `GET /api/v1/datasets?kind=&view=&tag=&attr=<k>=<v>&simulation_id=&analysis_id=&parca_dataset_id=&source=sim:<id>&available=true\|false\|any&since=&limit=&offset=` (`attr` repeatable, `attr.<k>=<v>` also accepted; JSONB containment; ordered `updated_at` desc; `limit` ≤ 200; `next_offset` pages) |
| one dataset / its bytes | `GET /api/v1/datasets/{id}`; `GET /api/v1/datasets/{id}/content` (streamed, media type by file; `parquet` / `parca-cache` kinds — `store` inherits this, #672 — and objects outside the API's storage bucket are refused with 409) |
| picker helpers | `GET /api/v1/datasets/attributes` (distinct values per key), `GET /api/v1/datasets/tags`, `POST /api/v1/datasets/{id}/tags` (union-merge, mirror of `/simulations/{id}/tags`) |
| what produced this dataset, from what? | `GET /api/v1/datasets/{id}/provenance` → producer run (kind, id, name, status, `source`, tags, `hpcrun_id`, `trace_id`), the span the event came from, and registered datasets the producer's `source` names by `uri` (one hop; `?depth=` and slice 2's `task.command` / `script_sha256` / `image` / `commit` *deferred*) |
| what did this run write? | `GET /api/v1/simulations/{id}/datasets` (the simulation's own and unclaimed bundles; `include_analyses=true` adds its analyses' datasets by `source`), `/analyses/{id}/datasets` (slice 2: `/tasks/{id}/datasets`); `GET /analyses/{id}` carries `status` and `n_datasets` separately (§2a) |
| who consumed this dataset? | *deferred:* `GET /api/v1/datasets/{id}/consumers` (from `analysis.source` / `task.inputs` / `artifact.read`) |
| the ptools consumer's picker unit | *deferred to the ptools step:* `GET /api/v1/datasets/groups` — see `plan-ptools-datasets.md` §3 |
| analyses, listed directly | `GET /api/v1/analyses` gains `status`, `backend`, `source` (`sim:1002`), `tag`, `since`, `limit`/`offset`, ordered `updated_at` desc; `ExperimentAnalysisDTO` gains `source` and `tags` |
| compatibility for the unpatched ptools page | `GET /api/v1/analyses/{id}/data` gains coordinate filters (`view`, `protocol`, `variant`, `seed`, `generation`), applied before any download; when the selection yields one file per view the response `filename` is aliased to `<view>.tsv` with the real key in a new `path` field. As implemented this also applies without filters (§12 q9) |

CLI: `atlantis dataset list | get | fetch | provenance | tags | tag | attributes`,
`atlantis analysis list | datasets`, `atlantis simulation datasets <id> [--include-analyses]`
(`dataset consumers` *deferred* with its route). The **TUI** (a Datasets domain: list with a
`key=value` filter string, record, provenance, fetch, tags, attributes; Analyses lists analyses
and Simulations gains Datasets; selecting a row opens the next hop) and the **marimo GUI** (one
Datasets & Analyses panel) carry the same operations as of `26d0f5b7`. All three read their
wording from `app/dataset_views.py`, so "written by" and "found under" cannot drift apart.

A standalone analysis run's events are stored but have no read route yet
(`/simulations/{id}/events` reads the simulation's run); `docs/OBSERVABILITY.md` §5 records
the gap.

## 8. Backfill (one-shot importer, `scripts/import_cd2_datasets.py`)

*Rewritten on 2026-09-15: the importer creates no run rows.* It takes the manifest path as an
argument (`--manifest`; the CD2 notes' `cd2_ptools_manifest.json` is not committed): 183
stores, of which 95 carry fill bundles — **97 bundles**, 27 `dedicated-fill` and 70
`append-metabolites` written into sim-time `analysis-mnp-*` directories — and 88 have none.
Every CD2 store is its own `simulation` row (verified). For each bundle it registers the
`ptools/*.tsv`, `viz/` figures and `analysis.json` through the walk's own `register_bundle`,
with the producer the walk would choose (the claiming analysis run, else the store's
simulation), tagged `cd2` and `cd2-<family>`, plus `cd2-fill` only for a dedicated fill (an
`append-metabolites` directory also holds the gather's own files). `--analyze` (default,
reads only the database) reports each bundle's producer and registered count; `--apply` lists
each bundle (read-only) and writes; `--family` / `--store` narrow either. Idempotent on `uri`.
Run by hand per site after the migration Job. Over waiting for the walk it adds the CD2 tags
and registers the bundles immediately. `run_identity.json` is not read in slice 1.

*Superseded:* `analysis` run rows per bundle directory (`backend = "fill"`) and a
`simulation_id` producer for `analysis-mnp-*` bundles. Those bundles keep the gather run that
claims them.

**Slice 2's importer** adds the 46 task rows from `cd2_tool_runs.json` and moves the files
those jobs wrote to them (`dataset.task_id`), including the metabolite TSVs the
`append-metabolites` fills wrote into sim-time bundles, which until then stay attributed to
that bundle's gather run.

## 9. PR map across repos (order matters)

| # | repo | change | depends on |
|---|---|---|---|
| 1 | viva-api | **this slice, draft #661:** `dataset` + `analysis.source/tags` + `ProvenanceRef`; standalone analyses as traced runs; the trace feeder; the walk reconciliation tick; `/datasets` API + list filters; CLI; importer; `docs/OBSERVABILITY.md` contract section | — |
| 2 | viva-api | post the §3 contract on #655 | 1 |
| 3 | v2ecoli | `workflow/events.py`: `artifact(uri, kind, **attrs)` helper (thin over `emit`); `analysis_runner.py` per-file + error events; `parquet_emitter.py` per-partition events at finalize; `run.py` reports; ParCa cache bundle event | 2 |
| 4 | sms-ecoli | pin v2ecoli; simulator rebuild; deploy dev — the live feed lights up for composites and analysis jobs | 3 |
| 5 | viva-api | **slice 2** (`plan-task-provenance.md`): task provenance + snapshot mode; tasks as HpcRuns; `dataset.task_id`; task importer; toolkit/sidecar call sites in sms-ecoli | 1 (can run in parallel with 3–4) |
| 6 | viva-api + SRI | the ptools consumer — `plan-ptools-datasets.md` | 1 (API), 4 (live feed) |

#661's commit order: schema; models + database service; standalone analysis as a traced run
(`ANALYSIS` ingest candidacy moved here from the database-service commit, because the ingest
tick's simulation lookup had to change with it); trace feeder; real-trace fixture; walk
feeder; API; CLI; importer; no fabricated analysis rows; docs; TUI + GUI parity. The version
bump is held until the plans are discussed; 0.9.143 (#660) and 0.9.144 (#663) are taken, so it
takes the next free version (0.9.145 as of 2026-09-15).

## 10. Retention and durability

- **The DB rows are the durable record**; events objects in S3 are transport. **Resolved
  (open question 4):** with `events_s3_prefix` unset, events objects land at
  `s3://<S3_WORK_BUCKET>/nextflow/work/<experiment_id>/events/<trace_id>/<job>-N.jsonl`
  (verified on dev), and the bucket lifecycle on **both** dev and prod expires only
  `ray-logs/`, so these objects do not expire. A site that sets `events_s3_prefix` under
  `ray-logs/` would get a 30-day expiry; ingest runs every 5 s and the walk covers gaps.
- Dataset objects themselves: whatever the store's lifecycle is; the row outlives the
  object with `available = false`, so provenance never dangles.
- Script bytes (slice 2): content-addressed in `task_script`, permanent regardless of S3.

## 11. Verification

1. Offline (done for #661; all against throwaway local Postgres containers and in-memory S3
   doubles, nothing hosted): `tests/simulation/test_dataset_registry.py`,
   `test_real_trace_fixture.py`, `test_dataset_walk.py`, `test_provenance_db.py`,
   `test_analysis_run_tracking.py`, `tests/api/test_datasets_router.py`,
   `tests/api/test_analysis_provenance_routes.py`, `tests/app/test_dataset_cli.py`,
   `tests/app/test_dataset_data_service.py`, `tests/scripts/test_import_cd2_datasets.py`,
   `tests/common/storage/test_file_service_stream.py`. They cover producer resolution from
   baggage and run rows, `error` → `available = false`, re-ingest as a no-op, the walk never
   overwriting an event row, a run recorded later taking a bundle over from the simulation,
   availability following the object, both naming conventions, list filters, content streams
   and tag merging. Confirm the tunnel is **down** first (`tests/api/app/test_cli_e2e.py` runs
   live when it is up).
2. Migration (Docker): `test_db_reconcile.py` (18 vectors, single head) and
   `test_dataset_migration.py`: MANAGED upgrade with rows, downgrade/upgrade, no-op on
   `create_all`, the reconciler adopting a current and a previous-release `create_all`
   database, and `create_all` vs migration agreeing on column types, defaults, indexes and
   constraints.
3. `make check`; `uv run pytest` (full, minus `test_cli_e2e.py`): 1802 passed, 58 skipped on
   #661's head. The TUI is driven headless (Textual's pilot against a mocked service) and a
   parity test pins that the CLI, TUI and GUI each reach every dataset call.
4. **Against real S3 and a restored copy of the dev database (done 2026-09-15;
   `docs/runbook-dataset-walk.md`, `scripts/survey_datasets.sql`).** The walk ran read-only
   against the live dev bucket with the dev database COPYed into a local Postgres, and the
   survey then checked what it wrote: 3,954 rows over 12 bundles from 4 simulations and 5
   analysis runs; 0 duplicate `uri`; 0 missing `display_name`, `size_bytes` or `source`; both
   "written by" and "found under" attribution exercised; and every apparent coordinate gap
   accounted for exactly (a missing `seed` is a multiseed aggregate, a missing `n_tp` is a
   `ptools_overview` header). Re-applying a simulation reported `0 registered, 449 unchanged`,
   so idempotency holds on real data. It covered 4 of 1,319 simulations, so it validates shape,
   not fleet coverage -- and it surfaced #672 (a run's own trajectory store gets no row) and
   analyses that sit in `COMPUTING` while owning complete bundles.
5. On dev after deploy + importer, through the clients (not yet run; the GUI has not been
   launched at all): `atlantis dataset list
   --kind ptools-analysis --tag cd2 --view ptools_rna --attr protocol=multiseed` returns the
   filled stores' multiseed tables; `atlantis dataset fetch <id>` byte-equals the S3 object;
   `atlantis dataset provenance <id>` names the claiming analysis run or, for a fill, "found
   under" its simulation; `atlantis simulation datasets <id>` counts match the manifest.
6. After step 4 of the PR map (emit side deployed): a fresh standalone analysis on dev
   yields `origin = event` rows with `n_tp` from the payload, before any walk runs.

## 12. Open questions for reviewers

1. ~~Fourth producer FK for ParCa~~ — taken: `parca_dataset_id` (a cache is reused by many runs).
2. **`artifact.read` in v1?** Cheap on the emit side, and the only way to learn consumers
   nobody declared. Leaning yes, `info` level, optional. Slice 1 stores it but does not
   consume it.
3. **Parquet granularity**: partition prefix per generation (proposed) vs per store.
4. ~~Where do events objects live per site~~ — resolved (§10): durable on dev and prod.
5. **Per-`kind` attribute schema** (validated) or free-form + conventions? Implemented as
   free-form + conventions + `GET /datasets/attributes`, so pickers are built from what exists.
6. ~~Table name~~ — taken: `dataset`.
7. ~~Backend label for fills on the `analysis` run row~~ — moot: no run rows are created for
   fills (§2a, §8); slice 2 makes them task runs.
8. **The walk on by default per site?** One S3 LIST per simulation per batch (25 simulations
   a minute). Decide per site before deploying (`datasets_reconcile_enabled`, batch size and
   interval).
9. **`/analyses/{id}/data` filename aliasing without filters** changes `filename` for current
   callers when a bundle holds one file per view. Keep (the unpatched ptools page's need), or
   alias only when a coordinate filter is given?
10. ~~How to model a run's trajectory store~~ — taken (2026-09-15): one `store` kind with
    `attributes.format = zarr | parquet` (§3), registered by listing `<out_uri>/`. Implementation
    is #672, not slice 1. The survey that found the gap is §11.4.
