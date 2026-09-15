# Data provenance across composites, analysis jobs and task jobs

**Status (2026-09-14): plan, not started.** Companion to
[`plan-task-provenance.md`](plan-task-provenance.md) (viva-api PR #656, issue #655), which
specifies the viva-api side — the `task` provenance columns, the `analysis_result` dataset
table, the `artifact.written` ingest hook, and tasks becoming HpcRuns. This document is the
**whole-system view**: one provenance model that every producer feeds — process-bigraph
composites (simulation runs), analysis jobs, and ad-hoc task jobs — and the PR map across
the repos that have to change for it. The ptools consumer is in
[`plan-ptools-datasets.md`](plan-ptools-datasets.md).

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
that stream says *which files were written*. This plan closes that gap by making artifacts
first-class in the trace, and making the trace the primary source of the dataset registry.

## 2. The model

```
producer run  ──(trace_id, baggage)──▶  spans  ──▶  artifact.written events
    │                                                       │
    │ HpcRun row (simulation | parca | analysis-on-sim | TASK)│  ingest (viva-api)
    ▼                                                       ▼
 task.inputs / analysis.source  (ProvenanceRef, declared)   analysis_result rows (datasets)
                                                            │  one per consumable file set
                                                            │  producer FK, attributes, tags
                                                            ▼
                                             consumers: ptools page, workbench, CLI, next runs
```

Four things, each with one home:

| concept | record | identity |
|---|---|---|
| **run** (a composite execution, an analysis job, a task job) | `hpcrun` (+ `simulation` / `analysis` / `task`) | `hpcrun.trace_id`; `PBG_TRACE_BAGGAGE` carries `sim_id`, `experiment_id`, and now `analysis_id` / `task_id` |
| **what a run read** | `task.inputs`, `analysis.source` — `ProvenanceRef` (declared at dispatch, best-effort, never enforced) and optional `artifact.read` events (observed at run time) | `{kind, ref, resolved_id, uri, coordinate}` |
| **what a run wrote** | `analysis_result` (a *dataset*: one row per consumable file set) | `uri` (unique), exactly one producer FK, `kind`, `attributes`, `tags` |
| **how we know** | `hpcrun_event` / `hpcrun_span` (the trace) | `origin = event` (from the trace) vs `origin = walk` (from listing S3) |

The engine stays domain-free (the rule in process-bigraph: domain ids only ride in opaque
baggage; the emitter never names `sim_id` or `variant`). `artifact.written` is therefore an
**event-name convention plus a payload schema**, not an engine feature: the emit helper lives
in v2ecoli's `workflow/events.py` (`emit(name, level, **payload)` already exists), and the
engine's `EventSink`s carry it like any other event.

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
           + analysis_id | analysis_name, task_id   → producer resolution
```

**Granularity rule:** one event per thing a consumer would fetch or mount as a unit. For
parquet that is a **partition prefix** (a generation's `history/…` partition, a
`daughter-state/` checkpoint), never a shard. For ptools it is one TSV. For a figure one
HTML/SVG. For a ParCa cache the bundle prefix.

**Optional companion, `artifact.read`:** `{uri, kind?}` when a run opens an upstream object
it did not write (a sim_data pickle, a store it aggregates). Best-effort; it lets the
registry derive *consumers* of a dataset without anyone declaring them. Not required for v1.

**Sidecar fallback for non-Python producers:** a bash toolkit writes the same JSON objects,
one per line, to `$CONTAINER_OUT_DIR/artifacts.jsonl`; the container entrypoint's existing
output sync ships it, and viva-api ingests it exactly like an events object
(`component = task`, `origin = event`).

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
- **Notes:** the ParCa stage is its own HpcRun (`job_type = PARCA`); its cache bundle is a
  `parca-cache` artifact with `attributes = {commit, inputs_hash, cache_variant}` and
  producer `parca_dataset_id` — that needs a fourth producer FK on `analysis_result`
  (**open question 1**). Multi-node Ray runs: `AWS_BATCH_JOB_ID` is per node, so the emitter
  must key its sink object per process (already the rule in OBSERVABILITY "the Ray path").

### 4b. analysis jobs (sim-time gather, standalone `POST /simulations/{id}/analysis`, fills)

- **Where:** v2ecoli `workflow/analysis_runner.py:1015-1047` — the TSV/HTML writes already
  sit inside the `analysis.group` span whose `group` attribute *is* the coordinate string
  (`variant=0/seed=3/gen=12/agent=…`). Emit one `artifact.written` per written file there,
  and one with `error` when `step.update` raises (today that error is only kept in the
  runner's `per_group` summary).
- **Kinds/attributes:** `ptools-analysis` for `ptools/*.tsv` with `{protocol: single|
  multiseed|multigeneration, variant, seed, generation, agent, n_tp}`; `figure` for
  `viz/*.html`; `analysis` for any other table; `report` for `analysis.json`.
- **Producer FK:** `analysis_id` when baggage carries `analysis_id` (standalone and
  verb-dispatched analyses register the `analysis` row *before* the job runs, so the id
  exists); the sim-time gather inside a simulation run carries only `sim_id` → producer
  `simulation_id`, and the registration hook still creates/attaches an `analysis` run row
  named from the bundle directory (`<store>/analyses/<name>/`).
- **Cross-process gap:** analysis spans from worker processes can attach to the trace root
  rather than under `task` (OBSERVABILITY §"What the tree actually looks like"). Producer
  resolution therefore uses **baggage, never span parentage**.

### 4c. task jobs (the ad-hoc verb, #655)

- **Where:** `ptools_flush.py` and any Python task call
  `v2ecoli.workflow.events.emit("artifact.written", …)` after each upload; bash toolkits
  append to `artifacts.jsonl` (§3 sidecar). `configure_for_task(out_dir)` already exists in
  `workflow/events.py` for exactly this shape.
- **Producer FK:** `task_id` (baggage `task_id`, injected by `_dispatch_task` once tasks are
  HpcRuns — `plan-task-provenance.md` §2d).
- **Inputs:** `task.inputs` declared at submit (`--input sim:1002`, `--input s3://…`) plus
  optional `artifact.read` events. Together they make a task's derived datasets navigable
  back to the simulation and forward to the ptools page.

## 5. viva-api side (specified in `plan-task-provenance.md`; summarized)

- `event_ingest._ingest_object` collects `artifact.written` / `artifact.read` events per
  pass; `register_datasets` upserts `analysis_result` on `uri` with `origin = event`; the
  sidecar file under a task's out prefix is read the same way.
- Producer resolution order: baggage `task_id` → baggage `analysis_id` → HpcRun job
  reference (`jobref_simulation_id` / `jobref_task_id` / `jobref_parca_dataset_id`).
- `reconcile_datasets` tick (hourly): S3 walk of READY producers' dest prefixes — registers
  rows with `origin = walk` where no `event` row exists, flips `available` when an object is
  gone. Events are best-effort (opt-in sink, size-skip, never raised), so the walk is the
  safety net; it never overrides an `event` row.
- One-shot importer for history (`assets/ptools/cd2_tool_runs.json` → tasks;
  `cd2_ptools_manifest.json` → analyses + datasets; `run_identity.json` → sim-time datasets).

## 6. Queries the model has to answer (API + CLI)

| question | route |
|---|---|
| what datasets exist with these attributes / tags? | `GET /api/v1/datasets?kind=&tag=&view=&attr.<k>=&producer=sim:1002` |
| what produced this dataset, from what, with which code? | `GET /api/v1/datasets/{id}/provenance` → producer run (+ `task.command`, `script_sha256`, `image`, `commit`), its `inputs`, the trace/span ids, and the upstream datasets those inputs resolve to (one hop; `?depth=` for more) |
| what did this run write? | `GET /api/v1/simulations/{id}/datasets`, `/tasks/{id}/datasets`, `/analyses/{id}/datasets` |
| who consumed this dataset? | `GET /api/v1/datasets/{id}/consumers` (from `task.inputs` / `analysis.source` / `artifact.read`) |
| is it still there? | `available` on every row; `?available=true` default |

CLI: `atlantis dataset list|get|fetch|provenance|consumers`, `atlantis task show` prints
its datasets, `atlantis simulation datasets <id>`.

## 7. Retention and durability

- **The DB rows are the durable record**; events objects in S3 are transport. If
  `events_s3_prefix` resolves under `ray-logs/` it expires in 30 days (bucket lifecycle),
  so ingest must run continuously (it does: every 5 s tick) and the reconciliation walk
  covers any gap. Verify per site where the events prefix actually lands (**open q. 4**).
- Script bytes: content-addressed in `task_script` (viva-api) — permanent regardless of S3.
- Dataset objects themselves: whatever the store's lifecycle is; the row outlives the object
  with `available = false`, so provenance never dangles.

## 8. PR map across repos (order matters)

| # | repo | change | depends on |
|---|---|---|---|
| 1 | viva-api | `plan-task-provenance.md` implementation: `task` provenance + snapshot mode; tasks as HpcRuns; `analysis_result`; ingest hook (synthetic-event tested); `/datasets` API; walk reconciliation; importer | — |
| 2 | viva-api | post the §3 contract on #655; `docs/OBSERVABILITY.md` section | 1 |
| 3 | v2ecoli | `workflow/events.py`: `artifact(uri, kind, **attrs)` helper (thin over `emit`); `analysis_runner.py` per-file + error events; `parquet_emitter.py` per-partition events at finalize; `run.py` reports; ParCa cache bundle event | 2 |
| 4 | sms-ecoli | pin v2ecoli; `ptools_flush.py` + toolkit call the helper / write the sidecar; move the CD2 toolkit into `scripts/` (the review-gate half of #655 §10) | 3 |
| 5 | viva-api | simulator rebuild + pin; `ptools.env` `SMS_API_HOST=` relative; deploy dev | 4 |
| 6 | viva-api + SRI | the ptools consumer — `plan-ptools-datasets.md` | 1 (API), 5 (live feed) |

Steps 1 and 6 can proceed in parallel against the walk-registered rows; the live event feed
lights up at step 5.

## 9. Open questions for reviewers

1. **Fourth producer FK for ParCa** (`parca_dataset_id`) on `analysis_result`, or model the
   cache as a `simulation`-produced artifact? Leaning FK: a cache is reused by many runs.
2. **`artifact.read` in v1?** Cheap on the emit side, and it is the only way to learn
   consumers nobody declared. Leaning yes, `info` level, optional.
3. **Parquet granularity**: partition prefix per generation (proposed) vs per store. Per
   generation matches how chain-dispatch writes and how workbench reads; per store is what
   the ptools page needs (it gets that via the `analysis` bundle anyway).
4. **Where do events objects live per site** (under the run's out_uri, or under
   `ray-logs/`)? Determines whether the 30-day lifecycle can eat un-ingested artifacts.
5. **Should `analysis_result.attributes` carry a schema per `kind`** (validated) or stay
   free-form with documented conventions? Proposed: free-form + conventions + a `GET
   /datasets/attributes` summary so pickers are built from what exists.
6. **Naming:** `analysis_result` reads as "analysis" even for parquet and reports. `dataset`
   or `artifact` may be the better table name; the API already says `datasets`.
