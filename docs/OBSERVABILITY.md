# Observability: turning it on, and reading it

**Last revised 2026-09-14.** The original (2026-09-13) was verified end to end on
viva-api `0.9.140` with simulation 1317 (simulator 207); this revision re-verifies
against the live `sms-api-stanford-test` deployment at **`0.9.141`** with
**simulation 1319** (simulator 209) and folds in the six viva-api PRs merged since,
plus the v2ecoli / sms-ecoli work they depend on. Every
number and response below was read off the running system or the merged source,
not transcribed from a design doc. Companion to [`DEPLOY.md`](DEPLOY.md); the
*design* lives in [`plan-observability.md`](plan-observability.md).

**One exception to "read off the running system":** [§4, "Artifacts become `dataset`
rows"](#artifacts-become-dataset-rows-data-provenance-slice-1) arrived with data provenance
slice 1 and was checked against its source and local tests only, not against a deployment.
Its design is [`plan-data-provenance.md`](plan-data-provenance.md) (viva-api#657).

For the **debugging procedure** — what to run, in what order, the blind spots, and
the measured DuckDB / OOM / `rc=0` facts — use the `diagnose-run` skill
(`.claude/skills/diagnose-run/SKILL.md`, in this repo). It carries the step-by-step
that a reference document conveys badly; this file deliberately does not repeat it.
The two are edited together: this revision updated both.

> ### Merged ≠ live. Read this before you conclude anything.
>
> `main` is ahead of the deployment. As of 2026-09-14 the pod on
> `sms-api-stanford-test` answers `/version` with **`0.9.141`**, which contains
> #641 and #642 but **not** #644, #645, #646 or #650. So on the live API today:
>
> | merged on `main` | in 0.9.141? | live symptom if you look for it |
> |---|---|---|
> | #641 Ray dispatch paths inject `PBG_*` | **yes** | — |
> | #642 chunked ingest + debug rows dropped | **yes** | — |
> | #644 `stage` renders parallel siblings correctly | no | `stage` still reads `parca.step > parca.fit_condition > … (x7)` |
> | #645 `--chrome-trace` export | no | the CLI flag exists locally; it reads the API, so it works from a checkout |
> | #646 standalone analysis injects `PBG_*` | no | `POST /simulations/{id}/analysis` runs still emit **nothing** |
> | #650 `/analysis-figures` + `/analysis-figure` | no | verified: the endpoint returns **404** on the live pod |
>
> #645 is the exception worth noting: it is a client-side read-model transform, so
> `uv run atlantis simulation events … --chrome-trace` from a current checkout works
> against the 0.9.141 API right now. Everything else in that table needs a deploy.

---

## 1. The one thing to know first

**Nothing about a run is opt-in at request time.** Whether a run emits events is
decided by two *independent* halves, neither of which is a flag on the submission:

| half | lives in | what decides it |
|---|---|---|
| **capability** — can this process emit at all | the **simulator image** | is the instrumented engine in it (process-bigraph ≥ #209 + the v2ecoli runner) |
| **activation** — is it told who it is | **viva-api at dispatch** | `with_events_env` injects `PBG_*`, gated only by `EVENTS_ENABLED` (default `true`) |

Both are satisfied on `sms-api-stanford-test` today. There is no per-request
switch, and adding one was never the design.

### The simulator images that matter

Read off `GET /core/v1/simulator/versions` on 2026-09-14 — all on `sms-ecoli` `main`:

| simulator | sms-ecoli | what it first carries |
|---|---|---|
| ≤ 206 | — | no instrumentation; `PBG_*` is injected and read by nobody |
| **207** | `9f05d46` | the observability chain lands: process-bigraph `55b70676` → v2ecoli `fc0253df`. Runner-layer events only (`run.*`, `lineage.*`, `task.*`, ticks) |
| **208** | `c57571d` | **ParCa and gather interiors** — v2ecoli#799's `parca.step` / `analysis.name` / `analysis.group` / `analysis.sample` |
| **209** | `6f1cbaf` | v2ecoli `f20e9376`: #800 per-condition ParCa progress, #802 `applicable`, #803 `lineage.debug` respects its gate |
| **210** | `9102846` | v2ecoli `31fe7dfb`: #801 multigen offsets-once, #804 `analysis_options` at the `run_analyses` boundary |
| **211** | `33ecd77` | v2ecoli `1283f4bc`: #806 multiseed per-generation-within-seed, plus sms-ecoli#410 (metabolites-multigen prune, completing #801's fifth view) |

`atlantis simulation status <id>` names the simulator. Batch job definitions are
`<RAY_CONTAINER_JOB_DEFINITION>-<sms-ecoli short sha>` —
`smsvpctest-ray-container-33ecd77` for simulator 211 — built per commit from the
base definition (`simulation_service_ray.py:1071`).

### The combination table

| simulator | viva-api | result |
|---|---|---|
| ≤ 206 | any | nothing. `PBG_*` present, runner no-ops |
| ≥ 207 | < 0.9.139 | nothing. Capable engine, no identity injected |
| ≥ 207 | 0.9.139 | events on **Nextflow, chain, MBP and the MNP composite** only |
| ≥ 207 | ≥ 0.9.140 | **events on every campaign dispatch path** — runner layer |
| ≥ 208 | ≥ 0.9.140 | **+ the ParCa and gather interiors** |
| any | merged, unreleased (#646) | + the standalone `POST /simulations/{id}/analysis` path |

The 0.9.139 row is not a footnote. Four of six dispatch methods carried the
identity in that release; `submit_ecoli_simulation_job` (the MNP ParCa +
simulation pair) and `_submit_analysis_job` (the gather) carried none, so a run on
the default Ray path emitted **nothing** while its `HpcRun` row still showed a
`trace_id` — an empty `/events` indistinguishable from the benign case below.
Fixed in #641 / 0.9.140, with an AST test asserting every dispatch path merges the
identity. #646 then found a **seventh** path the guard could not see; see §2.

### The capability half degrades silently, on purpose

`v2ecoli/workflow/events.py` imports the engine's events module under a guard:

```python
try:  # process-bigraph >= 1.9 (feat/events, #209)
    from process_bigraph import events as _pbg_events
except Exception:  # pragma: no cover - exercised only on a pre-#209 pin
    _pbg_events = None
```

so on an older simulator every emit resolves to a null emitter and does nothing —
`PBG_*` is injected, read by nobody, and the run is exactly as observable as it was
before. That is deliberate ("degrades to a no-op emitter, so the pin can lag one
image"), and it is why a **new viva-api over an old simulator is safe but silent**.

### Which sinks, and the `S3_WORK_BUCKET` trap

`events_env.py` builds `sinks = ["stdout"]` unconditionally, then appends an S3 sink
when `events_s3_prefix()` resolves. **That function has two ways to resolve, and the
second is easy to miss** (`viva_api/common/events_env.py:125-142`):

```python
template = settings.events_s3_prefix          # explicit, default ""
if template: return template.format(experiment_id=...)
bucket = settings.s3_work_bucket              # <- the derive path
if not bucket: return None
return f"s3://{bucket}/{work_prefix}/{experiment_id}/events/"
```

So **`EVENTS_S3_PREFIX` being unset does not mean S3 events are off.** On
stanford-test it is unset and `S3_WORK_BUCKET` *is* set, so the prefix derives to

```
s3://smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91/nextflow/work/{experiment_id}/events/
```

and **the S3 sink is ON**. Confirmed from the outside on 2026-09-14: sim 1319's
`/events` returned a `trace_id` and 452 ingested events including `parca.*` spans,
which only happens when S3 objects exist for the scheduler to ingest. Set `EVENTS_S3_PREFIX` only to send events somewhere
other than the work bucket; set `EVENTS_ENABLED=false` to turn the whole thing off.

### So why is `/events` empty on a run?

The reasons, in the order worth checking:

1. the run predates viva-api 0.9.139, or ran on simulator ≤ 206;
2. the job was **hand-dispatched** (`aws batch submit-job`), which bypasses viva-api
   entirely — see §5;
3. it was a **standalone re-analysis** (`POST /simulations/{id}/analysis`) on
   ≤ 0.9.141 — that path injected nothing until #646, which is merged and not yet
   deployed;
4. `EVENTS_ENABLED=false`, or a namespace with neither `EVENTS_S3_PREFIX` nor
   `S3_WORK_BUCKET` (stdout-only: the events exist, in CloudWatch, but nothing
   ingests them).

An empty array is a finding only after all four are excluded.

### The related knobs

All have working defaults (`viva_api/config.py:219-245`):

| setting | default | meaning |
|---|---|---|
| `EVENTS_ENABLED` | `true` | master switch for injecting `PBG_*` at dispatch |
| `EVENTS_S3_PREFIX` | `""` | explicit override; empty **derives** from `S3_WORK_BUCKET` |
| `EVENTS_FLUSH_SECONDS` | `60` | how often a task rewrites its `events.jsonl` object |
| `EVENTS_HEARTBEAT_SECONDS` | `30` | engine tick heartbeat, wall clock; also the default `analysis.sample` cadence |
| `EVENTS_INGEST_ENABLED` | `true` | scheduler-side ingest into `hpcrun_event` |
| `EVENTS_INGEST_MAX_OBJECTS_PER_TICK` | `50` | bound on ingest work per scheduler tick |
| `EVENTS_INGEST_IDLE_SECONDS` | `600` | skip rows whose newest event is older than this |
| `EVENTS_INGEST_TERMINAL_GRACE_SECONDS` | `900` | keep ingesting this long after a row goes terminal |
| `EVENTS_INGEST_STORE_DEBUG` | `false` | store `debug`-level events as rows too (see §4) |

---

## 2. What the dispatcher injects

Every dispatched unit of work — Nextflow task, chain container job, MNP node,
standalone analysis Job — gets a `PBG_*` block (`viva_api/common/events_env.py`):

| var | what it carries |
|---|---|
| `PBG_EVENT_SINKS` | `stdout` always, plus `s3://…` when a prefix resolves |
| `PBG_TRACEPARENT` | W3C `traceparent`, so spans nest across processes |
| `PBG_TRACE_BAGGAGE` | `key=value,…` — `sim_id` and `experiment_id` on every simulation dispatch; a standalone analysis adds `analysis_id` (the legacy K8s re-analysis names only `experiment_id`). `variant` / `lineage_seed` / `generation` are bound later, by the task itself |
| `PBG_EVENT_TAGS` | opaque to the engine, copied verbatim into every event |
| `PBG_EVENT_FLUSH_S` / `PBG_EVENT_HEARTBEAT_S` | cadence, from settings |

`PBG_` is a reserved prefix at the API boundary, so a caller cannot smuggle these in
through a request.

### The guard, and the two ways it was blind

`tests/simulation/test_dispatch_events_identity.py` reads each dispatch module's AST
and asserts that every `resolve_task_env(...)` result reaches `with_events_env(...)`.
That guard was added by #641 and still missed a path, **twice over** (#646):

1. `DISPATCH_MODULES` listed only `simulation_service_ray.py` and `job_scheduler.py`.
   `simulation_service_k8s.py` was never scanned.
2. Even scanned, the invariant is keyed on `resolve_task_env`.
   `submit_standalone_analysis` hand-builds its K8s Job env as a literal list of
   `V1EnvVar` and never calls `resolve_task_env` at all — so it is *structurally*
   invisible to that scan. **A test pinning the right pattern still misses a caller
   using a different one.**

Both halves are now covered: the k8s module joined `DISPATCH_MODULES`, and a second
test (`HAND_BUILT_ENV_DISPATCHES`) asserts that hand-built dispatch envs call
`events_env`/`with_events_env` at all. The six ray-side methods are pinned by name
in `test_the_known_dispatch_paths_are_all_still_covered`, so deleting one is visible.

Neither test can prove the env survives to Batch — only a live dispatch does that,
which is how #641 was found in the first place.

---

## 3. Reading a run

All verbs take `--base-url`, which is how you point at a tunnelled deployment.

```console
$ uv run atlantis simulation status <id> --base-url http://localhost:8080
$ uv run atlantis simulation tasks  <id> --base-url http://localhost:8080
$ uv run atlantis simulation events <id> --base-url http://localhost:8080
```

`events` flags: `--level`, `--event`, `--generation`, `--limit`, `--tree`/`--no-tree`,
`--follow`/`--no-follow`, `--chrome-trace <file>`.

* **`status`** — the run's own row: stage, generation, `last_event_at`, attempt,
  exit code, error source. `last_event_at` going stale while the status says
  `running` is the signal that a task has stopped talking.
* **`tasks`** — one row per unit of work (Nextflow tasks from `trace.csv`, chain seed
  jobs from Batch). This is where "which of the 40 things failed" is answered.
  **On the MNP path it is legitimately empty**: an MNP run is one Batch job with node
  ranges, not a fan-out of tracked tasks, so there is nothing to list. Verified on sim
  1317 — `/events` returned engine events while `/tasks` returned `[]`. Read an empty
  `tasks` against the run's backend before treating it as a finding.
* **`events --tree`** — the span tree (see below).
* **`events --follow`** — poll until terminal. Useful on a live run; pointless on a
  finished one.

`error_source` on `status` tells you *which* authority decided the status —
`nextflow_trace`, `k8s_condition`, `failure_record` — which matters because they
disagree, and the trace is the one that knows about a task that died under a head
that exited 0.

### `stage` and parallel siblings

`stage` is rendered from the run's currently-open spans. Until #644 it joined one
label per open **span** with `" > "`, which assumes the open spans form a single
nested chain — they do not the moment anything runs in parallel. ParCa fits its
conditions concurrently, so sim 1319 reported

```
parca.step > parca.fit_condition > parca.fit_condition > ... (x7)
```

for what is **one** level with seven siblings. The spans themselves were correct;
this was purely a rendering fault, in the first field anyone reads. #644 emits one
label per open **depth** — a lone span keeps its full label, `N>1` siblings of the
same name collapse to `name xN`, distinct names at one depth stay listed. **Not yet
deployed** (0.9.141), so a live `stage` today can still show the fake chain. Do not
read depth off it until the deploy lands.

### What the tree actually looks like

Read live from sim 1319 (`GET /simulations/1319/events?tree=true`):

```
analysis.name x17
parca.fit_condition x45     <- orphaned to the trace root; see §5, v2ecoli#805
parca.step x6
task x1
  analysis.group x25
  generation x2
  parca.fit_condition x6    <- the six that DID nest (main process only)
```

That is the real shape, not `campaign → parca/lineage/analysis → generation`. The
flatness at the top is the known cross-process span-context gap, not a display bug.

### Debug-level events never reach `/events`

**This is the most likely reason a span you know fires is missing from the API.**
Since #642, ingest drops every `debug`-level event (`UNSTORED_LEVELS = {"debug"}`),
on top of the by-name `UNSTORED_EVENTS = {"tick"}`. Debug events are still written to
both sinks, still folded into the row's progress columns, and still applied to the
span tree — they are simply not kept as queryable rows. Affected today:

| event | level | where to read it |
|---|---|---|
| `analysis.sample` (`duckdb_temp_mb`, `duckdb_memory_mb`, `rss_mb`) | `debug` | S3 `events.jsonl` / CloudWatch |
| `parca.fit_condition.progress` | `debug` | S3 `events.jsonl` / CloudWatch |
| `lineage.debug` (one per simulated timestep) | `debug` | S3 `events.jsonl` / CloudWatch |
| `tick` | any | never stored; see `status.last_event_at` |

Confirmed live: sim 1319's 452 ingested events are 450 `info` + 2 `warning` and
**zero** `debug`. `EVENTS_INGEST_STORE_DEBUG=true` turns storage back on, and is
for debugging the ingester itself — see §4 for why it is off.

### Exporting a trace to Perfetto (#645)

```console
$ uv run atlantis simulation events <id> --chrome-trace ./run.json --base-url http://localhost:8080
Wrote N trace events to ./run.json
```

Open it at **ui.perfetto.dev** (which runs entirely client-side, so nothing is
uploaded), in speedscope, or in `chrome://tracing`. A file and not a hosted view is
the whole point: no collector, no service, nothing leaves the machine — which is what
makes it usable on GovCloud, where the SaaS trace viewers are non-starters on
data-residency grounds. `plan-observability.md` records that the engine carries no
OTel SDK and that "an OTLP exporter is a later sink"; this is the consumer side of
that decision, a pure read-model transform (`viva_api/simulation/chrome_trace.py`)
that imports neither AWS nor engine code.

Two behaviours worth knowing before you read a picture off it:

* **Lanes are packed by overlap**, not by identity attributes (greedy interval
  partitioning, per phase). Keying on condition/step gave 59 lanes for 59 spans on
  sim 1319, drawing ParCa's six *sequential* steps as six parallel rows. Packed by
  overlap it is 10 lanes, and the count means something — 9 in the parca phase **is**
  its peak concurrency.
* **Open spans are drawn to `now` with `still_open: true`**, not dropped. An
  unfinished span is usually the interesting one, and omitting it is exactly how a
  viewer shows a dead run as complete.

The document carries its own `trace_id` and `simulation_id` in `otherData`, so an
exported file is self-describing regardless of what it was named.

### Analysis artifacts: figures and ptools tables (#650)

Two read endpoints, so a credential-less client (the hosted workbench pod, which has
no AWS identity) can reach rendered artifacts the API already holds S3 creds for:

```
GET /api/v1/simulations/{id}/analysis-figures              # list, metadata only
GET /api/v1/simulations/{id}/analysis-figure?analysis=&path=   # fetch one
```

The listing unions DB analysis records with a **direct S3 walk** of
`<out_uri>/analyses/*`, so hand-dispatched "fill" analyses that never created a DB
record still surface (`source: "record"` vs `"s3"`, `status: "s3-only"`). Fetch is
confined to `viz/` and `ptools/` with no traversal, and resolves the S3 location
server-side so the caller never handles a raw URI. Listing is metadata-only and a
fetch is one small object, so neither hits the all-or-nothing cost of
`/analyses/{id}/plots`.

**Merged, not deployed** — verified 2026-09-14: `/analysis-figures` returns **404**
on the 0.9.141 pod. Issue viva-api#648 (the motivating gap) is still open.

The same files are also rows in the dataset registry (§4, "Artifacts become `dataset`
rows"), which is the queryable form: `GET /api/v1/datasets?kind=ptools-analysis&view=ptools_rna`
filters by attribute and tag, and `GET /api/v1/datasets/{id}/content` streams one file.

---

## 4. Ingest: what makes it into Postgres, and how it stopped

The scheduler polls the S3 prefix and inserts into `hpcrun_event` / `hpcrun_span`.
Two bounds on that path exist because each one broke a live run:

**The statement is chunked.** `insert_hpcrun_events` built one multi-row `VALUES`
for every event in an object. asyncpg refuses a statement over **32,767** bound
parameters, and `hpcrun_event` has 15 columns, so the hard ceiling is **2,184 rows**.
On sim 1318 a rewritten object carrying 4,044 events attempted 60,005 parameters and
raised `InterfaceError: the number of query arguments cannot exceed 32767`. In a
*polling* ingester that is not a one-off: the writer rewrites the same object whole
every flush, so the same oversized insert was retried every ~8 s and failed every
time. **Ingest stopped dead ten minutes into the run while `/status` still read
`running`, the simulation itself was fine, and the only symptom was
`last_event_at` quietly ceasing to advance.** The chunk size is derived from the
table's real column count, so adding a column cannot silently re-breach the ceiling;
`on_conflict_do_nothing` applies per chunk, so a rewritten object stays idempotent.

**Debug rows are dropped** (§3). On sim 1318, **946 of the first 1000 stored events**
were `lineage.debug` — one per simulated timestep, 3.6 rows/s for a *single* lineage,
~13,000 rows/hour/lineage. A 10×10 campaign would have written hundreds of thousands
of rows nobody queries. Filtering on **level** rather than name makes the bound
structural: the next per-tick event added anywhere is stream-only by default.

The two are independent and both are load-bearing. The volume filter alone would
merely have pushed the parameter ceiling further out and made it look solved.

> The upstream cause of the `lineage.debug` flood was fixed separately in
> v2ecoli#803 (simulator ≥ 209): `lineage.py` emitted the event *outside* the
> `LINEAGE_DEBUG_DIVISION` gate its own mirror-print respects. The ingest filter is
> defence in depth either way.

### Artifacts become `dataset` rows (data provenance slice 1)

The ingester also turns the files a run reports writing into `dataset` rows: one row per
thing a consumer fetches as a unit, with exactly one producer (a simulation, a ParCa
dataset or an analysis run), attributes, tags and an `available` flag. **A dataset row is
never pre-created.** It is born when an event says the file was written, or when the S3
walk below finds the file. Design: [`plan-data-provenance.md`](plan-data-provenance.md).

> **No producer emits `artifact.written` yet.** The emit side is a separate v2ecoli PR
> (plan §9). Until it deploys, every row on a live system comes from the S3 walk, with
> `attributes.origin = "walk"`.

**The contract.** Emit it at `info` level (debug is stream-only, §3) inside the span that
wrote the file:

```
event:    artifact.written
payload:  uri         s3://bucket/key   an object, or a prefix for multi-file kinds
          kind        parquet | parca-cache | ptools-analysis | analysis | figure | report | other
          name, view  optional (view: ptools_rna, mass_fraction, ...)
          bytes       optional -> size_bytes
          sha256      optional
          attributes  {} open map: variant, seed, generation, agent, protocol, n_tp, ...
                      "lineage_seed" is read as "seed" when "seed" is absent
                      "tags": [...] adds tags; "display_name" overrides the generated one
          error       optional: the file was NOT produced, and why -> available = false
baggage:  sim_id, experiment_id, variant, lineage_seed, generation, analysis_id
```

One event per consumable unit: a parquet **partition prefix** (never a shard), one ptools
TSV, one figure, one ParCa cache bundle prefix. An event whose `uri` is not `s3://` or whose
`kind` is outside the vocabulary is **skipped and counted** (`IngestResult.artifacts_skipped`
and a warning naming the first few reasons), never raised: one bad event must not stop the
run's other files from registering.

**What a row records.** `attributes.origin = "event"`, plus `hpcrun_id` and `span_id` (the
way back to the span, which `GET /api/v1/datasets/{id}/provenance` follows) and `error` when
given. `tags` = the simulation's tags ∪ `attributes.tags`. `source` = the simulation plus the
coordinate: `variant` / `seed` / `generation` / `agent` / `protocol` from `attributes`, with
`variant`, `seed` (baggage `lineage_seed`) and `generation` falling back to the baggage. A
producer may spell the seed `lineage_seed` in `attributes` too, as the history partitions do:
the registry reads it as `seed` when `seed` itself is absent (`seed` wins when both are given),
and keeps the producer's key alongside.

**Producer resolution uses baggage and the run row, never span parentage** — worker spans
can orphan to the trace root (§5):

| the event | producer |
|---|---|
| `kind = parca-cache` | the run's simulation's `parca_dataset_id` (skipped if there is none) |
| baggage `analysis_id` naming a real analysis row | that analysis |
| on an `ANALYSIS` run | the run's own analysis |
| on a `SIMULATION` run | that simulation |
| anything else | skipped and counted |

**A database failure withholds the events cursor**, so the next tick re-reads the same
objects. Event inserts are idempotent on `(trace_id, source, seq)` and registration is an
upsert on `uri`, so the retry is safe.

**Standalone analyses are traced runs.** `POST /simulations/{id}/analysis` records the
`analysis` row *before* submitting, injects its id as `analysis_id` in
`PBG_TRACE_BAGGAGE` (correlation id `analysis-<id>`), and records an `HpcRun` with
`job_type = ANALYSIS`. The ingester reads `ANALYSIS` runs on the `k8s` and `ray` backends
as well as simulations, and the scheduler's `update_analysis_runs` tick ends the run's
`HpcRun` when its analysis resolves, so the ingest grace window keys on a real `end_time`.
An in-run gather still rides its simulation's trace, so its files resolve to the simulation
unless the gather's baggage carries `analysis_id`.

**The S3 walk (backfill and reconciliation).** Events are best-effort by design, so a
scheduler tick, `reconcile_datasets`, walks the next `datasets_reconcile_batch_size` (25)
simulations by id every `datasets_reconcile_batch_interval_seconds` (60 s), wrapping around;
`datasets_reconcile_enabled` turns it off. One LIST of `<out_uri>/analyses/` per simulation,
so a full cycle over N simulations costs N (paginated) listings. In each bundle directory it
registers, with `origin = "walk"`:

| path under the bundle | kind | notes |
|---|---|---|
| `ptools/*.tsv` | `ptools-analysis` | `n_tp` from a ranged read of the header, re-read only when the size changes |
| `viz/*.html`, `*.svg`, `*.png` | `figure` | |
| `analysis.json` | `report` | listed, never read (240 MB seen) |
| anything else (`driver.log`, …) | — | not a dataset |

View, protocol and coordinate come from v2ecoli's `<name>__<group>` file names
(`ptools_rna_multiseed__variant=0.tsv`, `ptools_rna__variant=0_seed=3_gen=12_agent=000.tsv`).

**The walk never creates run rows.** A bundle belongs to the analysis run whose `result_uri`
is the bundle directory; a bundle no run claims (a hand-dispatched fill, say) is attributed
to the **simulation** it sits under, and `origin = "walk"` says nothing claimed it. When a real
producer is recorded later, the next walk moves the rows: a write that names a producer
replaces the row's producer. The walk never rewrites an event-sourced row, but availability
is a fact about the object, so it marks any row whose object (or whole bundle directory) is
gone `available = false`, and back when the object returns. The CD2 backfill,
`scripts/import_cd2_datasets.py --manifest … [--apply]`, registers and tags the campaign's
bundles through the same code.

**Reading it.**

```console
$ uv run atlantis dataset list --kind ptools-analysis --tag cd2 --attr variant=0
$ uv run atlantis dataset provenance <id>          # producer run, trace, span, inputs
$ uv run atlantis dataset fetch <id> --dest ./debug/
$ uv run atlantis simulation datasets <id> [--include-analyses]
$ uv run atlantis analysis list --status completed --source sim:<id>
```

`GET /api/v1/datasets` takes `kind`, `view`, `tag`, `attr=<key>=<value>` (a JSON value keeps
its type: `variant=0` is the integer, `agent=000` a string), producer ids, `source=sim:<id>`,
`available=true|false|any`, `since` and `limit` ≤ 200 / `offset`. `GET /api/v1/analyses/{id}`
reports `status` and `n_datasets` separately: rows lag ingestion, so "running, 0 datasets"
and "done, 0 datasets" mean different things.

---

## 5. What is NOT instrumented, and the known gaps

Stated plainly, because looking for events that cannot exist wastes the most time.

### Fixed since the last revision — the interiors ARE instrumented now

The previous version of this file said "the analysis / gather stage has no interior
instrumentation" and that ParCa was likewise dark. **That is no longer true on
simulator ≥ 208.** The structural reason it was true is still worth knowing, because
it explains the shape of the fix: **a `Step` that no `Composite` *runs* is invisible
to the engine's hooks**, and there were two of them on the critical path — the ptools
analyses (constructed by `analysis_runner`, which calls `.analyze()` directly) and
ParCa's DAG (which executes at composite *build* time, never through `Composite.run`).

v2ecoli#799 closes both **without** a Composite, using the emitter's span API
directly, and #800 then opened up the fitting loop inside `parca.step`. What you now
get (the `since` column is the first simulator image that has it):

| span / event | carries | since |
|---|---|---|
| `parca.step` | `step`, `name` (the step class), duration, status | 208 |
| `parca.fit_condition` | `condition` | 209 (#800) |
| `parca.fit_condition.converged` / `.diverged` | per-condition outcome | 209 (#800) |
| `parca.fit_condition.progress` | periodic, **`debug`-level** | 209 (#800) |
| `analysis.name` | `name`, `scale` | 208 |
| `analysis.group` | `name`, `scale`, `group` (e.g. `variant=0/seed=0/gen=0/agent=0`), duration, status | 208 |
| `analysis.sample` | `duckdb_temp_mb`, `duckdb_memory_mb`, `rss_mb`, **while the query is still running**, `debug`-level | 208 |

`analysis.sample` is the one that changes what is knowable: every number needed to
understand a gather that dies mid-query lives inside a `step.update(...)` that can
block for hours, and before #799 it was sampled exactly once — *after* the call
returned, which on the path that matters never happens. It runs on a daemon thread at
`PBG_EVENT_HEARTBEAT_S`, deliberately **not** via `emitter.heartbeat()` (which emits
a `tick` and carries a process-global throttle that a co-resident simulation would
steal).

### Open: ParCa worker spans orphan to the trace root (v2ecoli#805)

**Span context does not cross the process boundary.** Each ParCa worker process
configures its own emitter from the same `PBG_TRACEPARENT`, so its `root_span_id` is
the campaign span and its `_current_span` ContextVar starts empty — `start_span`
therefore parents to the root. Measured on sim 1319 and reproduced live for this
revision: **45 of 51** `parca.fit_condition` spans parent to the campaign root
instead of their `parca.step`; only the 6 emitted by the main process nest correctly.

Why it bites: `parca.step` 4 and 5 are where ParCa spends most of its wall clock and
are exactly the steps that fan out, so the spans that would say *which step a
condition fit belongs to* are precisely the ones that lose it. "How long did
`TfConditionSpecsStep` really take, including its conditions" is unanswerable from
the tree today. Open; no workaround beyond reading the per-pid event objects in S3.

### Still not instrumented, and will not be

**Hand-dispatched Batch jobs.** A job submitted with `aws batch submit-job` and a
`CONTAINER_JOB_CMD` override bypasses viva-api, so nothing injects `PBG_*`, no
`HpcRun` row exists, and `/status` and `/events` know nothing about it. That is the
normal shape for ad-hoc probes — expect CloudWatch, not the API. This one is a
property of bypassing the dispatcher, not a gap in the engine.

**The standalone analysis path, until #646 deploys.** Covered in code, absent from
0.9.141. See the merged-vs-live table at the top.

### Open: a standalone analysis run's events are stored but have no read route

Its `hpcrun_event` / `hpcrun_span` rows are ingested (§4), but `GET
/simulations/{id}/events` reads the *simulation's* run, and there is no
`/analyses/{id}/events`. Today the way in is a dataset's
`GET /api/v1/datasets/{id}/provenance`, which names the analysis run's `hpcrun_id` and
`trace_id`, and the database itself.

---

## 6. Where the events physically are

| sink | location | lifetime |
|---|---|---|
| stdout | CloudWatch. **Chain/container jobs → `smsvpctest-ray-batch-…`; Nextflow tasks → `/aws/batch/job`** | log-group retention |
| S3 | `<prefix>/<trace_id>/<source>.jsonl`, rewritten whole every `EVENTS_FLUSH_SECONDS` | as long as the bucket keeps it |
| Postgres | `hpcrun_event` / `hpcrun_span`, ingested by the scheduler (§4) | with the row |

**The two log groups are the single most common way to waste twenty minutes.** A
container job's stream does not exist in `/aws/batch/job`, and `GetLogEvents` reports
`ResourceNotFoundException` — which reads exactly like "the job produced no output".
Get the group from the job definition rather than guessing:

```console
$ aws batch describe-job-definitions --job-definition-name <name> --status ACTIVE \
    --query 'jobDefinitions[0].containerProperties.logConfiguration.options'
```

The S3 objects are **per source process**, which is what makes the #805 diagnosis
above possible from outside the cluster: one `<source>.jsonl` per pid, so a
per-pid breakdown of parents is a read, not an instrumentation exercise.

S3 objects are rewritten whole on a timer, so a task's progress is visible **before**
it exits — the property the pre-events setup lacked entirely. The buffer is capped
(head + rolling tail, with an explicit `sink.truncated` marker carrying the drop
count), because an uncapped whole-object rewrite makes cumulative bytes grow with the
square of the event count.

---

## 7. Cost

* Heartbeat is wall-clock throttled (default 30 s), so a long generation costs a
  handful of events, not one per tick.
* `tick` events are **never** stored in Postgres; they fold into
  `stage`/`generation`/`last_event_at` on the run's row. Measured on sim 1317: the
  container log carried `tick` events (one folding **133 ticks** into a single event,
  ~30 s apart — the wall-clock throttle working), and `/events` returned only
  `run.start`/`run.end`. Ticks missing from the API is the design, not loss.
* `debug`-level events are stream-only for the same reason at a larger scale — see
  §3 and §4. They still cost their sink bytes; they cost no rows.
* `analysis.sample`'s sampler adds ~6 ms of its own work per period
  (`duckdb_memory()` + `getrusage`), measured against a real gather. Irrelevant at
  the 30 s default; visible only at test intervals.
* Per-invoke detail (`process.invoke`, `process.timing`) is opt-in via
  `PBG_EVENT_DETAIL` and off by default.
* A sink that raises is disabled after one `sink.error`. **Observability never raises
  into the simulation** — the division-seam carry report is wrapped for exactly this
  reason, and `analysis.sample`'s sampler swallows everything it throws.

---

## 8. Deploying a change to any of this

The schema lives in Alembic and the app bootstraps with `create_all`, so read
`DEPLOY.md` and the "Database migrations" section of `CLAUDE.md` first. Two rules
that have each already cost an incident:

1. **Run the `alembic-migrate` Job before rolling the app.** The reverse order gave
   dev an `UndefinedColumnError` every poll tick (2026-08-26).
2. **A stale migration-overlay pin fails silently** — the Job runs an image that does
   not contain the migration, applies nothing, and exits 0. Keep the
   `<ns>-db-migration` tag equal to the app overlay's.

And one specific to this stack: the capability half lives in a **different repo's
image**. A viva-api deploy cannot give you `parca.*` or `analysis.*` events; that
needs a simulator built from an sms-ecoli commit that pins the right v2ecoli. The
chain is engine release → v2ecoli pin → sms-ecoli (both pins) → simulator build →
viva-api. Check `GET /core/v1/simulator/versions` before concluding that an event is
missing rather than merely unbuilt.
