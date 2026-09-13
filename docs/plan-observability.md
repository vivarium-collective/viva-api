# Observability plan for whole-cell campaigns (viva-api · v2ecoli · process-bigraph)

> ## Status: **DELIVERED 2026-09-13. Merged, tagged, NOT deployed.**
>
> | repo | PR | commit |
> |---|---|---|
> | process-bigraph | #209 engine events | `55b70676` |
> | v2ecoli | #772 runner events, S3 sink plugin | `fc0253df` |
> | sms-ecoli | #395 the repin (both pins, one commit) | `9f05d466` |
> | — | simulator image | **207** |
> | viva-api | #636 `d7e2f4a6c8b0` migration fix | `316a0bbac` |
> | viva-api | #609 PR-A head poller | `2317adce0` |
> | viva-api | #612 PR-D ingester + API | `f3c6a3edd` |
> | viva-api | #638 bump 0.9.139 | `004ee1a85`, tagged **`v0.9.139`** |
>
> **Not built, not deployed.** stanford-test still runs 0.9.138 and its database is
> MANAGED at `c7d1f3a9b2e4`, so **three** revisions are pending there:
> `f76e43d01841 → d7e2f4a6c8b0 → a3b5c7d9e1f2 → e3a9c1d70b62`. Each was walked
> against that database's actual state (no `task`, no `hpcrun_event`, no
> `hpcrun_span`). **Run the `alembic-migrate` Job BEFORE rolling the app.**
>
> **Events are OFF by default** — no sink resolves unless `PBG_EVENT_SINKS` is set —
> so this ships inert and a path is enabled deliberately.
>
> ### What changed between plan and implementation
>
> * **`PARTIAL` was dropped** (`5a960e0c`), reversing §D4b. Nothing branched on it, Postgres
>   cannot drop an enum label, and "which tasks survived" belongs in the per-task rows
>   and `error_message`. This also keeps the migration free of the deploy-ordering
>   constraint an `ALTER TYPE … ADD VALUE` imposes.
> * **TWO migrations, not "one migration + one marker"** as §D4b says. `a3b5c7d9e1f2`
>   (columns + tables) and **`e3a9c1d70b62`** (`hpcrun_event.layer → component`). The
>   rename MUST be its own revision: a site stamped at `a3b5c7d9e1f2` by a #609-era
>   deploy is MANAGED, so `upgrade head` is a no-op and a rename living *inside* that
>   revision could never run — `UndefinedColumn` on every insert, silently, with the
>   Job exiting 0. Two fingerprint markers accordingly.
> * **An alembic audit (Jim's ask, 2026-09-13) found three more things**, all fixed:
>   `d7e2f4a6c8b0` on `main` could not apply to **any** database (double `CREATE TYPE`
>   + non-idempotent `CREATE TABLE`) — split out as #636; **no test anywhere asserted a
>   single alembic head**, though the two-heads bug had already happened once; and
>   `diff_schemas` compares column **names** only, so nothing caught type drift — the
>   class that shipped the VARCHAR-vs-`composejobstatusdb` defect. Both gaps now have
>   mutation-verified tests.
>
> ### Known gaps, recorded rather than closed
>
> * **The gather is uninstrumented interior.** The ptools analyses are `Step`
>   subclasses but never enter `Composite.run` — `analysis_runner` calls `.analyze()`
>   directly, and `workflow_nf` deliberately does not wrap analysis. So no engine hook
>   fires for them: no `run.start`, no `tick`, no `process.exception`, no spans. The
>   dispatcher layer still covers the stage *boundary*. Raised on sms-ecoli#166
>   (2026-09-13 17:56Z); **not** a defect decision yet.
> * **viva-api#637** — `alembic upgrade head` cannot run against a genuinely empty
>   database (`d3f9a1c72b84` ALTERs `analysis`, which no migration creates). Dormant
>   for every existing site; live for the first new one. @eagmon chose **Option 1**
>   (make the chain honest) with two conditions: guard the new `CREATE TABLE`s, and
>   ship the empty-DB `upgrade head` test as a real negative control.
> * **No user guide yet.** This file is the design of record, not instructions.
>   `PBG_EVENT_SINKS` appears nowhere outside these plan docs. A usage guide and a
>   debugging skill are queued for after the deploy, so each instruction can be
>   verified against a live run rather than against the source.
>
> ---
>
> <details><summary>Superseded status line (2026-09-10 16:15Z)</summary>
>
> **All four implementation PRs are complete, cluster-validated, and awaiting review. None is merged.** Approved by Jim 05:45Z, re-planned once at 06:50Z (§D1′ — the engine is general-purpose: `component`, dotted event names, opaque string baggage, no domain vocabulary in process-bigraph), extended at 15:50Z (§D5 — the Ray/multi-node path). See **§Where each piece actually is** below for heads and state. Progress rows go in `docs/plan-nextflow-act3.md`; this file is the design of record. Companion to `plan-nextflow-dispatch.md`, `plan-nextflow-act2.md`, `plan-nextflow-act3.md`.
>
> </details>


## Context — why now

On 2026-09-10 a one-line omission in v2ecoli's lineage carry policy (#765 carried the
per-tick `request`/`allocate` partition roots across division) killed every
multi-generation run built from `main` for five hours. Finding it needed:

- reading two CloudWatch log groups reachable only after `describe-jobs` for the stream
  name (the chain path logs to `smsvpctest-ray-batch-…`, Nextflow tasks to
  `/aws/batch/job`);
- reconstructing simulation id → Batch job id → log stream by hand, every time;
- a bisect image (simulator 192) to prove which of three co-shipped PRs was at fault;
- ~~the Run 3 dose firing ~2,900 s early on sim 898~~ (withdrawn 08:15Z: under the Nextflow path's
  window semantics it fired at 10,001 s; what looked like a clock bug was two dispatch paths with
  different lineage semantics — a generation ends at division on the chain path but runs the full
  window, daughters included, on the Nextflow path — found only by an opt-in debug print);
- noticing by accident that 943's Nextflow head was silently re-running a
  deterministic crash (retry 3 of 10, API status "running"), and that 749 said
  "running" for hours after its head had finished with four failed tasks.

The same session showed the pattern is general, not one bug: the wrong chassis on
the 42 Run 4 inductions (silent overwrite of a shared S3 slot), the `6.07` vs
`10^6.07` induction (never checked before compute burned), the MNP swap drop
(classic FBA ran where redux was declared), the zarr `_check_group` race on the
MNP path. Each was invisible until a downstream process threw or a human compared
numbers. The API layer answered "running" or "failed: backoff limit" throughout.

The engineering principle Jim set: **we fix pipeline/dispatch/communication bugs and
measure; we do not decide model questions.** Observability is the tool that lets us
do the first without a bisect and lets Chris and Eran see the second.

## Broad goals

1. **A run explains itself.** From `atlantis simulation status <id>` (and the
   workbench) a person can tell, without AWS credentials: what stage it is in, whether
   it is progressing, what it decided at every seam (chassis, cache key, carried
   state, seed, retry), and — on failure — which process, at which simulated time, in
   which generation, with what state summary.
2. **Instrumentation is automatic.** Process and step authors (Chris, Alex, Eran,
   the CD2 contributors) do not add log lines. The engine (process-bigraph) and the
   runners (v2ecoli lineage runner, viva-api dispatchers) emit the events, so any new
   composite, injected process, or fork inherits the same visibility.
3. **One identity, end to end, with nested context.** A simulation id is stamped into
   every artifact: Batch job names, log stream prefixes, the S3 output prefix, the
   event stream. One query finds everything about a run. Identity is modelled
   OpenTelemetry-style (Jim, 05:40Z): a `trace_id` per campaign (viva-api's existing
   `HpcRun.correlation_id`), `span_id`/`parent_span_id` for the nested contexts
   campaign → ParCa / lineage(variant, seed) / analysis → generation → (opt-in)
   process invoke, propagated to tasks in W3C `traceparent` style through an
   environment variable. No OTel SDK in the engine; the schema is OTel-compatible so
   an OTLP exporter can be added later as one more sink.
4. **Failures fail once, and say why.** Deterministic crashes do not retry ten times;
   the first traceback reaches the API's `error_message`; the run flips to FAILED when
   its head or any required task fails, not when someone notices.
5. **Cheap enough to be always on.** Heartbeat and seam events cost nothing
   measurable against a 30-minute generation; per-tick detail is opt-in.

## Desirable characteristics

| characteristic | meaning here |
|---|---|
| **Layered, not centralised** | engine events (tick, process timing, exception context) · runner events (generation, carry, checkpoint) · dispatcher events (submit, stage cache, retry, reap) · API view (aggregate). Each layer works alone; together they compose. |
| **Structured** | one JSON object per event with `sim_id`, `experiment_id`, `variant`, `lineage_seed`, `generation`, `global_time`, `wall_time`, `event`, `level`, `payload`. Text logs keep working (stdout mirror), but the JSON is the contract. |
| **Push, not pull** | a task writes its events as it goes (stdout → CloudWatch, plus a per-run JSON-lines object flushed on a timer), so a stalled task is visible *before* it exits. Today S3 sees nothing until the task ends. |
| **State-aware, not just log-aware** | the engine can summarise a store cheaply (counts, sizes, min/max, NaN/negative flags) at a seam or at the moment of an exception. That is what turns "negative count in polypeptide elongation" into "daughter's `request` root carried 14 stale process entries totalling N counts". |
| **Zero-config default, knobs for depth** | always: heartbeat every N seconds of sim time, seam decisions, run start/end summary, exception context. Opt-in: per-process timing, per-tick store deltas (the #733 `LINEAGE_DEBUG_DIVISION` class), full state dumps. |
| **Backend-agnostic** | the same events on Nextflow tasks, chain container jobs, MNP Ray actors, local `vwb` runs. The transport differs (CloudWatch group, Ray driver log, local file), the schema does not. |
| **Reachable from the tools people already use** | `atlantis simulation status/events/log`, the workbench run page, the act docs. Not a new dashboard first. |
| **Tested like code** | log-line/event tests with `capsys`/`caplog` next to the behaviour they observe; a static test that every runtime root store is classified (the check that would have caught #765). |
| **Pluggable and optional, infrastructure-agnostic** (Jim, 05:35Z) | the engine defines an event-sink interface and ships only stdout and local-file sinks; it never imports boto3, kubernetes or redis. Sinks are registered by the caller (`PBG_EVENT_SINKS="stdout,file:/path"` or a registry call), so the S3 events object, CloudWatch enrichment and the API ingester are viva-api/v2ecoli adapters, each independently switchable off. A laptop `vwb` run gets identical events with the file sink alone. The core event schema carries no AWS identifiers; job ids and log streams are payload fields the dispatcher adapter adds. Off, or stdout-only, needs zero configuration. |

## Where instrumentation lives (the question this plan must settle)

Three candidate homes, from most general to most specific:

- **process-bigraph engine** (`Composite._run_inner` → `run_steps`/`run_process` →
  `process_update` → `invoke`): tick heartbeat, per-process wall time, exception
  context (process path, `global_time`, store summary), run start/end. Reaches every
  composite in the ecosystem; needs an upstream PR (eagmon's repo; contribution
  boundary applies to Alex, not to us — check).
- **v2ecoli runners** (`LineageProcess`, `LineageStep`, `run.py`, emitters):
  generation/carry/checkpoint/seed events, chunk-flush heartbeat, structured failure
  record at the top-level except.
- **viva-api dispatchers + scheduler**: submit/stage/retry/reap events, Batch job id
  ↔ sim id correlation, failure reason capture into `error_message`, head-exit → run
  status mapping, `/simulations/{id}/events`.

The plan (next section, after exploration) picks concrete hook points and a rollout
order that gives value at each step without waiting for the engine PR.

## What exists today (verified 2026-09-10; the plan reuses these rather than adding code)

| primitive | where | state |
|---|---|---|
| `PROCESS_BIGRAPH_TRACE_FILE` JSONL sink + `_trace_invoke` | process-bigraph `composite.py:43-110`, fired at `:3049` | built, referenced nowhere, records only *successful* invokes — **removed by #209; compatibility decision in D2′** |
| `_summarize_value` (scalars, numpy → shape/dtype/sum/head, dict depth 3) | `composite.py:59-87` | reusable as the state summariser; lacks NaN/negative flags |
| `TimingSummary` / `Composite.timing_summary()` | `composite.py:150-192`, `:2724` | computed on every run, printed by nothing |
| `ReconcileSummary` (touched leaf paths + structural flag per tick) | `composite.py:3211-3220` | used for scheduling, then discarded |
| `PROCESS_BIGRAPH_PROFILE_PROCESSES` per-process timing | `composite.py:55`, `:1508` | env-flippable, undocumented — kept intact by #209 (D2′) |
| Nextflow `trace.csv` (per-task status + Batch `native_id`) | staged out by `_render_nf_command` (viva-api `simulation_service_ray.py:1585`) | never read |
| Nextflow weblog receiver + typed models | viva-api `common/hpc/nextflow_weblog.py` | wired only into the legacy SLURM path |
| `K8sJobService.get_pod_termination` (reason + exit code) | `common/hpc/k8s_job_service.py:116-146` | unused on the simulation path |
| Batch `exit_code` on `JobStatusInfo` | `simulation_service_ray.py:513-527`, `:4389` | computed, dropped by `update_hpcrun_status` |
| `HpcRun.correlation_id` (indexed) | `simulation/tables_orm.py:110` | joins worker events; becomes the trace id |
| chain Batch tags (`ExperimentId`, `Seed`, …) | `simulation_service_ray.py:3567-3579` | set, never queried back |
| `scripts/diagnose_sim.py` (NF log → per-task records, `.command.err` tails) | viva-api | client-side only |
| chain wrapper periodic out-dir sync (`CONTAINER_OUT_SYNC_INTERVAL`, 30 s) | sms-ecoli `docker/batch-container-entrypoint.sh:33` | free upload path for a file sink |

And the gaps they leave: no exception wrapping anywhere on the tick path (`process_update`
`composite.py:3039` invokes unguarded); the lineage runner is silent for a whole generation
(`lineage.py:901` is one blocking `run(interval)` of 3,600 s); no structured failure record
(`LineageStep.update` has no try/except); the scheduler never polls a Nextflow head (only a
user's `GET /status` does, mapping the K8s condition to FAILED with "backoff limit"); Nextflow
retries any non-zero exit three times (`nextflow_deploy.py:200-201`), each attempt invisible;
~~`.nextflow.log` is never uploaded on the v2ecoli path~~ (wrong: the head runs with `cwd=outdir`, which `_render_nf_command` stages wholesale, so it lands at `vecoli-output/<exp>/.nextflow.log`; the *reader* looked in the vEcoli key — fixed in PR-A); no CloudWatch client; no events endpoint.

## Design

### D1. Event schema (one JSON object per line; the contract every layer shares)

```
{ "v":1, "ts":"2026-09-10T05:34:12.123Z", "seq":17, "layer":"engine|runner|dispatcher|api",
  "event":"tick", "level":"debug|info|warning|error",
  "trace_id":"<HpcRun.correlation_id>", "span_id":"…", "parent_span_id":"…",
  "sim_id":"946", "experiment_id":"…", "variant":0, "lineage_seed":3, "generation":2,
  "global_time":1734.0, "wall_time":812.4, "source":"<host-pid or task id>",
  "tags":{…opaque, dispatcher-added…}, "payload":{…} }
```

- **Identity block** is all-optional (`null` when unknown). No AWS/K8s identifiers in the core
  schema: Batch job ids, K8s job names, log streams, attempt numbers live in `tags`/`payload`,
  added by the dispatcher adapter or at ingestion.
- **Nested contexts, OpenTelemetry-style, no SDK**: `trace_id` = the campaign (viva-api's
  `correlation_id`); spans nest campaign → ParCa / lineage(variant, seed) / analysis →
  generation → (opt-in) process invoke. `run_start`/`run_end`, `generation_start`/`_end`,
  `task_start`/`_end` are the span boundaries (each carries `span_id`, `parent_span_id`,
  and `span_end` repeats `start_ts` so a consumer that missed the start can still place it).
  `trace_id` = first 32 hex of `sha256(correlation_id)` — deterministic, because
  `correlation_id` is `{sim_id}_{commit}_{random}` (`compose/hpc_utils.py:44`), not W3C hex;
  the ingester can recompute it from the existing column. Context propagates in full W3C
  form, `PBG_TRACEPARENT="00-<trace_id>-<parent_span_id>-01"`, plus `PBG_TRACE_BAGGAGE`
  (JSON: `sim_id`, `experiment_id`, `variant`, `lineage_seed`; all optional — the runner
  binds what only it knows). The task entrypoint opens the task span and re-exports the
  new `traceparent` into `os.environ` so subprocesses and Ray `runtime_env` inherit it. A
  laptop run with no traceparent mints its own `trace_id`. Ids are a `contextvars.ContextVar`
  copied into the parallel process layer. Cost: three short ids per event, two events per
  span. An OTLP exporter is a later sink; nothing in the engine changes for it.
- **Events by layer** (payload in brackets):
  - engine: `run_start` [interval, n_processes, n_steps, detail]; `tick` heartbeat,
    wall-clock throttled, default 30 s [ticks, process_time, framework_time,
    leaf_paths_touched, structural_changes since last]; `structural_change` [n_paths,
    sample_paths] (this is the division signal); `exception` [path, cls, address, is_step,
    interval, exc_type, exc_msg, state_summary]; `run_end` [status, total, process_time,
    framework_time, top5]; opt-in `process_timing`, `invoke`; `sink_error`;
    entrypoint `task_start`/`task_end` [exit_code, traceback_tail].
  - runner: `generation_start` [gen_seed, lineage_offset, emitter{kind,target,batch_size},
    carry report of the previous division]; `division` [signal structural|flag|exception,
    t_division, dry_mass, carried_roots, dropped_roots{non_carried, edges, unclassified}];
    `generation_end`; `checkpoint`; `chunk_flushed` [num_emits, file, seconds];
    `warning` (one per occurrence, next to each `warnings.warn`); `failure_record`.
  - dispatcher (written straight to the DB, never through the engine): `submitted`,
    `cache_staged`, `retry_observed`, `head_exit`, `task_outcome`, `reaped`.

### D1′. The base-library boundary (Jim, 06:40Z, on process-bigraph#209): the engine is general-purpose

process-bigraph knows composites, processes, steps, runs, ticks, structural changes, protocol
runtimes, exceptions, and entrypoint invocations ("tasks"). It knows nothing about cells,
lineages, generations, seeds, variants, campaigns, dispatchers, runners, ParCa, analyses, or any
cloud. The redesign makes that a rule the code can be checked against, not a convention:

- **Schema**: `v, ts, seq, source, component, event, level, trace_id, span_id, parent_span_id,
  global_time, wall_time, baggage, tags, payload`. `layer` (a fixed `engine|runner|dispatcher|api`
  vocabulary) is replaced by **`component`**, a free-form string: the engine emits
  `"process_bigraph"`; callers set their own (`"v2ecoli.lineage"`, `"viva_api.dispatch"`).
  `baggage` (W3C semantics), span `attrs`, `tags` and `payload` are opaque maps; the engine never
  reads a key by name and coerces values by literal shape only (int/float/bool/str), never per key.
- **Event names are dotted and namespaced by component**; the `event` field is a free string. The
  engine documents only its own:

  | engine event | when |
  |---|---|
  | `run.start` / `run.end` | `Composite.run` (span `run`) |
  | `tick` | throttled heartbeat in `_run_inner` |
  | `structure.changed` | `apply_updates` reconcile summary reports a structural change |
  | `process.exception` | a Process/Step `invoke` raised (path, class, address, interval, state summary) |
  | `process.init` | a protocol runtime initialised a remote process (Ray `init_cell` timing) |
  | `runtime.error` | a protocol runtime flush failed (Ray shard) |
  | `task.start` / `task.end` | the CLI entrypoints (`run_composite`, `run_step`); span `task` |
  | `span.start` / `span.end` | every span boundary |
  | `sink.error` | a sink raised and was disabled |
  | `process.invoke`, `process.timing` | opt-in detail |

  Callers own everything else and the engine never enumerates it: v2ecoli emits
  `lineage.generation.start`, `lineage.division`, `lineage.generation.end`, `lineage.checkpoint`,
  `lineage.chunk.flushed`, `lineage.warning`, `lineage.failure`; viva-api emits `dispatch.submitted`,
  `dispatch.cache.staged`, `dispatch.retry`, `dispatch.head.exit`, `dispatch.task.outcome`,
  `dispatch.reaped`.
- **Identity**: `trace_id`/`span_id`/`parent_span_id` only. Domain identifiers (`sim_id`,
  `experiment_id`, `variant`, `lineage_seed`, `generation`) travel in `baggage`, set by the caller
  (`PBG_TRACE_BAGGAGE` from the dispatcher, `emitter.bind(**kv)` from the runner) and promoted into
  columns by viva-api's ingester. No `PBG_EVENT_IDENTITY`, no fixed identity block.
- **Entrypoints**: span name from `--span-name`, default the document/class basename (never
  `lineage`/`parca`); `failure.json` holds exception type/message, traceback tail, the engine's
  `pbg_context`, and the current baggage/attrs — nothing domain-shaped. The helper is
  `exception_record()`, not `failure_record()`.
- **Nextflow template**: `params.run_tag` (not `sim_tag`) → `tag` directive; `retry_exit_codes`
  and per-label `maxRetries`/`errorStrategy` stay (generic). Tests use generic labels (`light`,
  `heavy`), never `lineage`/`parca`/`analysis`.
- **Prose**: docstrings, comments, tests and the PR text reference only process-bigraph's own
  examples (the growth-division composite, `_two_increasers`, Gillespie); no sim numbers, no
  sms-ecoli/v2ecoli/viva-api names, no cloud names, no "campaign/runner/dispatcher/cell". The
  boundary paragraph stays, phrased generically ("domain identifiers belong in `baggage`").
- **Enforced by a test**: `test_events_vocabulary_is_domain_free` greps the shipped `events.py`,
  the hooks in `composite.py`/`protocols/ray.py`, the entrypoints and `nextflow_deploy.py` for a
  deny-list (`lineage, generation, variant, seed, campaign, dispatcher, runner, parca, cell,
  sim_id, experiment_id, viva, vecoli, batch job`) so the boundary survives future PRs.

Consequences downstream: v2ecoli (PR-C) names its events under `lineage.*`, binds its keys into
baggage, sets `component`; viva-api (PR-D) stores `component` instead of `layer`, promotes
`generation`/`variant`/`lineage_seed` out of baggage, and presents them first-class in the API
and CLI. The plan's D1 identity block and D2 event table above are superseded by this section.

**Concrete changes to process-bigraph#209** (audit of `feat/events` @ `487ba0ef` vs `78d1488`;
the identity block and per-key coercion are already gone in `487ba0e`, so this is the rest):

| what | where | change |
|---|---|---|
| `layer` field, default `'engine'`, doc'd as `engine\|runner\|dispatcher\|api` | `events.py:75, :495, :598` and every internal `'engine'` literal; `run_composite.py:130`; test `test_events.py:116-121` freezes the wire schema | rename to `component: str`, engine value `"process_bigraph"`, free-form; the schema-freeze test asserts `component` is a string |
| flat event names | all emit sites | `run.start`, `run.end`, `tick`, `structure.changed`, `process.exception`, `process.init`, `runtime.error`, `task.start`, `task.end`, `span.start`, `span.end`, `sink.error`, `process.invoke`, `process.timing`; document the set once in the module docstring |
| `sim_tag` | `nextflow_deploy.py:202-206`, test `:52-55` (`sim946`) | `run_tag`; test value `'task-a'` |
| global int coercion of baggage values (`events.py:675-680`; `007` → `7`) | | W3C baggage is string→string: keep values as strings on the wire; no coercion in the engine; consumers coerce |
| `failure_record()` | `events.py:751`, entrypoints | `exception_record()`; the `failure.json` sidecar keeps its name |
| `os.environ['PBG_TRACEPARENT'] = …` in library functions | `run_composite.py:129`, `run_step.py:156` | do it only in `main()` (CLI), behind `export_context=True`; in-process callers get no env mutation |
| domain prose | `events.py:4, 26, 29, 43-44, 49, 161, 15, 254`; `composite.py:3036`; `protocols/ray.py:492-493`; `nextflow_deploy.py:166` | "structural changes (a process added or removed)", "which experiment, which replicate, which parameter set", "the caller derived (e.g. a hash of an upstream correlation id)", "launchers render env through `docker --env`", "without walking a whole state tree", `mysink://host/path`, "callers may classify control-flow exceptions by type, so the original exception must propagate unchanged", "one bad composite … name the `proc_id`", drop the private anecdote |
| test fixtures | `test_events.py:105, 117, 279-290, 408-410`; `test_nextflow_deploy.py:27, 43-46, 575` | `{'experiment': 'exp-1', 'replicate': 3, 'stage': 'pilot'}`, `'boom'`, labels `heavy`/`reporting`, "workflow" not "campaign" |
| PR #209 body | | rewrite: engine-only framing, the growth-division example, the boundary rule, no sim numbers / repo names / internal hostname (`LT-0919652-32327` appears in the sample) |
| vocabulary test | new `process_bigraph/tests/test_events_vocabulary.py` | grep `events.py`, the hook regions of `composite.py`/`protocols/ray.py`, `run_composite.py`, `run_step.py` and the lines this PR adds to `nextflow_deploy.py` for the deny-list; fails on any hit |

Out of scope for #209 but noted: `nextflow_deploy.py` on `main` already says vEcoli ×6, ParCa ×5,
campaign ×4 in inherited prose (`:5, :29, :119, :148, :157, :197, :206, :283, :299`), and
`composite_generator.py:383-392` / `emitter.py:305-307` carry `experiment_id`/`run_id`. A separate
"generic vocabulary" cleanup PR to process-bigraph, if Eran wants it; not mixed into this one.

Stable API surface kept unchanged (consumers pin to it): `configure/get_emitter/set_emitter`,
`EventSink` + `register_sink_factory` + entry-point group + `resolve_sink(s)`, `bind`, `event`,
`span/start_span/Span.end`, `current_context/current_traceparent`, `heartbeat/count`, `exception`
→ `exc.pbg_context`, `parse_traceparent/mint_*`, `parse_baggage`, `summarize_state`,
`traceback_tail`, `retry_error_strategy`, `AWSBATCH_DEFAULTS['retry_exit_codes']`, the CLI flags
`--failure-out/--summary-out/--span-name`. Only names change: `layer→component`,
`failure_record→exception_record`, `sim_tag→run_tag`, event names to the dotted set.

### D2. Engine (process-bigraph PR, we author, eagmon reviews → release 1.9.0)

New module `process_bigraph/events.py`:
- `EventSink` (`emit/flush/close`); built-ins `StdoutSink`, `FileSink`, `MultiSink`,
  `NullSink` only. **Never imports boto3/kubernetes/redis/requests.**
- Registry + resolution: `PBG_EVENT_SINKS="stdout,file:/path,s3://bucket/prefix/"`;
  unknown schemes resolve via `register_sink_factory(scheme, fn)`, the entry-point group
  `process_bigraph.event_sinks`, or a `module:attr` spec. Unresolvable → one warning, dropped.
- `EventEmitter`: sinks, identity (from `PBG_EVENT_IDENTITY`/`PBG_TRACEPARENT`), lock-guarded
  `seq`, heartbeat throttle (`PBG_EVENT_HEARTBEAT_S`), detail flags (`PBG_EVENT_DETAIL=
  timing,invoke`), `bind(**identity)`, `span(name)` contextmanager, `event()`, `heartbeat()`,
  `exception()`. A sink that raises is disabled after one `sink_error`; **the sink can never
  raise into the simulation** (same wrapper as today's `_trace_invoke`).
- Defaults: library **off** (`NullSink`); the CLI entrypoints `run_composite.py`/`run_step.py`
  default to **stdout**, so a container gets events with zero configuration.
- Move `_summarize_value` here (re-export from `composite`), add `min/max/nan_count/neg_count`
  to the ndarray branch, add `summarize_state(state, max_roots=64)` (one level of root
  stores). Retire `PROCESS_BIGRAPH_TRACE_FILE`/`PROCESS_BIGRAPH_PROFILE_PROCESSES` as aliases
  — **see D2′ for what "alias" must mean; the branch as of `411203e` changes the trace file's shape.**

Hooks in `process_bigraph/composite.py` (all data already in scope):
- **H3 `run` `:2618-2629`**: `run_start` before the contextvar set; `run_end` in the
  `finally` with `timing_summary()` fields and `status=error` when an exception propagates.
- **H1 `_run_inner` loop top `:2640`**: `heartbeat(global_time=…)` — one monotonic read per
  tick when idle.
- **H4 `apply_updates` `:3211-3220`**: fold `len(summary.paths)` into the heartbeat counters;
  emit `structural_change` when `summary.has_structural`.
- **H2 `process_update` `:3038-3041`**: `try/except BaseException` around `invoke` → emit
  `exception` with `'/'.join(path)`, `type(instance).__name__`, `process['address']`,
  `interval == -1.0` (Step), `global_time`, `summarize_state(clean_state)`; attach
  `exc.pbg_context = {...}` (+ `add_note` on 3.11+) and **bare `raise`** — v2ecoli's
  `is_division_exception` needs the original type. Replace the `_TRACE_FH` block with
  `if detail_invoke: emitter.invoke(...)`.
- **H5**: `_flush_protocol_runtimes` `:1548-1551` try/except → `exception`
  [runtime class], re-raise; `protocols/ray.py` `_RayBatchActor.batch_update` `:478-488`
  per-`proc_id` try/except re-raising `RuntimeError("batch_update failed for proc_id=… class=…")
  from exc`; `flush_pending` `:651` try/except → `exception` [shard_idx, proc_ids], re-raise;
  `enqueue` `:617` times `init_cell` → `process_init` (cold starts visible).

Entrypoints: `run_composite.py` (`:66` configure default stdout; wrap `composite.run` `:116`
→ `task_end`, write `failure.json` via `_write_json` `:57`, re-raise; optional `--summary-out`),
`run_step.py` same around `invoke` `:146`. Exit codes unchanged.

Nextflow template `nextflow_deploy.py`: `AWSBATCH_DEFAULTS` gains
`retry_exit_codes=[137,143,104,134,139]` (OOM/SIGKILL/SIGTERM/the nf-core set) and
`_awsbatch_profile` `:200-201` renders
`errorStrategy = { (task.exitStatus in <list>) && task.attempt <= task.maxRetries ? 'retry' : 'finish' }`
— a Python exception (exit 1) is never retried. `_resource_lines` `:42-58` accepts
`maxRetries`/`errorStrategy` per label. Optional `params.sim_tag` → `tag` directive so Batch
job names carry the sim id (verify on the pilot).

Tests (`process_bigraph/tests/test_events.py`, extend `tests.py:3144-3162`): off-by-default
emits nothing; run_start/run_end with identity; heartbeat throttle; exception event names the
path and re-raises the original type; a raising sink never breaks the sim; entry-point and
`module:attr` sink resolution; `run_composite` writes `failure.json` and exits non-zero;
Ray `batch_update` error names the proc_id; **instrumentation on/off yields identical state**
(the existing invariant test, extended). `test_nextflow_deploy.py`: retry only on the exit set,
per-label override, tag directive.

### D2′. Legacy tracing vs the new events API — the compatibility decision (Jim, 2026-09-11, on #209)

process-bigraph already shipped two observability hooks before this plan. #209 must add the
OTel-style events **without breaking anyone who uses them**. This section records what each
one is, what the branch did to it, and the decision.

#### What `main` has today (the legacy mechanism)

Both switches are read **at import time** in `composite.py` (`:43-110`, hook at `:3049`).

```bash
# one flat JSONL record per *successful* Process/Step invoke; nothing else in the file
PROCESS_BIGRAPH_TRACE_FILE=/tmp/run_a.jsonl  python my_sim.py
PROCESS_BIGRAPH_TRACE_FILE=/tmp/run_b.jsonl  python my_sim.py
diff <(jq -c . /tmp/run_a.jsonl) <(jq -c . /tmp/run_b.jsonl)      # first diverging step

# per-process invoke time in TimingSummary.per_process (a dict write on the hot path, so opt-in)
PROCESS_BIGRAPH_PROFILE_PROCESSES=1 python my_sim.py
```

```python
# the same two things from code
composite._profile_per_process = True          # before run(); or the env var above
composite.run(3600.0)
print(composite.timing_summary())              # TimingSummary: total / process / framework, top-N paths
```

Trace record (one per line; `path` is a **list**, `gt` is the key for global time):

```json
{"path": ["agents", "0", "metabolism"], "cls": "Metabolism", "gt": 12.0, "interval": 1.0,
 "input": {"...": "_summarize_value(state)"}, "output": {"...": "_summarize_value(update)"}}
```

Properties worth keeping in mind: no overhead when unset; the file is opened once at import
with `buffering=1`, so a Ray worker that inherits the env writes its own lines to the same
path; a trace error is written as `{"trace_error": ...}` and never raised into the sim.
`TimingSummary` is a **pull** API (the caller asks after `run()`); the trace file is a **push**
API for one event type with no context (no ids, no timestamps, no failures).

#### What #209 adds (the new mechanism)

```bash
# push: a stream of enveloped events to one or more sinks, on only when asked
PBG_EVENT_SINKS=stdout,file:/tmp/events.jsonl \
PBG_EVENT_DETAIL=timing,invoke,spans \
PBG_TRACEPARENT=00-<32 hex trace_id>-<16 hex parent span>-01 \
PBG_TRACE_BAGGAGE='experiment=exp1,replicate=3' \
PBG_EVENT_TAGS='job=abc123,backend=batch' \
python my_sim.py
```

```python
from process_bigraph import events

em = events.configure('file:/tmp/events.jsonl')          # or rely on PBG_EVENT_SINKS
em.bind(experiment='exp1')                                # opaque baggage on every event
with em.span('task', name='gen-3'):                       # spans nest: task -> run -> (tick/invoke)
    composite.run(3600.0)                                 # run.start / tick / process.exception / run.end
em.event('my_app.checkpoint', path='s3://...', component='my_app')   # callers own their namespaces

class MySink(events.EventSink):                           # any destination; the engine imports no SDKs
    def emit(self, event): ...
events.register_sink_factory('myscheme', lambda spec: MySink(spec))   # PBG_EVENT_SINKS=myscheme:...
```

Every line is the D1 envelope; the per-invoke record (only with `detail=invoke`) is

```json
{"v": 1, "ts": "2026-09-11T14:02:11.318Z", "seq": 412, "source": "ip-10-0-1-7-311",
 "component": "process_bigraph", "event": "process.invoke", "level": "debug",
 "trace_id": "…32 hex…", "span_id": "…16 hex…", "parent_span_id": "…16 hex…",
 "global_time": 12.0, "wall_time": 41.2, "baggage": {"experiment": "exp1"}, "tags": {"job": "abc123"},
 "payload": {"path": "agents/0/metabolism", "cls": "Metabolism", "interval": 1.0,
             "input": {"...": "..."}, "output": {"...": "..."}}}
```

#### What the branch did to the legacy hooks (verified against merge-base `78d1488`, branch `411203e`)

| legacy construct | on the branch | compatible? |
|---|---|---|
| `PROCESS_BIGRAPH_PROFILE_PROCESSES` | read in `events.configure()` → `detail=timing` → `Composite._profile_per_process` | **yes** — same effect; `composite._profile_per_process = True` by hand still works (the run-time check only raises the flag, never clears it) |
| `TimingSummary`, `Composite.timing_summary()` | untouched, still exported; `run.end` additionally carries `top5` when `timing` is on | **yes** — the `tests.py:3144-3162` test passes |
| `_summarize_value` | moved to `events.py`, re-exported from `composite` | **yes** |
| `PROCESS_BIGRAPH_TRACE_FILE` | alias in `configure()`: `file:<path>` sink + `detail=invoke` | **file exists, contents differ** — see below |
| `_TRACE_FH`, `_trace_invoke` | deleted (private names) | no known importer |

The trace file a legacy client gets back is not the file it had: (a) each line is the full
envelope, the old fields are under `payload`, `gt` became top-level `global_time`, and `path`
is a slash-joined string instead of a list; (b) the file also receives `run.start`, `run.end`,
`tick`, `process.exception` and `sink.error`, so a line-by-line diff of two runs no longer
lines up; (c) the file is created at first `Composite` construction rather than at import
(harmless). No `DeprecationWarning` is emitted when the alias is used.

**Who uses the legacy hooks.** Jim (2026-09-11): no `_trace_invoke` invocation in any
checked-out project. Claude, same day: none in v2ecoli, sms-ecoli, viva-api,
vivarium-workbench or sms-cdk (`grep`, excluding `.venv`), and `gh search code` for both env
var names finds only process-bigraph itself and this document. **Scope of that search, stated
honestly:** GitHub code search covers public repositories plus private ones the token can
read, indexes default branches only, excludes forks by default, and lags new pushes — so it
rules out *indexed public* use, not private forks, feature branches, or anyone's laptop. The
env var was the only documented surface; `_trace_invoke`/`_TRACE_FH` are underscore-private.

#### The three options

| | keep legacy intact (A) | integrate as a compatible alias (B) | replace (C, the branch today) |
|---|---|---|---|
| what | leave the `_TRACE_FH`/`_trace_invoke` block in `composite.py` beside the new hook | `configure()` maps the env var to a **`LegacyTraceSink`** that filters `process.invoke` and writes the **old flat record** (`path` list, `gt` key); one-time `DeprecationWarning` naming `PBG_EVENT_SINKS=file:<p>` + `PBG_EVENT_DETAIL=invoke` and the removal release | env var → `file:` sink + `detail=invoke`; old record shape gone |
| legacy file byte-compatible | yes | yes | **no** |
| duplicate hot-path code | yes (two `if` checks, two summarisers) | no | no |
| removal path | none; two systems forever | one release with the warning, then delete ~30 lines | already removed |
| cost on #209 | 0 (revert the deletion) | ~30 lines + 1 test | 0 |

#### Decision

**B — integrate as a compatible alias.** Rationale: the env var was documented and is the
kind of thing a diagnostic script hard-codes; it costs thirty lines to keep the file
byte-compatible for one release, and a `DeprecationWarning` is the only way an unknown client
learns the new spelling. A keeps two hot-path branches and two summarisers alive with no end
date. C is defensible on the evidence (no known user) but makes "alias" mean "same env var,
different file", which is a silent break for exactly the user we cannot see.

Concretely for #209, before merge:

1. `events.py`: `LegacyTraceSink(path)` — `emit()` ignores everything but
   `event == 'process.invoke'`; writes `{"path": <payload.path split on '/'>, "cls", "gt":
   <global_time>, "interval", "input", "output"}` with `json.dumps(default=str)` to a
   line-buffered append handle; `flush`/`close` as `FileSink`.
2. `configure()`: `PROCESS_BIGRAPH_TRACE_FILE` → append a `LegacyTraceSink` to the resolved
   sinks (not `file:`), add `invoke` to detail, and `warnings.warn(DeprecationWarning)` once
   per process naming the replacement and the release in which the alias goes.
   `PROCESS_BIGRAPH_PROFILE_PROCESSES` stays exactly as it is (already identical in effect);
   give it the same one-time warning.
3. `test_file_sink_and_deprecated_aliases`: assert the legacy file has **only** invoke records
   in the **old shape** (`path` is a list, `gt` present, no `event` key), and that the
   warning fires once.
4. Docstring in `events.py` and the PR description: name both aliases, their replacements, and
   the removal release (the one after 1.9.0).
5. Not doing: a `_trace_invoke` shim. Private name, no importer found; if one surfaces the
   forwarding function is two lines.

Status: **proposed on #209 (comment, 2026-09-11); awaiting Jim/Eran confirmation before code
changes on `feat/events`.**

### D3. Runner (v2ecoli PR; pins `process-bigraph>=1.9.0`)

- `v2ecoli/workflow/events.py`: `get_emitter()` with a no-op fallback when the engine predates
  1.9 (so the pin can lag one image); `bind_generation(lp)`; `_ObservedEmitter` wrapper that
  delegates to the viva_emitters ParquetEmitter and emits `chunk_flushed` when
  `num_emits // batch_size` advances (viva_emitters is not ours; wrap from outside).
- `v2ecoli/workflow/event_sinks.py`: `S3JsonlSink(uri)` — buffered, a daemon timer rewrites
  `<uri>/<source>.jsonl` every `PBG_EVENT_FLUSH_S` (60 s) with `fsspec` (v2ecoli already
  depends on `s3fs`); `flush()` on `run_end`/`atexit`. Registered as the `s3` entry point.
  `source` = `AWS_BATCH_JOB_ID` if set else `host-pid` (the only AWS-aware line, and only for
  uniqueness). `LineageStep` also always adds `file:{out_dir}/events.jsonl` — on the chain path
  the wrapper's 30 s out-dir sync uploads it for free.
- **Heartbeat needs no slicing**: `self._composite.run(interval)` (`lineage.py:901`) enters the
  inner composite's `_run_inner`, so the engine heartbeat fires every 30 s wall with the right
  `global_time`; `_elapsed_after_run` and division detection stay untouched. (Slicing at `:901`
  is the fallback only if the engine pin cannot advance.)
- `_build_generation` (`:416-548`): `bind_generation` after `gen_seed` (`:424`); open the
  generation span; emit `generation_start` [gen_seed, `_lineage_offset`, emitter target,
  previous carry report]. Forward `time_step` (`_bio_kwargs` `:462-482` drops it; the inner
  composite reads `config.get('time_step', 1)` at `ecoli_baseline.py:910/931`) — call it out in
  the PR since it changes results for any campaign that set `time_step ≠ 1`.
- Carry report at the `select_carry_daughter` call in `_run_until_division` (after `:960`):
  `dropped = set(mother_snapshot) − set(carry) − {'_carried_listeners'}` partitioned into
  `non_carried` (`NON_CARRIED_ROOT_KEYS`), `edges` (`is_edge_node`), `unclassified` (→ level
  warning). Emit `division` [signal, t_division, dry_mass, report]; keep on
  `self._last_carry_report`.
- Replace the `print`s at `:997-1003`/`:1022-1103` with `generation_end`/`checkpoint` events
  (the stdout sink still shows them); add an event next to each of the 8 `warnings.warn` sites;
  emit `[lineage-debug]` as a `debug` event too.
- `LineageStep.update` (`lineage_step.py:275`): `configure(default='stdout')` + file sink; wrap
  `_run_lineage` in try/except → `failure_record` {exc_type, exc_msg, traceback_tail(40),
  `exc.pbg_context` (path, global_time, interval, state_summary), generation, wall_time} →
  `out_dir/failure.json` + event + `flush()` → re-raise.
- **Static classification test** `tests/test_root_store_classification.py` (`fast`): every
  single-element root port declared across `v2ecoli/steps`, `processes`, `composites` (the set
  enumerated tonight: request, allocate, central_fluxes, pinned_flux_targets, periplasm,
  imposed_flux_bounds, …) must be in `CORE_DIVISIBLE_KEYS ∪ NON_CARRIED_ROOT_KEYS ∪ registered
  dividers ∪ an explicit COPIED allow-list`; fails naming any unclassified root. A `slow` twin
  regenerates the set from a rendered `baseline()` agent document.
- Tests: `tests/test_workflow_lineage.py` (`_make` fixture `:5-46` + capsys: one
  `generation_start`/`generation_end` per generation, `gen_seed == _derive_generation_seed`);
  `tests/test_lineage_chain_seam.py` (`_FakeComposite`: `division` reports the three buckets);
  `tests/test_lineage_step.py` (failure record written and re-raised);
  `tests/test_lineage_checkpoint_observability.py` (checkpoint event); wrapper chunk test;
  `tests/test_lineage_time_offset.py` (`time_step` reaches the inner baseline).

### D4. Dispatcher, scheduler, API (viva-api; each piece behind a setting, each optional)

**(a) Identity + transport** — `Settings`: `events_enabled`, `events_s3_prefix` (default
`s3://{s3_work_bucket}/{s3_work_prefix}/{experiment_id}/events/`), `events_flush_seconds`,
`events_heartbeat_seconds`, `events_ingest_enabled`, `events_ingest_max_objects_per_tick`,
`cloudwatch_enrichment_enabled`, `cloudwatch_log_group_nf_tasks`, `cloudwatch_log_group_chain`.
`_awsbatch_nf_params` `container_env` (`simulation_service_ray.py:1440`), `_submit_container`
env (`:1030`), MNP `shared_env` (+ Ray `runtime_env.env_vars`) all get `PBG_EVENT_IDENTITY`,
`PBG_TRACEPARENT`, `PBG_EVENT_SINKS="stdout,s3://…/events/"`, `PBG_EVENT_FLUSH_S`,
`PBG_EVENT_HEARTBEAT_S`, `PBG_EVENT_TAGS` ({backend, seed, generation, …}); whitelist `PBG_*`
in the task-env validator. The dispatcher opens the campaign span (`span_id` stored on the row).
**Format (PR-A, viva-api#609)**: `PBG_TRACE_BAGGAGE` and `PBG_EVENT_TAGS` are W3C baggage form,
`key=value,key2=value2`, not JSON — values render into docker `--env`, and `validate_task_env`
forbids quotes, whitespace, `$` and backslashes. Trace ids are derived (`sha256(correlation_id)[:32]`;
campaign span `sha256("campaign:"+cid)[:16]`), not minted, because the handler submits before it inserts.

**(b) Storage + ingester** — `ORMHpcRunEvent` (`hpcrun_event`: hpcrun_id, source, seq, ts,
layer, event, level, generation, global_time, wall_time, span_id, parent_span_id, payload,
tags; unique `(hpcrun_id, source, seq)` for idempotent re-ingest). `ORMHpcRun` gains
`exit_code`, `attempt`, `events_s3_prefix`, `events_cursor` (JSONB `{key: bytes_read}`),
`stage`, `generation`, `last_event_at`, `error_source`, `trace_id` (indexed),
`campaign_span_id`; a materialised `hpcrun_span` table (`trace_id, span_id, parent_span_id,
name, attrs, start_ts, end_ts, status, error`) upserted from `span_start`/`span_end`, with
open spans closed as `status=unknown` when the row goes terminal; `stage` is derived as the
list of open span names (e.g. `["lineage[seed=3]", "generation[2]"]`), not free text.
~~`JobStatus`/`JobStatusDB` gain `PARTIAL`.~~ **Reversed in implementation (`5a960e0c`)** — the
enum is deliberately untouched. Nothing branched on `PARTIAL` (it was a terminal-set member and a
CLI colour), Postgres cannot drop an enum label once added, and "which tasks survived" belongs in
the per-task rows and `error_message`, not in a status. Keeping the enum fixed also keeps the
migration free of the deploy-ordering constraint an `ALTER TYPE … ADD VALUE` imposes. **Two**
Alembic migrations + **two** `LEGACY_FINGERPRINTS` markers (`db_reconcile.py`): `a3b5c7d9e1f2`
(columns + event/span tables) and `e3a9c1d70b62` (the draft `layer` → `component` rename, which
must be its own revision — a rename inside an already-applied revision can never run). `viva_api/simulation/event_ingest.py`: per active run,
list the prefix, Range-GET from the cursor, bulk-insert non-`tick` events (`ON CONFLICT DO
NOTHING`), fold `tick`/`generation_*` into `stage/generation/last_event_at`; capped per tick,
idle rows skipped. `JobScheduler._polling_loop` (`job_scheduler.py:143-167`) gains
`ingest_run_events()`.

**(c) Nextflow head poller + failure capture** (first PR; no upstream dependency) —
`list_active_nextflow_hpcruns()`; `JobScheduler.update_nextflow_heads()` mirrors
`update_multi_node_jobs`: on head terminal → `get_pod_termination` for exit/reason, read
`{results}/trace.csv` (new `common/hpc/nextflow_trace.py::parse_trace_csv`), status =
COMPLETED (all rows COMPLETED/CACHED) / PARTIAL (some rows FAILED but real work landed) / FAILED
(no rows, or nothing landed) — **the trace decides; the head's exit code is only a tie-break** (PR-A):
under `errorStrategy finish` the head always exits non-zero after any task failure (749's head exited 1),
so 943's shape (ParCa completed, lineage failed) reads PARTIAL, not FAILED; `error_message` precedence: latest `failure_record` → failed row's `.command.err`
tail (lift `diagnose_sim.py::_fetch_s3_text`) → CloudWatch (e) → K8s condition; persist
`exit_code`, `attempt`, `error_source`. `_render_nf_command` `:1560-1568` also uploads
`.nextflow.log` (mirror `simulation_service_k8s.py:274`); `_get_s3_nextflow_log` tries the
v2ecoli key; `_get_k8s_log` `:2002-2008` uses `get_simulation_service_for_job` (latent
TypeError). `update_hpcrun_status` (`database_service.py:1156`): replace the write-once
`if update.error_message:` with precedence by `error_source`. Chain `_finalize_campaign`
(`job_scheduler.py:740-787`) reports PARTIAL and the failed seeds' `failure_record`/status
reason instead of bare job ids.

**(d) API + CLI** — `GET /simulations/{id}/events` (filters `level, event, generation,
after_seq, limit≤1000`, `?tree=1` returns the span tree), `GET /simulations/{id}/tasks`
(trace rows for NF; chain job ids via `get_batch_job_statuses` `:4393`), richer `/status`
(`stage, generation, last_event_at, attempt, exit_code, error_source`; additive). `make spec`
+ `make api_client` with `SIMULATION_OUTDIR`/`HPC_SIM_BASE_PATH` overridden. CLI (`app/cli.py`
after `:1726`): `atlantis simulation events <id> [--follow] [--level] [--generation] [--tree]`,
`simulation tasks <id>`, `simulation status` shows stage / generation / last heartbeat /
attempt / exit code; the poll loop's terminal set gains `partial`.

**(e) CloudWatch enrichment** (optional, last, off by default) —
`common/hpc/cloudwatch_logs.py::tail_log_stream(group, stream, n)`; stream from
`describe_jobs` `container.logStreamName`; log groups from settings, never prose.

## Validation before any merge: a hash-pinned branch chain (Jim, 05:47Z)

Nothing here needs an upstream release to be tested end to end. v2ecoli already pins
process-bigraph by git rev (`pyproject.toml:191`, `rev = "78d14883…"`) and sms-ecoli pins
v2ecoli by rev (`:258`); simulator images build from a branch with `uv sync --locked`. That is
exactly how simulators 192/193 were built tonight. So:

```
process-bigraph  feat/events        (sha P)
v2ecoli          feat/events        pins process-bigraph @ P     (sha V)
sms-ecoli        feat/events        pins v2ecoli @ V             (sha S)  → simulator image "instrumented"
sms-ecoli        main (or 193's c233c7a)                                  → simulator image "control"
```

Same seed, same config, same chassis on both images; the control run proves the invariance
("instrumentation does not change results") on the real model, and the instrumented run
proves the events, `/status`, `/events`, and the failure path — all before Eran reviews a line.
The three branches ride the same review order afterwards (PR-B → PR-C → sms-ecoli pin), and
the pins move from branch hashes to the merge commits.

## Where each piece actually is (2026-09-10 16:15Z)

Nothing is merged. Everything below is built, tested and measured; the gate is review.

| piece | PR | head | state |
|---|---|---|---|
| **PR-B** engine — `events.py`, hooks H1–H5, entrypoints, retry template | process-bigraph#209 | `66bccbdd` | draft, review requested |
| **PR-C** runner — events wrapper, S3 sink plugin, carry report, failure record, classification test | v2ecoli#772 | `4d6a4e22` | draft |
| **PR-A** head poller, `trace.csv`, PARTIAL, error precedence, `PBG_*` env, the migration | viva-api#609 | `baad10a3` | open, review requested |
| **PR-D** ingester, `/events`, `/tasks`, richer `/status`, CLI, `run_pbg.py` bootstrap | viva-api#612 | `5b9b2d87` | open, stacked on #609 |
| **PR-E** CloudWatch enrichment | — | — | not started (optional, last) |

### What has been proved, on the cluster, not in argument

- **Invariance.** Instrumented image (simulator 196) vs control (194), row by row on `global_time`:
  Nextflow 956 vs 951 (4,204 rows) and chain 957 vs 952 (5,276 rows) — **0 differing bulk vectors,
  0.0 on six mass listeners**, identical divisions.
- **Overhead −0.1 %** (1,887 s instrumented vs 1,889 s not).
- **Zero-config works**: the instrumented image emitted task/lineage/generation spans, `run.start`,
  `tick` every ~30 s wall, `lineage.chunk.flushed` and baggage with no `PBG_*` env at all, because
  the CLI entrypoints default to stdout.
- **The stream diagnosed a real bug on its own** before #773 existed: 956 gen 0 read
  `structure.changed @2528` → `run.end @3600` → `lineage.division t_division=1072` — the window
  semantics, visible in one glance at the event stream rather than after a bisect.
- **Graceful degradation is real and now pinned by 13 tests** across four legacy shapes (an old DB
  row, a Nextflow run with no `trace.csv`, an image that never emits, a pre-#209 engine pin).
  Everything probed was already graceful; no behaviour changes were needed. The one thing that was
  *not* graceful lives outside these repos — the workbench's terminal-status buckets lacked
  `PARTIAL` (Alex fixed it in vwb#1045).
- **The migration is tested against the real alembic chain**, not `create_all` — see below.

### Two findings worth carrying forward

**The `create_all` testing gap, and the pre-existing defect under it (viva-api#618).** Every
`PARTIAL` test originally built its schema with `Base.metadata.create_all`, which always reflects
the current model and is therefore structurally incapable of catching migration-vs-ORM drift — the
exact class that produced the production `invalid input value for enum jobstatusdb: "CANCELLED"`.
`tests/simulation/test_observability_migration.py` now drives `alembic upgrade` from empty and
asserts the real `update_hpcrun_status(..., PARTIAL)` fails before the migration and succeeds after.
The migration itself needed no fix (`ALTER TYPE … ADD VALUE IF NOT EXISTS` does not need an
`autocommit_block()` on PG15). Doing that surfaced **viva-api#618**: the chain *cannot* build a
working schema from empty at all — `analysis` and the compose tables are ALTERed by migrations that
never CREATE them, and several `hpcrun` columns exist only via `create_all`. Pre-existing, filed
rather than fixed inside a PR under review, and it blocks the stated end goal of guarding
`create_all` off in production.

**Compatibility in both directions.** A newer viva-api with an older simulator is safe: the ingester
finds no trace id, makes zero object-store calls and writes nothing. An instrumented simulator under
a legacy viva-api still writes `events.jsonl` into its out_dir via the entrypoint default — the
events exist, they just never reach the database. A composite run against a pre-#209 engine gets the
null emitter, which absorbs every call including the error paths.

### Deploy chain, once reviewed

Order matters; each step is a pin.

```
process-bigraph release
  → v2ecoli pin bump
    → sms-ecoli pin bump   ← BOTH pins move together: sms-ecoli pins the ENGINE itself
      → simulator image       (pyproject.toml:273) as well as v2ecoli. Bumping one is a
        → viva-api deploy      uv lock conflict ("conflicting URLs for process-bigraph").
           (carries the migration)
```

## Rollout order (value at every step; parallel where independent)

> **All five steps are DONE as of 2026-09-13** — see the delivery table at the top of
> this file for the merge commits. Steps 1–4 shipped; step 5 (CloudWatch enrichment)
> was optional and was **not** built. The order below is kept as the record of how it
> was sequenced and why each step stood alone.

1. **viva-api PR-A** — head poller, `trace.csv`, `.nextflow.log` upload, `exit_code`,
   PARTIAL, error precedence, `_get_k8s_log` fix, identity env vars, the migration. No
   upstream dependency. Immediate effect: a Nextflow run flips to FAILED/PARTIAL with the first
   `.command.err` traceback within one scheduler tick of head exit; 749's and 943's cases
   become legible from `atlantis simulation status`.
2. **process-bigraph PR-B** — `events.py`, hooks H1–H5, entrypoints, retry template. Release
   1.9.0. Parallel with A.
3. **v2ecoli PR-C** — events wrapper, S3 sink plugin, generation/division/failure events,
   `time_step`, classification test. Developed against a git pin of B; merges once 1.9.0 is on
   PyPI. Then one sms-ecoli pin + one simulator image (`_ensure_container_job_def` copies the
   base job def, so no CDK change).
4. **viva-api PR-D** — events table ingester, `/events`, `/tasks`, richer `/status`, CLI.
   Parallel with B/C; tolerates an empty prefix until the new image runs.
5. **viva-api PR-E** — CloudWatch enrichment. Optional.

## Verification — a local ladder first (Jim, 06:10Z), the cluster last

Most of this runs on a laptop; the cluster pilots are the final rung, not the first:

1. **pytest** in each repo (D2/D3/D4 tests; `capsys` on the JSON lines).
2. **Local toy models on the engine**: process-bigraph's own composites — `_two_increasers`
   (`tests.py:3135`), the Gillespie composite (`:587`) and above all `test_grow_divide`
   (`:540`, a real structural division) — run with `PBG_EVENT_SINKS=stdout` and
   `detail=timing,invoke,spans`: `run_start` → `tick` → `structural_change` at the division →
   `run_end`, an injected raising process → `exception` with path and state summary, and the
   on/off invariance. Seconds, no vEcoli, no AWS.
3. **Local real model, short**: v2ecoli's local sweep runner (`v2ecoli/workflow/run.py --config …
   --max-sim-time 300`) on the bare cache with `V2ECOLI_SKIP_CACHE_VERIFY=1` and the file sink:
   `generation_start` with the carry report, heartbeats every 30 s, `chunk_flushed`, and a
   `failure_record` from an injected raising process. Minutes on a laptop.
4. **Local Nextflow**: `nextflow` 25.04.3 is installed and `generate_nextflow_config(executor=
   'local')` already exists, so a rendered `workflow_nf` document runs on the laptop with plain
   local processes (toy composite or the 300 s ecoli run): exercises the retry template
   (`errorStrategy` exit set), `trace.csv` and its parser, `.nextflow.log`/`failure.json`
   handling, and the file-sink events end to end. `nextflow -preview` stays the compile check.
5. **`vwb smoke`** for the workbench side; `PBG_EVENT_SINKS=file:./events.jsonl vwb run-study …`
   for the laptop event stream.
6. **Cluster**: the branch-chain pilots below, only after 1–5 pass.

- Unit: D2/D3 tests above; viva-api `tests/simulation/test_scheduler.py::TestUpdateNextflowHeads`
  (mirror `TestUpdateMultiNodeJobs:886` incl. the disjointness test `:987` and the loop-order
  test `:1358`), `tests/common/hpc/test_nextflow_trace.py` (all-completed / one-failed / cached
  / reclaimed fixtures), `tests/simulation/test_event_ingest.py` (cursor, idempotence,
  heartbeat folding), handler tests for `/events`, `/tasks`, `/status`, `_get_k8s_log` routing,
  `tests/simulation/test_nextflow_dispatch_axis.py` (identity env, log upload, exit-code retry
  list), a CLI test for `simulation events`.
- Integration on `smsvpctest`, reference shape = sims **946** (Nextflow) and **947** (chain),
  both COMPLETED 5 generations on #769 tonight: a 1×2 Nextflow pilot and a 1×2 chain pilot on
  the bare `6299ba5/` chassis with the new image. Expected per generation in
  `atlantis simulation events --tree`: `generation_start` (with `gen_seed`,
  `dropped_roots.unclassified == []`), ~`wall/30` ticks, `chunk_flushed ≈ ⌈emits/400⌉`, one
  `structural_change` + one `division`, `generation_end`, `checkpoint`; per task one
  `task_start`/`task_end`; `/status` shows `stage=running, generation=N, last_event_at` within
  60 s of real time; `/tasks` lists 1 ParCa + 2 lineage rows.
- Deliberate failure: inject a process that raises `ValueError("boom")` at `global_time ≥ 100`
  in generation 1. Expect one `exception` event with the process path, `generation=1`,
  `global_time=100`, a state summary; one `failure_record`; exactly one Batch attempt (exit 1
  not in the retry set); status FAILED with `error_message` ending `ValueError: boom` and
  `error_source=failure_record`.
- Invariance: same seed and config with sinks off vs `stdout,file,s3` + `detail=timing,invoke`;
  history parquet series (`dry_mass`, per-emit bulk sums) byte-identical; `run_end.total`
  overhead < 2 %.
- Laptop: `PBG_EVENT_SINKS=file:./events.jsonl vwb run-study …` produces the same event stream
  with no AWS configuration.

### D5. The Ray / multi-node path (added 2026-09-10, after Jim asked what it does with an instrumented simulator)

Alex's pbg-native Ray dispatch was not called out separately above. Checked against the branch
chain rather than reasoned about, it behaves better than expected in one way and worse in another.

**It instruments itself with no extra wiring.** Three facts compose:

1. `get_emitter()` lazily calls `configure(default='none')`, and `configure` still reads
   `PBG_EVENT_SINKS` from the environment — `default` only applies when the variable is absent.
   So *any* process that inherits the environment becomes a live emitter on first touch.
2. The dispatcher already puts the `PBG_*` block on every node: `with_events_env(...)` →
   `task_env` → `shared_env` → the single `"0:"` node-property override (the CDK base job
   definition declares one node range and the entrypoint self-branches head vs worker).
   Ray worker processes are forked by that node's raylet and inherit it.

   > **Correction, 2026-09-13 — the chain is right, the premise was not.** Every arrow above
   > holds, but the whole argument starts at `with_events_env(...)`, and this plan never
   > checked that the MNP path *calls* it. It did not. `submit_ecoli_simulation_job` (the
   > MNP ParCa + simulation pair — i.e. the very path D5 is reasoning about) and
   > `_submit_analysis_job` both passed a bare `resolve_task_env()`, so their jobs carried
   > **no `PBG_*` at all** and the actors had nothing to inherit. Found by dispatching sim
   > 1315 on simulator 207 / viva-api 0.9.139 and reading the submitted job's environment
   > back out of AWS Batch: 12 variables on the sim node, 8 on the ParCa node, none of them
   > `PBG_*`. Fixed in #641, with an AST test (`test_dispatch_events_identity.py`) that
   > asserts the premise instead of assuming it. Four of six dispatch methods were always
   > correct, which is why reading the code did not surface this — only a live dispatch did.
3. Inside an actor, `_RayBatchActor.batch_update` calls `composite.update(...)`, and
   `Composite.update` calls `self.run(interval)` — the fully hooked path.

So ticks, structural changes and process exceptions are emitted **from inside the actors**, not
only from the driver, and no `runtime_env.env_vars` plumbing is needed. The plan's earlier risk
note ("MNP actors need `PBG_*` through `runtime_env`") is **wrong and is retracted**: the Batch
node override already covers it, and the lazy `configure` does the rest. The driver additionally
contributes `process.init` (Ray `init_cell` cold-start timing) and `runtime.error` (shard flush).

**What the actor path does *not* give:** `_RayBatchActor.batch_update`'s per-`proc_id` wrapper
re-raises a better-named `RuntimeError` but emits no event of its own. The per-process exception
context comes from the inner composite's own `process.exception`, which is emitted inside the
actor and reaches the sink from there.

**Two defects this exposed downstream, both fixed in v2ecoli#772 (`4d6a4e22`):**

| defect | why the other paths never showed it |
|---|---|
| the S3 object key was `AWS_BATCH_JOB_ID`, so **every process on a node shared one object** and each whole-object rewrite dropped the others' events, silently | Nextflow and chain run one Python process per task, where the job id *is* unique. A multi-node child's id is `<mainJobId>#<nodeIndex>` — per node, not per process. Fixed by appending the pid. |
| the sink buffer was **unbounded**, and the object is rewritten whole, so cumulative bytes written grow with the square of the event count | a lineage task emits a few thousand events; a process running many composites at a high tick rate does not. Fixed by keeping head + rolling tail with an explicit `sink.truncated` marker. |

**One open engine-contract question, raised on process-bigraph#209 (comment 5621486676) for
Eran to decide.** `run.start`/`run.end` fire on *every* `Composite.run`, and on this path a
`Composite.run` is one tick of one cell, not one simulation:

```
run events  =  2 × n_composites_per_actor × driver_ticks
```

For 100 cells over 3,600 s at a 1 s step that is 720,000 events, against roughly 360 per
generation on the lineage path (where `LineageProcess` calls `run()` in 10 s slices — itself a
consequence of #773's stop-at-division slicing, which raised that path from 1 run per generation
to ~360). The heartbeat is wall-clock throttled and does not have this problem; only the run pair
does, because it is per-call rather than per-unit-of-time. Three options are on the PR; the
preferred one rate-limits the pair per Composite instance, always emitting the first run and any
run that ends in error, and folding suppressed runs into the next `run.end` as `runs=N, total=…`.

A depth-based rule (suppress nested runs) does **not** work here: in an actor process there is no
enclosing run, so every cell's run is the outermost one in its own context. The distinguishing
property is the rate, not the nesting.

**Status:** nothing is blocked on this. Events are off by default on every path, the lineage
paths are unaffected at their current call rate, and the downstream cap makes the Ray path
degrade visibly rather than without bound.

## Risks and open questions

- **Spot reclaim visibility**: Batch retries reclaim internally (`maxSpotAttempts`), so
  Nextflow's `errorStrategy` only sees the exhausted case; its exit status may be absent. Read
  one reclaimed task's `trace.csv` `exit` on the pilot before widening `retry_exit_codes`.
- **Volume/cost**: heartbeat 30 s + seam events ≈ 150 lines/hour/task on CloudWatch; `invoke`
  detail stays opt-in; `tick` is never stored in the DB.
- **S3 flush at 1,000 lineages**: whole-object rewrite every 60 s ≈ 17 PUT/s campaign-wide;
  cursor-based ingest capped per tick; idle rows skipped.
- **Threads**: emitter and `seq` under a lock (parallel process layers, the emitter's thread
  pool); the S3 sink's timer thread daemonised and flushed at exit.
- **viva_emitters boundary**: wrapped from outside only; a stalled parquet write still surfaces
  at the next `.result()`, now bracketed by heartbeats.
- **Exception type preservation**: bare re-raise is mandatory (division detection); `add_note`
  needs Python ≥ 3.11, else the attribute only.
- **Ray identity**: `_stable_proc_id = id(shadow)` carries no path; H5 names class + shard until
  the runtime records the path at enqueue. ~~MNP actors need `PBG_*` through `runtime_env`.~~
  **Retracted 2026-09-10 (see D5)**: the Batch node-property override already puts `PBG_*` on
  every node and Ray workers inherit it, so actors instrument themselves. The real Ray issues
  are the per-process object key and the `run.start`/`run.end` rate, both covered in D5.
- **Engine default off vs stdout**: a judgement call Eran may reverse; one line in `configure()`.
- **Alembic**: ~~one migration, one fingerprint marker; PARTIAL must be accepted by CLI/TUI/GUI
  and the workbench's terminal-status buckets (`remote_run_views.py:43-44`) — flag to Alex.~~
  **Settled**: `PARTIAL` was dropped, so there is nothing for the clients or the workbench to
  accept and nothing to flag to Alex. Two migrations, two markers (above). Still open on the
  Alembic side, and NOT introduced by this work: `alembic upgrade head` cannot run against a
  genuinely empty database — `d3f9a1c72b84` ALTERs `analysis`, a table no migration ever creates
  (likewise `compose_hpcrun` at `e5a7c9d10f21`), so `db_reconcile`'s FRESH path is dormant-broken
  and would bite the first brand-new site. Tracked separately.
- **`time_step` forwarding** changes results for any campaign that set it ≠ 1; called out in
  the PR, not silently fixed.
