# Observability: turning it on, and reading it

**Written 2026-09-13 against the live `sms-api-stanford-test` deployment, and
verified end to end on `0.9.140` with simulation 1317** (`sim207-obs-verify3-0913-4520`,
simulator 207) — every command and response below was run, not transcribed from the
source. The first attempt at that verification *failed*, and found two dispatch
paths that injected nothing (#641); do not weaken this file to a code reading. Companion to [`DEPLOY.md`](DEPLOY.md). The *design* lives in
[`plan-observability.md`](plan-observability.md); this file is how to use it.

For the **debugging procedure** — what to run, in what order, and the blind spots —
use the `diagnose-run` skill (`.claude/skills/diagnose-run/`), which carries the
step-by-step that a reference document conveys badly.

---

## 1. The one thing to know first

**Nothing about a run is opt-in at request time.** Whether a run emits events is
decided by two *independent* halves, neither of which is a flag on the submission:

| half | lives in | what decides it |
|---|---|---|
| **capability** — can this process emit at all | the **simulator image** | is the instrumented engine in it (process-bigraph ≥ #209 + the v2ecoli runner) |
| **activation** — is it told who it is | **viva-api at dispatch** | `with_events_env` injects `PBG_*`, gated only by `EVENTS_ENABLED` (default `true`) |

Both are satisfied on `sms-api-stanford-test` today: **simulator 207** (sms-ecoli
`9f05d466`, the re-pin onto v2ecoli `fc0253df` + process-bigraph `55b70676`) is the
first image with the capability, and **viva-api 0.9.139** is the first release that
injects. Any run dispatched after 2026-09-13 18:52Z on that namespace with simulator
≥ 207 emits. There is no per-request switch, and adding one was never the design.

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

| simulator | viva-api | result |
|---|---|---|
| ≤ 206 | any | nothing. `PBG_*` present, runner no-ops |
| ≥ 207 | < 0.9.139 | nothing. Capable engine, no identity injected |
| ≥ 207 | 0.9.139 | events on **Nextflow, chain, MBP and the MNP composite** only |
| ≥ 207 | ≥ 0.9.140 | **events on every path** — stdout always, S3 when a prefix resolves |

The 0.9.139 row is not a footnote. Four of six dispatch methods carried the identity
in that release; `submit_ecoli_simulation_job` (the MNP ParCa + simulation pair) and
`_submit_analysis_job` (the gather) carried none, so a run on the default Ray path
emitted **nothing** while its `HpcRun` row still showed a `trace_id` — an empty
`/events` indistinguishable from the benign case below. Fixed in #641 / 0.9.140,
with an AST test that asserts every dispatch path merges the identity.

### Which sinks, and the `S3_WORK_BUCKET` trap

`events_env.py` builds `sinks = ["stdout"]` unconditionally, then appends an S3 sink
when `events_s3_prefix()` resolves. **That function has two ways to resolve, and the
second is easy to miss:**

```python
template = settings.events_s3_prefix          # explicit, default ""
if template: return template.format(experiment_id=...)
bucket = settings.s3_work_bucket              # <- the derive path
if not bucket: return None
return f"s3://{bucket}/{work_prefix}/{experiment_id}/events/"
```

So **`EVENTS_S3_PREFIX` being unset does not mean S3 events are off.** On
stanford-test it is unset and `S3_WORK_BUCKET` *is* set (verified on the live pod,
0.9.139), so the prefix derives to

```
s3://smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91/nextflow/work/{experiment_id}/events/
```

and the S3 sink is **on**. Set `EVENTS_S3_PREFIX` only to send events somewhere other
than the work bucket; set `EVENTS_ENABLED=false` to turn the whole thing off.

### So why is `/events` empty on an older run?

```console
$ curl -s .../api/v1/simulations/1314/events
{"id":1314,"trace_id":null,"events":[],"tree":null,"next":null}
```

Because sim 1314 was dispatched **before 0.9.139 rolled**, so nothing injected
`PBG_*`. It is not a misconfiguration and not a bug — it is a run from before the
activation half existed, and no amount of config will backfill it. The three reasons
an array comes back empty, in the order worth checking:

1. the run predates viva-api 0.9.139, or ran on simulator ≤ 206;
2. the job was **hand-dispatched** (`aws batch submit-job`), which bypasses viva-api
   entirely — see §4;
3. `EVENTS_ENABLED=false`, or a namespace with neither `EVENTS_S3_PREFIX` nor
   `S3_WORK_BUCKET` (stdout-only: the events exist, in CloudWatch, but nothing
   ingests them).

### The related knobs

All have working defaults (`viva_api/config.py:215-237`):

| setting | default | meaning |
|---|---|---|
| `EVENTS_ENABLED` | `true` | master switch for injecting `PBG_*` at dispatch |
| `EVENTS_S3_PREFIX` | `""` | explicit override; empty **derives** from `S3_WORK_BUCKET` |
| `EVENTS_FLUSH_SECONDS` | `60` | how often a task rewrites its `events.jsonl` object |
| `EVENTS_HEARTBEAT_SECONDS` | `30` | engine tick heartbeat, wall clock |
| `EVENTS_INGEST_ENABLED` | `true` | scheduler-side ingest into `hpcrun_event` |
| `EVENTS_INGEST_MAX_OBJECTS_PER_TICK` | `50` | bound on ingest work per scheduler tick |
| `EVENTS_INGEST_IDLE_SECONDS` | `600` | skip rows whose newest event is older than this |
| `EVENTS_INGEST_TERMINAL_GRACE_SECONDS` | `900` | keep ingesting this long after a row goes terminal |

---

## 2. What the dispatcher injects

Every dispatched unit of work — Nextflow task, chain container job, MNP node — gets
a `PBG_*` block (`viva_api/common/events_env.py`):

| var | what it carries |
|---|---|
| `PBG_EVENT_SINKS` | `stdout` always, plus `s3://…` when a prefix resolves |
| `PBG_TRACEPARENT` | W3C `traceparent`, so spans nest across processes |
| `PBG_TRACE_BAGGAGE` | `key=value,…` — `sim_id`, `experiment_id`, `variant`, `lineage_seed` |
| `PBG_EVENT_TAGS` | opaque to the engine, copied verbatim into every event |
| `PBG_EVENT_FLUSH_S` / `PBG_EVENT_HEARTBEAT_S` | cadence, from settings |

`PBG_` is a reserved prefix at the API boundary, so a caller cannot smuggle these in
through a request.

---

## 3. Reading a run

All three verbs take `--base-url`, which is how you point at a tunnelled deployment.

```console
$ uv run atlantis simulation status <id> --base-url http://localhost:8080
$ uv run atlantis simulation tasks  <id> --base-url http://localhost:8080
$ uv run atlantis simulation events <id> --base-url http://localhost:8080
```

`events` flags: `--level`, `--event`, `--generation`, `--limit`, `--tree`/`--no-tree`,
`--follow`/`--no-follow`.

* **`status`** — the run's own row: stage, generation, `last_event_at`, attempt,
  exit code, error source. `last_event_at` going stale while the status says
  `running` is the signal that a task has stopped talking.
* **`tasks`** — one row per unit of work (Nextflow tasks from `trace.csv`, chain seed
  jobs from Batch). This is where "which of the 40 things failed" is answered.
  **On the MNP path it is legitimately empty**: an MNP run is one Batch job with node
  ranges, not a fan-out of tracked tasks, so there is nothing to list. Verified on sim
  1317 — `/events` returned engine events while `/tasks` returned `[]`. Read an empty
  `tasks` against the run's backend before treating it as a finding.
* **`events --tree`** — the span tree: campaign → parca / lineage(variant, seed) /
  analysis → generation.
* **`events --follow`** — poll until terminal. Useful on a live run; pointless on a
  finished one.

### Typical first move on a failed run

```console
$ atlantis simulation status <id>     # what does the API think, and when did it last hear anything?
$ atlantis simulation tasks  <id>     # which unit failed?
$ atlantis simulation events <id> --level error
```

`error_source` on `status` tells you *which* authority decided the status —
`nextflow_trace`, `k8s_condition`, `failure_record` — which matters because they
disagree, and the trace is the one that knows about a task that died under a head
that exited 0.

---

## 4. What is NOT instrumented

Stated plainly, because looking for events that cannot exist wastes the most time.

**The analysis / gather stage has no interior instrumentation.** The ptools analyses
are `process_bigraph.Step` subclasses by inheritance, but nothing runs them through
`Composite.run` — `analysis_runner` constructs them and calls `.analyze()` directly,
and `composites/workflow_nf.py` deliberately does not wrap analysis (*"`v2ecoli-analyze`
is already atomic and `s3://`-capable; wrapping it would duplicate `run_analyses`' own
fan-out"*). No engine hook fires for them: no `run.start`, no `tick`, no
`process.exception`, no spans.

The **dispatcher layer still covers the stage boundary** — you will see that the
analysis task failed, with its `.command.err` tail — but not where inside it. For the
interior you are reading CloudWatch. Raised as a scope question on sms-ecoli#166
(2026-09-13); not resolved.

**Hand-dispatched Batch jobs are not instrumented at all.** A job submitted with
`aws batch submit-job` and a `CONTAINER_JOB_CMD` override bypasses viva-api, so
nothing injects `PBG_*`, no `HpcRun` row exists, and `/status` and `/events` know
nothing about it. That is the normal shape for ad-hoc probes — expect CloudWatch, not
the API.

---

## 5. Where the events physically are

| sink | location | lifetime |
|---|---|---|
| stdout | CloudWatch. **Chain/container jobs → `smsvpctest-ray-batch-…`; Nextflow tasks → `/aws/batch/job`** | log-group retention |
| S3 | `<prefix>/<trace_id>/<source>.jsonl`, rewritten whole every `EVENTS_FLUSH_SECONDS` | as long as the bucket keeps it |
| Postgres | `hpcrun_event` / `hpcrun_span`, ingested by the scheduler | with the row |

**The two log groups are the single most common way to waste twenty minutes.** A
container job's stream does not exist in `/aws/batch/job`, and `GetLogEvents` reports
`ResourceNotFoundException` — which reads exactly like "the job produced no output".
Get the group from the job definition rather than guessing:

```console
$ aws batch describe-job-definitions --job-definition-name <name> --status ACTIVE \
    --query 'jobDefinitions[0].containerProperties.logConfiguration.options'
```

S3 objects are rewritten whole on a timer, so a task's progress is visible **before**
it exits — the property the pre-events setup lacked entirely. The buffer is capped
(head + rolling tail, with an explicit `sink.truncated` marker carrying the drop
count), because an uncapped whole-object rewrite makes cumulative bytes grow with the
square of the event count.

---

## 6. Cost

* Heartbeat is wall-clock throttled (default 30 s), so a long generation costs a
  handful of events, not one per tick.
* `tick` events are **never** stored in Postgres; they fold into
  `stage`/`generation`/`last_event_at` on the run's row. Measured on sim 1317: the
  container log carried `tick` events (one folding **133 ticks** into a single event,
  ~30 s apart — the wall-clock throttle working), and `/events` returned only
  `run.start`/`run.end`. Ticks missing from the API is the design, not loss.
* Per-invoke detail (`process.invoke`, `process.timing`) is opt-in via
  `PBG_EVENT_DETAIL` and off by default.
* A sink that raises is disabled after one `sink.error`. **Observability never raises
  into the simulation** — the division-seam carry report is wrapped for exactly this
  reason.

---

## 7. Deploying a change to any of this

The schema lives in Alembic and the app bootstraps with `create_all`, so read
`DEPLOY.md` and the "Database migrations" section of `CLAUDE.md` first. Two rules
that have each already cost an incident:

1. **Run the `alembic-migrate` Job before rolling the app.** The reverse order gave
   dev an `UndefinedColumnError` every poll tick (2026-08-26).
2. **A stale migration-overlay pin fails silently** — the Job runs an image that does
   not contain the migration, applies nothing, and exits 0. Keep the
   `<ns>-db-migration` tag equal to the app overlay's.
