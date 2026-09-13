# Observability: turning it on, and reading it

**Written 2026-09-13, against the live `sms-api-stanford-test` deployment at
`0.9.139`** — every command and response below was run, not transcribed from the
source. Companion to [`DEPLOY.md`](DEPLOY.md). The *design* lives in
[`plan-observability.md`](plan-observability.md); this file is how to use it.

---

## 1. The one thing to know first

**Events are off by default, and "off" has two different meanings.**

| what | default | how to turn on |
|---|---|---|
| events to **stdout → CloudWatch** | **already on** for every dispatched run | nothing to do |
| events to **S3**, which is what `/events` and the CLI read | **off** | set `EVENTS_S3_PREFIX` |

`viva_api/common/events_env.py` builds the sink list as `sinks = ["stdout"]` plus an
S3 sink *only* when a prefix resolves. `Settings.events_s3_prefix` defaults to `""`,
and its docstring is explicit: *"No bucket configured means no S3 sink (stdout only)
— never a half-formed URI."*

So on a deployment with no prefix configured — **which is stanford-test today** — the
engine and runner really are emitting events, into the task's CloudWatch log. The
database tables stay empty, and this is what you get:

```console
$ curl -s .../api/v1/simulations/1314/events
{"id":1314,"trace_id":null,"events":[],"tree":null,"next":null}

$ curl -s .../api/v1/simulations/1314/tasks
[]
```

**An empty array is not an error and not a bug.** It means no S3 sink was configured
for that run, so the ingester had nothing to read. Check `EVENTS_S3_PREFIX` before
concluding anything is broken.

### Turning the S3 path on

Set in the namespace's `shared.env` (see `DEPLOY.md` for how config reaches the pod):

```
EVENTS_S3_PREFIX=s3://<work-bucket>/<work-prefix>/{experiment_id}/events/
```

`{experiment_id}` is a template slot filled per run. Related knobs, all with working
defaults (`viva_api/config.py:219-237`):

| setting | default | meaning |
|---|---|---|
| `EVENTS_ENABLED` | `true` | master switch for injecting `PBG_*` at dispatch |
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
  `stage`/`generation`/`last_event_at` on the run's row.
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
