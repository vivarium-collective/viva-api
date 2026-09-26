# viva-api#655 — task provenance and tasks as HpcRuns — slice 2

**Tracked in [`plan-core.md`](plan-core.md) since 2026-09-26 (one coordinated effort).** This slice is **P4b** there — built once, in core shape, on the `JobStore` seam (#776, reshaped per D15): a task is a job with `owner_kind = 'task'`, never `hpcrun.jobref_task_id` / `dataset.task_id`. The status paragraph below is as of 2026-09-15 and is history; the design sections (snapshot mode, inputs, record-at-submit, the importer) still hold and are what P4b implements.

**Status (updated 2026-09-15): plan, not started. This is slice 2 of two.** Slice 1's plan is
**merged to `main`** ([`plan-data-provenance.md`](plan-data-provenance.md), #657) and its
implementation is **draft #661**: the `dataset` registry, the
`artifact.written` ingest hook, `analysis.source`/`tags`, the `/datasets` API, the walk
reconciliation and the bundle importer — delivered **without touching the task endpoints**.
This slice makes a task a first-class traced run (content-addressed script capture, toolkit
snapshots, best-effort inputs, an HpcRun row with `task_id` in its baggage) and thereby
**inherits** the registry: no new dataset code here beyond the `task_id` producer FK.
Sequencing decided by Jim on 2026-09-14. The ptools consumer is
[`plan-ptools-datasets.md`](plan-ptools-datasets.md).

Designed in plan mode from the evidence brief at `assets/ptools/HANDOFF_tools_runs_655.md`
(the brief predates the finding that #631 is already built — see Context). Reviewed once
against the code; the review's findings are folded in. This branch is rebased on `main`
(2026-09-15), which carries slice 1's plan; the code this builds on is #661's, so build it
after #661 lands. Issues: #655 (this), #631 (the task verb this
extends), #648 (artifact registration — closed by slice 1 + this).

## Context

Ad-hoc in-region jobs (the CD2 ptools fills, combines, probes) are hand-dispatched Batch
container jobs whose `CONTAINER_JOB_CMD` stages a toolkit from a **mutable** S3 prefix
(`s3://…/tools/`) at container start. Nothing records which bytes ran; Batch keeps the
invocation ~7 days; four incidents in one evening (handoff §5) came from exactly that gap.
A one-time backfill (`assets/ptools/cd2_tool_runs.json`: 46 jobs, 32 script versions with
bytes, 546 exact version links) already exists and is the input to this build.

**Reframing found during planning (the handoff did not know this):** #631 is already built.
PRs #632/#633/#635 (merged 2026-09-12) shipped `POST /api/v1/tasks`, `/tasks/upload`,
`/tasks/{id}/status`, `/tasks/{id}/logs`, the `task` table (migration `d7e2f4a6c8b0` +
fingerprint), and `atlantis task run|status|logs`. PR #632 explicitly deferred "provenance
polish". Today `_dispatch_task` (`viva_api/simulation/simulation_service_ray.py:3128`)
records name / script label / args / sim_data_refs / memory_class / job_id_ext / out_uri and
**not** the commit, image, job definition, queue, env, command, exit code, timestamps, or any
content hash; `job_name` exists on the row but is never written. Uploaded scripts go to
`tasks/scripts/<name>-<rand>/` (random, so already effectively immutable) but only `python
<one file>` can run, and the fills need a multi-file toolkit, `bash`, and env.

**Decisions (Jim, 2026-09-14):**
1. #655 lives on the existing `task` table and `/api/v1/tasks` verb. No parallel
   `tools/runs` surface. #655 is the provenance slice of #631.
2. v1 = provenance capture **plus** a toolkit-snapshot mode so the fills can move to the verb.
3. Backfill = one-shot import script run by hand per site; the JSON stays in `assets/ptools`.
4. ~~HpcRun linkage deferred~~ **Reversed later the same evening (decision 9): every task gets
   an HpcRun row + `PBG_*` identity in this PR**, because tracing is now the primary artifact
   feed and tasks must be traceable (§2d).
5. **Analysis-first registration** — *moved to slice 1* (`plan-data-provenance.md`); §2c here keeps only the task-specific part: a finished task's output bundles are
   registered as `analysis` rows; the analysis record is the consumer handle and points to
   its source (task, simulation + coordinate, or another analysis) through the same
   reference type `task.inputs` uses. The ptools consumer lists analyses, not simulations.
6. **Dataset records** — *moved to slice 1; table renamed `dataset`* (§2c): the ptools page consumes a list of already-available
   datasets and launches nothing. Each consumable file set is its own `dataset`
   row, produced by exactly one run (simulation / analysis / task, by FK), described by an
   extensible `attributes` map (variant, seed, generation, protocol, n_tp, display name,
   …) plus `tags`, and fetched by its own id. The page is patched to call `GET /datasets`
   and `GET /datasets/{id}/content` instead of walking simulation → analyses → data.
8. **Tracing-first artifact registry** — *moved to slice 1* (`plan-data-provenance.md` §5): producers record every file they
   write as an `artifact.written` event inside the current span — composites (parquet),
   analysis runs (ptools TSVs, figures, other tables) and tasks alike — and viva-api's event
   ingester turns those into `dataset` rows. The S3 walk remains for the one-time
   backfill and as a periodic reconciliation (sets `available`, catches lost events).
9. **HpcRun row per task, in this PR** (§2d): `JobTypeDB.TASK`, `hpcrun.jobref_task_id`,
   `PBG_*` injected by `_dispatch_task`. Tasks then appear in the observability views too.
10. **Sequencing:** viva-api lands first — dataset table, the ingest hook keyed on the
   documented `artifact.written` contract (tested with synthetic events), and the walk
   backfill so datasets exist immediately. The emit-side PRs (process-bigraph helper,
   v2ecoli call sites, sms-ecoli pin, simulator rebuild) follow; post the contract on #655
   for Eran/Alex before they start.
11. **Sequencing (Jim, later that evening): data provenance first, without the task
   endpoints; then this slice.** Every producer except tasks is already a traced run, so
   slice 1 lights up for composites and analysis jobs on its own; this slice only has to
   make a task a traced run and it inherits registration through baggage `task_id`.
   Two records, two birth times (`plan-data-provenance.md` §2a): the run row at submit,
   the dataset row only when the trace is scraped or the walk finds the object.
7. *(slice 1)* The revision creates `analysis` if absent before adding columns to it. **Correction:**
   this does not fix #637 — an empty DB fails earlier, at `d3f9a1c72b84`'s unguarded
   `add_column("analysis", …)`. #637's real fix (guard that older revision) stays separate.

**Non-goal (state it in the PR):** this is provenance and reproducibility, not authorization.
Arbitrary script recorded in a DB is still arbitrary script; `/tasks/upload` already accepts
arbitrary uploads, so no allowlist is added or removed here. Pre-execution review belongs to
moving the toolkit into a repo (follow-up).

---

## Design

### 1. Schema — two Alembic revisions off slice 1's head

Revision A (`<idA>_add_task_jobtype`): `ALTER TYPE jobtypedb ADD VALUE IF NOT EXISTS 'TASK'`
in an autocommit block (§2d), `down_revision = c9a1e3f5b7d2` (slice 1's `dataset` revision in
#661; `b2f6d8e0a4c7` before it adds `ANALYSIS` to `jobtypedb` the same way, and is the pattern
to copy).
Revision B (`<idB>_add_task_provenance`, `down_revision = <idA>`): everything below. One
fingerprint marker each.

**New column on `hpcrun`:** `jobref_task_id` INTEGER FK → `task.id`, nullable, indexed (§2d).
**New column on `dataset`:** `task_id` INTEGER FK → `task.id`, nullable, indexed — the
fourth producer (the CHECK constraint from slice 1 is widened to include it).
**New column on `analysis`:** `task_id` INTEGER FK → `task.id`, nullable, indexed — the task
that produced an analysis run's bundle (NULL for K8s/SLURM-dispatched analyses).

**New table `task_script`** — content-addressed; identity is the hash.

| column | type | note |
|---|---|---|
| `sha256` | TEXT PK | hex digest of the exact bytes |
| `name` | TEXT NOT NULL | basename as staged (`fanout_multi.sh`) |
| `size_bytes` | INTEGER NOT NULL | |
| `content` | TEXT NULL | exact UTF-8 text; NULL when binary or > `TASK_SCRIPT_EMBED_MAX_BYTES` (1 MiB) |
| `content_is_binary` | BOOLEAN NOT NULL default false | |
| `source_uri` | TEXT NULL | `s3://bucket/key` it was read from (upload → NULL) |
| `source_version_id` | TEXT NULL | S3 `VersionId` at read time (bucket is versioned) |
| `first_seen_at` | TIMESTAMPTZ default now() | |

**New columns on `task`** (all nullable so existing rows and the repo-path mode stay valid):

| column | type | what |
|---|---|---|
| `script_sha256` | TEXT FK → `task_script.sha256`, indexed | the **entry** script's bytes |
| `staged_files` | JSONB | `[{path, sha256, size_bytes, source_uri, source_version_id}]` — every file in the snapshot (toolkit mode); one entry for upload mode; `[]` for repo-path mode |
| `snapshot_uri` | TEXT | the immutable prefix the container synced (`CONTAINER_STAGE_S3`) |
| `interpreter` | TEXT | `python` \| `bash` |
| `command` | TEXT | the exact `CONTAINER_JOB_CMD` string submitted |
| `env` | JSONB | caller env as passed to Batch (validated, see §5) |
| `image` | TEXT | full ECR image URI |
| `commit` | TEXT | resolved simulator commit |
| `job_definition` | TEXT | `name:revision` actually submitted |
| `job_queue` | TEXT | queue actually used (after memory-class routing) |
| `exit_code` | INTEGER | from Batch on poll |
| `status_reason` | TEXT | Batch `statusReason` |
| `started_at`, `finished_at` | DateTime (naive UTC, matching the table's existing `created_at`) | from Batch `startedAt`/`stoppedAt` |
| `dedupe_key` | TEXT, indexed | `sha256(manifest_sha \| command \| canonical env)` — one indexed equality for the duplicate guard (JSONB `=` works but is unindexed) |
| `inputs` | JSONB | caller-declared upstream references, best-effort (see §2b) |
| `dest_prefixes` | JSONB | caller-declared S3 prefixes the run writes (feeds #648; not verified) |
| `source` | TEXT default `'api'` | `api` \| `backfill` |

Also start writing the existing `job_name` column from `_dispatch_task` (bug: never set —
the string is built inline at the `_submit_container` call; capture it first). `script`
(NOT NULL today) holds the entry's relative path in toolkit mode.

Indexes: `task.status` (the scheduler tick is its only reader), `task.dedupe_key`,
`task.script_sha256`, and `task_script(source_uri, source_version_id)` (the re-hash skip in §2).

Revision B, `alembic/versions/<idB>_add_task_provenance.py`, copying the guards in
`d7e2f4a6c8b0_add_task_table.py`: `inspector.has_table("task_script")` before `create_table`;
per-column `if col not in existing_cols` before `add_column`; every index guarded by name.
No new enum. Column types must match the ORM exactly (naive `sa.DateTime()` for the two
timestamps) or the types-parity test fails. Downgrade drops in reverse.

**Fingerprint contract** (`viva_api/simulation/db_reconcile.py`): two markers, in chain
order after slice 1's — `("<idA>", "jobtypedb has value 'TASK'")` with
`_enum_has_value(conn, "jobtypedb", "TASK")`, then `("<idB>", "table 'task_script' exists")`
with `_table_exists(conn, "task_script")` — appended to `LEGACY_FINGERPRINTS` and
`_LEGACY_PREDICATES` (positionally aligned). `tests/simulation/test_db_reconcile.py`: `HEAD`
→ `<idB>`; `REVS` gains both ids; every vector 17 → 19. Both single-head tests then cover
the new head for free.

**Types-parity test: write a separate one for this revision.** Do not add `task`/`task_script`
to `_OWNED_TABLES` in `test_observability_migration.py` — that harness drops the owned
tables and stamps at `d7e2f4a6c8b0`, so `task` would be dropped and never recreated. New
`tests/simulation/test_task_provenance_migration.py`: real `upgrade head` from empty;
create_all vs migration `(data_type, is_nullable, column_default)` snapshot for `task_script`
and the new `task` columns; a create_all DB matches the new marker and stamps at head.

### 2. Toolkit snapshot at submit time (the structural answer to incident §5a)

New request fields on `TaskRunRequest` (`viva_api/simulation/models.py:809`), mutually
exclusive script sources validated with a model validator:

```
script:       str | None   # repo path in the image (existing)
script_uri:   str | None   # s3://bucket/key — one file, snapshotted
toolkit_uri:  str | None   # s3://bucket/prefix/ — every object under it, snapshotted
entry:        str | None   # required with toolkit_uri: relative path of the file to run
interpreter:  Literal["python", "bash"] = "python"
env:          dict[str, str] | None     # → real Batch env entries via task_env
inputs:       list[ProvenanceRef] | None # best-effort upstream refs (§2b)
dest_prefixes: list[str] | None
dry_run:      bool = False
allow_duplicate: bool = False
```

`POST /tasks/upload` keeps its single-file multipart shape and gains `interpreter`, `env`
(JSON form field), `dest_prefixes`; the uploaded bytes go through the same snapshot writer.

**Snapshot writer** — new module `viva_api/simulation/task_snapshot.py` (sync boto3, same
client style as `_upload_task_script`):

1. Bucket must equal `settings.s3_work_bucket` (the bucket `RayLayout.results_uri` writes
   to — equal to `storage_s3_bucket` on both Stanford sites, but check the one the copy
   targets). Anything else → 400.
2. `list_objects_v2` under the prefix (skip zero-byte "directory" keys; cap: 200 files /
   512 MiB total → 400 beyond that; the harvested runs staged at most 15 files / 182 MB).
   Reject a toolkit containing `cache_version.json` (400): the entrypoint's `stage_inputs`
   would run the ParCa cache verifier on it and fail the job as a "stale cache".
3. For each key: `head_object` first; if a `task_script` row already exists for
   `(source_uri, source_version_id)` reuse its sha (the bucket is versioned, so the pair is
   immutable — this is what keeps a fill resubmission from re-downloading ~180 MB of
   pickles). Otherwise `get_object` (streamed), sha256 the bytes, take `VersionId` from the
   response. Text/binary decided by UTF-8 decode + size threshold.
4. `manifest_sha = sha256(sorted "path\0sha256\n" lines)`.
5. Write to `RayLayout.results_uri("tasks/snapshots/<manifest_sha>")/<relpath>` with
   `copy_object(CopySource={"Bucket","Key","VersionId"})` for files already in the bucket
   (server-side, pinned to the version whose bytes were hashed; precedent for the dict form
   at `simulation_service_ray.py:3698`) and `put_object` for uploads. The bucket has default
   SSE-S3, so no explicit `ServerSideEncryption` is needed. Skip the write when
   `head_object` on the destination already exists (same content → same key → idempotent,
   and nothing ever rewrites a snapshot prefix).
6. Upsert `task_script` rows by sha (insert if absent). Return the manifest.

The whole writer runs under `asyncio.to_thread` (precedent: `simulation_service_ray.py:4970`)
— it is sync boto3 doing up to hundreds of MB of I/O and must not block the event loop.

The job is then submitted with `stage_s3 = snapshot prefix`, `stage_dir = TASK_STAGE_DIR`
(existing entrypoint `aws s3 sync` path, which preserves sub-paths and already fails hard on
an empty prefix), and `job_cmd = f"cd {TASK_STAGE_DIR} && {interpreter} {shlex.quote(entry)}
{' '.join(shlex.quote(a) for a in args)}"` (mirror `_task_job_cmd`). The command never
carries script text (Batch 8192-char cap, handoff §9). A restage of `tools/` after submit
cannot reach the job: the job syncs the content-addressed prefix, not `tools/`.

**What the CD2 fills must change to run through this** (say so in the PR and on #655):
scripts must be CWD-relative (one of the 32 harvested versions hardcodes `/app/tk`); the
shared `simData.cPickle` must arrive via the toolkit prefix or `sim_data_refs`, not an ad-hoc
`aws s3 cp` in the command; and `AWS_DEFAULT_REGION` must not be passed in `env` (reserved
prefix; the job environment already supplies it — the entrypoint's own `aws s3 sync` works
without it).

`dry_run=True` runs steps 1–4 only (no writes, no submit) and returns the resolved manifest
and command as a `TaskDTO` with `database_id = -1`, `status = None`.

**Duplicate guard** (Jim's #631 comment, item 3): compute `dedupe_key` (§1); before submit,
look for a `task` row with the same key whose status is `COMPUTING`; if found and
`allow_duplicate` is false → 409 with the existing task id in `detail`.

### 2b. Inputs: declared, best-effort upstream references (Jim, 2026-09-14)

A task that post-processes a simulation's output should be able to say so. These are
**partially managed** edges: recorded as declared, resolved where possible at submit time,
never enforced, never a reason to refuse a submit, and no execution dependency (Batch
`dependsOn` chaining is orchestration and stays out of scope).

`TaskRunRequest.inputs: list[ProvenanceRef] | None` with
`ProvenanceRef = {kind: "simulation"|"task"|"analysis"|"s3", ref: str, coordinate?: {variant, seed, generation, agent, scale}}`
— one type, shared by `task.inputs` and `analysis.source` (§2c), so the graph is navigable
in both directions with one query shape:

- `simulation`: `ref` is a database id or an `experiment_id` (unique on `simulation`);
  resolved to `simulation.id` + its `config.emitter_arg.out_uri` when found.
- `task` / `analysis`: `ref` is a database id; resolved to the row's `out_uri`/`result_uri`.
- `s3`: `ref` is a URI (the fallback for sub-stores and bare objects like a shared
  `simData.cPickle`, which map to no row).

Stored on `task.inputs` as `[{kind, ref, resolved_id, uri, resolved_at, verified_at}]`.
`resolved_id`/`uri` are NULL when the reference does not resolve — recorded, not rejected —
and the response DTO carries a `warnings: list[str]` naming each unresolved input.
`verified_at` is set by one cheap `list_objects_v2(MaxKeys=1)` probe on the resolved/given
URI when the bucket is `s3_work_bucket`; skipped (NULL) otherwise. Only a *malformed*
reference (unknown `kind`, non-integer id, non-`s3://` URI) is a 400.

No FK constraints, no cascade, no existence guarantee: the upstream may be a hand-dispatched
job with no row, or data that lifecycle has expired. The edge outlives the data, as a record
of intent plus what resolved at submit time.

CLI: `--input sim:200 --input sim:sim200-cd2-run3-sweep-combo00-25fc --input task:12
--input analysis:7 --input s3://…` (repeatable). `atlantis task show` lists them with the
resolution result. `GET /tasks?input=sim:200` filters on the JSONB (containment).

Backfill: the importer populates `inputs` best-effort from each harvested command — every
`s3://` URI in the string becomes an `s3` input; store names that match a
`simulation.experiment_id` at import time become `simulation` inputs. `verified_at` NULL.

### 2c. What a task registers (the task-specific part of slice 1's model)

The dataset registry, `ProvenanceRef`, the `artifact.written` contract, the two feeders and
the `/datasets` API are all **slice 1** (`plan-data-provenance.md` §2–§7). This slice adds:

- **`task_id` in the baggage** (§2d), so slice 1's producer resolution attributes a task's
  `artifact.written` events to the task with no new ingest code; `dataset.task_id` and
  `analysis.task_id` are the FKs it lands on.
- **The task itself is the run record — no `analysis` rows.** *(SUPERSEDED 2026-09-15: this
  bullet used to upsert an `analysis` row per `dest_prefix`.)* Jim decided slice 1 fabricates
  no analysis rows, and `plan-data-provenance.md` §4c settles where a fill's files belong: a
  task's `task` row plus its HpcRun **is** the run record, and its bundles carry
  `dataset.task_id`. So in the `update_tasks` tick (§3), when a task reaches a terminal state,
  each `dest_prefix` shaped `<store>/analyses/<name>/` is registered through slice 1's walk
  (`register_prefix` over that prefix) with producer `task_id`, `tags` = the task's tags ∪ the
  source simulation's, and `source` = the task's first `simulation` input (with coordinate)
  else `{kind: task, ref: <task id>}`. An empty bundle is recorded on the **task**
  (`status_reason = "bundle empty"` when no object landed — **effect, not exit 0**), not on a
  synthetic analysis. `analysis.task_id` stays for the other direction: an analysis run that a
  task dispatched. Dataset rows still come from the trace, or from the walk over that prefix.
- **The sidecar for bash toolkits.** `ptools_flush.py` (Python) calls the v2ecoli helper;
  the fanout shell scripts append the same JSON lines to `$CONTAINER_OUT_DIR/artifacts.jsonl`
  (schema in slice 1 §3). This slice adds the ingester's read of that file under a task's
  out prefix (`_events_key_prefix` task branch, §2d) — the only task-specific ingest code.
- **`TaskRunRequest.tags: list[str]`** seeds the tags of everything a task registers.
- **Backfill, second pass** (§6): the 46 harvested jobs become `task` rows; idempotent on
  `uri`, the importer back-fills `dataset.task_id` / `analysis.task_id` on the bundles those
  jobs produced (matched through `cd2_ptools_manifest.json` `fill_jobs`).

**The ptools consumer** is `plan-ptools-datasets.md`; the compatibility path for the
unpatched page (`/analyses/{id}/data` coordinate filters + filename aliasing) is slice 1 §7.

### 2d. Tasks become HpcRuns (decision 9)

Events and spans attach to an `hpcrun` row (`hpcrun_event.hpcrun_id` is NOT NULL), and
`to_hpc_run` requires one job reference. So:

- **Enum:** `JobTypeDB.TASK = "task"` (+ `JobType.TASK` in `viva_api/common/models.py`).
  Its own revision, before the tables revision, using the
  `a1c3e5f7b9d2_add_cancelled_to_jobstatusdb.py` pattern (`ALTER TYPE jobtypedb ADD VALUE
  IF NOT EXISTS 'TASK'` inside `op.get_context().autocommit_block()` — a new enum label
  cannot be used in the transaction that adds it). Label = member NAME, as create_all does.
- **Column:** `hpcrun.jobref_task_id` FK → `task.id`, nullable, indexed (guarded
  `add_column`, in the tables revision). `to_hpc_run` and `insert_hpcrun`'s `job_type →
  jobref` routing learn the fourth reference.
- **Dispatch:** `_dispatch_task` inserts the HpcRun (`job_type = TASK`, `job_backend =
  "ray"`, `job_id_ext` = the Batch id, `correlation_id = task-<id>`) and injects the
  `PBG_*` block from `events_env(...)` seeded from that correlation id, exactly as the
  standalone analysis path does (`simulation_service_k8s.py:412-425`); `hpcrun.trace_id`
  and `hpcrun.events_s3_prefix` (= `<task out_uri>/events/`) are set on the row so
  `ingest_run_events` needs no simulation to find the prefix (`_events_key_prefix` gets a
  task branch). `PBG_` stays reserved in `validate_task_env`, so a caller cannot shadow it.
- **Scheduler:** `list_hpcruns_for_event_ingest` already returns any row with a trace; the
  other ticks (`update_multi_node_jobs`, `update_chain_campaigns`, `reconcile_local_tasks`)
  must **filter `job_type != TASK`** so a task row is never polled as a simulation. The
  `update_tasks` tick (§3) owns TASK rows and also finishes the HpcRun (`status`,
  `end_time`, `exit_code`, `error_source`).
- **Fingerprints:** two revisions → two markers (`jobtypedb has value 'TASK'` via
  `_enum_has_value`; `table 'task_script' exists`), vectors 16 → 18.
- **Read side:** `TaskDTO.hpcrun_id` + `trace_id`; `GET /tasks/{id}/events` and `/spans`
  reuse the simulation handlers keyed on the HpcRun (same DTOs, same tree option), so a
  task's spans and its `artifact.written` events are visible without new plumbing.

### 3. Record at submit, complete on poll

`_dispatch_task` records the full tuple: `job_name`, `image`, `commit`, `job_definition`,
`job_queue`, `command`, `env`, `interpreter`, `script_sha256`, `staged_files`,
`snapshot_uri`, `inputs`, `dest_prefixes`, `dedupe_key`, `source="api"`.

`job_queue`: do **not** change `_submit_container`'s return type (10 call sites). Extract its
memory-class routing block (`simulation_service_ray.py:1143-1153`) into
`_container_queue(memory_class, job_name) -> str` and call it from both places. While there,
fix the log line at `:1195`, which prints the configured queue instead of the one used.

`env`: `validate_task_env(request.env)` plus the secret-name check (§5); add
`TASK_SIM_DATA_REFS` to `TASK_ENV_RESERVED_NAMES` so a caller cannot shadow the value
`_dispatch_task` synthesises.

`get_task_status` (`simulation_service_ray.py:3170`) switches from `get_batch_job_statuses`
to the existing `get_batch_job_details` (`:4833`) plus `startedAt`/`stoppedAt` from the same
`describe_jobs` payload (extend `BatchJobDetail` with those two fields) and persists
`exit_code`, `status_reason`, `started_at`, `finished_at` via a widened
`update_task_status(...)`.

**Background completion** — small `JobScheduler.update_tasks()` tick: every poll, list
`task` rows with `status = COMPUTING` and a non-null `job_id_ext` (any `source` — the
backfill's 11 mid-flight rows must settle too), call the same detail fetch, and persist.
Without this a task nobody polls stays `COMPUTING` and its exit code is lost when Batch
forgets it, which defeats the acceptance criterion. Needs `DatabaseService.list_tasks
(status=...)`. Guard: `if self.simulation_service_ray is None: return` (the
`update_multi_node_jobs` pattern, `job_scheduler.py:897`). **Unknown-to-Batch branch:**
`get_batch_job_details` omits ids Batch no longer knows, so a row whose id is absent from the
response and whose `created_at` is older than 7 days is marked `FAILED` with
`status_reason = "job no longer known to Batch"` — otherwise it is polled forever. Note: the
`task` table has no owner column, so a second api replica would also poll these rows; that
is a duplicate `describe_jobs`, not a correctness problem.

### 4. Read endpoints + CLI

| route | operation_id | returns |
|---|---|---|
| `GET /api/v1/tasks` | `list-tasks` | `list[TaskDTO]`, filters `name`, `status`, `script_sha256`, `source`; `limit` ≤ 200, `offset` |
| `GET /api/v1/tasks/{id}` | `get-task` | full `TaskDTO` (no Batch call; what the DB holds) |
| `GET /api/v1/tasks/{id}/script` | `get-task-script` | the entry script bytes, `text/plain; charset=utf-8` (or `application/octet-stream` when binary; 404 when content not embedded — the DTO carries `source_uri`+`source_version_id` so it is still fetchable) |
| `GET /api/v1/tasks/scripts/{sha256}` | `get-task-script-by-sha` | any recorded script by hash (for diffing two runs) |

`TaskDTO` grows every new column plus `manifest_sha` and `warnings`. `TaskLogsDTO` unchanged.
Route ordering is not an issue (`/tasks/scripts/{sha256}` has a literal middle segment and
cannot collide with `/tasks/{task_id}/…`).

Error mapping on all three submit paths: `DispatchValidationError` → 400 (the tasks router
today catches only `HTTPException`, then `Exception` → 500; copy the clause from
`routers/sms.py:364-369`), snapshot/bucket/cap errors → 400, duplicate → 409.

`app/cli.py` (raw httpx via `app/app_data_service.py`, the existing convention):

- `atlantis task run` gains `--script-uri`, `--toolkit s3://…/tools/ --entry fanout_multi.sh`,
  `--interpreter bash`, `--env K=V` (repeatable; reuse `_parse_task_env`), `--dest s3://…`
  (repeatable), `--dry-run`, `--allow-duplicate`. The XOR check extends to the four sources.
- `atlantis task list [--name --status --limit]`, `atlantis task show <id>` (full provenance,
  manifest as a table), `atlantis task script <id> [--sha <sha256>] [--out FILE]`.
- TUI/GUI parity for the **task** verb: **not in this PR** (the #631 slices shipped CLI only);
  listed under follow-ups so the EUTE rule is not silently dropped. Datasets already have it:
  #661 (`26d0f5b7`) gave the TUI a Datasets domain and the GUI a Datasets & Analyses panel,
  with the shared wording in `app/dataset_views.py`, so a task's bundles show up there as soon
  as `task_id` lands.

Regenerate spec + client (`make spec` with `SIMULATION_OUTDIR`/`HPC_SIM_BASE_PATH`
overridden — it leaks `.dev_env`; grep the diff — then `make api_client`).

### 5. Env: validate, never store a secret

Reuse `validate_task_env` (`viva_api/common/dispatch_validation.py:101`) on `request.env` —
it already rejects `AWS_`/`CONTAINER_`/`PBG_`/… names, non-string values, and values with
whitespace/quotes/`$`. Add next to it `reject_secret_env_names(env)`: any name matching
`(?i)(secret|token|passw|credential|api_?key|private_?key)` → 400. Fail closed rather than
redact: in-region runs use the instance role and need no credentials, so a secret in `env` is
a mistake, not a feature. The same check runs in the backfill importer (redacting to
`"<redacted>"` there, since those rows are historical).

`env` rides to Batch through the existing `task_env` passthrough
(`task_env_as_batch_list`), so it is visible in `describe-jobs` as real env entries — the
thing the sms-ecoli#166 dispute could not see.

### 6. Backfill importer — `scripts/import_cd2_tool_runs.py`

Reads `assets/ptools/cd2_tool_runs.json`, writes through `DatabaseServiceSQL` (URL from
`SQLALCHEMY_DATABASE_URL` / `POSTGRES_*`, same as `db_reconcile`). `--analyze` prints what
would be written; `--apply` writes. Idempotent on `task.job_id_ext`.

Mapping: `scripts[sha]` → `task_script` (content, name, size, `source_uri =
s3://<bucket>/<s3_key>`, `source_version_id`); `staged_binaries[sha]` → `task_script` with
`content = NULL`, `content_is_binary = true`. `jobs[i]` → `task` with `name = job_name`,
`script` = the entry parsed from the command — a `(bash|python|\$PY)\s+(\S+\.(sh|py))` match
(suffix required: a bare `python` token false-matches `export PY=/…/python`), falling back to
`"<inline>"` rather than failing the insert (12 of 46 commands have no such token),
`script_sha256` = the staged file whose basename matches that entry (NULL if none),
`staged_files` from `staged_tools` (path = key relative to `tools/`; all 546 links are
`exact`, so no ambiguity flag is needed), `command = container_job_cmd`, `env` = the
`export K=V` pairs parsed from the command (secret-name redaction applied), `image`,
`commit = image_commit` (1 job lacks it → NULL), `job_id_ext`, `job_name`, `created_at`
from Batch `createdAt` (widen `record_task` to accept it, so backfilled rows sort by real
submit time), `status` via `JobStatus.from_batch_state` → `TaskStatusDB.from_job_status`,
`exit_code`, `status_reason`, `started_at`/`finished_at` from epoch ms, `job_queue =
"smsvpctest-ray-standalone"`, `job_definition` from the log stream's job-def name when
present (1 job lacks a stream → NULL), `source = "backfill"`, `snapshot_uri = NULL` (they
staged the mutable prefix — the row says so honestly), `inputs` per §2b, `dest_prefixes`
= the bundle URIs `cd2_ptools_manifest.json` attributes to that job (`fill_jobs[].job_name`
↔ `stores[].fill_jobs`). Then the importer runs the §2c registration for every
`stores[].fill_bundles` entry: the bundle's `dataset` rows — registered already by slice 1's
`scripts/import_cd2_datasets.py` or by its walk — get `task_id` stamped on them when the
producing job was harvested, with `source = {kind: simulation, ref: <store>}` resolved to the
simulation row. **No `analysis` rows are created** *(SUPERSEDED 2026-09-15; see §2c)*; a write
naming a producer replaces the row's producer, which is exactly how slice 1 intends these
bundles to move from the simulation to their task. Idempotent on `uri`.

**Mid-flight rows (blocker found in review):** 11 jobs were harvested `RUNNING` and 12 have
no exit code. On `--apply` the importer calls `describe_jobs` for every non-terminal id
that Batch still knows (they are inside the 7-day window today, not for long) and imports
the current state; anything still running lands as `COMPUTING` and the scheduler tick (§3)
settles it, since the tick no longer filters on `source`.

Run once per site by hand after the migration Job (`kubectl exec` into the api pod or a
port-forward). Not run by CI, not run by the app.

### 7. Files to change

| file | change |
|---|---|
| `alembic/versions/<idA>_add_task_jobtype.py` | **new** — `jobtypedb` ADD VALUE `TASK` (autocommit block; pattern `a1c3e5f7b9d2`) |
| `alembic/versions/<idB>_add_task_provenance.py` | **new** — `task_script`, `task` columns, `hpcrun.jobref_task_id`, `dataset.task_id`, `analysis.task_id`, all guarded |
| `viva_api/common/models.py` (`JobType`) + `tables_orm.py` (`JobTypeDB`, `ORMHpcRun.jobref_task_id`, `to_hpc_run`) | `TASK` job type and the fourth job reference |
| `viva_api/simulation/database_service.py` (`insert_hpcrun`, `get_hpcrun_by_ref`) | route `job_type = TASK` → `jobref_task_id` |
| `viva_api/simulation/event_ingest.py` | `_events_key_prefix` task branch (uses `hpcrun.events_s3_prefix`); sidecar `artifacts.jsonl` read under a task's out prefix — the ingest hook itself is slice 1 |
| `viva_api/simulation/job_scheduler.py` | `update_tasks` also finishes the task's HpcRun; `reconcile_datasets` hourly tick; `job_type != TASK` filters on the simulation-only ticks |
| `docs/OBSERVABILITY.md` | the `artifact.written` contract (payload schema above), the sidecar fallback, producer resolution from baggage |
| `tests/simulation/test_task_jobtype_migration.py` | **new** — revision A adds the label; a create_all DB matches its marker; `insert_hpcrun(job_type=TASK)` round-trips; single head |
| `tests/simulation/test_task_hpcrun.py` | **new** — `_dispatch_task` inserts the HpcRun and injects `PBG_*` (trace seeded from the correlation id; `events_s3_prefix` under the task out_uri); simulation-only ticks skip TASK rows |
| `viva_api/simulation/tables_orm.py` | `ORMTaskScript`; new `ORMTask` columns; `ORMDataset.task_id`, `ORMAnalysis.task_id`; `to_dto` |
| `viva_api/simulation/db_reconcile.py` | marker + fingerprint + predicate |
| `viva_api/simulation/models.py` | `TaskRunRequest` sources/env/inputs/dest/dry_run/allow_duplicate + validator; `TaskDTO` growth + `warnings`; `TaskStagedFile`, `ProvenanceRef` |
| `viva_api/simulation/task_inputs.py` | **new** — `ProvenanceRef` parse/resolve/probe (DB lookups via the existing `DatabaseService.get_simulation`, `get_simulation_by_experiment_id`, `get_task`, `get_analysis`) |
| `viva_api/simulation/models.py` (`TaskRunRequest`) | `tags: list[str]` (validated like `simulation` tags) |
| `viva_api/simulation/task_snapshot.py` | **new** — list/hash/copy/manifest; `sha256_bytes` helper |
| `viva_api/simulation/simulation_service_ray.py` | `_dispatch_task` full record (incl. `job_name`); `_container_queue` extracted + log fix at `:1195`; `submit_task` routes by source; `get_task_status` via details; `BatchJobDetail` +started/stopped; snapshot writer under `to_thread` |
| `viva_api/simulation/database_service.py` | `record_task` widened (+`created_at`); `update_task_status` widened; `list_tasks`; `upsert_task_script`; `get_task_script`; `get_task_script_by_source_version`; `find_inflight_duplicate(dedupe_key)` |
| `viva_api/simulation/job_scheduler.py` | `update_tasks` tick + unknown-to-Batch branch |
| `viva_api/common/dispatch_validation.py` | `reject_secret_env_names`; `TASK_SIM_DATA_REFS` reserved |
| `viva_api/api/routers/tasks.py` | four new GETs; upload form fields; `DispatchValidationError`→400, 409 mapping |
| `app/app_data_service.py`, `app/cli.py` | new options + `list`/`show`/`script` verbs |
| `scripts/import_cd2_tool_runs.py` | **new** — backfill importer |
| `viva_api/api/spec/…`, `viva_api/api/client/…` | regenerated |
| `tests/simulation/test_task_run.py` | snapshot mode: manifest, copy pinned to VersionId, command shape, env passthrough, dry-run makes no writes, duplicate → 409, full record fields |
| `tests/simulation/test_task_snapshot.py` | **new** — hashing, manifest determinism, bucket mismatch, caps, idempotent write |
| `tests/simulation/test_task_result_db.py` | new columns round-trip, `list_tasks`, script upsert (Docker-gated) |
| `tests/simulation/test_db_reconcile.py` | `HEAD` → `<idB>`, `REVS` (+`e3a9c1d70b62`, `<idA>`, `<idB>`), vectors 16 → 18 |
| `tests/simulation/test_task_provenance_migration.py` | **new** — real `upgrade head` through the new revision; create_all vs migration types parity for `task_script` + new `task` columns; create_all DB stamps at head (do NOT widen `test_observability_migration.py`'s `_OWNED_TABLES`) |
| `tests/simulation/test_job_scheduler_tasks.py` | **new** — `update_tasks` persists exit code/timestamps; unknown-to-Batch → FAILED after 7 days; no Ray service → no-op |
| `tests/api/test_tasks_router.py` | **new** — GETs via `ASGITransport`, DI getters patched (pattern: `tests/api/test_dispatch_validation_status.py`) |
| `tests/scripts/test_import_cd2_tool_runs.py` | **new** — mapping on a 2-job fixture slice; redaction; idempotence |
| `viva_api/version.py`, `pyproject.toml` | bump to the next free version (0.9.143 is #660, 0.9.144 is #663, and #661 takes one before this) — a migration ships, so the db-migration overlay tag must move with the app tag at deploy time |

Reuse, do not rewrite: `_ensure_container_job_def`, `_submit_container`, `_stage_out_env`,
`task_env_as_batch_list`, `validate_task_env`, `get_batch_job_details`, `_safe_task_name`,
`RayLayout.results_uri`, the `_container_settings` / `_fake_container_batch` doubles in
`tests/simulation/test_ray_backend.py`.

### 8. Follow-up issues to file (not in this PR)

1. **Emit side of the artifact contract** (decision 10), in order: process-bigraph
   `emitter.artifact(uri, kind, **attrs)` helper (engine stays domain-free: the helper
   takes opaque attributes) → v2ecoli: analysis runner emits per TSV/HTML inside
   `analysis.group` (with `error` on failure), the parquet emitter emits per partition
   prefix at finalize, `ptools_flush.py` calls the helper → sms-ecoli pin → simulator
   rebuild. Until then datasets come from the walk.
2. Move the CD2 toolkit into `sms-ecoli/scripts/` and reconcile the 155-line
   `combine_run4_fss.py` drift; publish to S3 by CI. (Handoff §10/§12.3.)
3. Route `POST /simulations/{id}/analysis` (Ray branch) through the task verb so a
   webapp-submitted ptools analysis is both a task and an analysis; closes #648 (a)–(c).
   With §2c landed, #648's remaining half is this plus filters on the #650 pair.
4. TUI/GUI parity for the `task` and new `analysis` verbs (EUTE rule).
5. `attemptDurationSeconds` on the container path (wedged jobs run forever — handoff §4).
7. #637: guard `d3f9a1c72b84`'s `add_column("analysis", …)` (create the table as of that
   revision when absent) so `upgrade head` works on an empty DB. Small, separate PR.
8. `GET /simulations/{id}/datasets`: cached coordinate summary per simulation.
9. ~~Span-derived feeder~~ — superseded: the ingest hook is in this PR (§2c Feeder 1);
   only the emit side (item 1) remains.
6. Close-out on GitHub: comment on #655 and #631 that #655 is the provenance slice of #631
   and link the PR; leave closing to Jim.

---

## Verification

1. **Unit, offline** — `uv run pytest tests/simulation/test_task_run.py
   tests/simulation/test_task_snapshot.py tests/api/test_tasks_router.py
   tests/scripts/test_import_cd2_tool_runs.py -v`. All boto3 via the existing doubles; no
   sockets. Confirm the tunnel is **down** first (`curl -s -m5 localhost:8080/version` must
   fail) — `tests/api/app/test_cli_e2e.py` runs against a live server when it is up.
2. **Migration, Docker** — `uv run pytest tests/simulation/test_db_reconcile.py
   tests/simulation/test_task_provenance_migration.py
   tests/simulation/test_observability_migration.py -v`: single head, real `upgrade head`
   from empty through the new revision, ORM ↔ migration agree on names **and** types,
   a create_all DB matches the new marker and stamps at head.
3. **Acceptance test for §5a** (the one the issue names): submit in toolkit mode with a
   fake S3 double; then "restage" (change the double's `tools/` content); assert the
   submitted `CONTAINER_STAGE_S3` is the manifest-sha prefix and its objects are unchanged.
4. **Backfill** — `uv run python scripts/import_cd2_tool_runs.py --analyze` against a
   testcontainers Postgres in the test; then by hand on dev after deploy: expect 46 task
   rows / 34 script rows, `GET /tasks?source=backfill` returns 46, `GET
   /tasks/{id}/script` for `cd2fill-run3-core5-s0-3` byte-equals the `fbVNUalsek5k…`
   version of `fanout_multi.sh`.
5. `make check` **twice** (first pass reformats and exits 1), then `uv run pytest`
   (full, minus `tests/api/app/test_cli_e2e.py`).
5b. **Task trace, on dev (after deploy):** `atlantis task run … --dry-run` then a real
   `fanout_probe.sh` run; `GET /tasks/{id}` shows `hpcrun_id` + `trace_id`;
   `GET /tasks/{id}/spans` shows the task root span; `artifact.written` rows appear only
   once the emit-side PRs ship — until then slice 1's walk-registered rows carry `origin = walk`, and this slice's importer has stamped `task_id` on them (`atlantis dataset list --task <id>`).
6. **Live E2E on `sms-api-stanford-test`, only after Jim says deploy** — through the CLI,
   not curl: `atlantis task run --toolkit s3://…/tools/ --entry fanout_probe.sh
   --interpreter bash --env PTOOLS_SKIP_N_GENS=1 --dry-run` (no writes), then a real
   `fanout_probe.sh` run, `atlantis task show <id>`, `atlantis task script <id>`.
   **Do not** touch `s3://…/tools/` itself and do not re-fire any `cd2fill-*` job — the
   other session is babysitting 11 of them (handoff §11). The snapshot writer only ever
   writes under `vecoli-output/tasks/snapshots/`.

---

## Related, NOT in this PR: serving fill TSVs to the ptools consumer (ideas, 2026-09-14)

Measured on dev: every CD2 store is its own `simulation` row (the `simNNN-` prefix is the
**simulator** id; Run-3 combo00 = simulation 1002). The many-to-one is *inside* a bundle:
1002 has `analysis-percell-run3` (400 TSVs, `ptools_rna__variant=0_seed=3_gen=12_agent=….tsv`)
and `analysis-ptools-multiseed` (one TSV per view per variant), both `s3-only` — no `analysis`
row. The ptools consumer contract is `list[TsvOutputFile]` with variant/lineage_seed/
generation parsed from vEcoli partition *directories*; fill files carry the coordinates in
the *filename*, so today they would all read as variant 0 with no seed/generation.

1. **Derived registration — now IN SCOPE, see §2c** (Jim, later the same evening: the
   analysis record is the consumer handle and points to its source). On task completion, for each `dest_prefix` shaped
   `<store>/analyses/<name>/`, upsert an `analysis` row (`experiment_id` = store,
   `simulation_id` from the resolved input, `result_uri`, `backend="task"`, `job_id_ext`,
   nullable `analysis.task_id`). READY only if the prefix holds TSVs (effect, not exit 0).
   Already-landed bundles: register from `assets/ptools/cd2_ptools_manifest.json`.
2. **Filename-aware coordinates.** Extend `parse_partition_metadata` to read `key=value`
   pairs from the stem (`variant`, `seed`→`lineage_seed`, `gen`→`generation`, `agent`) and
   add `scale` from the view suffix. One row then serves aggregate and per-cell bundles.
3. **Per-file serving.** `GET /analyses/{id}/files` (metadata + coordinates, filters
   view/scale/variant/seed/gen) + `GET /analyses/{id}/files/{name}` streamed; the same
   filters on `/simulations/{id}/analysis-figures` cover unregistered bundles first.
4. **`POST /simulations/{id}/analysis` → task verb on the Ray backend.** Inherits
   `memory_class`, `sim_data_refs`, `inputs`; keeps the endpoint's own registration. Closes
   #648 (a)-(c); a webapp-submitted analysis is then both a task and an analysis.
5. **Freshness.** `GET /analyses?simulation_id=&since=` ordered by `updated_at`.

Unverified: which path the Stanford ptools deployment calls (legacy `POST /analyses` is
501 on the K8s backend; the image is plain Pathway Tools + its Python bridge, host/path
from the `ptools-config` ConfigMap).
6. **Dataset coordinates (Jim's variant question).** The `simulation` row stores
   `config.variants` verbatim (the *definition*); the index→parameters expansion is done by
   v2ecoli `expand_branches` at run time and written to the store as `run_identity.json`
   (Run-3: under `seed_*/`), never to the DB. On the Ray path every CD2 store holds a single
   variant (all TSVs `variant=0`; sweeps are one simulation per point). Treat
   (variant, seed, generation, agent, scale) as the coordinate on every read endpoint, all
   axes optional: an omitted axis with exactly one value is implied; with several, a listing
   returns all with coordinates and a single-file fetch is 409 listing the values. Add a
   derived, cached `GET /simulations/{id}/datasets` summary (from `run_identity.json`,
   `seed_*/` prefixes and bundle TSV names). Legacy `POST /analyses` already has per-module
   variant/seed/generation filters but is SLURM-only (501 on K8s).
