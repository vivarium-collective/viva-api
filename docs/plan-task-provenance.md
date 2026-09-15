# viva-api#655 — task provenance, tasks as HpcRuns, and a tracing-fed dataset registry

**Status (2026-09-14): plan, not started.** Designed with Jim in plan mode from the
evidence brief at `assets/ptools/HANDOFF_tools_runs_655.md` (the brief predates the
finding that #631 is already built — see Context). Reviewed once against the code;
the review's findings are folded in. Scope widened the same evening, in three steps:
analysis-first registration (§2c, #648's registration half), dataset records as the
consumer unit (§2c), and tracing as the primary artifact feed with tasks becoming
HpcRuns (§2c Feeders, §2d). The emit side of the artifact contract lives in other repos
and follows (decision 10).
Branch off `main` @ `70edd4a0` (0.9.142).
Issues: #655 (this), #631 (the task verb this extends), #648 (artifact registration).

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
5. **Analysis-first registration is in this PR** (§2c): a finished task's output bundles are
   registered as `analysis` rows; the analysis record is the consumer handle and points to
   its source (task, simulation + coordinate, or another analysis) through the same
   reference type `task.inputs` uses. The ptools consumer lists analyses, not simulations.
6. **Dataset records** (§2c): the ptools page consumes a list of already-available
   datasets and launches nothing. Each consumable file set is its own `analysis_result`
   row, produced by exactly one run (simulation / analysis / task, by FK), described by an
   extensible `attributes` map (variant, seed, generation, protocol, n_tp, display name,
   …) plus `tags`, and fetched by its own id. The page is patched to call `GET /datasets`
   and `GET /datasets/{id}/content` instead of walking simulation → analyses → data.
8. **Tracing-first artifact registry** (§2c "Feeders"): producers record every file they
   write as an `artifact.written` event inside the current span — composites (parquet),
   analysis runs (ptools TSVs, figures, other tables) and tasks alike — and viva-api's event
   ingester turns those into `analysis_result` rows. The S3 walk remains for the one-time
   backfill and as a periodic reconciliation (sets `available`, catches lost events).
9. **HpcRun row per task, in this PR** (§2d): `JobTypeDB.TASK`, `hpcrun.jobref_task_id`,
   `PBG_*` injected by `_dispatch_task`. Tasks then appear in the observability views too.
10. **Sequencing:** viva-api lands first — dataset table, the ingest hook keyed on the
   documented `artifact.written` contract (tested with synthetic events), and the walk
   backfill so datasets exist immediately. The emit-side PRs (process-bigraph helper,
   v2ecoli call sites, sms-ecoli pin, simulator rebuild) follow; post the contract on #655
   for Eran/Alex before they start.
7. The revision creates `analysis` if absent before adding columns to it. **Correction:**
   this does not fix #637 — an empty DB fails earlier, at `d3f9a1c72b84`'s unguarded
   `add_column("analysis", …)`. #637's real fix (guard that older revision) stays separate.

**Non-goal (state it in the PR):** this is provenance and reproducibility, not authorization.
Arbitrary script recorded in a DB is still arbitrary script; `/tasks/upload` already accepts
arbitrary uploads, so no allowlist is added or removed here. Pre-execution review belongs to
moving the toolkit into a repo (follow-up).

---

## Design

### 1. Schema — two Alembic revisions off `e3a9c1d70b62` (current head)

Revision A (`<idA>_add_task_jobtype`): `ALTER TYPE jobtypedb ADD VALUE IF NOT EXISTS 'TASK'`
in an autocommit block (§2d). Revision B (`<idB>_add_task_provenance`, `down_revision =
<idA>`): everything below. One fingerprint marker each.

**New column on `hpcrun`:** `jobref_task_id` INTEGER FK → `task.id`, nullable, indexed (§2d).

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

**New columns on `analysis`** (nullable; legacy rows unaffected):

| column | type | what |
|---|---|---|
| `task_id` | INTEGER FK → `task.id`, indexed | the task that produced this bundle (NULL for K8s/SLURM-dispatched analyses) |
| `source` | JSONB | one `ProvenanceRef` (§2b): what this analysis is *of* — `{kind: simulation, ref, resolved_id, uri, coordinate: {variant, seed, generation, agent, scale}}`, or a `task` / `analysis` / `s3` ref. `simulation_id` stays as the indexed fast path when `kind = simulation`. |
| `tags` | JSONB NOT NULL default `[]`, GIN index `ix_analysis_tags` | consumer-facing selection tags (`cd2`, `run3`, …); same declaration as `simulation.tags` (`tables_orm.py:351`) |
| `n_tp` (existing) | — | now populated at registration from the TSV header (timepoint columns), since the ptools page labels and sizes by it |

**New table `analysis_result`** — the consumer-facing dataset record, one row per
consumable file set; full column list in §2c. Three nullable producer FKs (`simulation_id`,
`analysis_id`, `task_id`) with a CHECK that at least one is set, `uri` unique, GIN indexes
on `attributes` and `tags`, plain indexes on each FK and on `(kind, view)`.

The revision guards `analysis` with `inspector.has_table("analysis")` and, when absent,
creates it from the ORM (`ORMAnalysis.__table__.create(bind, checkfirst=True)` after an
explicit `analysisstatusdb` `create(checkfirst=True)` — the `d3f9a1c72b84` enum pattern);
then guarded `add_column` for the two columns and a guarded `ix_analysis_task_id`.

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
order — `("<idA>", "jobtypedb has value 'TASK'")` with `_enum_has_value(conn, "jobtypedb",
"TASK")`, then `("<idB>", "table 'task_script' exists")` with `_table_exists(conn,
"task_script")` — appended to `LEGACY_FINGERPRINTS` and `_LEGACY_PREDICATES` (positionally
aligned). In `tests/simulation/test_db_reconcile.py`: `HEAD` → `<idB>`; `REVS` is currently
15 entries and is missing `e3a9c1d70b62` — append it and both new ids; every hand-written
vector 16 → 18. Both single-head tests
(`test_db_reconcile.py:594-600`, `test_observability_migration.py:395-405`) then cover the
new head for free.

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

### 2c. Analysis-first registration (Jim, 2026-09-14)

The `analysis` row is the consumer-facing handle; it is **not** assumed one-to-one with a
simulation. It points out to its source via `analysis.source` (a `ProvenanceRef`) and back
to the run that made it via `analysis.task_id`.

**Registration at completion.** In the `update_tasks` tick (§3), when a task reaches a
terminal state, for each `dest_prefix` shaped `<store>/analyses/<name>/`:
- upsert an `analysis` row keyed on `result_uri` (new `get_analysis_by_result_uri`): `name`
  = `<name>`, `experiment_id` = `<store>` (the path segment before `/analyses/`),
  `simulation_id` = `get_simulation_by_experiment_id(store)` when it resolves,
  `backend = "task"`, `job_id_ext` = the Batch id, `task_id`, `source` = the task's first
  `simulation` input if any (with its coordinate) else `{kind: task, ref: <task id>}`,
  `config = {"task_id", "views": [...], "scales": [...]}` parsed from the bundle's filenames;
- `status = READY` only if the prefix lists at least one TSV/HTML (**effect, not exit 0**;
  the presence-vs-effect lesson), else `FAILED` with `error_message = "bundle empty"`.
  A dest prefix that does not match the `analyses/<name>/` shape is recorded on the task
  only (it is still provenance) and registers nothing.
- The same registration runs for the **already-landed bundles** in the backfill importer
  (§6), from `assets/ptools/cd2_ptools_manifest.json` `stores[].fill_bundles` (store →
  simulation row verified: every CD2 store is its own `simulation` row).

**Dataset coordinates.** Fill files carry their coordinates in the filename
(`ptools_rna__variant=0_seed=3_gen=12_agent=….tsv`, `ptools_rna_multiseed__variant=0.tsv`);
vEcoli's own output carries them in partition directories. Extend `parse_partition_metadata`
(`viva_api/analysis/analysis_service.py:377`, currently anchored `match` on path parts, so it
sees nothing in a fill filename) to also scan the stem for `key=value` pairs, mapping
`seed`→`lineage_seed`, `gen`→`generation`, `agent`→`agent_id`, and derive `scale` from the
view suffix (`_multiseed`, `_multigeneration`, else `single`). `OutputFileMetadata` gains
`scale` and `view`. **Rule for omitted axes:** a listing returns every file with its
coordinates; a single-file fetch with an omitted axis is implied when exactly one value
exists for that axis and is **409 listing the values** when several do.

**Read side (what the ptools consumer calls):**

| route | change |
|---|---|
| `GET /api/v1/analyses` | filters `status`, `backend`, `task_id`, `source` (`sim:1002`, `task:12`), `since` (ISO); `limit` ≤ 200 / `offset`; ordered `updated_at` desc. Replaces the "exhaustive; filtering/paging to come" note. |
| `GET /api/v1/analyses/{id}` | DTO gains `task_id`, `source`, `result_uri` (already), `n_files` |
| `GET /api/v1/analyses/{id}/files` | **new** — metadata + coordinates per file; filters `view`, `scale`, `variant`, `seed`, `generation`, `agent` |
| `GET /api/v1/analyses/{id}/files/{name}` | **new** — one file, streamed (`StreamingResponse`, content-type by suffix), `{name}` validated against the row's own `result_uri` listing (no traversal, no caller-supplied URI) |
| `GET /api/v1/analyses/{id}/data` | unchanged shape, now carries the parsed coordinates for fills too |

CLI: `atlantis analysis list [--status --since --source sim:1002 --task]`,
`atlantis analysis files <id> [--view --scale --variant --seed --generation]`,
`atlantis analysis fetch <id> <name> --out FILE`. (Existing verbs `get/status/log/plots`
untouched.)

**The real ptools consumer (verified 2026-09-14 from the extracted 30.0 web tree,
`htdocs/sms/sms.js`, SRI-maintained; base URL from the generated `/sms/env.js` =
`SMS_API_HOST` + `SMS_API_PATH`):**
1. `GET /api/v1/simulations` → simulation picker, with a **tag** filter on `sim.tags`.
2. `GET /api/v1/analyses?experiment_id=<id>` → "Analysis Configuration" picker, labelled
   only by `n_tp`; no status filter (failed rows show).
3. `GET /api/v1/analyses/{id}/data` → takes the entry whose **filename stem equals the
   value type** (`ptools_rna` / `ptools_proteins` / `ptools_rxns`), POSTs its text to
   PTools `/register-omics-dataset` with `datacolumns = "1-<n_tp>"`. Later matches
   overwrite earlier ones (a per-cell bundle would silently show the last cell).

So registration alone is not enough: fill rows need `n_tp` (derive at registration from
the TSV header column count, sampling one file per view) and the data call must be able
to return one file per view with a stem the page recognises.

**Consumer compatibility (no JS change).** `GET /analyses/{id}/data` gains the same
coordinate filters as `/files` (`view`, `scale`, `variant`, `seed`, `generation`); when the
selection yields exactly one file per view, the response `filename` is aliased to
`<view>.tsv` and the real key is returned in a new `path` field. Multiseed fill bundles then
work in today's page unchanged; per-cell bundles are (correctly) not servable unfiltered.

**Dataset records — the consumer unit (Jim, 2026-09-14).** The Stanford ptools page does
not launch anything; it consumes a list of *already available* datasets. So the consumer
unit is materialized as its own row, **one per consumable file set** (for ptools: one TSV),
produced by exactly one run and described by an extensible attribute map:

**New table `analysis_result`** (name chosen to read as "a result of an analysis"; the API
calls them datasets):

| column | type | what |
|---|---|---|
| `id` | PK | the stable id the page fetches by |
| `simulation_id` | FK → `simulation.id`, nullable, indexed | producer, when a simulation run wrote it (sim-time `analysis-mnp-*` bundles) |
| `analysis_id` | FK → `analysis.id`, nullable, indexed | producer, when an analysis run wrote it |
| `task_id` | FK → `task.id`, nullable, indexed | producer, when a task run wrote it |
| | CHECK | at least one producer FK non-null (`ck_analysis_result_producer`) |
| `kind` | TEXT NOT NULL | `ptools-analysis` \| `figure` \| `table` … (what a consumer can do with it) |
| `view` | TEXT | `ptools_rna`, `ptools_rxns`, `ptools_proteins`, `ptools_metabolites`, `ptools_overview`, … |
| `display_name` | TEXT | what the picker shows |
| `uri` | TEXT NOT NULL, unique | the S3 object (or prefix for multi-file kinds) |
| `size_bytes`, `sha256` | INTEGER / TEXT NULL | sha only when cheap (small text) |
| `attributes` | JSONB NOT NULL default `{}`, GIN | extensible: `variant`, `seed`, `generation`, `agent`, `protocol` (`single`/`multiseed`/`multigeneration`), `n_tp`, `experiment_id`, `family`, … — any key, filterable |
| `tags` | JSONB NOT NULL default `[]`, GIN | selection tags (`cd2`, `run3`, …) |
| `source` | JSONB | `ProvenanceRef` with coordinate — what it is *of* (usually the simulation + coordinate; kept even when `simulation_id` is set, for sub-stores) |
| `available` | BOOLEAN NOT NULL default true | flipped by a probe when the object is gone (lifecycle); rows are never deleted |
| `created_at`, `updated_at` | DateTime | |

The `analysis` row (§2c above) remains the *analysis run* record (one per bundle dir /
job) and gains only `task_id`, `source`, `tags`; datasets hang off it. `analysis.n_tp`
stays populated for the compatibility path.

**Registration by the walk feeder** (task completion tick, the reconciliation tick, and the
importer, §6; `origin = walk`, skipped where an `origin = event` row already holds the
`uri`): for every `ptools/*.tsv` under a registered bundle → one `analysis_result` row,
`kind = ptools-analysis` (`figure` for `viz/*.html`), `view` and
`attributes` parsed from the filename (`ptools_rna_multiseed__variant=0.tsv` →
`view=ptools_rna, protocol=multiseed, variant=0`; `ptools_rna__variant=0_seed=3_gen=12_agent=…`
→ `protocol=single, seed=3, generation=12, agent=…`), `n_tp` from the header of that
file (one small ranged GET), `display_name` = `<experiment_id> · <view> · <protocol>[ · s3 g12]`,
`tags` = task tags ∪ source simulation tags, `source` = the analysis's source + coordinate,
producer FKs = `analysis_id` + `task_id`. Idempotent on `uri`. The importer also registers
the sim-time `analysis-mnp-*/ptools/*.tsv` files (producer `simulation_id`), tagged from
the manifest family. A per-cell bundle yields 400 rows — that is the point: each is a
file set the page can load by id.

**Consumer API (what the patched page calls; generic, `kind` selects ptools):**

| route | returns |
|---|---|
| `GET /api/v1/datasets?kind=ptools-analysis&tag=cd2&view=ptools_rna&attr.protocol=multiseed&attr.variant=0&simulation_id=&task_id=&analysis_id=&available=true&since=&limit=&offset=` | rows ordered `updated_at` desc; `attr.<key>=<value>` filters map to JSONB containment on `attributes` (GIN) |
| `GET /api/v1/datasets/{id}` | one row |
| `GET /api/v1/datasets/{id}/content` | the object, streamed, content-type by kind (`text/tab-separated-values`) |
| `GET /api/v1/datasets/attributes` | distinct keys and values present (for building pickers), and `/datasets/tags` |
| `POST /api/v1/datasets/{id}/tags` | union-merge tags (mirror of `/simulations/{id}/tags`) |

The page then: `GET /datasets?kind=ptools-analysis&tag=cd2` → picker rows (display_name +
attributes) → `GET /datasets/{id}/content` → PTools `/register-omics-dataset` with
`datacolumns = "1-<attributes.n_tp>"` and `class` from `view`. Three calls become two, no
simulation hop, and per-cell datasets are addressable one at a time.

`TaskRunRequest.tags: list[str]` (new) seeds the tags of everything a task registers.

**Feeders — tracing is primary (Jim, decision 8).** `register_datasets` is one seam with
two feeders, both idempotent on `uri`, each stamping `attributes.origin`; `origin = event`
wins over `origin = walk` on conflict.

*Feeder 1 — the `artifact.written` event (primary, steady state).* The contract, to be
posted on #655 and implemented on the emit side (decision 10):

```
event:     "artifact.written"            # one per file (or per partition prefix, see kind)
component: "v2ecoli.analysis" | "v2ecoli.emitter" | "task" | ...
span:      the current span (analysis.group / lineage generation / task root)
payload: {
  "uri":        "s3://…/analyses/analysis-ptools-multiseed/ptools/ptools_rna_multiseed__variant=0.tsv",
  "kind":       "ptools-analysis" | "analysis" | "figure" | "parquet" | "report" | "other",
  "name":       "ptools_rna_multiseed__variant=0.tsv",
  "view":       "ptools_rna",                          # optional
  "bytes":      12345, "sha256": "…",                  # optional (sha only when cheap)
  "attributes": {"protocol": "multiseed", "variant": 0, "seed": 3, "generation": 12,
                 "agent": "…", "n_tp": 8, "display_name": "…"}   # extensible
}
baggage (already promoted by viva-api): sim_id / experiment_id, analysis_id or
           analysis_name, task_id  → producer resolution
```
Parquet: one event per **partition prefix** at emitter finalize (`kind = parquet`,
`uri` = the prefix), not per shard. Bash toolkits that cannot call the Python helper
write the same JSON lines to `$CONTAINER_OUT_DIR/artifacts.jsonl`; the entrypoint's
existing output sync ships it and viva-api ingests it as events (same schema, `origin =
event`, `component = task`).

viva-api side (this PR): in `event_ingest._ingest_object`, collect `artifact.written`
events per pass; after the pass, `register_datasets` upserts `analysis_result` rows —
producer FK from baggage (`task_id`, `analysis_id`) else the HpcRun's job reference
(`simulation_id`, or `jobref_task_id`, §2d); `kind`, `view`, `attributes`, `tags` (task tags
∪ source simulation tags ∪ event `attributes.tags`), `source` = simulation + coordinate.
Tested with synthetic `.jsonl` objects; nothing on the emit side exists yet, so the live
feed lights up only when the emit-side PRs ship.

Why it is the design center: the runner already scopes each write inside an
`analysis.group` span whose `group` attr *is* the coordinate
(`v2ecoli/workflow/analysis_runner.py:1015-1047`), so the event is ground truth — no
filename parsing — and it can record a coordinate whose view **failed**
(`payload.error`, row `available = false`), which a walk can only infer by absence.

*Feeder 2 — the S3 walk (backfill + reconciliation).* The importer (§6) registers
everything historical with `origin = walk`, attributes parsed from filenames. A periodic
`reconcile_datasets` scheduler tick (hourly, cheap) lists the dest prefixes of READY
analyses/tasks and (a) registers files with no row yet (events lost or emit side not
deployed), (b) flips `available` for rows whose object is gone. Events are best-effort by
design (opt-in sink, size-skip, never raised), so the walk is the safety net, never the
source of truth when an event exists.

The JS patch itself is not in this repo: `sms.js` ships inside SRI's export. Either
coordinate with SRI (Paley) or overlay a patched copy in `Dockerfile-ptools`. Free config
fix regardless: `SMS_API_HOST=` (set, empty) makes `simBaseUrl` relative (`/api/v1/`) —
verified by the other session against the Lisp image's `~A~A` format; do not delete the
key (a NIL would print as the literal `NIL`).

**Not in this PR (follow-ups, §8):** routing `POST /simulations/{id}/analysis` through the
task verb; a cached `GET /simulations/{id}/datasets` summary; filters on the #650
`analysis-figures` pair; the `sms.js` patch and `ptools.env` change (deploy-side).

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
- TUI/GUI parity: **not in this PR** (the #631 slices shipped CLI only); listed under
  follow-ups so the EUTE rule is not silently dropped.

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
`stores[].fill_bundles` entry: `analysis` rows with `task_id` (when the producing job was
harvested), `source = {kind: simulation, ref: <store>}` resolved to the simulation row,
`status` from a live TSV count, `config.views` from `view_types`. Idempotent on `result_uri`.

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
| `alembic/versions/<idB>_add_task_provenance.py` | **new** — `task_script`, `analysis_result`, `task`/`analysis`/`hpcrun` columns, all guarded |
| `viva_api/common/models.py` (`JobType`) + `tables_orm.py` (`JobTypeDB`, `ORMHpcRun.jobref_task_id`, `to_hpc_run`) | `TASK` job type and the fourth job reference |
| `viva_api/simulation/database_service.py` (`insert_hpcrun`, `get_hpcrun_by_ref`) | route `job_type = TASK` → `jobref_task_id` |
| `viva_api/simulation/event_ingest.py` | collect `artifact.written` events per pass (`_ingest_object`), sidecar `artifacts.jsonl` read, `register_datasets` call after the pass; `_events_key_prefix` task branch (uses `hpcrun.events_s3_prefix`) |
| `viva_api/simulation/job_scheduler.py` | `update_tasks` also finishes the task's HpcRun; `reconcile_datasets` hourly tick; `job_type != TASK` filters on the simulation-only ticks |
| `docs/OBSERVABILITY.md` | the `artifact.written` contract (payload schema above), the sidecar fallback, producer resolution from baggage |
| `tests/simulation/test_event_ingest_artifacts.py` | **new** — synthetic `.jsonl` with `artifact.written` → dataset rows (producer from baggage, from jobref; failed group → `available=false`); idempotent re-ingest; sidecar file; malformed payload skipped and logged |
| `tests/simulation/test_task_jobtype_migration.py` | **new** — revision A adds the label; a create_all DB matches its marker; `insert_hpcrun(job_type=TASK)` round-trips; single head |
| `tests/simulation/test_task_hpcrun.py` | **new** — `_dispatch_task` inserts the HpcRun and injects `PBG_*` (trace seeded from the correlation id; `events_s3_prefix` under the task out_uri); simulation-only ticks skip TASK rows |
| `viva_api/simulation/tables_orm.py` | `ORMTaskScript`; new `ORMTask` columns; `to_dto` |
| `viva_api/simulation/db_reconcile.py` | marker + fingerprint + predicate |
| `viva_api/simulation/models.py` | `TaskRunRequest` sources/env/inputs/dest/dry_run/allow_duplicate + validator; `TaskDTO` growth + `warnings`; `TaskStagedFile`, `ProvenanceRef` |
| `viva_api/simulation/task_inputs.py` | **new** — `ProvenanceRef` parse/resolve/probe (DB lookups via the existing `DatabaseService.get_simulation`, `get_simulation_by_experiment_id`, `get_task`, `get_analysis`) |
| `viva_api/simulation/tables_orm.py` (`ORMAnalysis`) | `task_id`, `source`; `to_dto` |
| `viva_api/analysis/models.py` | `ExperimentAnalysisDTO` + `task_id`/`source`/`n_files`; `OutputFileMetadata` + `scale`/`view`; `AnalysisFileList` |
| `viva_api/analysis/analysis_service.py` | `parse_partition_metadata` filename-aware + `scale` |
| `viva_api/common/handlers/analyses.py` | `register_task_bundles` (§2c, called by the tick and the importer); `list_analysis_files` / `fetch_analysis_file` (streamed); list filters |
| `viva_api/api/routers/sms.py` | `GET /analyses` filters+paging; `GET /analyses/{id}/files`, `/files/{name}`; `/analyses/{id}/data` coordinate filters + filename aliasing |
| `viva_api/simulation/tables_orm.py` (`ORMAnalysisResult`) | **new** — the dataset table (§2c), GIN on `attributes`/`tags`, producer CHECK |
| `viva_api/analysis/models.py` (`DatasetDTO`, `DatasetListDTO`) | **new** — id, producer ids, kind, view, display_name, uri, attributes, tags, source, available |
| `viva_api/common/handlers/datasets.py` | **new** — `register_bundle_datasets` (filename → view/attributes, `n_tp` from header), list with `attr.*` filters, streamed content, attributes/tags summaries |
| `viva_api/api/routers/datasets.py` + `viva_api/api/main.py` (`APP_ROUTERS` += `"datasets"`) | **new** — `GET /datasets`, `/datasets/{id}`, `/datasets/{id}/content`, `/datasets/attributes`, `/datasets/tags`, `POST /datasets/{id}/tags` (`get_router_config(prefix="api", version_major=False)`) |
| `viva_api/simulation/database_service.py` (datasets) | `upsert_analysis_result(uri)`, `list_analysis_results(filters, paging)`, `get_analysis_result`, `add_analysis_result_tags`, `distinct_attributes` |
| `viva_api/simulation/models.py` (`TaskRunRequest`) | `tags: list[str]` (validated like `simulation` tags) |
| `app/cli.py`, `app/app_data_service.py` | `atlantis dataset list [--kind --tag --view --attr k=v --since]`, `dataset get <id>`, `dataset fetch <id> --out FILE` |
| `tests/api/test_datasets_router.py` | **new** — list filters (tag, view, `attr.*`, producer ids), content streams with the right content-type, tags merge; unknown id 404 |
| `tests/common/handlers/test_datasets_registration.py` | **new** — filename → view/attributes for both naming conventions; `n_tp` from header; idempotent on `uri`; per-cell bundle → one row per file; sim-time bundle → `simulation_id` producer |
| `viva_api/simulation/database_service.py` (analyses) | `list_analyses` filters/paging; `get_analysis_by_result_uri`; `record_analysis` + `task_id`/`source`; `update_analysis` |
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
| `tests/common/handlers/test_analyses_registration.py` | **new** — `register_task_bundles`: dest prefix → row, empty bundle → FAILED, non-`analyses/` prefix registers nothing, idempotent on `result_uri`; filename coordinate parser (both conventions); omitted-axis rule (implied vs 409) |
| `tests/api/test_analyses_files.py` | **new** — list filters/paging; `/files` filters; `/files/{name}` streams and rejects a name not in the listing |
| `tests/scripts/test_import_cd2_tool_runs.py` | **new** — mapping on a 2-job fixture slice; redaction; idempotence |
| `viva_api/version.py`, `pyproject.toml` | bump 0.9.142 → 0.9.143 (a migration ships; the db-migration overlay tag must move with the app tag at deploy time) |

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
5b. **Registration, on dev after the importer:** `GET /analyses?source=sim:1002` returns the
   two fill bundles (`analysis-percell-run3` READY with 400 files, `analysis-ptools-multiseed`
   READY with 5) plus the pre-existing failed row; `GET /analyses/{id}/files?view=ptools_rna
   &scale=multiseed` returns exactly one file with `variant=0`; `GET /analyses/{id}/files/
   ptools_rna_multiseed__variant=0.tsv` streams it; the per-cell bundle's `/files?seed=3
   &generation=12` returns 5 files (one per view). Through `atlantis analysis list/files/
   fetch`, not curl.
5d. **Tracing feed, offline:** feed a synthetic events object carrying `artifact.written`
   events (one per view, one with `error`) through `ingest_run_events` for a TASK HpcRun;
   expect one `analysis_result` row per event with `origin = event`, producer `task_id`
   from baggage, the failed one `available = false`; re-ingest is a no-op; a later walk
   over the same prefix adds nothing and does not downgrade `origin`.
5e. **Task trace, on dev (after deploy):** `atlantis task run … --dry-run` then a real
   `fanout_probe.sh` run; `GET /tasks/{id}` shows `hpcrun_id` + `trace_id`;
   `GET /tasks/{id}/spans` shows the task root span; `artifact.written` rows appear only
   once the emit-side PRs ship — until then §5b's walk-registered rows carry `origin = walk`.
5c. **ptools compatibility, on dev:** with the tunnel up, open the ptools page
   (`/sms/sms.html` via the ALB), pick sim 1002 (tag `cd2`), pick the registered
   `analysis-ptools-multiseed` row (label shows its `n_tp`), display RNA + reactions — the
   overlay renders from the fill TSVs with no JS change. Then `atlantis dataset list --kind
   ptools-analysis --tag cd2 --view ptools_rna --attr protocol=multiseed` lists one row per
   filled store (expect 20 for Run-3 + 1 for Run-2 after the importer), and `atlantis
   dataset fetch <id>` byte-equals the S3 object.
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
