# Handoff: designing viva-api#655 (`tools/runs`) — evidence, constraints, open questions

**Written 2026-09-14 ~22:40Z** by the session that filed #655, for a fresh session that will
design it with Jim **in plan mode**.

**This brief contains no design and no plan.** That is deliberate — the design is Jim's
discussion to have. What follows is the evidence that motivated the issue, the constraints any
design must respect, and the questions that actually have to be settled. Everything numeric here
was measured tonight unless explicitly flagged as inference.

**Read the issue first:**
```bash
gh issue view 655 --repo vivarium-collective/viva-api
gh issue view 631 --repo vivarium-collective/viva-api
gh issue view 648 --repo vivarium-collective/viva-api
```

---

## 1. What #655 is, and how it relates to #631 and #648

Three issues, three different halves of one problem. They are **not** duplicates, and the
distinction matters for the fold-or-not question in §12.

| issue | question it answers | state |
|---|---|---|
| **#631** | *How* does an in-region job get run at all? (`atlantis task run` / a served submission verb) | OPEN, eagmon |
| **#648** | *Where did the output go?* (analysis registration + per-file artifact access) | OPEN, jcschaff |
| **#655** | *What actually ran?* (which script, which version, which args) | OPEN, jcschaff, filed tonight |

**#631 already claims this ground without defining it.** Its own description says the verb
"writes provenance" — but never says what that means or where it lands. #655 is arguably the
missing specification of that clause. The issue body says explicitly that **#655 could be folded
into #631**, since the schema is what #631 needs anyway. That is a live option, not a settled one.

### #648's hard blocker — why "just use the sanctioned path" is not an available answer

There is an existing endpoint, `POST /simulations/{id}/analysis`, whose `_run_standalone_analysis_ray_native`
branch already accepts the exact `{scale: {view: {}}}` dict the fills use, already derives
`result_uri`, and **already registers the analysis row**. So the obvious response to all of this is
"stop hand-dispatching, use that." Three measured gaps prevent it today (all documented in #648):

**(a) Resource starvation — the hard blocker.** The dispatch hardcodes:
```python
resources=k8s_client.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"},
    limits={"cpu": "1", "memory": "2Gi"},
)
```
Measured peak RSS for these same workloads, in-region on a 58.6 GiB cgroup:

| workload | peak RSS |
|---|---|
| `ptools_rxns_multigeneration`, post-v2ecoli#801 | **2.0 GiB** (exactly at the limit) |
| same view, pre-#801 | **32.5 GiB** |
| Run-2 `multiseed` | SIGKILLed at **58.6 GiB** (`oom_kill 1`) |

The sanctioned path is resource-starved by roughly an order of magnitude and would be OOM-killed.

**(b) Addressing is simulation-id-only.** It derives `out_uri` from the simulation. Two real cases
it cannot express: Run-1's data lives in a *sub-store* (`.../studies/.../parquet-runs/<uuid>/`), not
the simulation's own `out_uri`; and the Run-4 fill's **append mode** writes additional TSVs *into an
existing* `analyses/analysis-mnp-*/ptools/` bundle, whereas the endpoint always mints a fresh
`analysis_name` directory.

**(c) No explicit `sim_data` override.** sim201 stores carry no `run_identity.json`, so the fills
pass one shared base `simData.cPickle`; the endpoint resolves sim_data from the simulation's config.

**Consequence for the design discussion:** any proposal that routes ad-hoc work through the existing
analysis endpoint must close (a)–(c) first, and (a) is a real code change, not a config tweak.

---

## 2. Audit evidence — the toolkit is uncontrolled

Eleven scripts in `s3://smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91/tools/` drive every
CD2 ptools fill. Checked against all four repo checkouts tonight:

```
sms-api             0 tracked
vivarium-workbench  0 tracked
v2ecoli             0 tracked
sms-ecoli           1 tracked -- scripts/combine_run4_fss.py (#393)
```

**One of eleven is committed, and it has drifted 155 lines from the S3 copy that actually runs.**
(Measured: `diff` between `s3://.../tools/combine_run4_fss.py` and
`sms-ecoli/scripts/combine_run4_fss.py` → 155 differing lines.) The reviewable version is not the
executed version.

**Provenance of the scripts, stated plainly:** they were written by Claude (me, the session that
wrote this brief, and predecessor sessions) iteratively across the campaign, largely in reaction to
failures. They were never designed as a system; they accreted. They are edited in place in S3.

Full current inventory of `tools/` with last-modified times (2026-09-14):

| file | last modified | note |
|---|---|---|
| `ptools_flush.py` | 09-14 16:55 | the driver; reads `PTOOLS_SKIP_N_GENS` |
| `fanout_multi.sh` | 09-14 17:36 | multigen/multiseed fanout over stores |
| `fanout_run3_percell.sh` | 09-14 09:47 | per-cell, sharded (LO/HI) |
| `fanout_run1.sh` | 09-14 09:32 | Run-1 per-seed, takes LO/HI |
| `fanout_percell_fill.sh` | 09-12 13:27 | Run-4 fill, two modes |
| `fanout_probe.sh` | 09-13 14:46 | |
| `combine_run1_multiseed.py` | 09-14 00:17 | combine-first for Run 1 |
| `combine_run4_fss.py` | 09-12 19:40 | the one with a repo twin |
| `regen_probe.py` | 09-13 14:46 | |
| `regen_ptools_run1.py` | 09-12 12:15 | |
| `spill_probe.sh` | 09-14 01:01 | |

(`cd2_ptools_manifest.json/.tsv` and `cd2_tool_runs.json` also live there now — artifacts, not scripts.)

---

## 3. Retention facts — all verified tonight

| store | holds | lifetime | verified how |
|---|---|---|---|
| Batch `describe-jobs` → `container.environment[CONTAINER_JOB_CMD]` | the full invocation | **~7 days** | AWS Batch documented retention |
| CloudWatch log group `smsvpctest-ray-batch-RayBatchLogs2D18AAFC-vjj0f55ZeO5i` | stdout + `download:` staging lines | **7 days** | `describe-log-groups` → `retentionInDays: 7` |
| S3 object versions under `tools/` | script **content** | **durable, no expiry** | `get-bucket-versioning` → Enabled; lifecycle scoped elsewhere (below) |

**Bucket lifecycle configuration (measured):**
```json
{"Rules": [
  {"ID": "abort-incomplete-multipart-uploads", "Filter": {"Prefix": ""},
   "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}},
  {"ID": "expire-ray-logs", "Filter": {"Prefix": "ray-logs/"}, "Status": "Enabled",
   "Expiration": {"Days": 30}, "NoncurrentVersionExpiration": {"NoncurrentDays": 7}}
]}
```
The **only** `NoncurrentVersionExpiration` is scoped to `ray-logs/`. So `tools/` noncurrent versions
do **not** expire. Script content is genuinely permanent.

**The `ray-logs/` archive is not an escape hatch.** `CONTAINER_LOG_S3_PREFIX` on the job definition
points at `s3://.../ray-logs/`, which looks like a durable log archive. It is not, for this job class:
it contains only Ray `node-N.tgz` tarballs; **our hand-dispatched container jobs have ZERO objects
there** (checked two of tonight's job ids → 0 each); newest entry is 2026-09-09; and it expires at 30
days anyway. That path is populated by the Ray MNP dispatch, not by container jobs.

**The entrypoint does NOT echo `CONTAINER_JOB_CMD` into the log.** A stream begins:
```
[batch-container] job 5b5c8959-... starting
download: s3://.../tools/cd2_ptools_manifest.json to ./cd2_ptools_manifest.json
...
```
then the command's own stdout. The invocation itself never appears. So it exists in exactly one
place: the Batch job record, for ~7 days.

**Net:** content survives forever; the linkage between a run and the content it ran survives one week.

### The partial reconstruction path (works, but is inference not record)

The CloudWatch log *does* preserve which files were staged and when:
```
staged at 21:40:11Z: download: s3://.../tools/fanout_multi.sh to ./fanout_multi.sh
```
and S3 retains every version with a timestamp. So you can infer content by taking the staging
timestamp and finding the newest version at or before it. **This is what tonight's backfill (§7)
mechanised.** It fails in three ways: `aws s3 cp --recursive` never logs a `versionId`, so the link
is circumstantial; a restage *between* the download and the read breaks it silently; and once the
7-day CloudWatch window elapses the staging timestamps are gone and the S3 versions become an
undated pile with nothing pointing at them.

---

## 4. The dispatch mechanism, in full — this is what the endpoint must wrap or replace

**sms-api is not in the path at any point.** No endpoint is called. The entire dispatch is one
AWS API call:

```bash
aws batch submit-job \
  --job-name        "cd2fill-$NAME" \
  --job-queue       smsvpctest-ray-standalone \
  --job-definition  smsvpctest-ray-container-33ecd77 \
  --container-overrides '{"environment":[{"name":"CONTAINER_JOB_CMD","value":"<free-form shell string>"}]}'
```

**Job definition `smsvpctest-ray-container-33ecd77`:**
```
image      476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:33ecd77
command    ["/opt/batch-container-entrypoint.sh"]
env        CONTAINER_REPORT_PATH, CONTAINER_DEBUG_HOLD, CONTAINER_DEBUG_HOLD_SECONDS,
           CONTAINER_LOG_S3_PREFIX, CONTAINER_JOB_CMD
resources  VCPU 16, MEMORY 60000   (→ 58.6 GiB cgroup in practice)
jobRole    arn:aws-us-gov:iam::476270107793:role/smsvpctest-ray-mnp-job
logs       awslogs → smsvpctest-ray-batch-RayBatchLogs2D18AAFC-vjj0f55ZeO5i
```

The entrypoint's job is to read `CONTAINER_JOB_CMD` and execute its value as a shell command at
container start. Sequence:

1. Batch starts the container from `v2ecoli:33ecd77`.
2. The entrypoint runs the `CONTAINER_JOB_CMD` string, which `aws s3 cp --recursive`s the toolkit
   from `s3://.../tools/` into `/app/tk`.
3. That string then runs e.g. `bash fanout_multi.sh` with `LO/HI/FORCE/FAMILY/SCALES/SIM_DATA` and
   `PTOOLS_SKIP_N_GENS=1` as **shell exports inside the string** — note this, it caused a dispute
   tonight (§5a): they are *not* Batch env entries and do not appear in `container.environment[].name`.
4. The script calls `ptools_flush.py` per target and `aws s3 cp`s results back.

**Scripts are NOT baked into the image.** They are staged from S3 at container start. This is the
single most important mechanical fact for the design: editing the S3 copy changes what the *next*
job runs, and jobs already in flight keep whatever they staged.

**These jobs are invisible to viva-api — verified empirically, not assumed:**
```
cd2fill-run3-core5-s0-3       simulations rows: 0
cd2fill-run2j3-core5-skip1    simulations rows: 0
rows mentioning cd2fill/core5 anywhere: 0   (of 1,294 simulations in the DB)
```
Consequences: no `HpcRun` row, no simulation row, no analysis row (this is *why* ~19,662 ptools TSVs
are unreachable through the API — see #648); no `PBG_*` env, so they are absent from `/events` and
`/tasks` and the 0.9.142 observability work does not instrument them; no API-side retry, status
reconciliation or timeout. **The job definitions carry no `attemptDurationSeconds`, so a wedged job
runs forever** — they are killed by hand.

The contrast: a normal simulation goes `POST /api/v1/simulations` → viva-api creates the Batch job,
records the `HpcRun`, tracks it. These fills use viva-api's *image* and none of its *machinery*.

---

## 5. The four incidents that motivate the design

These are the argument. All four are from a single evening.

### (a) Mutable staging silently changed running jobs

`ptools_flush.py` was restaged at **16:55Z** (by help-team, adding `PTOOLS_SKIP_N_GENS` support);
`fanout_multi.sh` at **17:36Z** (by me, adding LO/HI + FORCE). **Both with jobs in flight.** Jobs
launched at 14:56Z were running different code than jobs launched at 21:40Z, with nothing to
distinguish them.

This produced a live dispute on sms-ecoli#166: Eran challenged whether
`cd2fill-run2j3-core5-skip1` actually carried `skip_n_gens=1`, having correctly observed that the
Batch env array showed only five names and none of them was `PTOOLS_SKIP_N_GENS`. The answer — that
it is a shell `export` *inside* `CONTAINER_JOB_CMD` — was settleable **only by grepping a Batch
command string**:
```bash
aws batch describe-jobs --jobs 8bababde... \
  --query 'jobs[0].container.environment[?name==`CONTAINER_JOB_CMD`].value|[0]' \
  | grep -o 'export PTOOLS_SKIP_N_GENS=[0-9]*'
# → export PTOOLS_SKIP_N_GENS=1
```
Eran's observation was correct; his inference was wrong; and nothing in the system could have told
him either way.

### (b) The only record expires

Because Batch retains ~7 days and nothing else captures the invocation, a **manifest had to be
hand-built by scanning S3** (`assets/ptools/cd2_ptools_manifest.json`, 183 stores) before the window
closed, purely to establish which image produced which output bundle. That manifest exists because
the system has no memory.

### (c) A script can bypass the very knob it appears to honour

`combine_run1_multiseed.py` (staged 00:17Z, never successfully run) invokes `ANALYSIS_REGISTRY`
views **directly**, not through `ptools_flush.py`:

```python
step = cls({}, core=core)          # line 128 -- EMPTY per-view config
results[name] = step.analyze(conn=con, history_sql=hist_sql, sim_data=sim_data)
```

`PTOOLS_SKIP_N_GENS` is read at `ptools_flush.py:35-36` and injected there. A script that never
invokes that driver cannot see it. Running it unchanged would have produced a clean-looking,
`5/5 views ok` bundle that was silently **`skip_n_gens=0`** — not the requested deliverable, with
nothing in the output to reveal it.

**Found only by reading the source.** That does not scale.

### (d) …and the same script was then fired with three defects in the invocation

Immediately after fixing (c), I fired it with:
- `--mem-limit 64GB` against a **58.6 GiB cgroup**. `_duckdb_s3` does `SET memory_limit='64GB'`
  verbatim with no clamping, so DuckDB would not spill in time → SIGKILL. (`combine_run4_fss`'s own
  default is **40GB** for exactly this reason; the 64GB default came from my own script header.)
- `FSS_IN_REGION` unset — the script keys on that, not on the `PTOOLS_IN_REGION` that `fire211.sh`
  exports, so it took the *laptop* credential path.
- **no `exit $RC`** — the container's status was `tail`'s, which always succeeds, so **Batch would
  have reported SUCCEEDED on an OOM**.

Caught before it burned, but only by sizing the input by hand (54.85 GB across 10 stores) and
comparing it to the cgroup. All three defects lived in an uncommitted script invoked by a
hand-composed shell string.

**Common factor across all four:** the defect was in an uncommitted script, staged from a mutable
prefix, invoked by a hand-composed shell string, and discoverable only by a human reading source.

---

## 6. Drift table — six versions of one script across 45 jobs in three days

Measured by resolving each job's `download:` timestamp against `list-object-versions` for
`tools/fanout_multi.sh`:

| versionId (prefix) | last modified (UTC) | size | jobs | example job |
|---|---|---:|---:|---|
| `tNORwXJkf_Uy…` | 2026-09-12 17:37:09 | 6143 | 4 | `cd2fill-run4-percell` |
| `xmCQhuQ3qjMz…` | 2026-09-14 04:17:23 | 6134 | 2 | `cd2fill-run3-percell-v2` |
| `kCYCk8wXnltI…` | 2026-09-14 04:37:56 | 6167 | 6 | `cd2fill-diag-run2-ms` |
| `jSKddtEvci8a…` | 2026-09-14 06:10:01 | 6266 | 15 | `cd2fill-probe-metab-noorder` |
| `.g5hoHG5xV.k…` | 2026-09-14 14:52:53 | 7174 | 7 | `cd2fill-run2-multiseed-806` |
| `fbVNUalsek5k…` | 2026-09-14 21:36:36 | 7417 | 11 | `cd2fill-run3-core5-s0-3` |

45 jobs, six distinct versions of a single script, three days.

---

## 7. The backfill artifact that already exists

**`assets/ptools/cd2_tool_runs.json`** (0.8 MB), also at `s3://.../tools/cd2_tool_runs.json`:

| key | contents |
|---|---|
| `jobs` | **46** cd2fill jobs — full `container_job_cmd`, status, exit code, timestamps, image commit, log stream, and `staged_tools` with a resolved `version_id` + `sha256` per staged file |
| `scripts` | **32** distinct tool versions, **exact text bytes embedded**, keyed by sha256 (483,227 bytes total) |
| `staged_binaries` | **2** sim_data pickles (`cand2_j3_lam075_simData.cPickle`, `carina_run4_genotype38_simData.cPickle`, ~86.5 MB each) — sha256 + versionId only, **content deliberately not embedded** |
| `fetch_failures` | absent — 0 failures |

Version-link resolution quality: **546 links, `exact=546`, `ambiguous=0`** — no job downloaded a file
during a window where a restage made the version indeterminate.

Generators (re-runnable, both in `assets/ptools/`):
- `harvest_cd2_tool_runs.py` — walks Batch + CloudWatch, resolves each staged file to the S3
  `versionId` current at its download timestamp
- `embed_cd2_scripts.py` — fetches each `(key, versionId)` pair and embeds the bytes by sha256

### Jim's explicit framing — do not get this backwards

When told that Batch and CloudWatch both retain 7 days, Jim said:

> "7 days is plenty to ressurect the important tool runs"

and then, correcting a misreading:

> "I mean 7 days is sufficient for a proper backfill, not that as long as we have a 7 day lookback
> from now on, this is sufficient"

**So:** the window is sufficient to do a **proper one-time backfill** — which is what
`cd2_tool_runs.json` is. It is explicitly **not** an endorsement of a rolling 7-day lookback as a
steady state. **A cron that re-harvests weekly is not the answer and should not be proposed as one.**
The harvest is *backfill input* for whatever #655 becomes; the mechanism is still owed.

The file is self-contained by design: it no longer depends on Batch retention, CloudWatch retention,
the bucket's versioning, or the bucket existing.

---

## 8. The sketch discussed so far — **a sketch, not a decision**

Recorded here only so the discussion does not restart from zero. Nothing below is settled; all of it
is open to being discarded.

**Schema sketch.** No existing table stores executable text. Current tables (from
`viva_api/simulation/tables_orm.py`): `simulator`, `hpcrun`, `hpcrun_event`, `hpcrun_span`,
`parca_dataset`, `simulation`, `worker_event`, `analysis`, `task`.

```sql
tool_script                      -- content-addressed; identity IS the hash
  sha256        TEXT PRIMARY KEY
  name          TEXT NOT NULL    -- 'fanout_multi.sh'
  content       TEXT NOT NULL    -- the exact bytes that ran
  size_bytes    INTEGER NOT NULL
  first_seen_at TIMESTAMPTZ
  source_uri    TEXT             -- s3 uri + versionId, when staged rather than inlined

tool_run                         -- one row per dispatch
  id              PK
  script_sha256   FK -> tool_script
  args            JSONB          -- {"FAMILY":"run3","LO":0,"HI":3,"FORCE":true}
  env             JSONB          -- redacted via a key allowlist
  image           TEXT
  queue / resources
  batch_job_id / batch_job_name
  targets         JSONB          -- store prefixes read
  dest_prefixes   JSONB          -- prefixes written  (overlaps #648's registration)
  status / submitted_at / started_at / finished_at / exit_code
  hpcrun_id       FK NULL
```

Content addressing gives free dedupe (same bytes → one row regardless of run count) and exact
diffing between two runs.

**Endpoint sketch.**
```
POST /api/v1/tools/runs
{ "tool": "fanout_multi.sh",
  "tool_version": "latest",          # RESOLVED to an immutable id at submit time
  "image": "v2ecoli:33ecd77",
  "args": {"FAMILY":"run3","SCALES":"multiseed","LO":0,"HI":3,"FORCE":true},
  "env":  {"PTOOLS_SKIP_N_GENS":"1"},
  "targets": ["sim200-cd2-run3-sweep-combo00-25fc"],
  "dry_run": true }

GET /api/v1/tools/runs/{id}          # status + resolved pins + args + dest prefixes
GET /api/v1/tools/runs/{id}/script   # the exact bytes that ran
GET /api/v1/tools/runs?tool=&status= # history
```

The claimed key property: resolving `latest` to an immutable id **at submit time** and writing
content to a content-addressed key (`tools/by-sha/<sha256>`) means a mid-flight restage **cannot**
change a running job — structural, not policy. That is the direct answer to §5(a).

---

## 9. Hard constraints any design must respect

| constraint | detail |
|---|---|
| **Batch command cap** | 8192 characters. **Already hit once this campaign.** Scripts are 3–7 KB and base64 inflates ~33%, so the script must **not** be inlined/base64'd into `CONTAINER_JOB_CMD`. This is why the sketch uses a content-addressed S3 key. |
| **Secret redaction** | `env` is free-form and will eventually carry a credential. Needs a key allowlist on write. |
| **Alembic + fingerprint contract** | One revision. Per this repo's `CLAUDE.md`, you **must also add a `LEGACY_FINGERPRINTS` marker** in `viva_api/simulation/db_reconcile.py` whenever you add a migration, or a `create_all`-bootstrapped DB stamps stale and **silently re-applies** migrations. Note the module's own comments currently say the *opposite* ("never add entries for migrations authored after adoption") — the comments are wrong, the code and CLAUDE.md are right. |
| **Related Alembic landmines** | **#637**: `alembic upgrade head` fails on a genuinely empty DB (`d3f9a1c72b84` adds a column to `analysis`, which no migration creates) — dormant for existing sites, live for any new one. A detailed audit of this contract and its traps is at `~/.claude/plans/virtual-launching-pine.md`. |
| **`make check` is two-pass** | ruff-format rewrites files and exits 1. The **first** run on new code always fails. Never chain it through a pipe. |
| **`make spec` leaks `.dev_env`** | Regenerating the OpenAPI spec bakes your untracked dev env into it. Override `SIMULATION_OUTDIR` / `HPC_SIM_BASE_PATH` and grep the diff before committing. |
| **Tests must never reach live infra** | `localhost:8080` **is** the SSM tunnel to the live cluster. A unit test once came one field away from a real Batch dispatch. Check conftest isolation. |
| **Merge style** | viva-api uses **merge commits** (never squash/rebase). sms-ecoli is **squash-only**. |

---

## 10. Explicit non-goal — state this plainly in any design

**This delivers provenance and reproducibility, NOT authorization.**

Arbitrary shell recorded in a database is still arbitrary shell. Capturing it makes every run
auditable, diffable and replayable *after the fact*; it does **not** provide the pre-execution review
that committing the toolkit to a repo and publishing via CI would.

The two are complementary, not alternatives:
- The **DB capture** half closes the "what ran?" gap and ships independently.
- The **repo + CI** half closes the "was it reviewed?" gap. Natural home is `sms-ecoli/scripts/`
  alongside `combine_run4_fss.py` — and that script's 155-line drift is the existing evidence of what
  happens without it.

An allowlist on `tool.name` layers on top of the DB half if pre-execution control is wanted.

---

## 11. Operational context — what the new session must NOT disturb

**The session that wrote this brief is still live and babysitting 11 running AWS Batch jobs.** It
holds armed watchers and a 20-minute `/github-sync` cron. Do not interfere with it.

**Specifically, the new session must not:**
- kill, re-fire, or modify any `cd2fill-*` Batch job;
- **write anything under `s3://smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91/tools/`** — jobs
  stage that prefix at container start, so an edit changes what in-flight and about-to-start jobs run.
  This is not hypothetical; it is exactly incident §5(a).

Reading is fine. Coordinate through Jim before any write.

**Jobs in flight as of ~22:40Z** (all on `smsvpctest-ray-standalone`, image `33ecd77` = simulator 211):

| job | started | note |
|---|---|---|
| `cd2fill-run3-core5-s{0-3,4-7,8-11,12-15,16-19,20-23,24-27,28-31,32-35}` | ~21:40Z | 9 shards × 4 combos = 36; ~198 min/combo; first verdicts ~00:58Z |
| `cd2fill-run2j3-core5-skip1` | ~21:21Z | single store, 5 core views |
| `cd2fill-run1-coupled-core5-skip1-v2` | ~22:28Z | combine-first over 10 stores (54.85 GB); highest-risk job |

**CD2 scope context, only as far as needed:** Eran cut the ptools deliverable to **multiseed core-5
only** on sms-ecoli#166 at **20:50Z tonight** (`ptools_{rna,rxns,proteins,metabolites,overview}_multiseed`,
`skip_n_gens=1`), dropping all per-cell, all multigeneration, and the `cd1_*` omics. Nine out-of-scope
jobs were killed. Run 3 is firing as 9 shards (per-combo, 180 TSVs); Run 1 as a single combine
(5 TSVs, granularity settled mechanically); **Run 4's granularity is still unanswered by Eran**, and
there is an unresolved question from Chris (09-11) that he was "99% sure" Run 4 ptools are out of
scope for SRI. None of this constrains the #655 design — it is context for why the toolkit is being
hammered tonight.

---

## 12. Open questions the design discussion has to settle

These are genuinely open. I have views on some; they are not decisions.

1. **Fold into #631, or keep separate?** #631 is the execution verb and already promises to "write
   provenance" without defining it. #655's schema is what #631 would need anyway. Two issues or one
   build? If folded, does #655 close or become #631's sub-task?

2. **Is an allowlist / review gate in scope for v1, or deferred?** §10 says capture ≠ authorization.
   Shipping capture alone is strictly better than today and is independently useful. But shipping an
   endpoint that runs arbitrary shell *as a sanctioned API* is a different posture from a human
   hand-dispatching it. Does v1 constrain `tool` to a registry, or accept anything and record it?

3. **Does the toolkit move to a repo as part of this, or after?** The DB half works on unreviewed
   scripts. The repo half is the only thing that gives pre-execution review. If the answer is
   "after", the 155-line `combine_run4_fss.py` drift should probably still be reconciled now, since
   it is live evidence of the failure mode.

4. **New tables, or reuse `hpcrun` / `task`?** `tool_run` overlaps both. `hpcrun` already carries
   backend/status/job-id plumbing and the observability tables (`hpcrun_event`, `hpcrun_span`) hang
   off it. Is `tool_run` a new table with an `hpcrun_id` FK, or a *kind* of `hpcrun`, or a row in
   `task`? This decides whether ad-hoc runs show up in `/events` and `/tasks` for free.

5. **How much of #648's registration should this subsume?** `tool_run.dest_prefixes` already captures
   where a run wrote. #648 needs an `analysis` row per bundle. If `tool_run` records the dest, the
   analysis registration is arguably a derived write from it — which would close #648's registration
   half automatically for everything dispatched through the endpoint, leaving only the one-time
   backfill of already-landed bundles. Is that in scope, or does it belong to #648?

6. *(smaller, but real)* **Does the endpoint own execution, or only recording?** A design where
   viva-api records a run that *something else* dispatched is much cheaper and could wrap the existing
   hand-dispatch path without replacing it. A design where viva-api submits the Batch job is the
   #631 shape. These have very different blast radii.

7. *(smaller)* **What is `tool_version: "latest"` allowed to mean?** Resolving at submit time is the
   whole point, but "latest" of *what* — the S3 prefix, a repo tag, a registry entry? This determines
   whether the repo question (3) is a prerequisite or not.

---

## Appendix — file map

| path | what |
|---|---|
| `assets/ptools/cd2_tool_runs.json` | the backfill: 46 jobs, 32 embedded scripts, 546 exact version links |
| `assets/ptools/harvest_cd2_tool_runs.py` | regenerates the job/version half |
| `assets/ptools/embed_cd2_scripts.py` | embeds script bytes by sha256 |
| `assets/ptools/cd2_ptools_manifest.json` | 183 stores: provenance + fill-bundle coverage (built for a different question, useful context) |
| `assets/ptools/cd2_ptools_manifest.tsv` | same, flat |
| `assets/ptools/cd2_manifest_reconciliation.md` | reconciliation of that manifest against Alex's CD2 deliverables report |
| `viva_api/simulation/tables_orm.py` | existing tables |
| `viva_api/simulation/db_reconcile.py` | `LEGACY_FINGERPRINTS` + the contract |
| `~/.claude/plans/virtual-launching-pine.md` | prior Alembic audit — contract traps in detail |
