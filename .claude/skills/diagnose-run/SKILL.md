---
name: diagnose-run
description: Work out why a whole-cell simulation run failed, stalled, or produced nothing — using /status, /tasks, /events, the two CloudWatch log groups, and the known blind spots. Use when a run says COMPLETED but produced no output, when a run is stuck, when a Batch job exits 0 with nothing to show for it, or when asked "what happened to sim N".
---

# diagnose-run

The failure mode this codebase produces most often is **silent success**: `rc=0`,
Batch `SUCCEEDED`, a green rollout, an exit-0 Job — and no result. Every step below
exists because that shape has bitten us. Reference: [`docs/OBSERVABILITY.md`](../../../docs/OBSERVABILITY.md).

## Before anything: is it even instrumented?

Five ways a run produces no events, and only one is a bug.

1. **The run predates the stack.** Events need *both* halves: an image with the
   instrumented engine (**simulator ≥ 207**, and **≥ 208** for anything `parca.*` or
   `analysis.*`) and a dispatcher that injects `PBG_*` (**viva-api ≥ 0.9.140**).
   Either half missing → `/events` returns `{"events":[],...}`. An old simulator
   no-ops the emitter *by design*, so this is silent. **0.9.139 specifically is a
   trap**: it injected on Nextflow/chain/MBP/MNP-composite but NOT on the MNP
   ParCa+sim pair or the gather (#641), so a run there shows a populated `trace_id`
   with zero events — which looks exactly like case 4. A populated `trace_id` proves
   the API derived one, NOT that the task was told it.
2. **Hand-dispatched job** (`aws batch submit-job` + `CONTAINER_JOB_CMD`) → bypasses
   viva-api entirely. No `HpcRun` row, no `PBG_*`, nothing in the API. Expected.
3. **A standalone re-analysis** (`POST /simulations/{id}/analysis`, `atlantis
   simulation analysis`) on viva-api **≤ 0.9.141** → that dispatch hand-builds its
   K8s Job env and injected no `PBG_*` at all, so it emits nothing while looking
   exactly like case 4. Fixed in viva-api **#646 — merged, not deployed** as of
   2026-09-14.
4. **Stdout-only namespace** → events exist, in CloudWatch, but nothing ingests them
   so the API stays empty. **Do not check `EVENTS_S3_PREFIX` alone**: it is unset on
   stanford-test and the S3 sink is still on, because `events_s3_prefix()` derives
   from `S3_WORK_BUCKET` when the explicit template is empty. Only a namespace with
   *neither* variable is stdout-only. (Verified 2026-09-14: sim 1319 returned 452
   ingested events on stanford-test with `EVENTS_S3_PREFIX` unset.)
5. **A `debug`-level event, which is never stored as a row** — see Blind spots. It is
   in S3 and CloudWatch, just never in `/events`.

Check 1 first — `atlantis simulation status <id>` gives you the simulator. **An empty
array on an old run is not a finding.**

## The ladder

```
atlantis simulation status <id> --base-url <url>   # what the API believes, and when it last heard
atlantis simulation tasks  <id> --base-url <url>   # WHICH unit failed
atlantis simulation events <id> --level error      # what it said
atlantis simulation events <id> --tree             # where in the span tree
atlantis simulation events <id> --chrome-trace t.json   # open at ui.perfetto.dev
```

`--chrome-trace` (viva-api #645) is a client-side transform over what `/events`
already returns, so it works against an older deployed API from a current checkout.
Reach for it when the question is *where the wall clock went* rather than *what
failed*; lanes are packed by overlap, so the lane count is real concurrency.

Read in this order and stop when you have the answer.

* **`status`** — `stage`, `generation`, `last_event_at`, `attempt`, `exit_code`,
  `error_source`. A `running` status with a stale `last_event_at` means a task stopped
  talking; that is different from a task that failed.
* **`error_source`** names which authority decided the status: `nextflow_trace`,
  `k8s_condition`, `failure_record`. **They disagree, and that disagreement is
  informative** — a head that exits 0 with a dead task reads COMPLETED to
  `k8s_condition` and FAILED to `nextflow_trace`. Trust the trace.
* **`tasks`** — the "which of the 40 things" answer. Nextflow rows come from
  `trace.csv`, chain rows from Batch. **Empty on the MNP path by design** (one Batch
  job with node ranges, no tracked fan-out) — verified on sim 1317, where `/events`
  had engine events and `/tasks` was `[]`. Check the backend before calling it a bug.

## CloudWatch: the two-log-group trap

**This wastes twenty minutes every single time it is not remembered.**

| dispatch path | log group |
|---|---|
| chain / container jobs | `smsvpctest-ray-batch-…` |
| Nextflow tasks | `/aws/batch/job` |

Querying the wrong one gives `ResourceNotFoundException`, which reads **exactly** like
"the job produced no output". Never conclude a job was silent from that error. Get the
group from the job definition:

```bash
aws batch describe-job-definitions --job-definition-name <name> --status ACTIVE \
  --query 'jobDefinitions[0].containerProperties.logConfiguration.options'
```

MNP jobs: the **parent** has a null `logStreamName`; the stream is on the child node
job (`<parentId>#<nodeIndex>`).

## Blind spots — do not look for events that cannot exist

**The gather/ParCa interiors were uninstrumented — that changed on 2026-09-14.**
The old rule ("`analysis_runner` calls `.analyze()` directly, ParCa's DAG runs at
composite *build* time, so no engine hook fires") held until **simulator 208**
(sms-ecoli `c57571d`, v2ecoli#799), which spans both **without** a Composite:
`parca.step`, `analysis.name`, `analysis.group`, `analysis.sample`. **Simulator 209**
(v2ecoli#800) then opened the fitting loop: `parca.fit_condition` plus its
`.converged` / `.diverged` / `.progress`. On an older image their absence is expected
and is not a finding — check the simulator before calling it a gap.

Three live gaps remain, and each one has cost time:

* **`debug`-level events never reach `/events`.** viva-api #642 drops them at ingest
  (on top of `tick`). That silently includes **`analysis.sample`** — the
  `duckdb_temp_mb` / `duckdb_memory_mb` / `rss_mb` readings taken *while the query is
  still running* — plus `parca.fit_condition.progress` and `lineage.debug`. They are
  in the S3 `events.jsonl` and in CloudWatch; read those, or set
  `EVENTS_INGEST_STORE_DEBUG=true`. **The numbers you most want for an OOM are
  exactly the ones in this bucket.**
* **ParCa worker spans orphan to the trace root** (v2ecoli#805, open). Span context
  does not cross the process boundary: each worker configures its emitter from the
  same `PBG_TRACEPARENT` with empty ContextVars, so `start_span` parents to the
  campaign root. Measured on sim 1319: **45 of 51** `parca.fit_condition` spans
  orphaned, only the 6 from the main process nesting. "Which `parca.step` did this
  condition belong to" is therefore unanswerable from `--tree` — the S3 event objects
  are per source process, so read the parents per pid instead.
* **`stage` renders parallel siblings as a fake chain** on ≤ 0.9.141
  (`parca.step > parca.fit_condition > … x7` for ONE level with seven siblings).
  Fixed in viva-api #644, merged not deployed. Do not read depth off `stage`.

For an analysis failure the ladder still effectively ends at `tasks` plus the S3
event objects; the free-form interior output is CloudWatch only.

## Analysis / DuckDB failures specifically

Known and measured, so do not re-derive:

* **`OutOfMemoryException … (166.8 GiB/166.8 GiB used) … max_temp_directory_size`** —
  that ceiling is **the disk**, not a setting to raise. DuckDB defaults
  `max_temp_directory_size` to ~90 % of the volume; the task hosts have a 200 GB gp3
  root. Raising the number converts a clean error into ENOSPC.
* **`MEM_LIMIT` passed to the regen scripts never reaches DuckDB.**
  `apply_analysis_duckdb_config` (`sweep_io.py:187`) runs right after
  `create_duckdb_conn` and overwrites `memory_limit` with `analysis_memory_limit()`
  (~70 % of cgroup). The env var that *does* win is **`V2E_ANALYSIS_MEMORY_LIMIT`**.
* **Each wide multigeneration view is individually unbounded** — measured
  2026-09-13: rna / rxns / proteins / metabolites each exhausted 166.8 GiB *alone*
  on a 5.83 GB dataset. Per-view splitting does not help.
* **The connection factory shows the PRE-override state.** `apply_analysis_duckdb_config` runs
  afterwards *in the caller*, so settings read inside the wrapped `create_duckdb_conn` report
  DuckDB's default (measured: `memory_limit = 46.8 GiB`, i.e. 80 % of a 58.6 GiB cgroup) while the
  query actually runs at **41.0 GiB** = `analysis_memory_limit()` = 70 % of cgroup. To see the real
  state, wrap `apply_analysis_duckdb_config` and read *after* it.
* **`preserve_insertion_order=false` was tried and does NOT fix the metabolites OOM** (2026-09-14).
  It verifiably applied (`current_setting` = False) and the failure was byte-identical. Do not
  re-litigate it; DuckDB's suggestion list is not a fix list.
* **A multiseed kill is memory OUTSIDE DuckDB's budget.** Run-2 multiseed was SIGKILLed at the full
  58.6 GiB cgroup while DuckDB's honoured cap was 41 GiB — so raising `memory_limit` cannot help.
* **v2ecoli#801 fixes `multigeneration` for rna/rxns/proteins/overview but NOT `metabolites`**, and
  does not touch `multiseed` at all. Validated in-region: rxns went 32.5 GiB -> 2.0 GiB peak RSS and
  216 s failing -> 123 s passing, controlled with only the image varying.
  **The two remaining holes are now closed upstream**, so check the image before
  re-deriving either: `metabolites` by sms-ecoli#410 and `multiseed` by v2ecoli#806,
  both first built as **simulator 211** (sms-ecoli `33ecd77` -> v2ecoli `1283f4bc`).
  #801 + #804 alone are simulator 210.
* **Check whether it is a RAM wall or a disk wall before tuning.** For these views spill peaked at
  **0.1 GiB** against ~180 GiB of headroom (`temp_directory=/tmp` on the 199 GiB root) — it is RAM.
* DuckDB's own error text names its candidate fixes (`SET threads`,
  `SET preserve_insertion_order=false`, `SET memory_limit`). Worth trying, but see the
  next section before believing a null result.

## `rc=0` proves nothing — the two mechanisms, both measured

**A pipeline hides a killed process.** `driver | grep …` (or `| tail`) returns the *downstream*
command's status, so with no `set -o pipefail` a driver killed by SIGKILL still yields `rc=0`, and the
Batch container logs `exited rc=0`. **Measured 2026-09-14** on the Run-2 multiseed fill:

```
bash: line 1:  53 Killed   python -u ptools_flush.py ... multiseed ...
DRIVER_RC = 137            # 128+9 = SIGKILL
memory.events: oom 1  oom_kill 1
memory.peak  = 62914560000 # EXACTLY memory.max (58.6 GiB)
[batch-container] exited rc=0     <-- container reported SUCCESS
```

**A cgroup OOM leaves no traceback.** SIGKILL is not an exception: no Python traceback, no `status:`
line, nothing. An empty driver log plus a clean exit is the signature — go read
`/sys/fs/cgroup/memory.events` (`oom_kill`) and `memory.peak`, and capture the driver's *own* exit
code by redirecting to a file instead of piping.

## When you add a diagnostic, make sure it can be seen

Learned the hard way, twice, on 2026-09-13:

* The Run 1 fanout pipes the driver through **`grep -E "status:|ERR "`**. Any line not
  matching those patterns is discarded. A `[probe]` settings line added to prove a knob
  applied was silently eaten — and its absence looked identical to "the knob did not
  apply". **Widen the grep in the same edit.**
* **A pipe buffers, so "not there yet" and "never" look identical while a job runs.**
  `… | grep -E … | sed …` — both `grep` and `sed` block-buffer (4 KB) when stdout is
  a pipe rather than a tty, so every matched line lands in CloudWatch in one burst
  *when the driver exits*. Measured on `run1probe-c-rna`: the `status:` and `ERR`
  lines share a single timestamp (18:24:24) at the end, while the unpiped `[spill]`
  monitor streamed every 30 s throughout. Do not read a mid-run absence as a null
  result. Use `grep --line-buffered` / `sed -u`, or `stdbuf -oL`, when you need the
  diagnostic live.

* Printing a setting at *connect* time proves you set it, not that it survived.
  `apply_analysis_duckdb_config` runs afterwards. Re-read settings **after** it to show
  the state the queries actually ran under. "I set it" and "it was set when the query
  ran" are different claims.

## Conclusions to state carefully

* **A guard that checks the wrong thing is worse than no guard.** A deploy script adapted with
  `sed` printed *"render carries 0.3.84 in 4 places"* while its grep still matched the OLD version
  (the pattern contained backslashes the sed never touched, but the echo string was rewritten). It
  reported total success and deployed nothing. Make a guard read the value it is asserting on and
  compare it to a variable — never grep a hardcoded literal next to an echo that can drift from it.
* `rc=0` / `SUCCEEDED` proves nothing. Check for the artifact (TSV count, S3 upload,
  row in the DB), not the exit code.
* A tunnel **504 is not a server failure** — the ALB drops a silent request while the
  server keeps working (measured: client 504 at 60.1 s, server 200 at 126–200 s). The
  dev ALB is 600 s now, but check the pod log before concluding.
* A migration Job that exits 0 may have applied **nothing** — a stale overlay pin runs
  an image without the migration. Verify with `db_reconcile --analyze` (read-only),
  not with the Job's exit code.
* `db_reconcile --analyze`'s `head revision` is a property of **the image it runs in**,
  not the database. Running it in an old pod after a migration shows `current` ahead of
  `head`; that is the artifact, not a problem.
