---
name: diagnose-run
description: Work out why a whole-cell simulation run failed, stalled, or produced nothing — using /status, /tasks, /events, the two CloudWatch log groups, and the known blind spots. Use when a run says COMPLETED but produced no output, when a run is stuck, when a Batch job exits 0 with nothing to show for it, or when asked "what happened to sim N".
---

# diagnose-run

The failure mode this codebase produces most often is **silent success**: `rc=0`,
Batch `SUCCEEDED`, a green rollout, an exit-0 Job — and no result. Every step below
exists because that shape has bitten us. Reference: [`docs/OBSERVABILITY.md`](../../../docs/OBSERVABILITY.md).

## Before anything: is it even instrumented?

Four ways a run produces no events, and only one is a bug.

1. **The run predates the stack.** Events need *both* halves: an image with the
   instrumented engine (**simulator ≥ 207**) and a dispatcher that injects `PBG_*`
   (**viva-api ≥ 0.9.139**, rolled on stanford-test 2026-09-13 18:52Z). Either half
   missing → `/events` returns `{"events":[],"trace_id":null}` and `/tasks` returns
   `[]`. An old simulator no-ops the emitter *by design*, so this is silent.
2. **Hand-dispatched job** (`aws batch submit-job` + `CONTAINER_JOB_CMD`) → bypasses
   viva-api entirely. No `HpcRun` row, no `PBG_*`, nothing in the API. Expected.
3. **Stdout-only namespace** → events exist, in CloudWatch, but nothing ingests them
   so the API stays empty. **Do not check `EVENTS_S3_PREFIX` alone**: it is unset on
   stanford-test and the S3 sink is still on, because `events_s3_prefix()` derives
   from `S3_WORK_BUCKET` when the explicit template is empty.
4. **The analysis/gather interior** → never instrumented on any path (see Blind spots).

Check 1 first — `atlantis simulation status <id>` gives you the simulator. **An empty
array on an old run is not a finding.**

## The ladder

```
atlantis simulation status <id> --base-url <url>   # what the API believes, and when it last heard
atlantis simulation tasks  <id> --base-url <url>   # WHICH unit failed
atlantis simulation events <id> --level error      # what it said
atlantis simulation events <id> --tree             # where in the span tree
```

Read in this order and stop when you have the answer.

* **`status`** — `stage`, `generation`, `last_event_at`, `attempt`, `exit_code`,
  `error_source`. A `running` status with a stale `last_event_at` means a task stopped
  talking; that is different from a task that failed.
* **`error_source`** names which authority decided the status: `nextflow_trace`,
  `k8s_condition`, `failure_record`. **They disagree, and that disagreement is
  informative** — a head that exits 0 with a dead task reads COMPLETED to
  `k8s_condition` and FAILED to `nextflow_trace`. Trust the trace.
* **`tasks`** — the "which of the 40 things" answer. Nextflow rows come from
  `trace.csv`, chain rows from Batch.

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

**The gather/analysis interior is uninstrumented on every path.** The ptools analyses
are `Step` subclasses, but `analysis_runner` calls `.analyze()` directly and
`workflow_nf` deliberately does not wrap analysis. No `run.start`, no `tick`, no
`process.exception`, no spans. The dispatcher layer gives you the stage *boundary* and
the `.command.err` tail; the interior is CloudWatch only.

So for an analysis failure the ladder ends at `tasks`, and you go to the log group.

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
* DuckDB's own error text names its candidate fixes (`SET threads`,
  `SET preserve_insertion_order=false`, `SET memory_limit`). Worth trying, but see the
  next section before believing a null result.

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
