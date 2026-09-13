# Real Nextflow artifacts for viva-api's trace/log tests

Produced on 2026-09-10 with Nextflow 25.04.3 (`nextflow -version`: build 5949) on a
laptop, from the toy workflow in `main.nf` (three processes shaped like a v2ecoli
campaign: `parca_v0` -> `lineage_v0_s0` -> `analysis_v0`, plus an opt-in `flaky_v0`
that exits 137 on attempt 1 under `errorStrategy 'retry'`). `nextflow.config` is
exactly what process-bigraph 1.8.4's `generate_nextflow_config(executor='local')`
emits. No AWS, no science.

| file | how |
|---|---|
| `trace.ok.csv` | `nextflow -C nextflow.config run main.nf -profile local -with-trace trace.ok.csv -work-dir work-ok` — every task COMPLETED |
| `trace.cached.csv` | the same command again with `-resume` — every task CACHED |
| `trace.retry.csv` | `... --flaky true -with-trace trace.retry.csv -work-dir work-retry` — `flaky_v0` FAILED (exit 137) then COMPLETED on attempt 2 |
| `trace.failed.csv` | `... --fail_analysis true -with-trace trace.failed.csv -work-dir work-failed` — `analysis_v0` FAILED (exit 1) after the other two completed; the head exited non-zero |
| `nextflow.failed.log` | the `.nextflow.log` of that failed run, verbatim |
| `command.err.failed` | the failed `analysis_v0` task's `.command.err` (`work-failed/5c/fdb0811581f9f8de0dfbe254e5b6bd/.command.err`) |

The trace `hash` column is the short form (`5c/fdb081`); the task's work dir is
`<work>/5c/fdb0811581f9f8de0dfbe254e5b6bd`, which is what
`viva_api.common.hpc.nextflow_trace.find_task_workdir` resolves by listing the
`5c/` prefix. Regenerate with the commands above if the Nextflow trace format
changes; the tests read these files rather than hand-written rows for every case
a local run can produce (a Spot-reclaimed task with no exit code cannot be, so
that one row stays hand-written in `tests/common/hpc/test_nextflow_trace.py`).
