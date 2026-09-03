# Multi-generation dispatch: chain-dispatch vs. pbg-native vs. Nextflow, and what's left

**Status (2026-09-02):** the headline defect this analysis found — chain-dispatch
generation jobs never engaging `LineageProcess` — was found and fixed independently
the same day by Alex as **item 103, viva-api PR #369 (v0.9.84)**, and verified by
MD5-comparing the daughter-state checkpoint across 3 real generations (sim 257:
byte-identical under the bug, all different after). This document reproduced that
diagnosis from code and git history alone, which corroborates it; the fix and its
regression test (`test_stop_at_division_is_always_set`) are on `main`. What follows is
the comparison the analysis was for, plus the work that is **still** open.

> **Line-number caveat.** v2ecoli references below were read from a local
> `origin/main` ref dated 2026-08-25 (`c28fc546`). The tree the image pins is
> newer — Chris reads the dispatch gate at `ecoli_baseline.py:1730`, not `:1574`,
> and v2ecoli #610 (`2ecb11ca`) is not in that ref at all. Treat v2ecoli line
> numbers as approximate; viva-api numbers are against `main` at 0.9.85.
> Full technical trace for item 103: `vivarium-workbench/.todo/backlog/103.md`.

## 1. Three ways a multi-generation campaign runs today

| | **Nextflow** (vEcoli-private, `COMPUTE_BACKEND=batch`) | **chain-dispatch** (item 71/103) | **pbg-native** (item 101) |
|---|---|---|---|
| Engine | Nextflow, launched as a K8s Job by viva-api (`simulation_service_k8s.py:266`) | viva-api's `JobScheduler` poll loop (`job_scheduler.py:183`) | Ray inside one MNP Batch job (`v2ecoli.composites.lineage_ray_batch`) |
| Unit of Batch work | one task per (variant, seed, generation, branch) | one container job per (seed, generation) | one MNP job for the whole campaign |
| Gen *i*→*i+1* | channel plumbing: `sim_gen_{i+1}(sim_gen_i_nextGen)`, unrolled in Python (`workflow.py:714-791`) | app-side gating: submit *i+1* only after *i* SUCCEEDED; no `dependsOn` since 0.9.52 | in-process: `LineageProcess` carries the daughter as a Python object |
| Daughter hand-off | URI in an 11-tuple `val()`, JSON written by the sim | deterministic S3 key `daughter-state/seed{N}/gen{G}.pkl` (`data_layout.py:150`) | no serialization at all |
| Jobs for 1000×10 | 10,000 | 10,000 (≤1000 in flight) | 1 |
| Binary tree | opt-in (`single_daughters=false`) | no — one in-flight job per seed by schema | no — `LineageProcess` raises per tick |
| Variants | first-class, count discovered at runtime | none | via composite params |
| Analysis fan-in | 5 scales, size-hinted `groupTuple` | one whole-campaign job after all seeds resolve | flush once on the driver |
| Proven at | production | 1000×10 (item 71), 1×3 post-fix (sim 257) | 100×10 (sim 254) |
| Script | — | `chain-dispatch.sh` | `pbg-dispatch.sh` |

The two v2 mechanisms are complementary, not competing. Chain-dispatch buys
generation-granularity retry/resume and Spot-safety at the cost of N×G submissions
and a lossy serialization boundary every generation. pbg-native buys zero boundaries
and one submission at the cost of a gang-scheduled on-demand cluster, no mid-run
autoscaling, and one node failure killing the campaign. The memory note in
`parallel_seeds.py:70-75` (7–9 GB RSS by generation 3–4 on a full 1000×10) cuts
against pbg-native on deep lineages.

## 2. Why the item-103 bug was invisible (record, for the next one like it)

`_seed_generation_command` sent `n_seeds=1, n_generations=1` plus the three
checkpoint keys. v2ecoli's gate (`ecoli_baseline.py:1574`) is
`n_seeds > 1 or n_generations > 1 or stop_at_division` — False — so the plain
single-cell build ran, the keys were inert (documented "Batch runs only"), and
`-n 1` meant one simulated second. Exit 0, Batch SUCCEEDED, campaign green.

Three things concealed it, all worth remembering:

- **`-n 1` is a batch-mode idiom.** It fires the one-shot `BatchBaselineRunner`
  Step. The superseded `_sim_command` branch (`simulation_service_ray.py:747-752`)
  passed real `n_seeds=N, n_generations=G` with the same `-n 1` and *says* why:
  "n_seeds/n_generations > 1 switches it into the batch/lineage shape." The
  item-33 rework kept the idiom and changed the overrides to `1/1`.
- **`run_pbg.py:81-105` stamps the hive partition itself** from
  `initial_generation_index`, so output *looked* per-generation.
- **Git history:** the keys were only ever added to the batch path
  (`fbfd24408 "…through ecoli_baseline's batch path"`); the gate at that commit was
  already `n_seeds > 1 or n_generations > 1`. It never worked.

The one-command detector, for any campaign:
`aws s3 ls s3://…/<experiment_id>/daughter-state/seed0/` — zero objects means
the chain didn't chain. Worth adding as a post-run assertion to `chain-dispatch.sh`.

## 3. Still open on `main` (0.9.85)

### 3a. Timed-out generation writes no checkpoint

`LineageProcess` skips the checkpoint when a generation hits `max_duration_per_gen`
without dividing (`lineage.py:453-454`, guarded on `daughter is not None`). Summary
records `"divided": false`; nothing raises. The scheduler then submits *G+1* pointing
`initial_carry_state_path` at an S3 object that does not exist.

Fix: in `JobScheduler._advance_seed_generations` (`job_scheduler.py:388-394`),
before submitting `gen+1`, require `RayLayout.daughter_state_uri(experiment_id,
seed, gen)` to exist. Absent ⇒ resolve the seed as failed. Test in
`tests/simulation/test_scheduler.py`.

### 3b. Stale narration that cost real time

- `viva_api/simulation/tables_orm.py:128` and `viva_api/simulation/models.py` —
  "AWS Batch's own dependsOn resolves each seed's chain natively." Untrue since
  0.9.52; the correct text sits in the block immediately below.
- `viva_api/common/storage/data_layout.py:168` — "still unwired from any HTTP
  router." It is wired (`submit_ecoli_simulation_job`, composite None + gens>1).
- `chain-dispatch.sh` header — "chained via `dependsOn`". Same stale model
  (external script, flag to Alex).
- `tests/integration/test_aws_batch_e2e.py:183-191` — asserts `chain_final_job_ids`
  non-empty right after `submit_chain_dispatch_job`, which now submits only ParCa.
  Skipped unless `AWS_BATCH_INTEGRATION=1`, so CI never catches it.
- Poll cadence: every chain docstring reasons about a "30s tick" — including the
  argument for why per-generation submits need no pacing — while `main.py:99`
  deploys 5s. At 5s the unpaced submit loop in `_advance_seed_generations` runs
  inside the open advisory-lock transaction.
- `viva_api/config.py:359-360` — `ray_array_queue` / `ray_array_job_definition`
  have no reader; already removed from prod's `shared.env`.

### 3c. `pbg-dispatch.sh` verification

Per its own header: the durable `n_workers` fix (v2ecoli #647, viva-api #366) needs
a simulator built from v2ecoli `8b293abf` or later; `SIMULATOR_ID=97` predates it,
so a dispatch with `N_WORKERS=""` against 97 re-runs the old default of 2.

### 3d. Open: `swap_processes` collapses every generation to one tick (post-#369)

From Chris's dispatches on `smsvpctest` (PR #375 thread, 2026-09-03). Same simulator
image, same ParCa dataset, `new_genes: off` on both sides, n=1 per cell:

| | `exclude_processes: ["exchange_data"]` | **empty** |
|---|---|---|
| **no swap** | — | **814.7 s / 805.4 s** per gen, 7 shards (`400 … 2528.pq`) |
| **swap** | sim262: 25–45 s/gen, `global_time 1.0` | **46.0 s / 25.6 s** per gen, `global_time 1.0`, one `1.pq` shard |

⇒ `exclude_processes` is not the trigger; **the swap alone is.** Wild-type chains
correctly post-#369; a swap config reproduces the exact pre-#369 symptom. Cause not
isolated. Not established: a real strain run (this was the swap *mechanism* only),
or a repeat.

`LineageProcess._run_until_division` has three OR'd division signals: (1) exception
sniff — now type-guarded and warned, and Alex saw zero warnings, so ruled out;
(2) `agents_after != agents_before` — any structural change to the agents map on
tick 1 fires it, `_remove` alone included; (3) `survivor.get("divide")` — the fresh
agent's `divide` flag. A swap that changes the agents map shape, or leaves `divide`
truthy on the rebuilt document, would trip (2) or (3) after one tick with no warning.

Next cut: log which signal fired at the generation close-out (one `warnings.warn`
per signal in `_run_until_division`), redispatch Chris's swap cell, read CloudWatch.
That separates the two remaining candidates in one run.

## 4. Hazards now live (chain jobs actually run `LineageProcess` since #369)

- **Division-by-exception is now type-guarded upstream** — *correction from Alex on
  PR #375.* The stale ref this analysis read shows a bare substring match
  (`"divide" in msg or "division" in msg`, `lineage.py:361-367`), and sms-ecoli has a
  measured case of `ZeroDivisionError` ("float division by zero") being read as a
  division (`sms_modules/processes/gillespie.py:331-341`). Since v2ecoli #610
  (`2ecb11ca`), `is_division_exception()` in `v2ecoli/library/division.py` excludes
  `ArithmeticError` and 8 other builtin types *by type*, and the caller
  `warnings.warn`s whenever it does treat an exception as a division. Alex checked
  CloudWatch for sim262/gen0: zero such warnings. So the exception signal is no longer
  the suspect for §3d — the other two signals are.
- **The JSON carry-state round-trip is lossy and untested.** `MetadataArray` metadata
  survives only under `unique`; nested objects degrade to truncated reprs; dict keys
  stringified; tuples not reconstructed (`v2ecoli/cache.py:45-127`). Every checkpoint
  test monkeypatches the I/O. The sibling *dill* path is known broken — resumed
  daughters never re-initiate replication
  (`sms-ecoli/docs/resume_dill_replication_init_bug.md`, open). Borrow its acceptance
  criterion: `divide_cell(roundtrip(state))` must equal `divide_cell(state)`
  field-for-field on the unique stores. sim 257's MD5 check proves the checkpoints
  *differ* across generations; it does not prove they are *faithful*.
- **`generations` is an absolute bound**, not a count (`lineage.py:460-461`,
  post-increment). `generations=1` + any `G` runs exactly one — fine. Any chunked-K
  design must send `G + K`; `run_native_chain.py` documents being burned by this.
- **Parquet and zarr disagree by one** on generation: parquet `self._generation`
  (0-based), zarr `len(agent_id)` (1-based).
- **The zarr store enforces the chain**: generation *N* refuses to open unless
  *N−1*'s partition exists *and* carries the success attr from that job's clean
  `close(success=True)`. A killed job breaks its successor.
- **The v2ecoli pin is a branch pin** (`sms-ecoli/pyproject.toml` `branch="main"`,
  locked to `268515f0`). `uv lock` moves `LineageProcess` under the deployment.

## 5. Verification

- Unit: `uv run pytest tests/simulation/test_ray_backend.py tests/simulation/test_scheduler.py`
- Smoke, on `smsvpctest` via the tunnel: `chain-dispatch.sh` (defaults = sim 257's
  1×3), then assert `daughter-state/seed0/gen{0,1,2}.pkl` all exist with distinct
  MD5s, three `generation=` parquet partitions, per-job runtime in minutes.
- Fidelity (follow-up): same seed, 3 generations chained vs. one pbg-native run;
  compare dry-mass-at-division per generation. Some divergence is by construction
  (listeners reset, RNG re-seeded per generation). A generation that never
  re-initiates replication is the dill bug's signature on the JSON path.
