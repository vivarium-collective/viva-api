# Multi-generation dispatch: chain-dispatch vs. pbg-native vs. Nextflow, and what's left

**Status (2026-09-04, rebased onto `main` @ `ee6f5775`, 0.9.90):** the headline
defect this analysis found — chain-dispatch
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
> numbers as approximate; viva-api numbers are against `main` at **0.9.90**.
> **§3d's fork is now resolved against the pinned tree — see §3d and §3e.**
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

## 3. Still open on `main` (0.9.90)

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

⇒ `exclude_processes` is not the trigger. **Chris's own correction (03:46Z):** the
swap alone triggers the one-tick completion *under chain-dispatch*. The same swap
runs fine locally — a hand-assembled 3-generation lineage produced real product
within ~6% of a cell-only lineage. So this is **not** "the swap is broken"; the
difference is in the dispatch path. Cause not isolated. Not established: a real
strain run (swap *mechanism* only, `new_genes: off`), a repeat, or *which local path*
the working run used (see below).

**What differs between local and chain-dispatch for the same swap.** Post-#369 every
chain job takes `baseline()`'s batch branch. The lineage layer does carry the swap:
`meta_composite.py:53` threads `config["injected_processes"]` into `LineageProcess`,
which passes it to the inner per-generation `baseline()` rebuild (`lineage.py:229,
240`). But the batch-dispatch call site *inside* `baseline()`
(`ecoli_baseline.py:1584-1594` on the 08-25 ref) forwards `knockouts`,
`config_overrides`, `media`, `variants` and the three checkpoint keys — **not
`injected_processes`** — and `_build_batch_document` has no such parameter;
`batch_baseline_runner.py` has zero references.

**Resolved: it is forwarded.** Chris checked the tree the image actually pins
(`268515f0`, PR #375 thread 04:23Z): `_build_batch_document` takes
`injected_processes: dict | None = None` at `:972`, the call site at `:1747` forwards
it alongside `knockouts`/`config_overrides`/`media`, and `batch_baseline_runner.py`
carries eight references (`:251-252` writing it into config, `:492` a schema default,
`:538` reading it, `:617` forwarding). That is **v2ecoli #648**, merged 2026-09-02 —
eight days after the 08-25 ref this document was written from.

So the fork below collapses onto its second branch, which is the worse one:

> ~~dropped ⇒ a swap campaign silently runs wild-type~~ — not what happens here;
> **forwarded ⇒ the swap runs inside `LineageProcess`**, which is the combination
> `run_native_chain.py:3-13` is deprecated for ("run_workflow's composite round-trip
> DROPS the injected config … empty `met_map` → `KeyError` on the first exchange
> molecule"), and *that* is a tick-1 failure.

⇒ **The tick-1 completion in the table above is the live failure mode**, and it is
still not root-caused. The swap reaches the run; something in the generation-boundary
machinery closes the generation after one tick.

**Reads from data Chris already has.** #648 answers what (2) was for — the swap is
forwarded — so these now serve to confirm rather than to separate:

1. `final_state.json` top-level keys — `batch` / `batch_runner` means the batch
   branch ran; `agents` means the plain single-cell path did.
2. `1.pq` columns — the swapped process's listener columns present confirms the swap
   was applied inside the run. Expected present post-#648; **absent would mean a
   second, separate drop downstream of the forwarding.**
3. Which local runner produced the working 3-gen lineage — `run_workflow` /
   `LineageProcess`, or a hand-rolled loop (`run_condition_multigen_parquet.py`
   deliberately does *not* use `LineageProcess`). Only the former exonerates
   `LineageProcess`+swap. **Still open, and now the most valuable of the three:** it
   is what separates "`LineageProcess`+swap is broken" from "the dispatch path breaks
   it".

If (1) is batch and (2) is applied, the remaining question is which of
`_run_until_division`'s signals closed the generation on tick 1: exception (now
type-guarded and warned — Alex saw zero warnings on sim262/gen0, so unlikely),
`agents_after != agents_before`, or `survivor.get("divide")`. One `warnings.warn`
per signal at the close-out, one redispatch of Chris's swap cell, one CloudWatch
read.

### 3e. Two distinct failure modes, not one — and #387 may trade between them

*Added 2026-09-04, after #387 merged (`db47201f`).*

The silent-wild-type report (#385) and the tick-1 completion (§3d) have been
discussed as one bug. They are two, with **opposite** symptoms, and the discriminator
is the **submit shape**:

| | submit shape | swap reaches the run? | observed |
|---|---|---|---|
| **#385** | **nested** — `extra_params={"injected_processes": {...}}` | **no** — dropped in viva-api | **full-length** wild-type: 206 columns, 4 daughter checkpoints, identical to a no-swap control |
| **§3d (Chris)** | swap present at the runner | **yes** | **25–46 s/gen**, `global_time 1.0`, one `1.pq` shard |

A *dropped* swap produces a normal-length wild-type run — which is exactly what #385
measured. It cannot produce a 25-second generation. So:

**#387 fixes #385, and does not explain §3d.** Its root cause is real and narrow:
`injected_processes_from_config` (`simulation_service_ray.py:265`) read only the FLAT
top-level `swap_processes`/`add_processes`/`exclude_processes` extras, so a nested
block returned `None` and the swap was dropped at every hop after. The helper now
accepts both shapes. Nothing about that touches the generation-boundary machinery.

**The risk this creates.** Before #387, a nested submit dropped the swap and ran
wild-type to full length. After #387, that same submit **carries the swap** — which is
precisely the configuration §3d measured collapsing to one tick. So #387 may convert a
silent-wild-type failure into a tick-1 failure rather than into a correct run. That is
still a strict improvement (loud-ish beats silent), but it is **not** the same thing as
Run 2's W3 being unblocked.

**Why the proposed acceptance test would not catch it.** #385 recommends checking
`injected_processes` is non-null on the returned artifact. Post-#387 that is true in
*both* outcomes — it asserts **presence, not effect**. #362's `_assert_emitted_output()`
does not close the gap either: it requires a *non-empty* emitted store, and a tick-1 run
writes `1.pq`, which is non-empty.

⇒ **Pair the presence check with an effect check** before calling W3 green. Either is
cheap and both come from data the run already produces:

- **wall-clock per generation** — ~800 s is a real cell; 25–46 s is not;
- **shard count / max shard index** — 7 shards through `2528.pq` vs a single `1.pq`;
- **`global_time` on the final state** — `1.0` is the tell.

The shard/`global_time` check is the more robust of the two, since wall-clock varies
with instance type.

#### Measured: the pre-#387 control (sim 294, `smsvpctest`, 2026-09-04)

The nested-shape half of the table above is no longer inferred. Submitted #385's exact
request shape against `sms-api:0.9.90` — verified on the running pod to predate #387
(`grep -c NESTED` → 0; the helper reads only `getattr(config, "swap_processes")` and
returns `None`), simulator 109 (`sms-ecoli@4da4e43`), 1 seed × 2 generations,
`new_genes: off`, `condition basal`.

**The drop is mechanical, and visible before the run finishes:**

| hop | `injected_processes` |
|---|---|
| submitted (nested `extra_params`) | sent |
| POST response's resolved config | **present, intact** |
| gen0's `CONTAINER_JOB_CMD --overrides` | **absent** |

gen0's dispatched overrides carry twelve keys — `n_seeds`, `n_generations`,
`stop_at_division`, `cache_dir`, `out_dir`, `experiment_id`, `analyses`, `parallel`,
`seed`, `initial_generation_index`, `initial_carry_state_path`,
`daughter_state_out_path` — and **none of them is the swap**. So the block reaches the
resolved config and dies at the dispatch hop, exactly as #385 traced.

> **Read `CONTAINER_JOB_CMD`, not the Batch job's `command`.** The container command is
> only `/opt/batch-container-entrypoint.sh`; the real `run_pbg.py` invocation and its
> `--overrides` live in that env var. Checking argv yields a meaningless "absent" for
> every key. Job names also key on the **simulator** id, not the `database_id`
> (`chain-seed0-gen0-sim109-…`), which makes a `sim294` search silently match nothing.

**And it then ran wild-type**, which is the half that matters for §3e's risk claim:

| | Chris's control (no swap) | Chris's swap runs | **sim 294 gen0** |
|---|---|---|---|
| duration/gen | 814.7 / 805.4 s | 25–46 s | **796.8 s** (SUCCEEDED, exit 0) |

Within ~2% of the no-swap control and 17–32× the fast-completion mode.

⇒ **This is the pre-#387 baseline, and it is internally consistent**: no swap in the
command, no swap effect in the runtime. It does **not** test the risk in the paragraph
above — that needs a build carrying #387 deployed to `smsvpctest` and the identical
submit re-run. Until then §3e's trade remains a prediction, with a precise baseline to
compare against.

*(Incidental confirmations from the same run: `stop_at_division: true` is present in the
dispatched overrides, so #369 is live on this path; and the `[batch-container]` log
prefix confirms recent per-commit images do ship `batch-container-entrypoint.sh`.)*

#### Measured: the post-#387 run (sim 296, `smsvpctest` @ 0.9.91, 2026-09-04)

**The risk above is no longer a prediction. It happened.**

0.9.91 was cut and deployed for this (PR #394; pod verified — the `NESTED` marker went
0 → 1). The pre-#387 control could **not** be re-created: #362's
`allow_default_fallback=False` now hard-404s the default
`simulation_config_filename=api_simulation_default.json`, which is the embedded template
sim 294 actually ran on. So the post run uses `mecillinam_wellmixed.json` — chosen because
it carries **flat** `add_processes` and **no nested block**, making flat and nested
payloads textually distinguishable in the dispatched command.

**#387 works.** gen0's `CONTAINER_JOB_CMD` now carries the nested block:

```json
"injected_processes": {"swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
                       "add_processes": [], "exclude_processes": [], "fork_repo": ""}
```

Against sim 294's twelve keys with no `injected_processes` at all. The nested read is live.

**And the generation collapsed:**

| | 0.9.90 (sim 294) | 0.9.91 (sim 296) |
|---|---|---|
| nested block reaches runner | no — dropped | **yes** |
| gen0 duration | **796.8 s** | **44.5 s** |
| parquet shards | 7 (through `2528.pq`) | **1** (`1.pq`, 500 bytes) |
| daughter state | written | **none** |
| reported status | SUCCEEDED | **SUCCEEDED, exit 0** |

44.5 s sits inside Chris's 25–46 s fast-completion band, against his 814.7/805.4 s no-swap
control. No daughter state means gen1 has nothing to resume from either.

**Neither proposed guard catches it.** `PBG_REQUIRE_OUTPUT=1` was set on this very command
(#362 is live) and passed — it requires a *non-empty* store, and a 500-byte `1.pq` is
non-empty. `injected_processes` non-null on the artifact is also true. Both assert
presence; neither asserts effect. This is the concrete case for the §5 effect check.

> **Confound, stated rather than glossed.** #387 makes the nested block **replace** the
> flat fields, so this run also lost the config's own `add_processes`
> (`permeability`, `antibiotic-transport-odeint`, `concentrations_deriver`, `gillespie`) —
> visible as `"add_processes": []` above. So *this run alone* cannot separate "the swap
> causes tick-1" from "the dropped processes cause tick-1". Chris's 2×2 already isolated
> the swap on his own config, so the combined evidence points at the swap; this run is a
> hybrid and is not claimed as more. **The operational conclusion is unaffected either
> way:** a swap-carrying chain-dispatch generation completed in 44.5 s, wrote one
> 500-byte shard and no checkpoint, and reported success.
>
> That replace-not-merge behavior is a separate defect, filed as its own issue.

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
- **Swap effect, not just presence (§3e).** For any dispatch declaring a swap, assert
  the run actually ran: **>1 parquet shard** (or a max shard index well past `1.pq`)
  and **`global_time` ≫ 1.0** on the final state. `injected_processes` being non-null
  on the artifact is necessary but not sufficient — post-#387 it is true whether the
  run executed or collapsed on tick 1.
- Fidelity (follow-up): same seed, 3 generations chained vs. one pbg-native run;
  compare dry-mass-at-division per generation. Some divergence is by construction
  (listeners reset, RNG re-seeded per generation). A generation that never
  re-initiates replication is the dill bug's signature on the JSON path.
