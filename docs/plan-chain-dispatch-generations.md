# Multi-generation dispatch: chain-dispatch vs. pbg-native vs. Nextflow, and what's left

> **⚠ Retraction (2026-09-04).** This document cited `global_time == 1.0` as evidence of a
> one-tick collapse. That was wrong — it reads ≈1.0 on **every** chain-dispatch generation,
> pass or fail. And "the swap causes tick-1" is wrong as stated: a real swap has since been
> observed dividing normally. See the boxed correction in §3d. Wall-clock and shard count
> survive as signals; nothing drawn from `global_time` does.

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
| **Capacity model** *(checked live 2026-09-04)* | `vecoli-task-amd64`: **Spot-first** — CE order 1 `Amd64SpotComputeEnv` (`SPOT`), order 2 on-demand | `ray-standalone` → `RayStandaloneCE`, type **`EC2`** — on-demand only | `ray-mnp` → `RayBatchOnDemandCE`, type **`EC2`** — on-demand only |
| Proven at | production | 1000×10 (item 71), 1×3 post-fix (sim 257) | 100×10 (sim 254) |
| Script | — | `chain-dispatch.sh` | `pbg-dispatch.sh` |

The two v2 mechanisms are complementary, not competing. Chain-dispatch buys
generation-granularity retry/resume ~~and Spot-safety~~ at the cost of N×G submissions
and a lossy serialization boundary every generation. pbg-native buys zero boundaries
and one submission at the cost of a gang-scheduled on-demand cluster, no mid-run
autoscaling, and one node failure killing the campaign. The memory note in
`parallel_seeds.py:70-75` (7–9 GB RSS by generation 3–4 on a full 1000×10) cuts
against pbg-native on deep lineages.

> ⛔ **Two corrections to that paragraph, both checked live 2026-09-04.**
> **"Spot-safety" is not a chain-dispatch property** — it runs on `smsvpctest-ray-standalone`,
> whose only compute environment is type `EC2`. Neither v2 mechanism touches Spot; only the
> **Nextflow** path's queue is Spot-first. So spot reclaim is not the hazard here, and
> chain-dispatch's generation granularity is not buying protection from it.
> **"retry" is not one either** — see §3f. The granularity exists; the retry does not.

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

## 3. Still open on `main` (0.9.99)

> *Re-checked against `origin/main` and live `smsvpctest` on 2026-09-04, after this doc was
> first written at 0.9.90. 3a–3c are still open; 3f and 3g are new.*

### 3a. Timed-out generation writes no checkpoint

`LineageProcess` skips the checkpoint when a generation hits `max_duration_per_gen`
without dividing (`lineage.py:453-454`, guarded on `daughter is not None`). Summary
records `"divided": false`; nothing raises. The scheduler then submits *G+1* pointing
`initial_carry_state_path` at an S3 object that does not exist.

Fix: in `JobScheduler._advance_seed_generations` (`job_scheduler.py:388-394`),
before submitting `gen+1`, require `RayLayout.daughter_state_uri(experiment_id,
seed, gen)` to exist. Absent ⇒ resolve the seed as failed. Test in
`tests/simulation/test_scheduler.py`.

> **Still open — and v2ecoli#680 did not close it, it joined it.** *(Re-verified
> 2026-09-04; the guard has moved to `v2ecoli/workflow/lineage.py:651`.)* #680's new
> per-generation `checkpoint_dir` writer sits behind the **same** `if out_path and daughter
> is not None` condition as the daughter-state write — `out_path` merely selects *which*
> file (`checkpoint_dir/gen_NNNN.pkl` or `daughter_state_out_path`). So a generation that
> hits `max_duration_per_gen` without dividing now writes **neither**. #680 improves the
> *divided* case; the timed-out case is uncovered for both writers, and remains what
> viva-api#402 tracks.

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
- **`simulation_service_ray.py:~195` — the costliest one, because it is load-bearing.**
  It states that per-generation retry "no longer needs a manual override here as of item 71
  Phase 4: chain-dispatch generations now submit as container-type jobs, whose job
  definition already bakes in `retryStrategy.attempts=2` — see sms-cdk's
  `RayContainerJobDef`." True of the **base** definition; false of every definition anything
  actually runs. It is the stated justification for *removing* the retry override, so this
  stale narration did not merely mislead a reader — it removed a mechanism. See §3f.
- *Fixed since, viva-api#410 (2026-09-04):* `chain-dispatch.sh`'s `EXTRA_PARAMS` default
  emitted **invalid JSON** on any call that left it unset; and `_parca_command()` appended
  `--new-genes`/`--bundle-overrides` to the `build_cache.py` invocation as well as to
  `v2ecoli-parca`, which has neither flag — ParCa succeeded (572.5 s, 669.3 MB, correct
  composed vio+GFP genes) and the hydrate died one command later on "unrecognized
  arguments".

### 3c. `pbg-dispatch.sh` verification

Per its own header: the durable `n_workers` fix (v2ecoli #647, viva-api #366) needs
a simulator built from v2ecoli `8b293abf` or later; `SIMULATOR_ID=97` predates it,
so a dispatch with `N_WORKERS=""` against 97 re-runs the old default of 2.

### 3d. ~~Open: `swap_processes` collapses every generation to one tick~~ — **RETRACTED**

> **The retraction stands — but a real one-tick-collapse mechanism was found later, from a
> different direction, and a reader should not conclude none exists.** v2ecoli#682/#683
> (merged 2026-09-04): a native swap **target with no explicit `process_config`** and no
> `fork_sim_data` reaches `apply_injected_processes` with `config_dict=None` and is built on
> `config_schema` defaults — for metabolism-redux that is an **empty stoichiometry and 0
> homeostatic targets**. The process does nothing, the generation collapses after one tick,
> and the run **reports success**. That is sms-ecoli#210 §3d, and it is what stopped
> violacein. #683 threads `cache_dir` into the injection spec; #682 makes the config-less
> case raise instead of silently building an empty config.
>
> So: retracted for the runs measured here (swaps *with* config, which genuinely ran), and
> real for the config-less case, which none of the measurements in this section covered.

> The measurements below were withdrawn by their author (@cplong90) and the premise is
> false: `LineageProcess` + swap divides normally. Kept for the record; see the boxed
> correction below and §3e.

From Chris's dispatches on `smsvpctest` (PR #375 thread, 2026-09-03). Same simulator
image, same ParCa dataset, `new_genes: off` on both sides, n=1 per cell:

| | `exclude_processes: ["exchange_data"]` | **empty** |
|---|---|---|
| **no swap** | — | **814.7 s / 805.4 s** per gen, 7 shards (`400 … 2528.pq`) |
| **swap** | sim262: 25–45 s/gen | **46.0 s / 25.6 s** per gen, one `1.pq` shard |

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

### 3e. ~~Two distinct failure modes, not one~~ — **RETRACTED in full (2026-09-04)**

> This section argued that #385 (nested submit, swap dropped, full-length wild-type) and
> §3d (swap present, one-tick collapse) were **two bugs with opposite symptoms**, and that
> #387 would trade the first for the second. **Both halves of that are now known to be
> wrong**, and the cut should not be built on. Kept rather than deleted because it was
> cited across #375, #387 and #408.

**What went wrong, in order:**

1. **§3d's symptom was withdrawn by its author.** @cplong90 retracted the 25–46 s
   measurements: those runs had no shards, no `daughter-state/`, `batch: {}` — *"they did
   not simulate at all."* **There was never a one-tick completion to root-cause**, so the
   second half of the "two modes" cut had no referent.
2. **`global_time == 1.0` was never evidence** — see the boxed correction in §3d. It reads
   ≈1.0 on *every* chain-dispatch generation, pass or fail.
3. **My own confirming run was a different bug.** I read sim 296 (0.9.91, nested swap,
   44.5 s, one 500-byte shard, no checkpoint, `SUCCEEDED`) as the predicted trade. It was
   **viva-api#401**: `mecillinam_wellmixed.json` carries no swap and no `cache_dir`, and my
   nested block **replaced its four flat `add_processes`** — so the composite built without
   them and ran to completion producing nothing. @cplong90: *"Two observations, one
   event."*
4. **And the premise is false.** `LineageProcess` + swap **works** via chain-dispatch:
   @AlexPatrie's run divided at t=2527 s with a real 11.5 MB daughter checkpoint.

⇒ **There is no trade.** #387 fixes a real drop (#385) and does not convert it into
anything.

#### What replaced it: a real, scoped defect

The run that divided at t=2527 s **wrote no `history/` partition at all** — 3 `.pq` files
(`configuration/`, `success/`, and the 500-byte outer-emitter `default/history/1.pq`)
against **97** for the no-swap baseline, with the 53 MB
`…/generation=0/agent_id=0/800.pq` simply absent. Confirmed independently by @AlexPatrie
against S3.

**Scoped, not root-caused.** pbg-native dispatch 313 — same config, same swap — divided at
the identical t=2527 s with a **fully populated** `history/`: 7 chunks, 45–53 MB each.
So this is **chain-dispatch-specific**, plausibly the ParquetEmitter "default
`experiment_id`" fallback seen in dispatch 287.

> **⚠ Live hazard.** `summary.json` records `duration: 2527.0`, so **#408's new check passes
> this run**, and `PBG_REQUIRE_OUTPUT` passes because the store is non-empty. #408 correctly
> removes a bad signal — but that signal was *masking* the missing history, so removing it
> turns a loud wrong-reason failure into a **silent pass on a run with no data**. The
> analysis over it already reports `status: OK` with eight empty modules (#403).
> **Companion check wanted: assert a hive-partitioned `history/` partition exists**, not
> merely a non-empty store.

#### The acceptance check that survives

§5's effect check stands, with the emphasis moved: assert **a real per-generation
`duration`** (the sum across `summary.generations`, per #408) **and a `history/`
partition**. Duration alone passes a run that produced nothing; a non-empty store passes it
too.

#### Why this is worth keeping as a record

Every retraction above has one shape: **a sound observation carrying an unverified
explanation.** The 25–46 s numbers were real. `global_time = 1.0` was real. Sim 296's
44.5 s and 500-byte shard were real. Each was attached to a cause nobody had checked —
including by me, twice. That is the same failure this document catalogues in the code
(#401/#402/#403), applied to our own reasoning about it.

### 3f. Per-commit job definitions carry **no retry and no timeout** — and the code says otherwise

*New 2026-09-04. Found by @cplong90 on the MNP side; verified here across the whole
population and extended to the container path that chain-dispatch actually uses.*

Live on `smsvpctest`:

```
smsvpctest-ray-container              rev1   retry={'attempts': 2}   timeout=None   ← base, nothing runs on it
smsvpctest-ray-container-<sha>        rev1   retry=None              timeout=None   ← every per-commit def
smsvpctest-ray-array                  rev1   retry={'attempts': 2}   timeout=None
```

Not a sample — **every** `smsvpctest-ray-container-<sha>` definition returns
`retryStrategy=None`. The cause is one line in each cloner: `_ensure_container_job_def`
registers `type="container", containerProperties=…` and `_ensure_mnp_job_def` registers
`type="multinode", nodeProperties=…`. Neither copies `retryStrategy` or `timeout` from the
base definition it deep-copies everything else from, so both inherit AWS Batch's defaults:
**`attempts: 1`, no timeout.**

⇒ A generation lost to an OOM or a node failure is **not retried by anything**, and a hung
generation runs **forever**. On the on-demand CEs of §1 that is the realistic failure, not
reclaim.

**The fix is small, and half of it is already written.** `_submit_container` and
`_submit_mnp` both accept a `retry_strategy` argument and pass it "verbatim as
`SubmitJob.retryStrategy`" (their own docstring) — and `grep 'retry_strategy='` across the
module returns **zero callers**. So either pass it at the call sites, or add
`retryStrategy` + `timeout` to the two `register_job_definition` calls. Prefer the latter:
it also covers anything submitting against those definitions from outside this module.

> **Contrast worth recording, since it cuts the other way:** on the Nextflow path nf-amazon
> sets **both** `retryStrategy` and `timeout` on the `SubmitJobRequest` itself, so a
> per-commit definition's `attempts: 1` never binds there. The gap in this section is
> specific to the two v2 mechanisms. (See `plan-nextflow-dispatch.md` §11.1b.)

### 3g. Every generation after the first silently lost its trailing parquet **and** its success sentinel

*Root-caused 2026-09-04 — **v2ecoli issue #687**, fixed by **v2ecoli PR #688**
(`10ebc4c2`, open, 6/6 tests passing). Recorded here because it invalidates a check §5
recommends and a hazard §4 describes.*

Division finalizes the parent emitter by looking it up in the process-global registry,
deriving the key from `_PARQUET_EMITTER_OVERRIDE`'s metadata and falling back to
`self.agent_id`. On the lineage path that override is **guaranteed `None` at division time**
— `_build_generation` sets it, calls `baseline()`, and clears it in a `finally` *before*
`Composite(doc)` is constructed — so the lookup is always `"0"`, while emitters are
registered under the runner's per-generation id (`"0"`, `"00"`, `"000"`). They coincide only
for generation 0. `flush_parquet` doesn't cover it either: `Division` has already returned
`{'agents': {'_remove': [...]}}`, so the walk finds no live emitter.

**And `summary.json` still reported `divided: true` with a full duration.** Measured on a
2-generation lineage, no swap, full `run_pbg` path:

```
gen0   7 chunks, last 2528.pq, success=1
gen1   6 chunks, last 2400.pq, success=0     ← ~338 ticks (~20 MB) dropped
```

**The missing sentinel is the larger loss**, not the truncation: `success_sql` SEMI JOINs
unsuccessful sims out of the dataset SQL, so the generation **disappears from any analysis
that filters on it**.

The fix finalizes in the object that owns the key — `LineageProcess` calls
`finalize_emitter_for_agent(self._agent_id)` alongside `flush_parquet`; the two cover
disjoint cases (timed out vs divided) and both are idempotent. Ships with
`tests/test_lineage_emitter_finalize.py` (6 passed). **MERGED as v2ecoli#688** (`76def1f8`,
2026-09-04) and **now deployed**: sms-ecoli pins `ee85b95f`, and simulator **128** is the first
image carrying it. Confirmed on real infra twice — the throwaway v2ecoli-only build (simulation
316) and the production stack (simulation 326).

Two consequences for this document:

- **§4's "the zarr store enforces the chain"** — generation *N* refuses to open unless
  *N−1* carries the success attr from a clean `close(success=True)`. That sentinel was
  **never written for any generation ≥ 1**, so the hazard was firing, not hypothetical.
  Closed by v2ecoli#688; anything written *before* simulator 128 still carries the gap.
- **§5's "assert >1 parquet shard"** — a truncated trailing batch still leaves multiple
  shards, so the check passed while the data was short. Assert the **success sentinel**
  per generation as well as the shard count.

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
- **`baseline()`'s injection resolver is chosen by `sys.path`, not by the pin.** *(New
  2026-09-04, v2ecoli#684.)* v2ecoli's wheel does not ship `scripts/`, so
  `ecoli_baseline.baseline()`'s bare `from scripts._compare.inject import …` resolves
  **whichever repo's `scripts/` is on `sys.path`** — on the GovCloud pod that is
  **sms-ecoli's vendored copy**, the one carrying the native-redux builder v2ecoli's own
  copy lacks. Chain-dispatch only works because viva-api#359 injects
  `PYTHONPATH={V2ECOLI_DIR}` into `PBG_RUNNER_ENV` at all three `run_pbg.py` call sites,
  after a real dispatch died on `ModuleNotFoundError('scripts')`. #684 wheel-ships one
  absolute-imported `v2ecoli/library/inject.py` as step 1 of 3; until steps 2–3 land, which
  resolver runs is a property of the launch environment. Pairs with the pin bullet below:
  the pin names a commit, and does not determine which `inject.py` executes.
- ~~**The v2ecoli pin is a branch pin**~~ — **RESOLVED 2026-09-04.** It read `branch="main"`
  (locked to `268515f0`), so `uv lock` could move `LineageProcess` under the deployment without
  anyone deciding to. sms-ecoli#221 replaced it with an explicit rev
  (`rev = "ee85b95f…"`), and process-bigraph with a release tag (`tag = "v1.8.4"`), so both
  now move only by an edit visible in a diff. Left in place rather than deleted: the failure
  mode is worth remembering, and **v2ecoli itself still has zero tags**, so its consumers name
  it by a 40-character hash with no version — the same ambiguity one layer down.

## 5. Verification

- Unit: `uv run pytest tests/simulation/test_ray_backend.py tests/simulation/test_scheduler.py`
- Smoke, on `smsvpctest` via the tunnel: `chain-dispatch.sh` (defaults = sim 257's
  1×3), then assert `daughter-state/seed0/gen{0,1,2}.pkl` all exist with distinct
  MD5s, three `generation=` parquet partitions, per-job runtime in minutes.
- **Swap effect, not just presence (§3e).** For any dispatch declaring a swap, assert
  the run actually ran: **>1 parquet shard** (or a max shard index well past `1.pq`)
  and a **real per-generation `duration`** (the sum across `summary.generations`, per
  viva-api#408 — **not** `global_time`, which reads ≈1.0 on every chain-dispatch
  generation regardless of outcome). **Add the per-generation success sentinel** — no
  generation ≥ 1 writes one today, while `summary.json` says `divided: true` (§3g). `injected_processes` being non-null
  on the artifact is necessary but not sufficient — post-#387 it is true whether the
  run executed or collapsed on tick 1.
- Fidelity (follow-up): same seed, 3 generations chained vs. one pbg-native run;
  compare dry-mass-at-division per generation. Some divergence is by construction
  (listeners reset, RNG re-seeded per generation). A generation that never
  re-initiates replication is the dill bug's signature on the JSON path.
