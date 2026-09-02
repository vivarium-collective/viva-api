# Make chain-dispatch actually chain generations

## Context

sms-api dispatches multi-generation E. coli campaigns on the Ray/AWS-Batch backend as
one container job per `(seed, generation)`, with `JobScheduler` gating generation *i+1*
on *i* succeeding, and daughter-cell state handed between jobs through a deterministic
S3 key. That is a faithful port of vEcoli-private's Nextflow `sim.nf` task granularity.

While comparing that design against the Nextflow generator, the analysis turned up a
defect that invalidates the premise: **the generation jobs never engage v2ecoli's
`LineageProcess` at all.** Each job builds a plain single cell, ignores the parent's
daughter state, writes no daughter state of its own, and runs for one simulated second.
It then exits 0, so AWS Batch reports SUCCEEDED and the campaign completes "green".

### Evidence

`_seed_generation_command` (`viva_api/simulation/simulation_service_ray.py:849-864`) sends:

```python
{"n_seeds": 1, "n_generations": 1, ...,
 "initial_generation_index": G,
 "initial_carry_state_path": "s3://…/gen{G-1}.pkl",
 "daughter_state_out_path":  "s3://…/gen{G}.pkl"}
```

v2ecoli's dispatch gate is `v2ecoli/composites/ecoli_baseline.py:1574`:

```python
if int(n_seeds) > 1 or int(n_generations) > 1 or stop_at_division:
    return _build_batch_document(...)     # the only path that reaches LineageProcess
```

`1 > 1 or 1 > 1 or False` → **False**. Control falls through to the plain single-cell
build. The three checkpoint keys are documented as batch-mode-only
(`ecoli_baseline.py:1510-1515`) and are inert there — no `load_initial_state`, no
`save_initial_state`, no division stop. `ecoli_baseline.py:1484-1496` states it outright:
*"`n_generations` … is inert unless n_seeds>1 or n_generations>1."*

Three independent corroborations:

1. **The command ends `-n 1`**, parsed as `--steps` (`viva_api/compose/run_pbg.py:444`)
   and driving `composite.run(1)` (`run_pbg.py:422`). For a *batch* document that fires
   the one-shot `BatchBaselineRunner` Step — correct. For a plain single cell it is one
   1-second tick.
2. **`run_pbg.py:81-105` re-implements the hive partitioning itself**
   (`parquet_vecoli(..., generation=overrides["initial_generation_index"])`), which is
   only necessary because nothing downstream is stamping it.
3. **The superseded `_sim_command` multigen branch documents the invariant that was
   lost** (`simulation_service_ray.py:747-752`): *"n_seeds/n_generations > 1 switches it
   into the batch/lineage shape."* It passed real `n_seeds=N, n_generations=G` with the
   same `-n 1`. The item-33 rework kept the `-n 1` idiom and changed the overrides to
   `1/1`, which is exactly the pair that falls out of batch mode.

Git history rules out this ever having worked: the checkpoint keys were added to
`ecoli_baseline` by a single commit, `fbfd24408 "fix(item 34): thread checkpoint/resume
keys through ecoli_baseline's **batch path**"`, and the gate at that commit was already
`if int(n_seeds) > 1 or int(n_generations) > 1:`.

### Intended outcome

Chain-dispatch produces a real lineage — generation *G* starts from generation *G−1*'s
daughter, writes the `generation=G` hive partition, and the campaign's multigeneration
analyses see G distinct cells per seed instead of G copies of a 1-tick gen-0 stub.

## Open architectural question (deliberately not decided here)

Whether to keep one job per `(seed, generation)` at all is a separate call — v2ecoli runs
a whole lineage in-process by default (`_run_seed_worker` under Ray already does exactly
one-job-per-seed), so a 1000×10 campaign could be 1000 jobs rather than 10,000. This plan
**fixes the shape we have** and leaves that decision open; the fix is a prerequisite for
measuring either shape honestly, since today neither produces a lineage.

## Plan

### Step 0 — Confirm before changing (blocking)

The local `v2ecoli` clone does not contain the pinned commit, so the gate must be read at
the version the image actually runs.

- sms-ecoli pins v2ecoli as a **branch** pin (`pyproject.toml` `branch = "main"`), locked
  in `uv.lock:4717` to `268515f0dd8a14c81a7a5ac13c4ca4e77f8bb1db`. Fetch v2ecoli and read
  `ecoli_baseline.py` at that SHA. Confirm (a) the dispatch gate, (b) that
  `stop_at_division` exists there — it was added by `576f887b8`, later than the checkpoint
  keys, so an older pin may not have it.
- Confirm empirically on `sms-api-stanford-test`: run a 2-generation, 1-seed campaign and
  list `s3://…/<experiment_id>/daughter-state/seed0/`. **Zero objects confirms the
  diagnosis.** Also check the seed's parquet prefix for a `generation=1` partition.

If `stop_at_division` is absent at the pin, bump the pin first — there is no override-only
fix on an older gate.

### Step 1 — The fix

`viva_api/simulation/simulation_service_ray.py::_seed_generation_command` — add one key to
the overrides dict:

```python
"stop_at_division": True,
```

This is the documented opt-in (`ecoli_baseline.py:1497-1509`) that routes
`n_seeds=1, n_generations=1` through the lineage path. Keep `n_generations: 1`: with
`initial_generation_index=G`, `LineageProcess`'s terminal check is post-increment
(`lineage.py:460-461`, `self._generation += 1; if self._generation >= generations`), so
`G+1 >= 1` holds for every G and exactly one generation runs, correctly labelled
`generation=G` / `agent_id="0"*(G+1)`.

Rewrite the `_seed_generation_command` docstring's cross-repo contract paragraph: it
currently asserts the keys thread through to `baseline()`, which is the belief that made
this invisible.

### Step 2 — Handle the timed-out generation

`LineageProcess` writes **no** checkpoint when a generation hits `max_duration_per_gen`
without dividing (`lineage.py:453-454`, guarded on `daughter is not None`); the summary
records `"divided": False` and nothing raises. Job *G+1* then points
`initial_carry_state_path` at an object that does not exist.

In `JobScheduler._advance_seed_generations` (`job_scheduler.py:388-394`), before submitting
generation `gen+1`, require that `RayLayout.daughter_state_uri(experiment_id, seed, gen)`
exists in S3. Absent ⇒ resolve the seed as failed rather than submitting a job that will
fail opaquely (or, worse on a lossy path, silently start fresh).

### Step 3 — Guard rails

`tests/simulation/test_ray_backend.py`, alongside the existing 8192-byte command test:

- Assert the built command's `--overrides` JSON carries `stop_at_division: true` **and**
  `initial_generation_index == G`. This is the regression that matters: the pair of them
  is what selects the lineage path.
- Assert generation 0 sends `initial_carry_state_path == ""` and G>0 sends the
  `gen{G-1}` URI.

`tests/simulation/test_scheduler.py`: a seed whose generation succeeded but wrote no
daughter state resolves as failed and submits no successor (Step 2).

### Step 4 — Fix the stale narration this hid behind

These docstrings actively assert the superseded design and cost real time during this
analysis:

- `viva_api/simulation/tables_orm.py:122-136` and `viva_api/simulation/models.py:47-51` —
  still say *"AWS Batch's own dependsOn resolves each seed's chain natively"*. Untrue since
  0.9.52; the correct description sits in the block immediately below.
- `viva_api/common/storage/data_layout.py:167-171` — *"this whole capability is still
  unwired from any HTTP router."* It is wired (`simulation_service_ray.py:1211-1214`).
- `tests/integration/test_aws_batch_e2e.py:190-201` — asserts `chain_final_job_ids` is
  non-empty right after `submit_chain_dispatch_job`, which now submits only ParCa. Skipped
  unless `AWS_BATCH_INTEGRATION=1`, so CI never catches it.
- Poll cadence: every chain docstring reasons about a "30s tick" (including the argument
  for why per-generation submits need no pacing); `viva_api/api/main.py:99` deploys 5s.
  Reconcile one to the other — at 5s the unpaced submit loop in
  `_advance_seed_generations` runs inside the open advisory-lock transaction.
- `viva_api/config.py:359-360` — `ray_array_queue` / `ray_array_job_definition` have no
  reader; already removed from prod's `shared.env`.

## Known hazards this fix exposes (document, don't fix here)

Once jobs actually run `LineageProcess`, these become live:

- **Division detection is a substring match on exception text** (`lineage.py:361-367`:
  `if "divide" in msg or "division" in msg`). sms-ecoli has a *measured* case of its own
  process fooling it — a `ZeroDivisionError` ("float division by zero") read as a cell
  division, spawning a daughter every tick until ATP starvation
  (`sms_modules/processes/gillespie.py:331-341`; guarded per-process, not fixed upstream).
- **The JSON carry-state round-trip is lossy and untested.** `MetadataArray` metadata
  survives only under `unique`; nested objects degrade to truncated reprs; dict keys are
  stringified; tuples are not reconstructed (`v2ecoli/cache.py:45-127`). Every checkpoint
  test monkeypatches `save_initial_state`/`load_initial_state` — there is no round-trip
  test of a real daughter. The sibling *dill* checkpoint path is **known broken**: resumed
  daughters never re-initiate replication (`sms-ecoli/docs/resume_dill_replication_init_bug.md`,
  still open). The acceptance criterion stated there is the right one to borrow:
  `divide_cell(roundtrip(state))` must yield daughter unique-stores field-identical to
  `divide_cell(state)`.
- **Parquet and zarr disagree by one on the generation number** — parquet uses
  `self._generation` (0-based), zarr uses `len(agent_id)` (1-based). Any cross-sink join
  needs the offset.
- **The zarr store enforces the chain**: generation *N*'s emitter refuses to open unless
  generation *N−1*'s partition exists *and* carries the success attr written by that job's
  clean `close(success=True)`. A killed job breaks its successor.
- **The v2ecoli pin is a branch pin**, so `uv lock` moves `LineageProcess` out from under
  the deployment. Worth a rev pin given how much of this contract lives there.

## Verification

1. **Unit** — `uv run pytest tests/simulation/test_ray_backend.py tests/simulation/test_scheduler.py`
2. **Static** — `make check`
3. **End-to-end on dev**, which is the only check that actually settles it. Deploy to
   `sms-api-stanford-test` per `docs/DEPLOY.md`, grep the marker on the live pod (Pitfall 1),
   then:
   ```bash
   uv run atlantis simulation run chaintest <SIMULATOR_ID> --seeds 1 --generations 3 --poll
   ```
   Then assert, in order:
   - `s3://…/<experiment_id>/daughter-state/seed0/gen{0,1,2}.pkl` all exist and are
     non-trivial in size. **This is the primary signal** — today there are zero.
   - The seed's parquet prefix has three distinct `generation=` partitions, not one.
   - Per-generation job runtime is minutes, not seconds.
   - `GET /simulations/{id}/chain-progress` reports 1 seed succeeded at generation 2.
4. **Fidelity (follow-up, not a gate)** — compare a 3-generation chained campaign against
   a single in-process 3-generation run of the same seed on dry-mass-at-division per
   generation. Chained ≠ in-process is expected to some degree (the carry state resets
   every listener and re-seeds the RNG per generation by construction); a *qualitative*
   divergence, e.g. a generation that never re-initiates replication, is the dill bug's
   signature reappearing on the JSON path.
