# Design: chain-dispatch — the proven, external-orchestration multigeneration/multiseed mechanism

**Audience**: written for Jim, who is new to this system. No prior context assumed. Companion to
`design-pbg-native-for-jim.md`; read both together, ideally side by side with
`../composites/*.json`. Every claim here is traced to real, current source (file:line) or
real dispatched output — this is the mechanism that has actually run this ecosystem's flagship
1000-seed × 10-generation production campaign, and every number below is from that real run or its
successors, not a projection.

## 1. What this is, in one paragraph

For an N-seed × G-generation campaign, chain-dispatch submits **N × G independent, standalone AWS
Batch jobs** — one job per (seed, generation) pair — each running ONE generation of ONE seed's
lineage, in a plain (non-multi-node) container. Consecutive generations of the same seed are chained
together by handing off a serialized "daughter state" checkpoint through S3, gated by an
application-level scheduler poll loop rather than AWS Batch's own native job-dependency graph. Every
seed's chain progresses fully independently of every other seed's — the same asynchronous,
per-seed-independent execution shape vEcoli-private's own Nextflow pipeline uses.

## 2. Why this exists, and why it looks the way it does

**The original design (item 68, real, dated) used AWS Batch's own native `dependsOn` chaining**,
submitting every generation's job for every seed up front. This hit a real, measured production
incident: at 1000-seed × 10-generation scale (~10,000 jobs), AWS Batch's compute-environment scaling
reconciliation never engaged — `desiredVcpus` spiked once to 2048, then dropped to 16 and stayed there
for 30+ minutes despite ~1000 jobs sitting RUNNABLE. CloudTrail showed zero scaling API activity during
that window. The likely cause: AWS Batch's own scaling logic does not handle a huge, all-at-once
`dependsOn` backlog gracefully.

**The fix (item 71)** moved job submission out of the native dependency graph and into
`JobScheduler`'s own existing 30-second poll loop (`_advance_chain_campaign`) — a DB-driven,
restart-safe scheduler that submits exactly ONE generation per seed at a time, only once the previous
generation (or ParCa, for generation 0) is confirmed SUCCEEDED. This is app-level gating instead of a
native Batch dependency chain. It also switched every job from a multi-node-parallel (MNP,
`numNodes=1`) shape to a plain container-type job — MNP jobs carry real per-job infrastructure
overhead this design doesn't need, since no job in this chain ever needs to talk to another node.

This was proven at real, full 1000×10 production scale: campaign 173/171, simulation phase 49m6s,
zero failures, analysis phase 53m33s, exit 0.

## 3. Architecture

**N × G independent Batch jobs, chained by app-level polling, not by document structure.**

```
seed 0: [ParCa] -> job(seed=0, gen=0) -> job(seed=0, gen=1) -> ... -> job(seed=0, gen=G-1)
seed 1: [ParCa] -> job(seed=1, gen=0) -> job(seed=1, gen=1) -> ... -> job(seed=1, gen=G-1)
...
seed N: [ParCa] -> job(seed=N, gen=0) -> job(seed=N, gen=1) -> ... -> job(seed=N, gen=G-1)
```

Each arrow is a real, separate AWS Batch container boot — every generation pays a full fresh-container
startup cost (measured: 7-12.5s real inter-job gaps between successive jobs of the same seed, on top
of a fresh container boot each time). There is no single composite document representing "the whole
campaign" the way pbg-native's `lineage_ray_batch` document does (see companion doc §3) — the campaign
exists only as rows in viva-api's own database and a sequence of independently-submitted Batch jobs.

**What actually gets dispatched for ONE generation, though, IS a real process-bigraph composite** —
just a much smaller one than it might look: `ecoli_baseline` in its own internal batch/lineage mode
(§5), not a bare single-cell build.

## 4. The shared entrypoint — `run_pbg.py`

**Chain-dispatch uses the exact same generic script pbg-native uses**: `viva_api/compose/run_pbg.py`.
See the companion document §4 for the full trace of what this script does; it is composite-agnostic
and has zero chain-dispatch-specific code anywhere in it. The only difference between the two
mechanisms, AT THIS LAYER, is which `--composite-id` and `--overrides` get passed.

## 5. What each generation's job actually builds: `ecoli_baseline`'s batch/lineage mode

`v2ecoli.composites.ecoli_baseline.baseline()` (`v2ecoli/composites/ecoli_baseline.py:1603`) is a
single function with two entirely different document shapes depending on its arguments:

- **Plain single-cell mode** (`n_seeds=1, n_generations=1, stop_at_division=False`, the function's own
  defaults): builds one `EcoliWCM`-shaped composite directly. Division fires structurally inside the
  cell but nothing stops the RUN at that point — it keeps simulating the daughter for the rest of the
  requested step budget. **Chain-dispatch never uses this mode** (see the real bug this caused, §7).
- **Batch/lineage mode** (`n_seeds>1` OR `n_generations>1` OR `stop_at_division=True`): routes into
  `_build_batch_document()` (`ecoli_baseline.py:951-1107`), which returns a completely different,
  much smaller document:

```json
{
  "state": {
    "batch": {},
    "global_time": 0.0,
    "batch_runner": {
      "_type": "step",
      "address": "local:v2ecoli.steps.batch_baseline_runner.BatchBaselineRunner",
      "config": { "n_seeds": 1, "n_generations": 1, "stop_at_division": true, "...": "..." }
    },
    "emitter": { "_type": "step", "address": "local:ParquetEmitter", "...": "..." }
  }
}
```

**This is the real, complete, actual document** (not simplified) — see
`../composites/chain-dispatch-ecoli-baseline-composite.json` for the full, real output.

The key architectural fact to absorb: **this top-level document has exactly ONE process-bigraph node
of real substance, a single `Step` (`BatchBaselineRunner`)**. All of the actual seed-level fan-out —
building each seed's own `LineageProcess`, running it, collecting its result — happens INSIDE that
Step's own Python code (`BatchBaselineRunner.run_workflow`, `v2ecoli/steps/batch_baseline_runner.py`),
invisible to process-bigraph's own object model. This is architecturally the SAME kind of
"opaque-Step-hides-the-real-fan-out" pattern chain-dispatch's OWN external job-chaining exhibits one
layer up — just one level further in. For a single chain-dispatch generation job (`n_seeds=1` always,
per `_seed_generation_command`, §6), this one-seed `BatchBaselineRunner` step still routes through the
exact same `LineageProcess` machinery pbg-native's own `ray:LineageProcess` nodes use — the generation-
chaining logic itself (structural `_add`/`_remove` deltas at division) is shared code, not duplicated.

**Note the `emitter` node's own config**: `{"emit": {"global_time": "node"}}` — this top-level document
observes almost nothing on purpose (`"analyses": "none"` is set by `_seed_generation_command`, §6).
The real, analysis-critical biological output is written by `LineageProcess`'s OWN inner
`baseline()` composite, one level down inside the actor/Step's own execution — not by anything visible
in this outer document at all. This mirrors pbg-native's own two-output-stream design (companion doc
§6) almost exactly, since both ultimately delegate to the same `LineageProcess`.

## 6. Building one generation's real command — `_seed_generation_command`

`SimulationServiceRay._seed_generation_command`
(`viva_api/simulation/simulation_service_ray.py:921-1062`) builds ONE seed's ONE generation's command,
submitted as its own standalone Batch job:

```
cd /app/v2ecoli && aws s3 cp <runner-s3-uri> /tmp/run_pbg.py \
  && PYTHONPATH=/app/v2ecoli python /tmp/run_pbg.py \
     --composite-id v2ecoli.composites.ecoli_baseline.ecoli_baseline \
     --overrides '{
       "n_seeds": 1, "n_generations": 1, "stop_at_division": true,
       "cache_dir": "...", "out_dir": "s3://.../seed=<N>/", "experiment_id": "...",
       "analyses": "none", "parallel": "",
       "seed": <seed>, "initial_generation_index": <gen>,
       "initial_carry_state_path": "<prior generation's daughter-state S3 URI, or empty for gen 0>",
       "daughter_state_out_path": "<this generation's own daughter-state S3 URI>"
     }' -n 1
```

`stop_at_division: True` is **unconditional, always set** — this is item 103's real fix (below).
`injected_processes`/`variants` are included when the caller's config supplies them (generic
passthrough of `baseline()`'s own same-named kwargs — item 93). `composite_id` itself is
caller-selectable (item 105, defaulting to `ecoli_baseline` when omitted) — added specifically so
composites other than `ecoli_baseline` that support this same overrides shape (e.g.
`reactor_bird_coupled`) can also be reached through chain-dispatch.

**Checkpoint/resume mechanics**: `initial_carry_state_path`/`daughter_state_out_path` are
`RayLayout.daughter_state_uri`'s own deterministic, per-`(experiment_id, seed, generation_index)` S3
path. Generation 0 has no prior generation (`initial_carry_state_path=""`, `LineageProcess`'s own
documented default for a fresh cell). Every generation writes its own daughter state out — including
the last one, a harmless no-op if the chain ends there. This is a real, synchronous
`boto3.client("s3").upload_file()`/`.download_file()` call inside `v2ecoli/cache.py`'s own
`save_json`/`load_json`, entirely separate from the container's own generic periodic-sync mechanism.

## 7. A real, severe bug this mechanism had — and its fix, definitively proven

**Without `stop_at_division: True`, `baseline()`'s own dispatch gate
(`n_seeds>1 or n_generations>1 or stop_at_division`) evaluated False** for every chain-dispatch call
(which always passes `n_seeds=1, n_generations=1` — one job is always exactly one seed's one
generation). Every chained "generation" therefore silently ran the PLAIN single-cell build for exactly
1 simulated second, regardless of generation index — and the checkpoint/resume fields above were never
consumed, since they only apply inside the gated branch.

**Empirically confirmed, not just reasoned about**: campaign 171's own real production S3 output
(the flagship "1000×10 in 48 minutes" deliverable) — generation 0, 5, and 9 of the same lineage were
byte-for-byte identical files, `global_time: 1.0` after all 10 "generations."

**Fixed** (viva-api PR #369, `v0.9.84`): `stop_at_division: True` unconditional in
`_seed_generation_command`. Routes every chain-dispatch generation through `ecoli_baseline`'s own
batch/lineage path (§5) at `n_seeds=1`, so `LineageProcess._run_until_division` genuinely halts the
generation at the first real division, and the checkpoint/resume fields above are genuinely consumed.

**Definitively proven, not just deployed**: a real verification dispatch (`database_id=257`, 1 seed ×
3 generations) MD5-compared the same relative checkpoint file across all 3 real generations — completely
different hashes (was byte-identical under the bug), plus real, distinct daughter-state files per
generation.

**A corrected, fair per-generation timing number**, obtained from this same verification dispatch's 3
real chained AWS Batch jobs: **759.0s / 757.2s / 777.0s real container wall time (~764s/gen average,
~12.7 min)** — replacing an earlier, invalid "~55s/gen" figure that had measured the pre-fix,
non-division-gated 1-simulated-second fake generations. This is now in the same ballpark as
pbg-native's own real measured pace for equivalent division-gated biology (see the companion doc).

## 8. Orchestration: `submit_chain_dispatch_job` + the scheduler poll loop

`SimulationServiceRay.submit_chain_dispatch_job`
(`viva_api/simulation/simulation_service_ray.py:2046-2143`) submits ONLY ParCa up front, as a plain
container-type job — the N × G generation jobs are NOT submitted upfront. It writes an initial
campaign-tracking DB row (`chain_current_job_ids`/`chain_current_generation` = `[None] * n_seeds`,
`chain_parca_done=False`, `chain_final_job_ids=[]`) and returns immediately once ParCa is submitted —
no N×G-submission wall time to wait out inline.

`JobScheduler`'s existing 30-second poll loop (`_advance_chain_campaign`) does the actual chaining:
DB-driven, restart-safe, submits exactly one generation per seed at a time once the prior one is
confirmed SUCCEEDED. This is what gives chain-dispatch its real asynchronous-per-seed property — seed
5 can be on generation 8 while seed 800 is on generation 1, throttled only by available compute, never
by a cross-seed barrier — without relying on native Batch `dependsOn` at all (that native mechanism is
still used for the single ParCa → generation-0 edge per seed, and generation-to-generation within one
seed's own chain, but never across the whole campaign at once).

## 9. How to actually run it — a reproducible end-to-end call

Prerequisite: a tunnel to the target environment must be up (see companion doc §9).

The real dispatch script, `ecosystem/scripts/dispatch/chain-dispatch.sh` (paths relative to the
`ecosystem/` workspace root, sibling to this `docs/` checkout). **Structurally different from
`pbg-dispatch.sh`**: this hits the standard `POST /api/v1/simulations` endpoint
(`viva_api/api/routers/sms.py:230`), whose real parameters are QUERY parameters, not a JSON body —
`extra_params` is the only JSON-body field, reserved for composite-specific overrides beyond the named
query params below:

```bash
curl -sS -X POST -G \
  --data-urlencode "simulator_id=${SIMULATOR_ID}" \
  --data-urlencode "experiment_id=${EXPERIMENT_ID}" \
  --data-urlencode "simulation_config_filename=${SIMULATION_CONFIG_FILENAME}" \
  --data-urlencode "num_generations=${NUM_GENERATIONS}" \
  --data-urlencode "num_seeds=${NUM_SEEDS}" \
  --data-urlencode "run_parca=${RUN_PARCA}" \
  "${VIVA_API_BASE}/api/v1/simulations"
```

**`composite` MUST stay unset for the request to land in chain-dispatch at all** — the real fork this
endpoint's own handler makes (`SimulationServiceRay._sim_command`/the caller one level up,
`simulation_service_ray.py:1316-1317`): `if composite is None and n_generations > 1: → chain-dispatch`.
Setting `composite` explicitly routes to an unrelated two-engine comparison driver instead.

A real, working invocation (mirrors sim257's own already-proven, cheap scale):

```bash
SIMULATOR_ID=95 EXPERIMENT_ID=my-first-chain-dispatch-run \
NUM_SEEDS=1 NUM_GENERATIONS=3 RUN_PARCA=true \
./scripts/dispatch/chain-dispatch.sh
```

**A real, complete example of ONE generation's resulting composite document** — the actual document
`run_pbg.py` builds and runs for a single chained job, produced by actually calling
`ecoli_baseline.baseline()` with the exact overrides `_seed_generation_command` constructs, against a
real downloaded ParCa cache — is checked in at
`../composites/chain-dispatch-ecoli-baseline-composite.json`. Reproduction steps: same file's
sibling `README.md`.

## 10. Proven results — real evidence, at real production scale

- **The flagship deliverable**: campaign 173/171, full 1000-seed × 10-generation cd1 pipeline
  (ParCa → simulation → analysis), simulation phase 49m6s / 0 failures, analysis phase 53m33s / exit
  0. This remains the ecosystem's only mechanism proven at this scale.
- **Item 103's fix, independently corroborated multiple ways**: MD5-diff proof (§7); a fresh, separate
  dispatch by a teammate (Chris/cplong90) reaching the same conclusion independently; a live GovCloud
  sweep of 9/10 real antibiotic-tier configs completing fork-free end-to-end against the fixed
  mechanism.
- **Real, corrected per-generation cost** (§7): ~764s/gen average — a genuine, not artifactual, number
  now directly comparable to pbg-native's own measured pace.

## 11. Known, real, structural limitations — why item 101/109 exist at all

- **Per-generation container-boot cost, paid every single generation.** Confirmed via real 7-12.5s
  inter-job gaps on top of a fresh container boot each time. pbg-native's `LineageProcess` amortizes
  this to once per seed (one actor, many generations) instead.
- **The seed-level (and, within one job, the whole document-level) fan-out is invisible to
  process-bigraph's own object model** — both the external job-chaining AND the internal
  `BatchBaselineRunner` Step hide real orchestration logic from the composite's own structure. This is
  the precise gap Eran's original challenge identified, and the reason item 101/109 exist as a parallel,
  not-yet-fully-proven-at-scale alternative (see the companion doc).
- **Native `dependsOn` chaining does not scale past ~1000 jobs submitted at once** — this is WHY the
  app-level poll loop (§8) exists; it is not a limitation of the current design so much as the reason
  the current design looks the way it does.

## Appendix A — the real script used to produce the composite document in `../composites/`

```python
from process_bigraph.composite_generator import apply_core_extensions
from process_bigraph.composite_spec import discover_specs, get as get_spec
from v2ecoli.core import build_core

discover_specs()
core = build_core()
spec = get_spec("v2ecoli.composites.ecoli_baseline.ecoli_baseline")
core = apply_core_extensions(spec, core)

# The exact overrides _seed_generation_command builds for one real chained job.
doc = spec.to_document(overrides={
    "n_seeds": 1, "n_generations": 1, "stop_at_division": True,
    "cache_dir": "/path/to/a/real/downloaded/parca/cache",
    "out_dir": "s3://<your-bucket>/seed-0000/",
    "experiment_id": "my-first-chain-dispatch-run",
    "analyses": "none", "parallel": "",
    "seed": 0, "initial_generation_index": 0,
    "initial_carry_state_path": "",
    "daughter_state_out_path": "s3://<your-bucket>/daughter-states/seed=0000/generation=0000.json",
}, core=core)
```

This one runs through the real `to_document()` call (unlike the companion doc's own Appendix A) since
`ecoli_baseline`'s batch/lineage path does not touch Ray for a `parallel=""` request — no local-dev
workaround was needed here.
