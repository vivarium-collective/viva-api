# Design: the pbg-native multiseed/multigeneration dispatch (item 101 / item 109)

**Audience**: written for Jim, who is new to this system. No prior context assumed beyond general
process-bigraph familiarity. Every claim here is traced to real, current source (file:line) or real
dispatched output — nothing is illustrative or simplified for exposition. A companion document,
`design-chain-dispatch-for-jim.md`, covers the OTHER mechanism this one is contrasted against; read
both together, ideally side by side with `../assets/composites/*.json`.

## 1. What this is, in one paragraph

A single process-bigraph composite document that wires N independent whole-cell lineages directly
into its own state tree, each addressed via process-bigraph's own `ray:` remote-address protocol.
One dispatch, one AWS Batch multi-node-parallel (MNP) job, N real `ray:`-distributed actors. No
external orchestration script decides "run seed 5's generation 3 now" — process-bigraph's own
scheduler runs every `ray:LineageProcess` node exactly the way it would run a local one, and the
`ray:` protocol transparently proxies each call to the actor holding that lineage's real state.

## 2. Why this exists

Chain-dispatch (the other mechanism, see the companion doc) is real and proven at 1000-seed
production scale, but it is not process-bigraph-native: it is an external AWS-Batch-job-dependency
orchestrator that happens to run `ecoli_baseline` composites as its payload. Eran's direct challenge,
relayed by Alex: process-bigraph already has a native protocol for distributing work across real
nodes (`ray:`) — why does dispatch need a bespoke external orchestrator at all?

Investigation (this session, against primary source — `process_bigraph/protocols/ray.py`,
`process_bigraph/emitter.py`) found he was substantially right. The `ray:` protocol already unifies
distributed state correctly and natively: `RayShadowProcess.update()` returns via a plain
synchronous `ray.get()`, exactly like a local process — the outer composite's state tree comes back
unified with zero special-case code anywhere in the composite layer. `v2ecoli`'s own colony composite
(item 88) had already proven this works across real physical AWS nodes for spatially-interacting
agents; this work (item 101) applies the identical mechanism to independent lineages instead.

## 3. Architecture

**One composite, N top-level process nodes, one dispatch.**

```
composite document
├── lineages: {}                      (unused namespace key, never populated directly)
├── lineage_0000: ray:LineageProcess  (seed 0's own full multi-generation lineage)
├── lineage_0001: ray:LineageProcess  (seed 1's own full multi-generation lineage)
├── ...
└── lineage_NNNN: ray:LineageProcess  (seed N's own full multi-generation lineage)
```

Each `lineage_XXXX` node is a REAL, independent state-tree entry — not a Python loop variable, not
an item in an internal list some Step iterates over. process-bigraph's own scheduler discovers and
runs each one exactly as it would any other process; the `ray:` prefix on `address` is what tells
process-bigraph's core to proxy this node's `update()` calls to a remote Ray actor instead of calling
a local Python object directly (`process_bigraph/protocols/ray.py`'s `RayShadowProcess` class).

**`LineageProcess` itself is unchanged by this work** — it already ran a lineage's own
generation-to-generation progression using real process-bigraph structural deltas (`_add`/`_remove`
at each division) before item 101 started. What item 101 added is composing MANY of them natively,
in one document, instead of hiding the seed-level fan-out inside one opaque Step's own Python loop
(which is what the older `BatchBaselineRunner` mechanism chain-dispatch's own composite uses
internally — see the companion doc).

## 4. The shared entrypoint — `run_pbg.py`

**Both mechanisms in this ecosystem — pbg-native AND chain-dispatch — run through the exact same
generic script**: `viva_api/compose/run_pbg.py`. This is not a coincidence or a simplification for
this document; it is the real, load-bearing design. The script is entirely composite-agnostic:

```
python run_pbg.py --composite-id <id> --overrides '<json>' -n <steps>
```

`<id>` is any id resolvable by `process_bigraph.composite_spec.get()` — the single registry every
`@composite_generator` decorator registers into, not specific to any one workspace or mechanism. The
script (`_resolve_document()`, `run_pbg.py:313-364`):

1. Resolves the composite spec by id (`get_spec(composite_id)`, discovering specs if not yet imported).
2. Applies any `core_extensions` the spec declares (`apply_core_extensions`) — for
   `lineage_ray_batch` this is `register_ray_lineage`, which registers `LineageProcess` for the
   `ray:` protocol and registers the protocol's own types on the core (`run_pbg.py:344`,
   `v2ecoli/workflow/batch_lineage_ray.py:43-57`).
3. Calls `spec.to_document(overrides=overrides, core=core)` — this is the literal function call that
   builds the real document a dispatch runs. For `lineage_ray_batch` this resolves to
   `v2ecoli/composites/lineage_ray_batch.py`'s `lineage_ray_batch()` function.
4. Registers process-bigraph's remote-address protocols (`ray`/`rest`/`parallel`/`git`) on the core
   (`register_protocol_types`) — required for ANY `ray:`-addressed node to resolve at all.
5. Redirects any file-backed emitter's output location to `results_dir` (`_redirect_emitters`) — a
   no-op for `lineage_ray_batch`'s own document, since it has no emitter node at its OWN top level;
   each `LineageProcess` node opens its own emitters internally (see §6).
6. Constructs `Composite(document, core=core)` and calls `composite.run(steps)`.

**What makes a `ray:LineageProcess` node different from a local one, mechanically**: nothing, from
`run_pbg.py`'s point of view. It never special-cases the address prefix. The `ray:` resolution
happens entirely inside process-bigraph's own core, triggered by the address string alone.

## 5. Pool sizing — the one thing that must happen before any `ray:` address resolves

`process_bigraph.protocols.ray.RayProtocolRuntime` sizes its actor pool for a `(class_name, config)`
key on FIRST creation only. `Composite.__init__` resolves every `ray:` address as it builds the state
tree — if nothing sizes the pool first, it gets created with the protocol's own bare default
(`os.cpu_count()` on whichever single node happens to be the Ray driver — on this ecosystem's AWS
Batch MNP topology, that is the HEAD node specifically, which itself runs with `--num-cpus=0` in
Ray's own resource accounting and never runs an actor itself; its own core count has zero
relationship to real cluster capacity elsewhere).

`prewarm_lineage_pool(core, n_workers)` (`v2ecoli/workflow/batch_lineage_ray.py:60-79`) must run
BEFORE the document's `ray:LineageProcess` addresses are resolved — `lineage_ray_batch()`'s own body
calls it first, then builds the document (`lineage_ray_batch.py:108-121`). `n_workers=None` (the
composite's own default — `v2ecoli/composites/lineage_ray_batch.py:40-53`) falls through to the
`RAY_SHARDS_DEFAULT` environment variable, which viva-api's own dispatch code computes correctly from
real per-node vCPUs × real node count for every multi-node dispatch
(`SimulationServiceRay._submit_multi_node_composite`, §8 below).

**A real, proven incident, worth knowing**: an earlier concrete default of `n_workers=2` silently
shadowed that computation entirely (the env var is only read when the caller passes `None`
explicitly). A real 16-node/256-vCPU dispatch ran with its actor pool capped at 2 the whole time —
measured ~9x slower than it should have been. Fixed by changing the default to `None`
(v2ecoli PR #647). The lesson generalizes: a composite parameter's own default can silently override
an environment-derived value if the fallback logic only triggers on `None`, not on "unset by the
caller."

## 6. `LineageProcess`'s two output streams

`LineageProcess` (`v2ecoli/workflow/lineage.py`) writes TWO separate, independent output streams per
generation — this distinction matters a lot and is easy to conflate (an earlier draft of this exact
design conflated them before being corrected against source):

- **Parquet** — the real, analysis-critical stream every `cd1_*` analysis reads. Written by a Step
  wired *inside* `baseline()`'s own inner composite (`set_parquet_emitter_override`,
  `_build_generation()`, `lineage.py:271-293`) — this is the SAME `baseline()` function chain-dispatch
  uses (see the companion doc), called once per generation from inside the actor.
- **XArray/zarr** — a coarser, dashboard-oriented snapshot, opened lazily on the first populated tick
  and updated once per tick within a generation via `_emit_xarray()` (`lineage.py:389-409`), gated
  entirely on `config["emitter"] in ("xarray", "both")` (`_is_xarray()`, `lineage.py:202-204`).

Both streams already natively support direct S3 output — `parquet_vecoli()`'s own docstring: `out_dir`
may be a local path OR an `s3://` URI; `_open_xarray_emitter`'s own `is_s3_uri(out_dir)` branch does
the same. Neither needed new code for this design — both already read `self.config["out_dir"]`.

**The entire "how does a multi-node pbg-native dispatch get its data out" question resolves to one
dispatch-time parameter**: `out_dir` is already a real, registered, generically-passable field on
`lineage_ray_batch`'s own `@composite_generator` registration
(`v2ecoli/composites/lineage_ray_batch.py:56`), and viva-api's dispatch code threads a request's
`params` straight through with zero composite-specific code (`mnp_dispatch.get("params")`, §8).
**Pass `out_dir="s3://..."` at dispatch time and both streams write there directly — no result
aggregation layer, no gather step, nothing new crossing the `ray:` actor boundary.**

This was verified for real, not just argued: dispatch `database_id=282` (2026-09-03) set
`out_dir="s3://smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91/item109-out-dir-verify/"` and,
checked DURING the run (not after), real parquet chunk files were already growing under that exact
prefix for both seeds — see §10.

**A genuine open question found today, not yet resolved**: that same dispatch's xarray/zarr stream
never appeared anywhere in S3, despite `emitter="both"` and confirmed real division events on both
actors (`DIVISION at t=2527s`, read directly from the real archived Ray actor logs). The most likely
mechanism, traced to source but not yet confirmed: `_open_xarray_emitter`'s own view-filtering step
(`filter_view_to_existing_leaves`) may return empty for this composite's real state shape, which
triggers a silent early return (a `warnings.warn()` call that — separately, also unconfirmed — appears
not to reach these containers' CloudWatch logs at all; the same absence was independently observed on
an unrelated chain-dispatch debug dispatch the same day). Worth a dedicated follow-up; does not affect
the parquet stream, which is the one every `cd1_*` analysis actually depends on.

## 7. A real risk that was seriously considered and found not to apply here

The colony composite (item 88, `v2ecoli/colony_emitter.py`) once tried "one Emitter observing the
whole composite's state, including every agent's full embedded state" and caused a real, measured,
severe production RAM leak — process-bigraph's own emit-gather machinery **deep-copies** the full
state handed to an Emitter-type node on every tick, while a `Process` wired to the same state gets a
cheap typed view instead. `LineageProcess` embeds the identical `EcoliWCM`-shaped pattern colony's
cells do, so this risk was taken seriously here too. It does not apply to the design that shipped:
neither parquet nor xarray output crosses the `ray:` actor boundary at all — both are written from
*inside* each lineage's own embedded inner composite, exactly like colony's own cells already do
safely. No top-level Emitter was added anywhere in this design.

## 8. Real dispatch mechanics

**Submission** (`SimulationServiceRay._submit_multi_node_composite`,
`viva_api/simulation/simulation_service_ray.py:1685-1762`): a single caller-supplied `composite_id`
(here, `v2ecoli.composites.lineage_ray_batch`) plus a `num_nodes` count plus arbitrary `params`
(threaded straight through to `--overrides`, zero composite-specific code). Submits ParCa first (1
node, gated the same way every dispatch shape is), then the real composite job (N nodes), the second
gated on the first via `dependsOn`.

**The head-node command** (`_multi_node_composite_command`, same file, lines 1643-1683) is:

```
cd /app/v2ecoli && aws s3 cp <runner-s3-uri> /tmp/run_pbg.py \
  && PBG_RESULTS_DIR=... PBG_CORE_BUILDER=v2ecoli.core:build_core PYTHONPATH=/app/v2ecoli \
     RAY_SHARDS_DEFAULT=<real per-node vCPUs × num_nodes> \
     python /tmp/run_pbg.py --composite-id v2ecoli.composites.lineage_ray_batch \
     --overrides '{"n_seeds": ..., "n_generations": ..., "out_dir": "s3://...", ...}' -n <steps>
```

This is the exact same `run_pbg.py` script §4 describes, staged fresh from S3 on every dispatch. No
new Ray multi-node "pre-connect" code is needed anywhere in this chain: the entrypoint
(`ray-batch-entrypoint.sh`, `sms-cdk/scripts/`) already exports `RAY_ADDRESS` on the head node before
this command runs, and `RayProtocolRuntime.__init__`'s bare `ray.init(ignore_reinit_error=True, ...)`
already respects `RAY_ADDRESS` from the environment per Ray's own SDK — confirmed via real Batch
CloudWatch logs showing "Using address ... set in the environment variable RAY_ADDRESS".

**Infrastructure**: the same MNP job definition/queue (`RayBatchOnDemandCE`/`ray-mnp`) the colony
composite already used — no new CDK job definition, no new compute environment. Real 2-node and
16-node dispatches have both been proven this way.

## 9. How to actually run it — a reproducible end-to-end call

Prerequisite: a tunnel to the target environment (e.g. `smsvpctest`) must be up:

```bash
cd sms-cdk/scripts && AWS_PROFILE=stanford-sso AWS_DEFAULT_REGION=us-gov-west-1 \
  ./sms-proxy.sh -s smsvpctest
```

Two equivalent ways to fire this, both in `ecosystem/scripts/dispatch/` (paths relative to the
`ecosystem/` workspace root, sibling to this `docs/` checkout):

**Option A — the real dispatch script, `pbg-dispatch.sh`** (full source, current as of this document):

```bash
VIVA_API_BASE="${VIVA_API_BASE:-http://localhost:8080}"
curl -s -X POST "${VIVA_API_BASE}/api/v1/simulations?simulator_id=${SIMULATOR_ID}&experiment_id=${EXPERIMENT_ID}" \
  -H "Content-Type: application/json" \
  -d '{
    "extra_params": {
      "multi_node_dispatch": {
        "composite_id": "v2ecoli.composites.lineage_ray_batch",
        "num_nodes": '"${NUM_NODES}"',
        "params": {
          "n_seeds": '"${N_SEEDS}"',
          "n_generations": '"${N_GENERATIONS}"',
          "base_seed": '"${BASE_SEED}"',
          "cache_dir": "'"${CACHE_DIR}"'",
          "experiment_id": "'"${EXPERIMENT_ID}"'",
          "emitter": "'"${EMITTER}"'",
          "max_duration_per_gen": '"${MAX_DURATION_PER_GEN}"',
          "time_step": '"${TIME_STEP}"',
          "media": "'"${MEDIA}"'"'"${n_workers_field}${out_dir_field}"'
        },
        "steps": '"${STEPS}"'
      }
    }
  }'
```

**Option B — the real `atlantis composite run` CLI command** (viva-api PR #382), fired directly or
through `pbg-dispatch-atlantis.sh`, its own thin env-var-driven wrapper:

```bash
atlantis composite run "${EXPERIMENT_ID}" "${SIMULATOR_ID}" \
  --composite-id v2ecoli.composites.lineage_ray_batch \
  --num-nodes "${NUM_NODES}" --seeds "${N_SEEDS}" --generations "${N_GENERATIONS}" \
  --cache-dir "${CACHE_DIR}" --emitter "${EMITTER}"
```

Verified byte-for-byte identical to Option A's own request body for the same params (a hermetic,
`httpx.MockTransport`-based comparison, not just eyeballed) — see viva-api's own Sphinx docs,
`docs/source/architecture/pbg-native-composite-dispatch.rst` and
`docs/source/guides/composite-dispatch.rst`, for the full CLI reference and a second, independent
tutorial.

**A third, dedicated script — `verify-pbg-dispatch.sh`** — pins the exact scenario that closes the
loop on §"Automatic post-completion analysis" below: 2 seeds × 4 generations, deliberately never
setting `out_dir`, so the real biological data and the automatic post-completion analysis job read
from the same place. This is the decisive end-to-end (ParCa → simulation → analysis) check, not
just a dispatch-mechanics smoke test.

### Automatic post-completion analysis — a real mechanism, with a real, now DEFINITIVELY CONFIRMED gap

**Updated 2026-09-03, correcting this section's own earlier prediction.** Every multi-node composite
dispatch (not colony-specific, not limited to `lineage_ray_batch`) already gets a real, automatic
post-completion analysis job once the MNP job reaches `COMPLETED` (`JobScheduler.
_advance_multi_node_job` → `SimulationServiceRay.submit_multi_node_analysis`, live since backlog
item 88). This was under-appreciated in an earlier draft of this document — worth stating plainly,
not glossed over.

Dispatch `database_id=282` used a custom `out_dir`, and its auto-analysis found nothing —
`"No in-memory emitter history was captured for this run."` The earlier version of this section
predicted this was an `out_dir`-mismatch artifact (the analysis always reads the deployment-standard
location, unaware of a custom `out_dir`) and that leaving `out_dir` unset would fix it.

**Dispatch `database_id=283` tested this prediction directly — deliberately with `out_dir` unset —
and got the SAME result.** Real S3 output (`_manifest.json`, `status: "done"`): exactly one file
produced, `emitter_history_summary.html`, containing the identical message: `"No in-memory emitter
history was captured for this run."` This DEFINITIVELY confirms the gap is structural, not an
`out_dir` artifact: `run_multi_node_analysis.py` (`v2ecoli/scripts/`) only ever downloads two flat
files, `emitter_history.json`/`final_state.json`, into a local temp directory, and calls `run_flush`
on THAT local directory — never the real S3 location where `lineage_ray_batch`'s actual
hive-partitioned parquet tree lives. Those two flat files are themselves near-empty for this
composite: `_redirect_emitters` finds no top-level emitter node in a `lineage_ray_batch` document
(every real emitter is nested inside each `LineageProcess`'s own dynamically-built inner composite,
invisible to a static top-level scan), so `_persist_emitter_history`'s own `gather_emitter_results`
call finds nothing to persist either.

**Practical consequence, right now**: don't rely on this automatic analysis for a `lineage_ray_batch`
dispatch's real `cd1_*`-shaped output — read the real parquet directly from wherever `out_dir`
pointed (or the deployment-standard location if unset), the same way `run_standalone_analysis.py`
already does correctly for chain-dispatch (`out_uri` straight into `run_duckdb_analyses`, real hive
parquet, no local-download step). **The fix** (not yet built, correctly deferred behind higher CD2
priorities): give `run_multi_node_analysis.py` an equivalent direct-S3-read path, mirroring
`run_standalone_analysis.py`'s own approach, rather than its current flat-file-download design.

A real, working invocation (defaults mirror the real dispatch that verified `out_dir`, `database_id=282`):

```bash
SIMULATOR_ID=109 EXPERIMENT_ID=my-first-pbg-native-run \
NUM_NODES=2 N_SEEDS=2 N_GENERATIONS=1 \
OUT_DIR="s3://<your-bucket>/my-first-pbg-native-run/" \
./scripts/dispatch/pbg-dispatch.sh
```

`SIMULATOR_ID` must reference an already-built sms-ecoli simulator on the target environment
(`atlantis simulator latest --repo-url https://github.com/CovertLabEcoli/sms-ecoli --branch main`).

**A real, complete example of the resulting composite document** — not simplified, not
hand-written, produced by actually calling `build_lineage_ray_batch_document()` with these exact
params against a real downloaded ParCa cache — is checked in at
`../assets/composites/pbg-native-lineage-ray-batch-composite.json`. Reproduction steps: same file's
sibling `README.md`.

## 10. Proven results — real evidence, not projections

- **Real division survives the `ray:` actor boundary at scale.** A 16-node dispatch (`database_id=253`
  → refired as `254`) formed a real 16/16-node Ray cluster (confirmed via CloudWatch, not status
  fields), and real hive-partitioned parquet history landed in S3 for concurrently-running lineages.
- **The `out_dir` dispatch-time parameter works, checked live.** Dispatch `database_id=282`: the real
  submitted AWS Batch job command (`RAY_JOB_CMD` env var, pulled directly via `aws batch
  describe-jobs`) contained the exact `out_dir` value passed at dispatch time. `aws s3 ls --recursive`
  on that exact prefix, while the job was still RUNNING, showed real, growing (400→2528 simulated
  seconds) parquet history chunks for both seeds, with **zero local-then-sync step** — this is the
  direct, positive proof the whole design's central claim needed.
- **A durable `n_workers` fix, profiled at real scale.** With the fix (`n_workers=100` stopgap, then
  the durable `None`-default fix): all 100 seeds of a real dispatch started within an 11-second window
  and completed generation 0 within a 6m46s window — ~30x faster than the pre-fix pace (~1 new seed
  starting every 4-6 real minutes).

## 11a. Strain/injection/variant support — real, proven on real infra (added 2026-09-03)

**This capability already existed at the `LineageProcess`/document-builder level before today** —
`build_lineage_ray_batch_document()` and `LineageProcess` itself both already accepted and correctly
threaded `injected_processes`/`variants`/`config_overrides` into every generation's real `baseline()`
call. The ONLY real gap was that `lineage_ray_batch()` — the thin `@composite_generator` wrapper, the
one layer viva-api's dispatch path can actually reach — never forwarded them, so any attempt hit
`KeyError: unknown override(s)`. This was a near-trivial, mechanical exposure fix, not new capability
engineering — worth stating precisely, since an earlier characterization of this as "no strain/
injection/variant capability at all" was wrong and directly, correctly challenged.

**Fixed and proven end-to-end on real GovCloud infra.** `v2ecoli` PR #663 exposes
`variants`/`injected_processes`/`config_overrides`/`emitter_arg` on the wrapper (all default `None`,
byte-identical behavior for every existing caller). Fired via `atlantis composite run` against a real
CD2 config (`configs/cd2/run2_j3_injected_metabolism.json`, `swap_processes: {"ecoli-metabolism":
"ecoli-metabolism-redux"}`, `exclude_processes: ["exchange_data"]`) — `database_id=288`. The real
submitted request carried the exact `injected_processes` dict, the composite constructed on a real
`ray:`-addressed `LineageProcess` actor, and execution reached deep into
`sms_modules/processes/metabolism_redux.py` (the injected swap) — real biological code, genuinely
executing — before hitting an unrelated, pre-existing `IndexError` in that file (`self.
intermediates_idx` not an int/bool array; a modeling-code bug, not a dispatch/plumbing failure).

A real, updated example composite document (this time WITH `injected_processes` set, matching
dispatch 288's real content) is checked in at
`../assets/composites/pbg-native-lineage-ray-batch-composite.json` (regenerated 2026-09-03 against
the `feat/item109-lineage-ray-batch-injection-exposure` branch).

**A companion PR, stacked on #663**: `v2ecoli` PR #662 adds `variant_grid` (a genuine (variant, seed)
cross-product — the older `variants` param could only apply one shared override to every node; this
crosses N variants × M seeds into real, independently-configured `ray:LineageProcess` nodes) and
`required_leaves` (an `emitter_arg` option that makes `_open_xarray_emitter` RAISE, not silently
warn-and-skip, when a declared KPI column is absent from composite state — directly targets the
"looked successful, ran the wrong thing" class of bug this whole project has repeatedly hit
elsewhere, and is the most likely fix for §6's own xarray-mystery, below).

## 11. Known, real, currently-open gaps

- **The automatic post-completion analysis mechanism** — now DEFINITIVELY confirmed structurally
  broken for `lineage_ray_batch` dispatches (§9's own updated subsection above, dispatch 283's real
  result). Fix pattern known (mirror `run_standalone_analysis.py`'s direct-S3-read approach), not
  built — correctly deferred behind higher CD2 priorities.
- **Result aggregation across N lineages at 1000-seed scale** — not yet built, real safety finding: an
  originally-proposed "gather Step, triggered when every lineage reports complete" design was found
  UNSAFE against the real AWS Batch MNP entrypoint's own sync semantics (no shared filesystem across
  nodes; the final, authoritative per-node sync to S3 only starts once a worker receives SIGTERM,
  which only happens after the head process exits — a gather Step running inside the head process
  would read a stale or incomplete S3 union). A corrected design (per-lineage self-sync inside
  `LineageProcess.update()`'s own completion branch, mirroring chain-dispatch's own already-proven S3
  handoff mechanism) was chosen but is not yet built.
- **The xarray/zarr stream's silent absence** — §6's open question. `required_leaves` (§11a) gives a
  way to make this loud instead of silent going forward, but the root cause of the ORIGINAL absence
  (dispatch 282) is still not confirmed.
- **This mechanism has never been run at real 1000-seed production scale** — chain-dispatch has (see
  companion doc); this design has been proven at 2-node and 16-node/100-seed scale only. Per Alex's
  own explicit re-set priority (2026-09-03): this is now a LOWER priority than proving the mechanism
  can run CD2's real configs as they currently are, regardless of scale or first-attempt results.
- **`_mnp_node_vcpus`'s own real race condition** — on a commit's FIRST-EVER multi-node dispatch, this
  function can query a job definition's vCPU count moments after registering it, before AWS's own
  eventual consistency catches up, silently under-sizing the actor pool for that one dispatch. Fixed
  with retry-with-backoff (viva-api PR #372).
- **A real, separate, newly-found bug**: a multi-node composite's auto-analysis job's own DB status
  record can freeze at `running` indefinitely even after the real underlying AWS Batch job has
  genuinely succeeded (confirmed: dispatch 283's own analysis job showed `running` for 90+ minutes
  after the real job had finished in ~41 seconds). Not yet root-caused or fixed.
- **No workbench-UI dispatch path yet** — scoped in detail, not built:
  `../report/report-gaps-ui-triggered-pbg-native-dispatch.md`. Headline: a real, mature remote-dispatch
  mechanism already exists in the Study tab with a generic parameter passthrough that could carry a
  `multi_node_dispatch` payload with zero new server-side code — the real gap is narrower than
  building a new mechanism (no composite-id selector, no client-side form for these params yet).

## Appendix A — the real script used to produce the composite document in `../assets/composites/`

```python
from process_bigraph.composite_generator import apply_core_extensions
from process_bigraph.composite_spec import discover_specs, get as get_spec
from v2ecoli.core import build_core
from v2ecoli.workflow.batch_lineage_ray import build_lineage_ray_batch_document

discover_specs()

# Calls the pure document-builder directly -- byte-identical to what
# lineage_ray_batch()'s own composite_generator body returns, since
# prewarm_lineage_pool() (the other thing that function does) is a pool-sizing
# side effect on an out-of-band Ray runtime, not a document mutation. Bypasses
# a real dispatch's own ray.init() call, which is unnecessary for just
# inspecting the document's structure and fails in an unconfigured local dev
# environment for reasons unrelated to the document's real content.
doc = build_lineage_ray_batch_document(
    n_seeds=2, n_generations=1, base_seed=0,
    cache_dir="/path/to/a/real/downloaded/parca/cache",
    experiment_id="my-first-pbg-native-run",
    emitter="both", max_duration_per_gen=3600.0, time_step=1.0, media="minimal",
    out_dir="s3://<your-bucket>/my-first-pbg-native-run/",
)
```

A REAL dispatch instead calls `spec.to_document(overrides=overrides, core=core)` through the full
`run_pbg.py` path (§4) — this appendix's shortcut exists only to make local reproduction easy without
a live Ray/AWS environment.
