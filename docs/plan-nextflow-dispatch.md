# Nextflow as a third dispatch path: coarse outer DAG, process-bigraph inner engine

**Status (2026-09-04):** design, nothing implemented. Code-grounded against viva-api
`main` @ `1fa6fb14` (0.9.91), process-bigraph PR
[#197](https://github.com/vivarium-collective/process-bigraph/pull/197) (git ref `pr197`,
and shipped identically in PBG **1.8.3**), v2ecoli and sms-ecoli working trees, and
vEcoli's `runscripts/nextflow/`. Claims marked ⚠UNVERIFIED were not checked.

> Companion documents: [`plan-chain-dispatch-generations.md`](plan-chain-dispatch-generations.md)
> (the three current dispatch paths, and what is still open on chain-dispatch) and
> sms-ecoli's `docs/govcloud_pbg_native_design.md` (Eran, on branch
> `docs/govcloud-pbg-native-design`), which proposes the *other* answer to the same problem.

---

## 1. The problem

`ParCa → N lineages → analysis` is a DAG of a few **coarse** units. v2ecoli/sms-ecoli reach
AWS Batch two ways today, and neither expresses that DAG well:

| | shape | cost at Run 4 (336 lineages × 8 generations) |
|---|---|---|
| **chain-dispatch** | one Batch job per *(seed, generation)* | **2,688 jobs**, each paying container start + cache stage + cell rebuild |
| **pbg-native / Ray** (item 101) | one MNP job holding N `ray:LineageProcess` actors | **1 job**, but one *homogeneous* allocation for the whole campaign |

The Ray shape is the better of the two and is proven on real infrastructure. Its structural
limit is that ParCa wants ~14 cores for a few minutes, a lineage wants ~12 GB for hours, and
analysis is small — but they all get the same allocation, held for the whole run. A single
lineage OOM (Eran's risk 3a: lineages reach 7–9 GB by generation 3–4) takes the allocation
with it, and there is no per-unit retry.

Nextflow at **lineage granularity — never at Step granularity** — buys three things the MNP
shape structurally cannot: per-task retry, per-task resources, and `-resume`.

This is the composition Alex asked about on process-bigraph#197 and that nobody answered:
**Nextflow owns the campaign DAG; process-bigraph — and the `ray:` actors inside a
lineage — own everything below a task.** It is explicitly *not* "one Nextflow task per
process-bigraph Step", which is the granularity Alex correctly objected to.

## 2. The thesis — and what falsifies it

Eran's design doc, risk 2, concludes that the ParCa cache is local-filesystem-only
(`load_cache_bundle` reads `sim_data_cache.dill` with a plain `open()`; `read_cache_version`
uses `os.path.exists`), that MNP nodes share no filesystem, and that this **forces** the
two-job ParCa→sim shape — leaving "is per-node refit acceptable?" open.

Nextflow answers that generically: under an S3 work-dir it stages a `path` output into the
consuming task's scratch automatically, with no dill/S3 support needed anywhere in v2ecoli.

> **If the ParCa→lineage handoff cannot travel as a staged `path`, this path has no
> advantage over Ray/MNP and we stop at Phase 0.** It must not land as a third path that is
> merely *different*.

## 3. Scope and positioning

Decided: a **real third dispatch path** intended for production · **all three repos** ·
**local executor first, then AWS Batch** · **purely additive** — nothing is deleted and the
consolidation question is deferred · acceptance is **Run 4 scale**.

Positioning against Eran's proposal matters, because his §(e) has an explicit "viva-api
stops doing" table that deletes chain-dispatch. **That proposal and this plan agree on the
load-bearing claim** — generations stay in-process inside `LineageProcess` — and differ only
on who owns the outer DAG. Concretely, his **N1–N4 are shared prerequisites**: do them once,
in v2ecoli, before splitting effort.

## 4. What already exists (reuse inventory)

Verified 2026-09-04. The point of this table is that most of the machinery is already built.

| Already there | Where |
|---|---|
| The renderer, `run_composite`, `build_from_recipe` | **PBG 1.8.3**, `nextflow.py` byte-identical to `pr197`; installed in sms-ecoli's and vivarium-workbench's venvs |
| `--build RECIPE.json` → a named `@composite_generator` + overrides + **`artifacts`** (an `ArtifactRef` whose `store` — a bundle *directory* — is injected into overrides) | `process_bigraph/workflow/recipe.py` — **this is the ParCa→lineage channel** |
| `lineage_ray_batch` generator; N per-seed `ray:LineageProcess` nodes | `v2ecoli/composites/lineage_ray_batch.py:21`, `workflow/batch_lineage_ray.py:128-166` |
| **`parca` is already a registered generator** | `v2ecoli/composites/parca.py:94` — *no registration shim needed* |
| `v2ecoli-parca` / `v2ecoli-analyze` console scripts, both `s3://`-capable | `v2ecoli/pyproject.toml:162,166` |
| A **proven GovCloud Nextflow profile** | `vEcoli/runscripts/nextflow/config.template:98-120` |
| K8s head-Job submit shape; `.nextflow.log` → S3 | `viva_api/simulation/simulation_service_k8s.py:213-331` |
| Weblog receiver + NDJSON event parsing, tested against a real fixture | `viva_api/common/hpc/nextflow_weblog.py`, `common/hpc/models.py:435-467` |
| Runner staging to S3 (the 8192-byte Batch command cap) | `viva_api/simulation/simulation_service_ray.py:925` |
| `MAX_INTERVAL_TIME = 100_000`, so a 28,800 s lineage is already accepted | `viva_api/api/routers/compose.py:55` (shipped in 0.9.91) |

**Nextflow is already live in viva-api — for vEcoli, not v2ecoli.** `ComputeBackend.BATCH`
= "AWS Batch via Nextflow" (`config.py:416-421`), running vEcoli's own
`runscripts/workflow.py` inside a per-commit `vecoli:<sha>-amd64-submit` image;
`ComputeBackend.SLURM` is Nextflow with the local/slurm executor. `RAY` — where
v2ecoli/sms-ecoli dispatch — is the one backend that never touches Nextflow. So this work
adds a *second, different* route to Nextflow, for the process-bigraph world.

> **Do not reuse `NextflowLayout`** (`common/storage/data_layout.py:180-204`) despite the
> name. It encodes vEcoli's **double-nested** `{prefix}/{eid}/{eid}` download prefix;
> v2ecoli's emitters write the single-nested `RayLayout` shape. Keeping `RayLayout` is
> required or result download breaks silently.

## 5. The four gaps in the renderer

`render_composite` (`process_bigraph/nextflow.py:407`) has **two** loops. The Step-network
loop is fully supported. The second (`:506-543`) already emits **one task per nested
`Composite`**, shelling out to `run_composite` — the coarse unit this plan needs — but its
docstring calls it experimental, and it is:

1. **Not in the topological order.** The Step loop derives `order` from `node_dependencies`;
   this loop splices calls with `workflow_lines.insert(-1, …)` (`:541`, `:543`) in
   `process_paths` iteration order.
2. **No document staging.** `doc_ref = doc_map.get(name, f'{name}_document.json')` (`:513`) —
   the caller supplies paths by hand and nothing copies them into the task work dir.
3. **First port only.** `next(iter(…))` at `:243` and `:525`. The comment at `:521` already
   admits declaring `output:` for ports 2..n would break. **The same bug is on the supported
   Step path** at `:493`, where a multi-output Step silently loses channels 2..n.
4. **`Composite` subclasses `Process`, not `Step`** (`composite.py:383`), so a bare
   `Process` — `LineageProcess` is one — gets **no task at all**. The boundary is inferred
   from Python type rather than declared.

A fifth, needed for the fan-out: **`_cardinality` is dead.** It is read at `:486` and passed
into `_channel_expr_for_input` (`:323`), whose body never references it.

## 6. The annotation

**`nextflow_task = True`, a class attribute** — with a companion instance method
`nextflow_task_recipe()` returning a `build_from_recipe` document.

The naming is not cosmetic. Everything the renderer actually reads is an **unprefixed class
attribute**: `nextflow_script()` (`:197`), `nextflow_directives` (`:285`),
`nextflow_operator` (`:127`, `:364`), `nextflow_port_decls` (`:292`). Every
underscore-prefixed name *documented* as an annotation but never read — `_nextflow`,
`_nextflow_directives` — is a port key that was never implemented, and
`_port_to_nextflow_decl`'s own docstring (`:82-84`) says why: schema keys bypass
bigraph-schema's `reify_schema` parameter walk, which is exactly why `nextflow_port_decls`
ended up on the class. `nextflow_operator` — a plumbing Step declaring that it renders as a
channel operator rather than a process block — is the direct precedent: a node declaring
*how it renders*.

`nextflow_task` promotes **any** node — `Step`, `Process`, or `Composite` — into the task
loop, and a task **swallows its subtree**. That closes gap 4 without asking consumers to
restructure their objects to satisfy an `isinstance` check.

The contract has **two halves**, and both must be documented at the annotation — this is
@cplong90's point on #197, and it is right:

- **Granularity** — this node is one invocation; nothing below it is scheduled, retried, or split.
- **Durability** — what survives an interruption is whatever the emitter already flushed.
  There is **no resume point**, and the wall-clock ceiling is set by the executor, not by
  process-bigraph.

Stated together the trade is legible at the point of use. Stated separately it is a trap:
the two properties that make an atomic task attractive are the same two that remove the
intermediate recovery points.

## 7. Phases

### Phase 0 — prove the handoff, zero framework changes

New `v2ecoli/composites/workflow_nf.py`: three thin `Step`s carrying **no simulation
logic**, each declaring only ports plus a `nextflow_script()` (the already-sanctioned CLI
escape hatch):

- `ParcaTaskStep` → `outputs {cache_dir: {_is_file: True}}`; script = `v2ecoli-parca …`
- `LineageTaskStep` → `inputs {cache_dir (_is_file), seed}`; script =
  `python -m process_bigraph.run_composite --build lineage.recipe.json --set base_seed=${seed}`
- `AnalysisTaskStep` → `inputs {sweep_dirs: {_is_file, _cardinality: many}}`; script = `v2ecoli-analyze …`

Then `nextflow_deploy.deploy(..., executor='local', launch=True)`.

**Go/no-go 0:** the run completes with 3 process blocks and the lineage task reads a
`sim_data_cache.dill` that ParCa produced in a *different* work directory. That is §2's
thesis, tested for the cost of a day. These shadow-Steps are throwaway; Phase 1 deletes them.

### Phase 1 — process-bigraph

On top of `pr197`, all in `nextflow.py` / `nextflow_deploy.py`:

- **Script precedence** — one dispatcher shared by both loops: `nextflow_script()` →
  `nextflow_task_recipe()` (`--build`) → `Composite` (`--document`) → plain `Step`
  (`run_step`).
- **One ordering pass** — replace both loops with a single pass over
  `step_paths ∪ process_paths`, minus descendants of a task node. `node_dependencies`
  **cannot** be reused (it is built from Steps only), so extend `_topological_order`'s
  producer/consumer fallback to be **prefix-aware** — otherwise a lineage writing
  `('lineages','lineage_0000','summary')` never connects to an analysis reading
  `('lineages',)`.
- **Staging** — `render_composite(stage_dir=…)` writes `<step>.recipe.json` per task node;
  delete the `:513` guess. `deploy()` passes `stage_dir=outdir`.
- **All ports** — full loops in place of `next(iter(…))`; `--initial-state` driven by an
  explicit annotation, not positional "first port"; named `emit:` labels on the Step path.
- **Implement `_cardinality`** — `many` on a consumer → `.flatten()` (scatter); `many` on
  analysis → `.collect()` (gather); `one` on the broadcast ParCa cache → `.first()`. This is
  what makes 336 lineages **3** process blocks instead of 336; roughly 15 lines.
- **`deploy()` fixes** — `render_options.setdefault('python', sys.executable)` (`:105`) is a
  latent Batch bug: the head's interpreter will not exist inside a task container. Default to
  `sys.executable` only for `executor='local'`. Add `resume`, `report`, `trace`,
  `weblog_url` and a `nextflow_args` passthrough; **`deploy()` never passes `-resume` today**.

### Phase 2 — v2ecoli / sms-ecoli

**Shared prerequisite first:** Eran's **N1** — widen `lineage_ray_batch`'s generator
`parameters` (`composites/lineage_ray_batch.py:30-70`) to accept `injected_processes`,
`config_overrides`, `variant_grid`, `emitter_arg`. The *builder* already accepts three of
them (`batch_lineage_ray.py:94-96`); only the façade omits them, and `--build` addresses the
façade.

- **Lineage.** `LineageProcess` gains `nextflow_task = True`, `nextflow_directives`
  (cpus / memory / **time**) and `nextflow_task_recipe()` returning `lineage_ray_batch` with
  `n_seeds=1, base_seed=<seed>` and `run.steps = generations * max_duration_per_gen` —
  **total simulated time**, the trap `batch_lineage_ray.py:105-110` warns about.
  `inputs()` goes from `{}` to `{cache_dir: {_is_file, _cardinality: 'one'}}`. **This is a
  change to a load-bearing class**: it must fall back to `self.config['cache_dir']` when the
  port is unwired, so Ray dispatch is untouched, and the Ray regression suite must pass
  before merge.
- **ParCa — use the CLI, not `--build`.** The generator is registered but explicitly
  *"Structural, not auto-run"*: it carries `raw_data=None` and does not set
  `run_steps_on_init`, so `Composite(doc).run(n)` would jump `global_time` and **run
  nothing, exiting 0** — exactly the silent-success class of viva-api #401/#402/#403. Use
  `nextflow_script()` emitting `v2ecoli-parca`. Eran's N5 (`SimInputWriteStep`) is what would
  later make `--build {"generator": "parca"}` honest.
- **Analysis — do not wrap yet.** `v2ecoli-analyze` is already atomic and `s3://`-capable; an
  `AnalysisTaskStep` with `_cardinality: many` gets gather semantics for free. Wrapping the
  `AnalysisStep`s as a generator would duplicate `run_analyses`' own thread-pool fan-out.
- **`@composite_generator("workflow_nf")`** assembling ParCa → N lineage nodes (reusing
  `build_lineage_ray_batch_document`, so the seed×variant loop stays shared with pbg-native)
  → analysis. This document is **rendered, never `run()` in-process**.

### Phase 3 — viva-api

**Recommendation: a new `SimulationServiceNextflow`, selected by a per-request
`nextflow_dispatch` config block — not a new `ComputeBackend`, and not
`ComputeBackend.BATCH`.**

- *Not `BATCH`*: `SimulationServiceK8s` is hard-wired to vEcoli — it builds a
  `vecoli:<sha>-amd64-submit` URI, `sed`s vEcoli's config template and runs vEcoli's
  `runscripts/workflow.py` — and its `NextflowLayout` would break v2ecoli result download (§4).
- *Not a new backend value*: `compute_backend_for_repo` maps **repo → backend**
  (`config.py:436-469`) and `layout_for` maps **backend → layout**. A `NEXTFLOW` member
  would either reroute all v2ecoli traffic or be dead configuration.
- *The right axis already exists*: chain-dispatch and pbg-native are chosen **per request**
  inside `SimulationServiceRay.submit_ecoli_simulation_job`, via the `multi_node_dispatch`
  extra (`simulation_service_ray.py:1195-1213`). `nextflow_dispatch` is the same shape — and
  it makes A/B against Ray trivial: same repo, same image, one config field different.

New: `viva_api/simulation/simulation_service_nextflow.py`, and
`viva_api/compose/render_nf.py` — a sibling of `run_pbg.py`, staged the same way, reusing
its `composite_spec.get()` + `apply_core_extensions` + `PBG_CORE_BUILDER` resolution
(`run_pbg.py:472-486`) rather than reinventing it, then calling `deploy(..., launch=False)`.

Verify **inside a Batch container job with `executor='local'` first** (reusing
`_submit_container`), to separate "does render+launch work in our real image" from "does the
awsbatch executor work".

### Phase 4 — the `awsbatch` profile

`generate_nextflow_config` emits literally `// STUB (untested in v1)` plus
`process { executor = 'awsbatch' }` — **no queue, work-dir, region, container or
errorStrategy**. "Untested" understates it; it is structurally incomplete. Model the real
one on vEcoli's proven profile:

| Emit | Source |
|---|---|
| `queue` | `settings.batch_amd64_queue` |
| `container` | ECR URI built as in `simulation_service_k8s.py:233-236` |
| `containerOptions --env AWS_DEFAULT_REGION` | required for S3 access in-container |
| `aws.region` | `settings.batch_region` |
| **`aws.client.endpoint`** | `https://s3.<region>.amazonaws.com` — the GovCloud-only line currently injected by a `sed` hack at `simulation_service_k8s.py:265-267`; emitting it natively deletes the hack |
| `aws.batch.maxSpotAttempts` / `maxTransferAttempts` | vEcoli precedent |
| `workDir` | `s3://{s3_work_bucket}/{s3_work_prefix}/{eid}/work` |
| per-label **`time`** | already supported by `_resource_lines`; the only bound on a runaway task |

Use `errorStrategy = 'finish'`. vEcoli uses `'ignore'`, which is safe **only because** it
also sets `workflow.failOnIgnore = true` (`config.template:96`); `'finish'` is safe
unconditionally, and an ignored failed lineage is precisely the silent-null mode Eran's N4
exists to prevent.

## 8. Go/no-go before Phase 4

1. Local `ParCa → 2 lineages → analysis` produces hive-partitioned parquet with a real `global_time`.
2. **The handoff travels as a staged `path`**, not as `--state-out` JSON.
3. `-resume` after a mid-lineage kill re-runs only the lineage, not ParCa.
4. `n_seeds=336` still renders **3** process blocks, and `-stub-run` compiles it.
5. Nextflow head overhead vs a direct `run_composite` on the same 2-lineage job < ~2 min.

**If (2) or (3) fails, stop.** Those two are the entire justification for a third path.

## 9. Risks

1. **An atomic multi-hour task has no intermediate resume point.** A retry restarts
   generation 0. The seam already exists and is unused: `LineageProcess.config_schema`
   declares `initial_carry_state_path`, `initial_generation_index` and
   `daughter_state_out_path`, added so a wave orchestrator could retry at generation
   granularity. A follow-on can turn one lineage task into a per-generation chain.
2. **`-resume` needs a durable session cache, not just a durable work-dir.** Nextflow's
   `.nextflow/history` and cache DB live on the **head's local filesystem** — an ephemeral
   K8s pod in this design. Plan the S3 sync of `.nextflow/` around the run; it is easy to
   forget until the first head OOM.
3. **`--state-out` swallows serialization failures and exits 0**, writing a
   `{note, error, composite}` marker instead — and v2ecoli's `LabeledArray` under the pinned
   `bigraph_schema==1.6.0` is the cited case. **Hard rule: never make `--state-out` a
   load-bearing edge.** Every real inter-task dependency is an `_is_file` port over a real
   artifact. Worth proposing `--state-out-required` upstream.
4. **No task timeout today.** Our Batch job definitions report `timeout=NONE` with
   `retry=2` (a retry restarts from scratch). Emit `time` per label — and ⚠verify
   empirically that Nextflow maps it to `attemptDurationSeconds`.
5. **This is a third path in a repo where a live design doc proposes deleting one.**
   Positioning is additive by decision, but N1–N4 are shared prerequisites; do them once.
6. **PBG version skew is a hard sequencing gate.** v2ecoli pins `process-bigraph` at git
   `branch = "main"` and currently resolves to **1.5.0** — the older 438-line `nextflow.py`,
   **no `run_composite.py`, no `workflow/recipe.py`**. Everything in Phase 2 needs `--build`.
   So **#197 must land on `main` and both venvs must be re-locked before Phase 2 is even
   testable** — which is also why #196 (removing the module from main) would have broken
   v2ecoli.

## 10. Verification

- **process-bigraph** — `pytest process_bigraph/tests/test_nextflow_{render,deploy}.py
  test_run_composite*.py`, plus new tests for: `nextflow_task` on a bare `Process`; a task
  node ordered *between* Steps; recipe staging; `_cardinality` → `.flatten()`/`.collect()`;
  multi-output named channels. `nextflow -C nextflow.config run main.nf -stub-run` is a cheap
  syntax gate. Note today only the `local` executor has an end-to-end test, and it is skipped
  unless the `nextflow` binary is present.
- **v2ecoli** — render `workflow_nf` at `n_seeds=2`, assert exactly 3 `process {` blocks and
  a `.flatten()`/`.collect()` pair; then `deploy(launch=True)` locally with
  `max_duration_per_gen=60`. Assert the hive partition columns
  (`variant=`/`lineage_seed=`/`generation=`/`agent_id=`) **and a real `global_time`** — an
  effect check, not a presence check (see #403).
- **AWS Batch** — config snapshot → `nextflow config -profile awsbatch` → `-stub-run` → one
  trivial `echo` task (proves IAM, the S3 endpoint and the ECR pull before any science) →
  `n_seeds=1, n_generations=1` → kill the head, `-resume`, assert ParCa shows `CACHED` in the
  trace CSV.
- **Acceptance** — Run 4: 336 lineages × 8 generations, with a same-seed subset compared
  against a Ray-dispatched run.

## 11. ⚠UNVERIFIED — check before relying on

- Whether Nextflow's `time` directive maps to AWS Batch `attemptDurationSeconds` at our
  pinned Nextflow version.
- Whether the `batch-submit` service account may submit to `batch_amd64_queue` and write the
  S3 work-dir prefix (it works for vEcoli, but that is a different queue selection).
- Whether an ECR image for v2ecoli/sms-ecoli with Java + the Nextflow binary exists —
  `_build_command(submit_image=True)` builds one only for vEcoli.
