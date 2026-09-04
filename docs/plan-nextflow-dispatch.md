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

The general campaign is **not** a single chain. It is a two-level scatter with a gather:

```
for each of N variants v:
      ParCa(v)                 ← variant-SPECIFIC: its own strain/expression cache
         └─► for each of M seeds m:  lineage(v, m)     ← hours each
                                        │
              all N×M lineages ─────────┴─► analysis tasks over ALL the data
```

At **Run 4** that is N=84 variants × M=4 seeds: **84 ParCa runs, 336 lineages** (×8
generations each), then analyses that span every variant.

Two facts about ParCa make it a per-variant node rather than a shared prologue.
`cache_version.py` folds `new_genes`, `bundle_overrides` and `perturbations` into the
cache's `inputs_hash`, explicitly because *"Two strains that differed only [in those] …
wrong-strain cache verified clean"* — so a variant that changes strain or expression
**needs its own cache**. Today v2ecoli does not express that: `variants` is folded into
`config_overrides` at the *lineage* level (`workflow/batch_lineage_ray.py:145-150`), whose
own comment concedes *"A real variant sweep across `ray:` lineages is real, separate,
not-yet-scoped."*

v2ecoli/sms-ecoli reach AWS Batch two ways today, and neither expresses this DAG well:

| | shape | cost at Run 4 |
|---|---|---|
| **chain-dispatch** | one Batch job per *(seed, generation)* | 84 ParCa jobs + **2,688 generation jobs**, each paying container start + cache stage + cell rebuild |
| **pbg-native / Ray** (item 101) | one MNP job holding N `ray:LineageProcess` actors | **1 job**, but one *homogeneous* allocation for the whole campaign |

The Ray shape is the better of the two and is proven on real infrastructure. Its structural
limit is exactly what the shape above exposes: **84 ParCas want ~14 cores for ~10 minutes
each; 336 lineages want ~12 GB for hours each; the analyses are small** — three wildly
different resource profiles inside one campaign, all served by a single allocation held for
the whole run. A single lineage OOM (Eran's risk 3a: 7–9 GB by generation 3–4) takes the
allocation with it, and there is no per-unit retry.

A two-level scatter over heterogeneous, independent units with a final gather is the
canonical case for a DAG engine. That — not "one more way to run a job" — is the argument.

**A free consequence of content-addressing:** because a cache is identified by
`inputs_hash` over exactly the variant-defining inputs, two variants that differ only in
sim-time config resolve to the *same* ParCa. A DAG engine dedupes that automatically
(and `-resume` reuses it across campaigns); hand-rolled dispatch has to be told.

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

### Phase 0 result (2026-09-04): **PASSED**

Run against process-bigraph 1.8.3 with Nextflow 25.04.3, using **no v2ecoli code** — the
same shape with trivial shell commands, so a failure would have been unambiguously
Nextflow/renderer rather than v2ecoli. The renderer emitted exactly:

```
workflow  { ch_cache_dir = parca()
            ch_sweep_dir = lineage(ch_cache_dir)
            ch_report    = analysis(ch_sweep_dir) }
```

- **Default staging: passed, but proves less than it appears.** Content flowed end to end,
  yet the consumer's `cache` was a **symlink into the producer's work dir** — the local
  executor always has a shared filesystem, which is the very thing risk 2 says MNP nodes
  lack. Taking that as proof would have been a presence-check masquerading as an
  effect-check.
- **With `stageInMode 'copy'` + `scratch true`: the real result.** The consumer's `cache`
  was a **real directory, materialized independently of the producer's filesystem**, and a
  plain `open()` inside the task read it.

⇒ The mechanism risk 2 says is missing does exist: a consumer can receive a materialized
copy in its own scratch, needing **no shared filesystem and no S3 support in
`load_cache_bundle`**.

**Carry into Phase 4:** `stageInMode`/`scratch` is the lever, and it arrives via
`nextflow_directives` (now confirmed working). Set it explicitly rather than trusting an
executor default.

**Still unproven:** real S3 transfer (needs `awsbatch`); the real 90 MB + 165 MB cache
(the stand-in was 23 bytes).

### Go/no-go 4 tried early (2026-09-04): **FAILS — three independent blockers**

Because §1's shape made this the decisive question, it was tested ahead of schedule by
rendering 84 variants × 4 seeds. The result is worse than "renders 420 blocks", and it is
the reason Phase 1f is not a small job:

1. **Scatter cannot be expressed.** A scalar port fed by a list store makes the composite
   **fail to construct** — `bigraph_schema` raises *"cannot resolve subtypes for key
   'seed': List(_element=Integer) vs Integer"* inside `Composite.__init__` → `core.realize`
   → `resolve`, before the renderer runs at all. There is no "this port consumes one
   element of that store" concept, which is exactly what a scatter is.
2. **Widening the ports to list types renders 3 blocks — and 3 tasks.** The block count
   "passes" while the semantics are wrong: `parca(params.variant)` is **one** ParCa handed
   all 84 variants, not 84 ParCas. None of `flatten`, `combine`, `collect`, `cross`,
   `groupTuple`, `Channel.from`, `channel.of` appears anywhere in the output.
3. **It does not even run.** Composite state never reaches the `params {}` block, so the
   emitted `params.variant` is undefined:
   `WARN: Access to undefined parameter 'variant'` →
   `ERROR ~ A process input channel evaluates to null -- Invalid declaration 'val variant'`.
   Zero task work-dirs created. `_channel_expr_for_input`'s fallback (`:349`) emits
   `params.<joined_path>`, but `generate_nextflow_config` only emits params the *caller*
   passed to `deploy(params=…)` — nothing bridges a store's value into them.

`_cardinality: 'many'` was present on every port in both attempts and changed nothing —
**dead, confirmed empirically** rather than by reading.

⇒ **Phase 1f is not "~15 lines to add `.flatten()`".** It needs: a channel-source construct
(a list-valued store/param → `Channel.from(...)`); cardinality actually consulted; a
cross/join for variant×seed keyed by variant rather than a broadcast; the plumbing
operators wired in and tested; and a bridge from composite state into `params`. Treat it as
the **critical path of Phase 1**, not a footnote.

**Bonus `deploy()` bug for Phase 1g:** a *relative* `outdir` makes Nextflow treat
`<outdir>/main.nf` as a remote project name — it tried to pull
`https://api.github.com/repos/nextflow-io/scale_out/contents/main.nf`. `deploy()` should
pass absolute paths.

**Risk 6 arrived early.** Neither venv can run Phase 0 with real commands: v2ecoli has the
composites but PBG **1.5.0** (no `run_composite`, no `workflow/recipe`), while sms-ecoli has
PBG 1.8.3 but no importable `v2ecoli.composites`. Resolving that is a prerequisite for the
real-command half, not just for Phase 2.

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
- **Implement `_cardinality`, and make it two-dimensional.** It is read at `:486`, passed
  into `_channel_expr_for_input` (`:323`), and never used. The campaign in §1 needs a
  **cross product**, not a single scatter: a variant channel and a seed channel combined
  into N×M lineage tasks, each carrying *its own* variant's ParCa output — so the ParCa
  cache is **not** a broadcast `.first()`, it is the left side of a join keyed by variant.
  Then one `.collect()` for the gather.

  **The operators already exist** as plumbing Steps (`process_bigraph/plumbing.py`):
  `Combine` → `combine`, `GroupBy` → `groupTuple`, `Collect` → `collect`, `Join` → `join`,
  `Mix` → `mix`, emitted by `_emit_plumbing_call` (`nextflow.py:357`). ⚠ **No test in the
  PR exercises plumbing emission at all**, so treat these as unproven-but-present and cover
  them first.

  **Measured, not estimated** — see the go/no-go-4 result under Phase 0. This is the
  critical path of Phase 1, and it is four things, not one: a **channel-source** construct
  (list-valued store → `Channel.from`), cardinality actually **consulted**, a **cross/join**
  keyed by variant, and a **state → `params`** bridge without which the rendered workflow
  does not even launch.
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
- **`@composite_generator("workflow_nf")`** assembling the §1 shape: **one ParCa node per
  variant**, each feeding that variant's M lineage nodes, all N×M gathering into the
  analysis node. Reuse `build_lineage_ray_batch_document` for the seed loop so it stays
  shared with pbg-native. This document is **rendered, never `run()` in-process**.

  > **This is where the plan asks for something v2ecoli does not have yet.** Today
  > `variants` collapses into `config_overrides` on the lineage
  > (`batch_lineage_ray.py:145-150`) — one shared cache for the whole sweep. The
  > per-variant ParCa node needs the variant's strain inputs (`new_genes`,
  > `bundle_overrides`, `perturbations`) threaded to *ParCa*, not to the lineage. That is
  > the same widening Eran's **N1/N2** describe (generator façade parameters, and the
  > seed loop generalized to (variant, seed) pairs), which is another reason to do N1–N4
  > once, first, rather than twice.

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

1. Local **2 variants × 2 seeds** (2 ParCas → 4 lineages → analysis) produces
   hive-partitioned parquet with a real `global_time`, and **each lineage used its own
   variant's cache** — assert the `inputs_hash` in each lineage's `cache_version.json`
   differs by variant. A single shared cache silently passing here would hide the whole
   per-variant question.
2. **The handoff travels as a staged `path`**, not as `--state-out` JSON. *(Mechanism
   proven locally in Phase 0; this re-tests it with the real cache.)*
3. `-resume` after a mid-lineage kill re-runs only that lineage — not its ParCa, and not
   the other variants.
4. **84 variants × 4 seeds still renders ~3 process blocks**, not 420, and `-stub-run`
   compiles it. This is the `_cardinality` + plumbing-operator work; it is the difference
   between a DAG and a generated pile.
5. Nextflow head overhead vs a direct `run_composite` on the same small job < ~2 min.

**If (2) or (3) fails, stop.** Those two are the entire justification for a third path.
**If (4) fails, the shape is wrong** even if it runs — 420 hand-emitted blocks is not an
improvement on hand-rolled dispatch.

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
