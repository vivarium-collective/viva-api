# Nextflow as a third dispatch path: coarse outer DAG, process-bigraph inner engine

**Status (2026-09-04):** design, nothing implemented. Code-grounded against viva-api
`main` @ `1fa6fb14` (0.9.91) — *main has since moved to 0.9.95; §11.4 records what changed
that touches this plan* — process-bigraph PR
[#197](https://github.com/vivarium-collective/process-bigraph/pull/197) (git ref `pr197`,
and shipped identically in PBG **1.8.3**), v2ecoli and sms-ecoli working trees, and
vEcoli's `runscripts/nextflow/`. The three claims left unverified have since been checked
against nf-amazon's bytecode and live GovCloud infrastructure — see §11.

> Companion documents: [`plan-chain-dispatch-generations.md`](plan-chain-dispatch-generations.md)
> (the three current dispatch paths, and what is still open on chain-dispatch) and
> sms-ecoli's `docs/govcloud_pbg_native_design.md` (Eran, on branch
> `docs/govcloud-pbg-native-design`), which proposes the *other* answer to the same problem.

---

## 1. The problem

The general campaign is **not** a single chain. It is a two-level scatter with a gather —
but *which* level is expensive depends on where the variant lives.

**Variants come in three tiers** (@cplong90, 2026-09-04). The middle one is CD2's common
case, and it is the cheap one:

| tier | what varies | cost |
|---|---|---|
| **ParCa-level** | bundle overrides, expression adjustments — anything reaching **reconstruction** | **N ParCas.** The cache *is* the variant |
| **Cache-level** ← *the common case* | new-gene expression **vectors** — what distinguishes one producer strain from another | **one ParCa + N short derived builds** |
| **Run-time** | process config, media, injected processes | **one cache serves all N** |

So for a set of strains the shape is **N independent jobs off ONE ParCa**, not an N×M
matrix of ParCas. Only the top tier forces N ParCas.

```
ParCa (ONE)  ──►  parca_state.pkl
                     └─► for each variant v:  derived build (short)
                             └─► for each seed m:  lineage(v, m)      ← hours each
                                       │
                     all N×M lineages ─┴─►  analyses over ALL the data
```

At **Run 4**: 1 ParCa + 84 derived builds + **336 lineages** (×8 generations), then
analyses spanning every variant.

The two-step producer is real and already exists: `v2ecoli-parca` writes
`parca_state.pkl`, then `scripts/build_cache.py` hydrates it into the loadable bundle
(`sim_data_cache.dill`, `initial_state.json`, `cache_version.json`) via `save_sim_input`,
and `scripts/build_new_gene_cache.py --state <parca_state.pkl>` is the derived build that
stamps an induction level. **`v2ecoli-parca` deliberately does not write a
`load_cache_bundle`-readable directory** — that is the hydrate step's job, and mistaking
this for a defect cost one wrongly-filed issue (v2ecoli#681, withdrawn).

> **The two steps do not share a flag surface**, and assuming they did was a live bug:
> `_parca_command()` appended `--new-genes`/`--bundle-overrides` to the `build_cache.py`
> invocation as well as to `v2ecoli-parca`, and `build_cache.py` has neither — ParCa
> succeeded (572.5 s, 669.3 MB, correct composed vio+GFP genes) and the hydrate died one
> command later on "unrecognized arguments" (viva-api#410, merged 2026-09-04). A
> `ParcaTaskStep` therefore emits **two commands with different flags**, not one.

v2ecoli/sms-ecoli reach AWS Batch two ways today, and neither expresses this DAG well:

| | shape | cost at Run 4 |
|---|---|---|
| **chain-dispatch** | one Batch job per *(seed, generation)* | 1 ParCa + 84 derived builds + **2,688 generation jobs**, each paying container start + cache stage + cell rebuild |
| **pbg-native / Ray** (item 101) | one MNP job holding N `ray:LineageProcess` actors | **1 job**, but one *homogeneous* allocation for the whole campaign |

The Ray shape is the better of the two and is proven on real infrastructure. Its structural
limit is what the tiers above expose — **four** distinct resource profiles in one campaign:

| unit | count at Run 4 | wants |
|---|---|---|
| ParCa | 1 | ~14 cores, ~12 min (measured: 12m 6s at `--mode fast`, 8 cpus) |
| derived cache build | 84 | short, cheap |
| lineage | 336 | ~12 GB, **hours** each |
| analysis | a few | small, and only after everything |

One allocation, held for the whole run, serves all four. A single lineage OOM (Eran's risk
3a: 7–9 GB by generation 3–4) takes the allocation with it, and there is no per-unit retry.

A two-level scatter over heterogeneous, independent units with a final gather is the
canonical case for a DAG engine. That — not "one more way to run a job" — is the argument.

**A partial consequence of content-addressing:** a cache is identified by `inputs_hash`
over the variant-defining inputs, so variants differing only in run-time config resolve to
the same ParCa and a DAG engine dedupes that for free. **But `inputs_hash` does not include
ParCa *mode*** — a `--mode fast` and a `--mode full` cache hash **identically**
(@cplong90). It is detectable only at retrieval, by the debug TF-condition count (1 vs 23);
nothing in the fingerprint will tell you. Any campaign that mixes modes can therefore reuse
the wrong cache silently, which is the same failure family as viva-api #401/#402/#403.

**And a second silent one, about the M seeds** (@cplong90). Whether M seeds share a founder
is **path-dependent**: a path that reads a baked `initial_state.json` out of a cache gives
**all M seeds the same generation 1** (2+ diverge); a path that regenerates a
per-(condition, seed) sub-cache does not. The machinery is seed-parameterised either way —
what decides it is whether you read a cache or rebuild one.

> It **passes every structural check**: M seeds write M distinct partitions, with genuinely
> differing contents and distinct hashes, and a validator reports success either way. For
> *"sample the stochastic trajectory"* a shared founder is fine and invisible. For
> *"independent cells to average over"* it is wrong — and nothing detects it.

⇒ This must be an **acceptance check on the science, not a structural check**, and §8
carries it. It is the same presence-vs-effect shape as everything else on this page.

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
| `scripts/build_cache.py` (hydrate `parca_state.pkl.gz` → loadable bundle) and `scripts/build_new_gene_cache.py --state` (the derived build) | v2ecoli `scripts/` — the two-step producer the tiers rely on |
| **`POST /parca/new-gene-cache`** — the derived build made remotely reachable, so N builds run off one ParCa without staging from a laptop | viva-api#378, **merged 2026-09-03**, live at `api/routers/sms.py:525`. ⚠ **was never exercised end-to-end**, as its own PR body said — and its first real call, 2026-09-04, **501'd** (viva-api#412, §11.4). Read the "verify early" flag as vindicated, not retired |
| A **proven GovCloud Nextflow profile** | `vEcoli/runscripts/nextflow/config.template:98-120` |
| K8s head-Job submit shape; `.nextflow.log` → S3 | `viva_api/simulation/simulation_service_k8s.py:213-331` |
| Weblog receiver + NDJSON event parsing, tested against a real fixture | `viva_api/common/hpc/nextflow_weblog.py`, `common/hpc/models.py:435-467` |
| Runner staging to S3 (the 8192-byte Batch command cap) | `viva_api/simulation/simulation_service_ray.py:925` |
| `s3_work_bucket` / `s3_work_prefix` / `s3_output_prefix` settings, **already populated live** (`S3_WORK_BUCKET` = the `smsvpctest` shared bucket, the one the IRSA role grants — §11.2) | `viva_api/config.py:215-217` |
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

> **Superseded by prototyping (2026-09-04).** The reasoning below stands as an analysis of
> the renderer's vocabulary, but it is **no longer the recommended change**. Wrapping the
> lineage in a **Step** (§2a) removes the need for a new annotation entirely: a Step already
> renders through `run_step`, the fully-supported path. Keep this section for the naming
> argument, which still applies if a `Process`-shaped node ever does need a task.

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

> ### ⛔ …and that same lever re-triggers a bug viva-api already fixed once
>
> *Found 2026-09-04 while rechecking the repos; it corrects the recommendation directly
> above.* `scratch true` is exactly what moves the task's cwd off `/app/v2ecoli` — and
> v2ecoli's imports do not survive that.
>
> - The image sets `WORKDIR /app/v2ecoli` and **no `PYTHONPATH` at all**.
> - Bare `from scripts._compare…` imports are load-bearing throughout —
>   `composites/ecoli_baseline.py:56,2273` (the `baseline()` that **`LineageStep` calls**),
>   `workflow/parca_study.py`, `workflow/comparison_materialize.py`, `v2ecoli/core.py:121`,
>   `composites/vecoli.py:72`. They resolve only because cwd is the repo root.
> - This already bit chain-dispatch: a real swap dispatch died on
>   `ModuleNotFoundError('scripts')`, and **viva-api#359** fixed it by putting
>   `PYTHONPATH={V2ECOLI_DIR}` into a shared `PBG_RUNNER_ENV` at all three `run_pbg.py`
>   call sites — noting there that `cd {V2ECOLI_DIR}` alone never worked, because CPython
>   puts the *script's* directory on `sys.path[0]`, not the cwd.
>
> **That fix does not travel to this path.** Under Nextflow the invoker is the generated
> `main.nf` process block, which knows nothing about `PBG_RUNNER_ENV`. So Phase 4 must
> re-emit `PYTHONPATH=/app/v2ecoli` itself, and "it works on chain-dispatch" is not evidence
> that it works here. The deeper fix is the seam reconciliation of shared prerequisite 2.

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

### The real finding: the renderer discards the hierarchy (2026-09-04)

Everything above this line described *symptoms*. The cause is one implementation choice.

`render_composite` has exactly two behaviours for a nested `Composite`, with nothing
between them: **flatten** (inner Steps become peers in one flat workflow) or **collapse**
(`isinstance(instance, Composite)` → one opaque `run_composite` task, `nextflow.py:506-543`).
The document keeps the hierarchy; the renderer throws it away.

Verified — a nested `Composite` exposes everything a recursive renderer needs:

```
step_paths        : ['lineage_0','lineage_1','lineage_2','lineage_3']
step_dependencies : present      node_dependencies : present
bridge            : {'inputs': {}, 'outputs': {}}
('lineage_0',)    : inputs={'cache': ['cache']} outputs={'o': ['sweep_0']}
```

The missing mapping is **one Nextflow sub-workflow per nested Composite** — ordinary DSL2,
with `take:`/`emit:` mapping onto the composite's own `bridge`:

```groovy
workflow runs {
    take: cache
    main: lineage(cache)          // ONE process, invoked over a channel
    emit: lineage.out.collect()   // N results as ONE collected channel
}
workflow { ch_results = runs(parca()); analysis(ch_results) }
```

**This dissolves all three gather failures at once**, because every one of them is an
artifact of the *unrolled* form — N lineages as N *distinct* process blocks, each with its
own channel, which is the only reason anything needs merging:

| measured failure | under sub-workflow emission |
|---|---|
| 255-parameter limit (`bad parameter count 257` at 256 inputs; JVM abort ≥264) | **gone** — `analysis(ch_results)` takes one argument |
| `Mix` unrepresentable (`TypeError: unhashable type: 'list'`; renders a TODO stub on the legal wiring; zero usages, zero tests) | **gone** — nothing to merge; `lineage.out` is already one channel |
| shared store silently drops N−1 producers (exit 0, no warnings, analysis saw **1** of 4) | **gone** — N invocations of one process, not N producers on one path |

**What survives from the unrolling experiment:** it is still true that unrolling gets
fan-*out* for free (421 blocks, 0.02 s, correct ordering, plain scalar ports), and that is
a usable fallback. It is just the wrong shape — it flattens what the document nests, and
then needs a merge that does not exist.

### A sixth silent failure, found while prototyping: config was never threaded

`run_step`'s CLI accepts `--config`, and the renderer **never emitted it** — only
`--class`, `--in`, `--out`. An unrolled sweep is N nodes differing *only by config*, so
**every node ran with defaults**: three lineages configured `seed=0,1,2` would all have run
seed 0.

> It passes every structural check — N tasks, N distinct work dirs, N outputs, N distinct
> hashes — and nothing reports a problem. It silently destroys a **parameter sweep** rather
> than a gather, and it is the same hazard @cplong90 flagged about shared founders,
> arriving by a completely different route.

Fixed in process-bigraph#201: a `config_ref` threaded through `_script_body`/`_process_block`,
each node's resolved config collected into `options['_staged_configs']`, and `deploy()`
writing those files beside `main.nf`. Verified — three staged configs carrying
`seed=0,1,2` and distinct `experiment_id`s, with a test asserting they do not collapse.

**This is the sixth instance of the page's organizing pattern**, and the one that would
have been hardest to notice: nothing about the output *shape* differs between a real sweep
and N copies of one run.

⇒ **Phase 1f is replaced by sub-workflow emission — nesting *plus* two more things**
(the second found only by building it; see the config finding directly above).
Not "implement `_cardinality`", not "fix `Mix` + the overwrite": preserve the hierarchy the
document already carries. But **nesting alone is not sufficient**, and this is the part
that only showed up on implementing it: the merge *inside* a sub-workflow is still N-wide,
and a naive `a.mix(b, c, …)` is a Java method call that dies at the same 255 limit. The
fix is to **chain binary mixes** (`x = x.mix(y)`, N statements of arity 1). Nesting makes
the parent's gather one channel; binary chaining makes the child's merge expressible at
336. Still no bigraph-schema change and no new plumbing operator.

**Prototyped and measured** — process-bigraph#201, stacked on #197. `parca → runs(N) →
analysis` on the local executor: **N=336, exit 0, the analysis task received 336 of 336**
(338 tasks, 13 s). Four changes beyond the two above were needed to get there, each a real
defect in its own right: a **unified topological sort** over Steps *and* nested Composites
(a Steps-only sort cannot see a dependency running *through* a composite node, so the two
ends were ordered arbitrarily); **registering a nested Composite's outputs as producers**
(otherwise a consumer resolves to `params.<path>` and the run dies with "A process input
channel evaluates to null"); and resolving **`take:` ports as bare identifiers**.

> Still open in that prototype, and flagged on the PR rather than assumed: `emit:` gathers
> every terminal channel in scope, which is right for a fan-out but ignores a composite's
> declared `bridge` outputs when it has them; and the silent shared-store overwrite is
> **not** fixed — it stops being load-bearing but stays reachable and quiet.

> The silent-overwrite behaviour is still worth making loud on its own merits — it is
> reachable from the documented API and fails quietly — but it stops being load-bearing.

**Three shapes are now on the table**, and the choice is a real one:

| | per-lineage retry / sizing | needs |
|---|---|---|
| **container as one task** (works today) | ✗ — structurally the Ray/MNP design | nothing |
| **unrolled + working merge** | ✓ | the overwrite repair + a working `Mix` |
| **sub-workflow emission** | ✓ | recursion in `render_composite` |

**Bonus `deploy()` bug for Phase 1g:** a *relative* `outdir` makes Nextflow treat
`<outdir>/main.nf` as a remote project name — it tried to pull
`https://api.github.com/repos/nextflow-io/scale_out/contents/main.nf`. `deploy()` should
pass absolute paths.

**Risk 6 arrived early.** Neither venv can run Phase 0 with real commands: v2ecoli has the
composites but PBG **1.5.0** (no `run_composite`, no `workflow/recipe`), while sms-ecoli has
PBG 1.8.3 but no importable `v2ecoli.composites`. Resolving that is a prerequisite for the
real-command half, not just for Phase 2.

#### The four-stage campaign, prototyped end to end (2026-09-04)

The §1 shape — **1 ParCa → N derived caches → N×M lineages → gather** — now renders and
runs. Derived builds and lineages share **one nested scope**, which is what keeps the
parent at three nodes regardless of N×M:

```groovy
workflow  { ch_parca_store   = parca()
            ch_results_store = runs(ch_parca_store)
            ch_report        = analysis(ch_results_store) }

workflow runs {
    take: parca_state
    main: ch_cache_v000 = derived_v000(parca_state)     // one ParCa, N derived
          ch_cache_v001 = derived_v001(parca_state)
          ch_sweep_v000_s00 = lineage_v000_s00(ch_cache_v000)   // each seed reads
          ch_sweep_v000_s01 = lineage_v000_s01(ch_cache_v000)   // ITS variant's cache
          ch_sweep_v001_s00 = lineage_v001_s00(ch_cache_v001)
          ch_sweep_v001_s01 = lineage_v001_s01(ch_cache_v001)
          _merged = …mix chain…
    emit: _merged.collect() }
```

Because each cache is **consumed inside** the scope, it is not terminal — so `emit:`
collects exactly the sweeps, and the analysis takes one argument at any N×M.

| check | result |
|---|---|
| `parca → runs → analysis` | ✅ 3 parent nodes, 8 blocks at 2×2 |
| one ParCa → N derived builds | ✅ |
| each lineage reads **its own** variant's cache | ✅ |
| **go/no-go 1: per-variant provenance distinct** | ✅ `2 × strain_000 (expr 1.0)`, `2 × strain_001 (expr 2.0)` |
| end to end | ✅ exit 0, 8/8 tasks |

**Two defects in my own process-bigraph#201 were found by running this**, both the page's
pattern turned on the author:

1. **The config was never a declared input.** `deploy()` wrote `<node>.config.json` and the
   script referenced `--config`, but Nextflow stages only *declared* inputs — so every task
   opened a path absent from its work dir. **All four lineages read `{}`: the variant sweep
   silently collapsed to one strain, with 8/8 tasks green.**
2. **Groovy does not interpolate single-quoted strings.** `file('${projectDir}/x')` is a
   literal dollar sign; the task failed during staging with **no `.command.err` at all**.

Both fixed (`23c339b`) with a regression test. The lesson generalises past this PR: *the
artifact existed and the flag was emitted — what was never checked is whether the consumer
could read it.*

**A third, cheaper trap, hit twice:** Nextflow stages an input under the **producer's**
filename, so a consumer declaring `path cache_dir` receives `cache/`. `stageAs` is
mandatory, not stylistic.

**Still unproven** — both need real biology rather than stand-ins: go/no-go **1b**
(founders differ across seeds, compared on content) and **1c** (ParCa mode recorded
out-of-band, since `inputs_hash` cannot distinguish fast from full).

> **1b got sharper rather than closer, 2026-09-04.** v2ecoli#680's `seed_overrides` is
> **pure addressing** — @cplong90 and @AlexPatrie both confirmed it copies three fields
> (`cache_dir`, `initial_carry_state_path`, `initial_generation_index`) and derives no
> state. So it gives independent *caches*, and independent *founders* only where the caches
> were built with different founders. That is exactly sufficient for §1's shape (1 ParCa →
> N derived strain caches, each a distinct founder by construction) and **not** sufficient
> for M statistically-independent replicates off one strain — for which the M caches still
> have to be built, and which is flagged in `batch_lineage_ray.py`'s own comment as real,
> separate, unscoped work. ⇒ **1b is a v2ecoli question, not a renderer question**, and this
> plan should not claim it.

## Phase 1 — process-bigraph

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

  **Replaced by sub-workflow emission** (see Phase 0). Neither `_cardinality` nor a
  `Mix`/overwrite repair is the right change: all of those patch symptoms of flattening a
  document that already says *nest*. Make `render_composite` **recurse into a nested
  `Composite` and emit a Nextflow sub-workflow** (`take:`/`emit:`) instead of collapsing it
  to a single `run_composite` task — **and chain binary mixes** in the `emit:`, because
  nesting alone still leaves an N-wide merge that hits the same 255-parameter limit.
  Prototyped in process-bigraph#201 and measured at 336 (see Phase 0); no bigraph-schema
  change and no new plumbing operator.
- **`deploy()` fixes** — `render_options.setdefault('python', sys.executable)` (`:105`) is a
  latent Batch bug: the head's interpreter will not exist inside a task container. Default to
  `sys.executable` only for `executor='local'`. Add `resume`, `report`, `trace`,
  `weblog_url` and a `nextflow_args` passthrough; **`deploy()` never passes `-resume` today**.

### Phase 2 — v2ecoli / sms-ecoli

**Shared prerequisite 0 — retire the swap-inside-`LineageProcess` risk** (@cplong90,
2026-09-04). Post-#369 `stop_at_division=True` is dispatched on **every** chain generation,
so **chain-dispatch and `lineage_ray_batch` both run through `LineageProcess` today**. A
failure in that layer therefore invalidates *both* plans, which is exactly the argument for
doing it once, first.

Evidence so far is split and neither half is sufficient:

| route | swap inside `LineageProcess` |
|---|---|
| chain-dispatch | a generation **completed** — real division at t=2527 s |
| `lineage_ray_batch` (pbg-native) | **crashed immediately**, `intermediates_idx` `IndexError` |

> **Acceptance bar: a completed generation that WROTE A HISTORY PARTITION** — not merely a
> division event. The chain-dispatch run above divided, wrote an 11.5 MB daughter
> checkpoint, and produced **no `history/` partition at all** — only a 500-byte
> outer-emitter fallback. So "completed a generation" and "produced the generation's data"
> have already come apart once on this exact question.
>
> *Corrected 2026-09-04:* an earlier version of this line compared 3 shards against 97 for
> "the no-swap baseline". **That comparison is withdrawn** — the 97 shards were written by a
> different, four-hours-older pbg-native run sharing the same `vecoli-output/<experiment_id>/`
> prefix. Cross-run S3 comparisons need isolated prefixes or a timestamp check against the
> job's own `createdAt`.
>
> **Scope, confirmed independently by @AlexPatrie:** this is **chain-dispatch-specific**.
> pbg-native dispatch 313 ran the same J3 config and the same swap, divided at the identical
> t=2527 s / ~826 fg boundary, and wrote a fully populated `history/` — 7 chunks at 45–53 MB,
> verified by direct parquet read. ⇒ **The `lineage_ray_batch` leg of the table above is not
> blocked by it**, and neither is this plan's use of `LineageStep`, which invokes the same
> layer the populated run used. Suspected cause is the ParquetEmitter "default experiment_id"
> fallback already seen on dispatch 287; not root-caused.

⚠ **Nothing in this plan's Phase 0 or the four-stage prototype touches this.** Both use
stand-in Steps carrying no biology; the `LineageStep` wrapper of §2a changes *who invokes*
the layer, not whether a swap survives inside it.

**Shared prerequisite 1 — ✅ LANDED, not ours to do.** Eran's **N1** (widen
`lineage_ray_batch`'s generator `parameters`, which `--build` addresses, to match the builder
that already accepted them) was merged as **v2ecoli#663** on 2026-09-04. The façade now
exposes `injected_processes`, `config_overrides`, `variants` and `emitter_arg`, plus
`seed_overrides` from #680. `--build {"generator": "lineage_ray_batch", …}` can therefore
carry a swap and a per-seed cache today, which is what Phase 2 needed from it.

> **One piece is still open: `variant_grid`.** #663 exposes `variants` (a flat override
> block), not the **(variant, seed) cross-product** — that is v2ecoli#662, still open. The
> §1 campaign shape needs the cross-product, so either #662 lands or the generating script
> unrolls the pairs itself. Unrolling is the cheaper path here and is what the four-stage
> prototype already does; #662 only matters if the pbg-native leg wants the same shape.

**Shared prerequisite 2 — the injection resolver seam (v2ecoli#682/#683/#684).** *Added
2026-09-04; it is the same root as the `PYTHONPATH` hazard in Phase 0, seen from the other
end.* A native swap target with no explicit `process_config` reached
`apply_injected_processes` with `config_dict=None` and was built on `config_schema`
defaults — for metabolism-redux that is an **empty stoichiometry and 0 homeostatic
targets**, so the process did nothing, the generation collapsed after one tick, and the run
**still reported success** (sms-ecoli#210 §3d — the violacein blocker). #683 threads
`cache_dir` into the injection spec; #682 makes the config-less case fail loud.

**#684 is the structural half and it is the one this plan depends on.** v2ecoli's wheel does
not ship `scripts/`, so `baseline()`'s bare import resolves *whichever repo's `scripts/` is
on `sys.path`* — on the GovCloud pod that is **sms-ecoli's vendored copy**, which carries the
native-redux builder that v2ecoli's own copy lacks. Cwd-dependent shadowing, decided by where
the process happens to be launched from. #684 wheel-ships a single absolute-imported
`v2ecoli/library/inject.py` as step **1 of 3**.

⇒ **Steps 2–3 belong in the shared set, alongside Eran's N1–N4** — call it **N5**. Every
outer DAG calls the same `baseline()`, and a resolver chosen by cwd is a hazard for all of
them. It is *sharper* for this plan than for the others, because `scratch true` changes the
cwd deliberately.

- **Lineage — wrap it in a `LineageStep`, do not annotate the Process.**
  *Prototyped and rendered 2026-09-04.* `run_step` calls `instance.invoke(state)` **once**,
  while `LineageProcess.update()` advances **one generation per call** — so registering the
  Process directly as a Step would run generation 0 and report success, another silent
  truncation. The wrapper's `update()` builds a one-node composite and runs it for
  `generations × max_duration_per_gen` of **simulated time**; the generation loop stays
  exactly where it is.

  Three problems dissolve at once:

  | | |
  |---|---|
  | `RayShadow_LineageProcess` | gone — no `ray:` registration, so the renderer sees a real class |
  | `inputs: {}` | fixed — `cache_dir` becomes a **real wire**, so ParCa→lineage is an edge the renderer stages as a `path` |
  | two output ports (`summary`+`complete`) | avoidable — emit one result port; `complete` is internal |

  It also needs **no new process-bigraph feature**: `run_step` is the supported path. That
  is what retires the `nextflow_task` annotation in §6.

  > **What it does NOT retire:** whether a *swapped* process survives inside
  > `LineageProcess`. The wrapper changes who invokes the layer, not what happens within
  > it — and both dispatch paths inherit that layer either way. See shared prerequisite 0.

  **And it makes Ray optional for fan-out.** `LineageProcess` contains no Ray
  (`grep 'import ray|ray\.'` on `lineage.py` is empty) — `ray:` is purely the addressing
  layer, and `local:LineageProcess` builds fine. A lineage is internally **sequential**
  (one generation per tick, each depending on the previous daughter state), so Ray's only
  contribution is running N *independent* lineages concurrently — which task scheduling
  also provides, with per-lineage retry and sizing that the actor form structurally cannot.
  Ray remains right for the colony/MNP composite, which is genuinely multi-node.

  **The shape, prototyped and rendered 2026-09-04:**

  ```python
  class LineageStep(Step):
      config_schema = {'seed', 'generations', 'max_duration_per_gen', 'cache_dir',
                       'out_dir', 'experiment_id', 'emitter', 'time_step', 'media',
                       # per-task biology -- see "separability" below
                       'injected_processes', 'config_overrides',
                       'variant_index', 'variant_name'}
      nextflow_port_decls = {'cache_dir': 'path cache_dir', 'sweep_dir': 'path "sweep"'}

      def inputs(self):  return {'cache_dir': {'_type': 'string', '_is_file': True}}
      def outputs(self): return {'sweep_dir': {'_type': 'string', '_is_file': True}}

      def update(self, state):
          # build a one-node composite around LineageProcess and run it for
          # generations * max_duration_per_gen of SIMULATED time
  ```

  **Separability: the swap is pushed down into each task's own config.** This is the
  property that makes N lineages N independent jobs. Each node's config is staged as its
  own `<node>.config.json` and read by `run_step --config`, so a task reads **its own file
  and nothing else** — no shared store, no ordering constraint between siblings, no barrier
  until the gather. A variant that changes biology is simply a different
  `injected_processes` in a different file; the 2-D sweep is expressed by *which configs
  exist*, not by any renderer feature. Demonstrated at 2 variants × 2 seeds:

  ```
  lineage_v0_s0.config.json   seed=0  variant=baseline  swap=none
  lineage_v0_s1.config.json   seed=1  variant=baseline  swap=none
  lineage_v1_s0.config.json   seed=0  variant=redux     swap=redux
  lineage_v1_s1.config.json   seed=1  variant=redux     swap=redux
  ```

  Contrast today's chain-dispatch, where `injected_processes` travels as a command-line
  `--overrides` blob → `baseline()` → `_build_batch_document` → `runner_config` →
  `BatchBaselineRunner` → `build_workflow_config` → `_lineage_node` → each generation's
  `baseline()`. **Seven hops, each a place to drop it** — and viva-api#385 was exactly a
  drop at the first one. Per-task config has one hop.

  > **Carry this detail into the implementation:** pass **no key at all** rather than an
  > empty `injected_processes`/`config_overrides`. "No swap requested" and "swap requested,
  > empty" are not the same thing downstream, and conflating them is the shape of
  > viva-api#401 (a nested block *replacing* a config's flat fields).

  **What this design does not fix:** a swapped generation currently emits **no history
  parquet** (12 MB daughter state, real division at t=2527 s, and zero `history/`
  partition — see viva-api#408 discussion). That lives inside the biology, so this shape
  neither causes nor cures it. It does mean the eventual fix lands in one place rather than
  along the seven-hop path.

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

- *Not `BATCH`* — **and there is now live evidence this ambiguity bites.** viva-api#412:
  `run_new_gene_cache` resolved its service through `get_simulation_service()`, i.e. the
  deployment's own `COMPUTE_BACKEND` default, which on `sms-api-stanford-test` is
  **`batch` — meaning "AWS Batch via Nextflow, for vEcoli"**. It 501'd on a deployment where
  every other route dispatches Ray fine, because those resolve repo-aware via
  `get_simulation_service_for_repo`. A *second* Nextflow route entering that same enum makes
  the ambiguity worse, not better — which is the strongest argument yet for the per-request
  axis below. Structurally: `SimulationServiceK8s` is hard-wired to vEcoli — it builds a
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

**Prerequisite, found 2026-09-04:** there is no v2ecoli/sms-ecoli image with Java and the
Nextflow binary — the submit layer is built only for vEcoli, and ECR confirms it (§11.3).
Add it before anything here can dispatch.

`generate_nextflow_config` emits literally `// STUB (untested in v1)` plus
`process { executor = 'awsbatch' }` — **no queue, work-dir, region, container or
errorStrategy**. "Untested" understates it; it is structurally incomplete. Model the real
one on vEcoli's proven profile:

| Emit | Source |
|---|---|
| `queue` | `settings.batch_amd64_queue` |
| `container` | ECR URI built as in `simulation_service_k8s.py:233-236` |
| `containerOptions --env AWS_DEFAULT_REGION` | required for S3 access in-container |
| **`containerOptions --env PYTHONPATH=/app/v2ecoli`** | **not optional under `scratch true`** — the image sets no `PYTHONPATH`, and v2ecoli's bare `scripts._compare` imports resolve on cwd alone. This is viva-api#359's fix, which does not reach a Nextflow-emitted process block (§Phase 0) |
| `aws.region` | `settings.batch_region` |
| **`aws.client.endpoint`** | `https://s3.<region>.amazonaws.com` — the GovCloud-only line currently injected by a `sed` hack at `simulation_service_k8s.py:265-267`; emitting it natively deletes the hack |
| `aws.batch.maxSpotAttempts` / `maxTransferAttempts` | vEcoli precedent — and **not optional**: the default is 0 and this queue is Spot-first (§11.1b) |
| `workDir` | `s3://{s3_work_bucket}/{s3_work_prefix}/{eid}/work` |
| per-label **`time`** | already supported by `_resource_lines`; the only bound on a runaway task. **Confirmed** to reach Batch `attemptDurationSeconds`, with a 60 s floor (§11.1) |

Emit `errorStrategy` **with `maxRetries`** — Nextflow-side resubmission is the only thing
that retries an OOM or a code fault, and it is what makes risk 1's "the outer DAG is the
resubmitter" true rather than aspirational (§11.1b).

Use `errorStrategy = 'finish'`. vEcoli uses `'ignore'`, which is safe **only because** it
also sets `workflow.failOnIgnore = true` (`config.template:96`); `'finish'` is safe
unconditionally, and an ignored failed lineage is precisely the silent-null mode Eran's N4
exists to prevent.

## 8. Go/no-go before Phase 4

1. Local **1 ParCa → 2 derived builds → 4 lineages → analysis** produces hive-partitioned
   parquet with a real `global_time`, and **each lineage used its own variant's derived
   cache** — assert the `inputs_hash` in each lineage's `cache_version.json` differs by
   variant. A single shared cache silently passing here would hide the whole per-variant
   question.
   **1b. Founders must differ across seeds.** Assert generation 1 of seed *m* differs from
   generation 1 of seed *m'* for the same variant. Structural checks pass either way (M
   distinct partitions, distinct hashes), so this has to compare *content*, not shape — see
   §1. If the campaign is "independent cells to average over" and founders are shared, the
   science is wrong and every green light stays green.
   **1c. Record the ParCa mode alongside the cache.** `inputs_hash` does not include it, so
   assert the mode out-of-band (the debug TF-condition count, 1 vs 23) rather than trusting
   the fingerprint.
2. **The handoff travels as a staged `path`**, not as `--state-out` JSON. *(Mechanism
   proven locally in Phase 0; this re-tests it with the real cache.)*
3. `-resume` after a mid-lineage kill re-runs only that lineage — not its ParCa, and not
   the other variants.
4. **84 variants × 4 seeds renders and RUNS, and the gather actually gathers** —
   assert the analysis task sees **336** sweeps, not 1. Counting process blocks passes on
   the broken version; only counting sweeps catches it. (Unrolled, 421 blocks render fine
   in 0.02 s but the 336-way gather cannot be expressed; under sub-workflow emission the
   count is small and `lineage.out` carries all 336.)
   **4b. Assert the emitted COLUMNS, not just the sweep count.** @cplong90's 2026-09-04
   correction on sms-ecoli#210 is the reason: a run that appeared to prove "the platform
   cannot emit 'omics" was really a **curated path list** — the leaf is simply absent from
   history while its *name* survives in `output_metadata`, and nothing errors. A 336-way
   gather of 336 column-starved sweeps passes every count-based check.
5. Nextflow head overhead vs a direct `run_composite` on the same small job < ~2 min.

**If (2) or (3) fails, stop.** Those two are the entire justification for a third path.
**If (4) fails, the shape is wrong** even if it runs — 420 hand-emitted blocks is not an
improvement on hand-rolled dispatch.

## 9. Risks

1. **Durability: checkpointing now exists; RESUMPTION is this plan's obligation.**
   *Rewritten 2026-09-04 — v2ecoli#680 overtook the original text, and @cplong90's reading
   of it sharpens what remains.* `LineageProcess` always wrote a per-generation checkpoint;
   it was a no-op on the pbg-native path because nothing gave it a real destination.
   **v2ecoli#680 (`checkpoint_dir` + `seed_overrides`) fixes that**, verified live on
   `smsvpctest` — a distinct `gen_{N:04d}.pkl` per generation, and a resumed lineage that
   read `gen_0000.pkl` byte-unchanged and went straight to `gen_0001.pkl`.

   > **But a checkpoint is only recovery if something resubmits** (@cplong90). The
   > per-commit job definitions every real dispatch runs on carry **no retry** (see risk 4),
   > so #680 converts *"no recoverable state"* into *"manually recoverable state"* — a real
   > gain (a 2-hour lineage failing at 1.5 h now loses 30 minutes, not everything), but not
   > automatic resumption. **On this plan the resubmitter is the outer DAG, i.e. Nextflow.**

   And that obligation is more specific than "Nextflow gives you retry": **`-resume` reuses
   cached *successful* tasks on a NEW invocation**; automatic in-run retry is
   `errorStrategy` + `maxRetries` — a config choice, and **absent from the `awsbatch`
   profile stub entirely** (§Phase 4). Emit it, or the outer DAG inherits `attempts: 1` and
   offers no more automatic recovery than today.

   Also not to over-read: **`seed_overrides` is pure addressing** — it copies `cache_dir` /
   `initial_carry_state_path` / `initial_generation_index` into one seed's config and
   touches neither state nor founders. It gives independent *caches*, and independent
   *founders* only if the caches it points at were built with different founders. Exactly
   right for 1 ParCa → N derived strain caches; **not** a founder-generation mechanism for
   replicate cells (see go/no-go 1b).

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
   **no retry** — *corrected 2026-09-04*. The base definitions carry
   `retryStrategy={'attempts': 2}`, but **nothing dispatches against them**: both
   `_ensure_mnp_job_def` and `_ensure_container_job_def` clone only `nodeProperties` /
   `containerProperties` and never `retryStrategy`, so every per-commit definition gets
   AWS Batch's default `attempts: 1`. Verified: base `smsvpctest-ray-container`
   `attempts: 2`; per-commit `…-container-68e2c67` `retryStrategy=None`. (Found by
   @cplong90 on the MNP side; it extends to the container path.) This plan previously
   asserted `retry=2`, from reading a definition nothing uses.

   > **Checked 2026-09-04, and it changes the shape of this risk on *this* path.** `time`
   > per label does map to Batch `attemptDurationSeconds` (60 s floor), and nf-amazon sets
   > **both** timeout and retry on the `SubmitJobRequest` — so the per-commit `attempts: 1`
   > above, real as it is for the Ray/chain dispatches, is overridden per submission here.
   > What replaces it is a worse default: `maxSpotAttempts` is **0** unless set, this queue
   > is **Spot-first**, and `errorStrategy` defaults to `terminate`. Both knobs must be
   > emitted — see §11.1b.
5. **This is a third path in a repo where a live design doc proposes deleting one.**
   Positioning is additive by decision, and the shared prerequisites are proving that out in
   practice: **N1 landed as v2ecoli#663** and the durability half as **#680**, both done by
   the pbg-native effort and both consumed unchanged by this plan. **N2–N4 remain, and N5 —
the #684 resolver-seam reconciliation — was added on 2026-09-04**; do them once.
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

## 11. The three ⚠UNVERIFIED items — checked 2026-09-04

All three are now settled. Two of them changed something in this plan: one adds a
**build prerequisite** Phase 4 did not carry, the other sharpens risk 4.

### 11.1 `time` → AWS Batch `attemptDurationSeconds` — **CONFIRMED, at both versions we pin**

nf-amazon's `AwsBatchTaskHandler` builds the `SubmitJobRequest`:

```groovy
// nf-amazon 2.15.0 (pulled by Nextflow 25.04.3 — Phase 0's version), :794-802
final time = task.config.getTime()
if( time ) {
    def secs = time.toSeconds() as Integer
    if( secs < 60 ) secs = 60            // Batch minimum
    result.setTimeout(new JobTimeout().withAttemptDurationSeconds(secs))
}
```

nf-amazon **3.4.2** (pulled by Nextflow **25.10.2**, the version the submit-image
Dockerfile pins) ships no source, so this was read out of the bytecode: `javap -v` on
`AwsBatchTaskHandler.class` shows the AWS SDK **v2** `JobTimeout` class and the
`attemptDurationSeconds` constant at adjacent constant-pool entries. Same mapping, newer SDK.

Two consequences worth carrying into Phase 4:

- **It is set on the `SubmitJobRequest`, not on the job definition** — a per-submission
  override, so no job-def change is needed to bound a runaway lineage.
- **There is a 60-second floor.** Any `time` below that is silently raised.

> **Version skew to resolve.** Phase 0 ran **25.04.3** locally; the only Nextflow we
> actually ship anywhere is **25.10.2**, baked into the vEcoli submit image. Pin one
> deliberately in 11.3's new image rather than inheriting whichever the Dockerfile's
> `ARG` default happens to be.

### 11.1b The same method also settles risk 4 — and the answer is worse than "no retry"

`retryStrategy` is attached to the submit request **only if `maxSpotAttempts() > 0`**, and
that default is **0** (`:736-743`; it is 5 only under Fusion snapshots). Meanwhile
`batch_amd64_queue` = `smsvpctest-vecoli-task-amd64` is **Spot-first** — verified live:
CE order 1 `Amd64SpotComputeEnv`, order 2 `Amd64OnDemandComputeEnv`, queue ENABLED/VALID.

⇒ On the stub profile as written, **a spot reclaim of a multi-hour lineage gets no Batch
retry and no Nextflow retry** (`errorStrategy` defaults to `terminate`), and takes the run
with it. Two independent knobs, both absent from the stub:

| knob | who retries | what it survives |
|---|---|---|
| `aws.batch.maxSpotAttempts` | **Batch**, same job, new instance | spot reclaim only — Nextflow pins `EvaluateOnExit`: `RETRY` on `Host EC2*`, `EXIT` on `*` |
| `errorStrategy 'retry'` + `maxRetries` | **Nextflow**, as a *new* Batch submission | everything else, including OOM |

This also resolves how risk 4's per-commit `attempts: 1` interacts with this path: it is
still true of the Ray/chain dispatches, but it does **not** bind here, because Nextflow
overrides retry *and* timeout per submission. **Risk 1's claim that "the resubmitter is the
outer DAG" is only true once the second row above is emitted** — it is a config line, not a
property of using Nextflow.

### 11.2 `batch-submit` may submit to `batch_amd64_queue` and write the work dir — **CONFIRMED (effect-checked)**

Not read off the policy — executed from inside the running api pod under the pod's own
IRSA identity (`kubectl exec … /app/.venv/bin/python`):

```
identity  = …assumed-role/smsvpctest-batch-BatchSubmitIrsaRole31BE49CD-xETZl8o1n6cE/botocore-session-…
s3://…-sharedbucket…-abfvwv0day91/nextflow/work/   put → get → delete   OK
batch:SubmitJob  smsvpctest-vecoli-task-amd64  →  ClientException
                 "JobDefinition definitely-not-a-real-jobdef-permcheck does not exist"
```

The SubmitJob probe names a deliberately nonexistent job definition, so it reaches the
service's own validation **without creating a job**; authorization is evaluated first, so
`ClientException`-not-`AccessDeniedException` is the proof. `S3_WORK_BUCKET` in the live
configmap is exactly the bucket the policy grants, and `s3_work_prefix` is free to be
anything under it (the grant is bucket-scoped, not prefix-scoped).

Backing policy `BatchSubmitIrsaRoleDefaultPolicy…`: `batch:SubmitJob` /
`RegisterJobDefinition` / `DescribeJob*` / `ListJobs` / `TerminateJob` / `TagResource` and
`logs:*` on `*`, plus full read/write on that one bucket. **`RegisterJobDefinition` matters
more than it looks** — the `awsbatch` executor auto-registers a job definition per
container image, and it is granted.

Two constraints found while checking:

- **`iam:PassRole` is limited to four named roles** (`smsvpctest-ray-mnp-execution`,
  `-ray-mnp-job`, `BatchComputeRole…`, and the IRSA role itself). So leave
  **`aws.batch.jobRole` unset**; any other value fails at registration/submit.
- With it unset the task container runs as the **instance profile**
  `smsvpctest-batch-BatchComputeRole…`, which independently carries the same full
  read/write on that same bucket (checked) — so staging in *and* out of the S3 work dir
  works from the task side, not just the head side.

### 11.3 A v2ecoli/sms-ecoli image with Java + Nextflow — **CONFIRMED ABSENT; it is a Phase 4 prerequisite**

- `SimulationServiceK8s._build_command(submit_image=True)` appends the submit layer
  (`default-jre-headless`, then `ARG NEXTFLOW_VERSION=25.10.2`) — and it is reached only
  from `_run_build`'s **amd64 vEcoli** build.
- `SimulationServiceRay._build_command` — the v2ecoli/sms-ecoli path — has **no
  `submit_image` parameter at all**; it clones the repo and runs
  `docker/build-and-push-ecr.sh`, full stop.
- Neither v2ecoli's nor sms-ecoli's root `Dockerfile` mentions java / jre / jdk / nextflow
  (zero hits in either).
- Live ECR (`us-gov-west-1`, 476270107793): repository `vecoli` carries **30** `*-submit`
  tags; repository `v2ecoli` carries **113** tags and **zero**.

⇒ Before Phase 4 can dispatch anything, either add a `submit_image` branch to the Ray build
path (mirroring the k8s one) or give v2ecoli its own submit Dockerfile. It is a small job,
but it is a **prerequisite, not a detail** — and it is the place where 11.1's Nextflow
version gets pinned.

### 11.4 Repo recheck (2026-09-04, ~17:00 UTC) — what moved under this plan

The doc is code-grounded at viva-api `1fa6fb14` (0.9.91). Since then:

| landed | why it touches this plan |
|---|---|
| **viva-api#410** → 0.9.95 — `_parca_command()` leaked `--new-genes`/`--bundle-overrides` onto `build_cache.py`; `chain-dispatch.sh`'s `EXTRA_PARAMS` default emitted invalid JSON | §1's two-step producer: the steps have **different flag surfaces** |
| **viva-api#412** (open) — `run_new_gene_cache` resolved the deployment-default backend (`batch`) instead of Ray; 501'd on its first-ever real call | §4's "never exercised" flag on #378, and Phase 3's "not `ComputeBackend.BATCH`" |
| **v2ecoli#682 / #683** — config-less native swap built an empty config → one-tick collapse **reporting success**; now threaded and fail-loud | shared prerequisite 2; the layer `LineageStep` calls |
| **v2ecoli#684** — wheel-shipped injection resolver, step 1/3 of un-shadowing `scripts._compare` | **N5**, and the same root as the Phase 0 `PYTHONPATH` hazard |
| **v2ecoli#685** — `ptools_rxns` tolerates an injected reaction widening the flux array (2831 vs 2830) | not this plan; noted so the delta is complete |
| **sms-ecoli#210** (Chris, 16:26 UTC) — the 'omics blocker is a **curated path list**, not emitter capability | go/no-go 4b |
| **process-bigraph#197 / #201** — both still open, no reviews | **the sequencing gate has not moved**; risk 6 stands unchanged |

Nothing here contradicts the plan's thesis. One thing contradicted a *recommendation* in it —
the `scratch true` / `PYTHONPATH` collision in Phase 0 — and that is corrected in place.

### Still unverified after this pass

- **Real S3 staging of the real cache** (90 MB + 165 MB, vs Phase 0's 23-byte stand-in).
- Whether an emitted `time` actually lands as `attemptDurationSeconds` on a *submitted*
  job. The mapping is proven in the code path; only a live submission proves the profile
  reaches it — and §10's "one trivial `echo` task" closes that together with 11.2, once
  11.3's image exists.
