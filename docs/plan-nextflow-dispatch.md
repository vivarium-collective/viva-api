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
| **`POST /parca/new-gene-cache`** — the derived build made remotely reachable, so N builds run off one ParCa without staging from a laptop | viva-api#378, **merged 2026-09-03**, live at `api/routers/sms.py:525`. ⚠ **never exercised end-to-end** (its own PR body says so) — verify early |
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
> division event. The chain-dispatch run above divided and wrote **no `history/` partition
> at all** (3 `.pq` files vs 97 for the no-swap baseline; see viva-api#408 discussion), so
> "completed a generation" and "produced the generation's data" have already come apart
> once on this exact question.

⚠ **Nothing in this plan's Phase 0 or the four-stage prototype touches this.** Both use
stand-in Steps carrying no biology; the `LineageStep` wrapper of §2a changes *who invokes*
the layer, not whether a swap survives inside it.

**Shared prerequisite 1:** Eran's **N1** — widen `lineage_ray_batch`'s generator
`parameters` (`composites/lineage_ray_batch.py:30-70`) to accept `injected_processes`,
`config_overrides`, `variant_grid`, `emitter_arg`. The *builder* already accepts three of
them (`batch_lineage_ray.py:94-96`); only the façade omits them, and `--build` addresses the
façade.

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
   asserted `retry=2`, from reading a definition nothing uses. Emit `time` per label — and
   ⚠verify
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
