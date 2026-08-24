# Design review: process-bigraph-native dispatch routing

**Status**: proposed, not started. Companion backlog item: `vivarium-workbench/.todo/backlog/49.md`.

## Summary

`viva_api/simulation/simulation_service_ray.py:964-971` (`submit_ecoli_simulation_job`) decides
which AWS Batch dispatch architecture to use — a single multi-node Ray job, or a full per-seed
chain-dispatch campaign — using two incidental scalars pulled off the raw request:

```python
composite = getattr(config, "composite", None)          # a routing-string enum, NOT a
                                                          # process_bigraph.Composite object
n_generations = int(config.generations or 1)
if composite is None and n_generations > 1:
    return await self.submit_chain_dispatch_job(...)
```

The composite this dispatches — `batch_baseline` (`sms-ecoli/v2ecoli/composites/batch_baseline.py`)
— already declares `n_seeds` and `n_generations` as real, typed parameters in its own
`@composite_generator(parameters={...})` schema. The routing decision never consults that
declaration. It re-derives the same N-seeds × G-generations shape by hand
(`submit_chain_dispatch_job`'s `for seed in range(n_seeds): for generation_index in
range(n_generations):` loop), in AWS Batch's vocabulary, instead of reading it off the composite's
own vocabulary.

**Verdict**: the routing conditional is inconsistent with process-bigraph's own design philosophy —
not because the framework has no principled representation for multi-instance/multi-generation
execution (it has one, at multiple levels, covered below), but because the *decision of which
dispatch shape to use* is made one layer above where any of that representation lives.

**Scope boundary — this is NOT a re-litigation of item 46.** Backlog item 46
("v2-per-cell-wall-time-reduction", `vivarium-workbench/.todo/backlog/46.md`) already investigated
a related-sounding but distinct question — how per-task *compute* should be isolated/allocated at
the infrastructure level (CPU capping, container bin-packing, Spot contention) — and concluded
"process-bigraph confirmed to have no opinion on this layer... 'process-bigraph-native' isn't a
coherent category for the fix, by design not gap." That finding stands and this proposal does not
touch it. Item 46 is about *how compute is physically executed*; this proposal is about *how the
decision of which dispatch shape to invoke is made*. Bigraph theory has nothing to say about CPU
cgroups; it has a great deal to say about how a composite's own declared structure should drive
what happens to it.

## Theoretical grounding

Primary source: Agmon, E. & Spangler, R.K., **"Process Bigraphs and the Architecture of
Compositional Systems Biology,"** arXiv:2512.23754 (main text + formal supplement). Earlier
foundational work by the same author: "Foundations of a Compositional Systems Biology," arXiv:2408.00942;
Agmon et al., *Bioinformatics* 38(7):1972-1979 (2022, Vivarium 1.0). Directly relevant applied
precedent cited by the 2026 paper: Skalnik et al., *PLOS Comp. Biol.* 19(6):e1011232 (2023) —
E. coli colony division modeled via the same structural-update idiom this proposal leans on.

A **bigraph** (Milner) is two graph structures sharing nodes: a **place graph** (hierarchical
containment) and a **link graph** (connectivity). Process Bigraph keeps the place graph and
replaces the link graph with a **process graph** — processes are first-class nodes wired to typed
stores via ports. The formal object (supplement §3.6) is `B = (Σ, x, R_T, R_L)`: a typed schema
tree, a concrete state tree, a type registry, and a link registry. A **Composite** (§3.7) packages
an entire internal process-bigraph behind an external interface — composites nest recursively and
are substitutable for atomic processes.

The directly relevant mechanism is **structural deltas** (supplement §3.5.2, Table S1). Alongside
ordinary value updates, the framework defines `insert`/`delete`/`move`/`rewrite`/`rewire`
operations that modify the *shape* of the state tree itself, expressed as typed deltas through the
same update semantics as numeric updates — the paper's own example is cell division. This is not
abstract: `process_bigraph/processes/growth_division.py`'s `Divide` Step (installed in this
ecosystem's own vendored `process_bigraph`, canonical `vivarium-collective/process-bigraph`
v1.8.2) returns `{'environment': {'_remove': [mother], '_add': daughters}}` on trigger — exactly
this vocabulary. The same package ships `ProcessEnsemble` (`composite.py:913`, a container
exposing a combined interface over multiple sub-processes) and `ParameterScan`
(`processes/parameter_scan.py`, builds N independent sibling sub-composites from a parameter grid
inside one `Composite`), plus a `parallel_steps`/`parallel_processes`/`parallel_workers` config
surface on `Composite` itself. Multi-instance execution is supported at the library level, not a
gap application code has to invent around.

This ecosystem's own model layer already answers the question twice, deliberately:
`v2ecoli/v2ecoli/steps/division.py`'s `Division` Step is the production, bigraph-native answer —
`{'agents': {'_remove': [self.agent_id], '_add': [(d1_id, d1_cell), (d2_id, d2_cell)]}}`, matching
the paper's own worked example almost exactly. Separately, `v2ecoli/library/vivarium_ecoli_engine.py`
implements an alternate, explicitly *non*-structural single-lineage generation driver, with its own
docstring stating plainly this is a deliberate, documented tradeoff to avoid a known `_add`-path
bug — not a claim that bigraph theory lacks the representation. Both still run inside
process-bigraph's own object model.

## Evidence this is fixable now, not just theoretically ideal

`CompositeSpec` is process-bigraph's *own* dataclass, not an ecosystem convention layered on top
(`process_bigraph/composite_spec.py:240`), with a single shared registry
(`_REGISTRY`/`register`/`get`, lines 431-447) that every `@composite_generator`/`@composite_spec`
decorator populates and `discover_specs()` walks. `CompositeSpec.parameters` already backs
`_merged_params`'s override-wins-over-declared-default resolution (lines 327-334) — the canonical
implementation of exactly the precedence semantics this ecosystem has repeatedly wanted (see the
item-35 dispatch-params work this same investigation grew out of).

Three independent call sites in this ecosystem already resolve composite identity/parameters
through this one shared registry:

1. **`viva_api/compose/run_pbg.py:169-179`** — same repo as the flagged code, one directory over.
   `_resolve_document()` calls `get_spec(composite_id)`, falling back to `discover_specs()` then
   retrying. Its own docstring states the intended scope directly: *"This is what lets a
   model-specific dispatcher (e.g. viva-api's vEcoli ensemble endpoint) submit a config-driven run
   through the exact same execution mechanism a hand-authored `.pbg` document uses"* — naming the
   very subsystem containing the flagged conditional as the intended consumer it doesn't yet have.
2. **`vivarium_workbench/lib/composite_resolve.py`** — `classify_run_kind()` (lines 106-143) walks
   a composite's resolved state tree and classifies it `temporal`/`workflow`/`unknown` purely from
   each node's own `_type` field (`"process"` vs `"step"` — process-bigraph's own native scheduling
   discriminant, not an ecosystem-invented flag). Wired end-to-end into `/api/composite-resolve`
   and the Configure-run form's field generation (`static/configure-run.js`).
3. **`vivarium_workbench/lib/pbg_export.py`** — the workbench's own Composites-tab remote-run
   export path, per `run_pbg.py`'s own comment, resolves through the identical registry.

Only the flagged routing decision in `simulation_service_ray.py` still reasons about request-shape
proxies instead of resolving the named composite through this already-shared mechanism.

`CompositeSpec.requires` (line 250, threaded through `resolve_composite()`'s payload and
`CompositeSpec.from_file`'s `requires:` YAML key) is a declared, already-plumbed extension field
with zero current consumers anywhere in this ecosystem (confirmed by repo-wide search) — a ready
seam for dispatch-architecture metadata if a composite ever needs to state something about its
execution shape beyond what `parameters` already implies.

## Scope of the problem — full inventory

The pattern is concentrated in viva-api, keying off two `SimulationConfig` fields: `composite`
(a `Literal["v2ecoli","vecoli"]` routing enum — despite the name, not a `process_bigraph.Composite`
object) and `generations` (a plain int):

| # | Location | What it decides |
|---|---|---|
| 1 | `simulation_service_ray.py:964-971` | Single MNP job vs. full chain-dispatch campaign (the flagged instance) |
| 2 | `simulation_service_ray.py:516-599` (`_sim_command`) | Among **three** different scripts (`run_comparison_ensemble.py` / generic `run_pbg.py` against `batch_baseline` / `run_phase0_xarray_ensemble.py`) |
| 3 | `simulation_service_ray.py:1013` | Whether to stage a runner S3 URI at all |
| 4 | `simulation_service_ray.py:731` | A weaker instance — `isinstance(modules, str)`, a type-check rather than composite metadata |
| 5 | `common/handlers/simulations.py:508-515` | Origin point: where the `composite` routing flag first enters `SimulationConfig` as an "extra" passthrough key |
| 6 | `config.py:326-349` (`compute_backend_for_repo`) + `common/handlers/simulations.py:440,1633-1650` | SLURM vs. K8s/Nextflow vs. Ray, by string-matching `simulator.git_repo_url` — a more defensible variant (repo-identity routing) but still "architecture decided by something outside composite structure" |

One further instance one layer down, inside the model repo itself:
**`v2ecoli/v2ecoli/workflow/run.py:58-121`** (`_resolve_parallel`/`run_workflow`) — Ray
worker-per-seed fan-out vs. single-process meta-composite, decided by an incidental `mode` string
plus `has_variants = bool(config.get("variants"))`; the docstring admits variant sweeps are
excluded from the parallel path "for now" — an acknowledged carve-out, not a structural
requirement.

**Vivarium-workbench does not have this problem** — its local-vs-remote execution decision is
already centralized in one resolver (`lib/run_core.py:18-21` `run_target_for` +
`lib/remote_pinned.py:63-100` `resolve_run_target`), built specifically (per its own docstring,
citing item 18) to stop different call sites from resolving local/remote differently. Worth citing
as the existing "centralize this decision" precedent to follow, even though its own inputs
(a `.viv-build.json` marker, an env var) aren't composite-structure-derived either. Two workbench
call sites already ARE composite/study-structure-derived and are the shape to generalize toward:
`lib/study_runs.py:679-701` (routes on the variant's own declared `kind:` in `study.yaml`) and
`lib/composite_runs.py:132-158` `run_with_division` (stops on a real runtime signal the composite
itself produces, not a config flag).

**`pbg-template` cannot have this problem** — it ships no execution/dispatch concept at all
(confirmed by full-repo search); execution is entirely downstream of the scaffold it provides.

## Proposed design directions

**Direction A — schema-declared topology (recommended, minimal diff, extends an existing
same-repo mechanism).** Replace the flag-based routing condition in
`simulation_service_ray.py:964-971` with one that resolves the named composite via
`process_bigraph.composite_spec.get()`/`discover_specs()` — the exact mechanism `run_pbg.py`
already imports and uses two files over in the same package — and reads dispatch-relevant shape
off its own declared `parameters` (`batch_baseline` already declares `n_seeds`/`n_generations`)
rather than off `SimulationConfig`'s incidental fields. This is a bounded, surgical change: one
routing site starts consulting a registry the repo already depends on and already trusts elsewhere,
in place of two ad-hoc scalar checks. Satisfies this ecosystem's own "extend an existing pattern
before inventing a new one" rule directly — `run_pbg.py` is the existing pattern.

**Direction B — structural-delta-driven generation progression (theoretically purer, deliberately
NOT proposed for immediate implementation).** Re-express generation-to-generation progression as
an actual process-bigraph structural delta (`_add`/`_remove`, matching
`v2ecoli/steps/division.py`'s own pattern) rather than a hand-built AWS Batch `dependsOn` chain.
Closer to the framework's own idiom for "produce the next generation," but a substantially larger
change — it would touch the same dispatch-chain mechanism adjacent to item 46's infrastructure
findings, is not concretely scoped yet, and its value over Direction A is not established. Recorded
here as a real, evidence-grounded future direction, not silently dropped — but explicitly out of
this proposal's scope, the same way item 46 deferred its own concrete infra fixes to dedicated
follow-up items (33/34/35) rather than attempting them inline.

Recommendation: **Direction A**. It closes the exact gap identified — dispatch-shape decisions
made without consulting the composite's own declared structure — with the smallest correct diff,
reusing a mechanism already proven and depended on in the same repo, and without touching
territory item 46 already carefully mapped.

## Proposed revision plan (Direction A)

1. In `simulation_service_ray.py`'s `submit_ecoli_simulation_job`, resolve the target composite via
   `process_bigraph.composite_spec.get(composite_id)` (mirroring `run_pbg.py:169-179` exactly,
   including its `discover_specs()` retry-on-miss) instead of reading `composite`/`n_generations`
   off `SimulationConfig` directly.
2. Read the resolved `CompositeSpec.parameters` for `n_seeds`/`n_generations` (or their declared
   equivalents) to decide chain-dispatch vs. single-job, rather than the current
   `composite is None and n_generations > 1` check. Explicit request-level overrides (a caller
   passing `num_seeds`/`num_generations` directly, as `remote_run_submit` does today) continue to
   win over the composite's own declared defaults — same override-wins-over-default precedence
   `CompositeSpec._merged_params` already implements, not a new precedence rule to invent.
3. Keep `submit_chain_dispatch_job`'s actual per-seed/per-generation AWS Batch submission mechanism
   unchanged — this proposal only changes how the ROUTING DECISION is made, not the dispatch
   architecture itself (that's Direction B's territory, explicitly deferred).
4. Real regression tests: a composite declaring `n_seeds`/`n_generations` routes to chain-dispatch
   without the caller passing `num_generations` explicitly; a composite with no such declaration
   and no explicit override falls back to the existing single-job path unchanged; an explicit
   request-level override still wins over a composite's own declared default (do not mock
   `composite_spec.get()` itself — exercise the real registry, per this ecosystem's standing
   anti-mock rule).
5. Bundle the fix, its tests, and any version bump into one PR, following viva-api's own documented
   release protocol (`viva-api/CLAUDE.md` "Release Protocol").
6. Deploy on the ecosystem's normal tag → release → build → deploy cadence, once merged.

Not in scope: Direction B (deferred, above); item 46's compute-substrate/CPU-isolation work
(tracked separately, items 33/34/35); the `requires` extension field (noted as available, not
needed for Direction A specifically).

## Revision, 2026-08-14 — architectural correction after verifying against real planning docs

An earlier draft of this proposal additionally suggested adding a generic `overrides: dict`
passthrough directly to `POST /api/v1/simulations`, so any composite-declared parameter could be
set through that endpoint by its own real name. **Withdrawn** — verified directly against
`viva-api/docs/PRE-DEMO-PLAN.md` and `PRE-DEMO-MASTER-PLAN.md` (real, dated planning docs, not
inferred) plus the current source tree, this would work against an already-made, already-partially-
built architectural decision:

- `PRE-DEMO-PLAN.md:34`: *"Generalization is the hinge to the grand design, not a bonus."*
  `:42`: *"Requirement: generalized + production-grade for ANY composite in ANY pbg-template
  workspace; v2ecoli is use-case #1."*
- `PRE-DEMO-PLAN.md:130`: *"We do NOT converge on the `/api/v1/simulations` endpoint (it's
  vEcoli-hardwired...) but we DO reuse the Batch machinery beneath it... Generic `run_pbg.py` is
  the driver for any composite; the vEcoli ensemble driver is the specialization."*
- This is not aspirational — it's real, current code. `viva_api/api/routers/compose.py` (mounted
  at `/compose/v1/`), `viva_api/compose/simulation_service_ray.py`'s `ComposeSimulationServiceRay`
  (docstring: *"Runs the GENERIC `run_pbg.py` runner on the same Batch MNP machinery... instead of
  the vEcoli ensemble driver"*), and `viva_api/compose/simulation_service.py`'s
  `ComposeSimulationServiceHpc` (the SLURM-side equivalent) already exist, already share the same
  registry-based per-backend-selection pattern the vEcoli ensemble path pioneered
  (`_init_simulation_service`'s `dict[ComputeBackend, SimulationService]`), and already run *any*
  composite document via `run_pbg.py`'s domain-agnostic `Composite(doc).run(steps)`.

**The correct picture**: `/compose/v1/*` is the ALREADY-DESIGNATED, ALREADY-PARTIALLY-BUILT home
for "faithful, any-composite, any-domain access" — not something Direction A needs to build.
`/api/v1/simulations` is deliberately kept as the vEcoli-ensemble specialization (its own
parameter surface, its own chain-dispatch machinery for the specific N-seeds × G-generations
checkpoint/resume shape that vEcoli's multi-generation lineages need) — a legitimate, intentional
specialization sitting *beside* the generic path, not something to collapse into it.

**Direction A's core fix is unaffected and, if anything, better justified by this**: making
`submit_ecoli_simulation_job`'s internal routing decision read the target composite's own declared
`parameters` (via the same `process_bigraph.composite_spec` registry the generic compose path
already depends on) rather than ad-hoc scalars is consistent with — not a workaround around —
this same "composites are the source of truth" principle, applied at the layer where this specific
endpoint actually operates.

**A genuinely separate, NOT YET DECIDED question this surfaced, deliberately not resolved here**:
should item 35's canonical multi-seed/multi-generation dispatch eventually run through
`/compose/v1/*` instead of `/api/v1/simulations`, now that a generic path exists? Unclear —
`/compose/v1/*` does not currently appear to have `submit_chain_dispatch_job`'s per-seed
checkpoint/resume chain-dispatch capability (that machinery looks like it may be
vEcoli-ensemble-specific by design, not yet generalized). Real, worth a dedicated future
investigation; explicitly out of scope for this item.

## MAJOR SCOPE EXPANSION, 2026-08-14 — the answer to the question above is now yes, scoped in full

The project owner made the deferred question above the new mandate: `/api/v1/simulations` is
explicitly back-compat only; the canonical fully-remote dispatch **must** run through
`/compose/v1/*`, non-invasively (reuse existing machinery, don't duplicate). Three parallel research
streams answered the open questions this raised, each independently verified against real source
(file:line citations throughout, not inferred).

### Is this pattern genuinely domain-agnostic? Yes — strong, convergent evidence, not assertion

The strongest evidence is internal to this ecosystem: the exact topology this needs (one shared
prep step → N parallel branches → each branch internally sequential) is **already implemented
twice, independently** — Nextflow's `vEcoli-private/runscripts/nextflow/sim.nf`/`template.nf` (a
real, shipped, general-purpose workflow engine's native idiom, not invented for whole-cell
modeling), and viva-api's own hand-rolled AWS Batch `dependsOn` chain
(`submit_chain_dispatch_job`), whose own docstring admits it was built *"mirroring vEcoli-private's
own Nextflow task granularity."* Two independent convergent implementations of the same shape is
real evidence the shape is the pattern, not an artifact of one implementation's choices. Further
corroborated by domain analogies with real precedent (MCMC multi-chain sampling, HPC
checkpoint/restart across ensemble members, genomics cohort pipelines — all the same "N × G, one
shared setup" shape) and by `batch_baseline` itself already generalizing past pure biology
internally (`variants` as a generic sweep-grid axis, independent of the seed axis).

`ParameterScan` and `ProcessEnsemble` (process-bigraph's own library) were checked directly and are
**not** usable building blocks: `ParameterScan` has no internal-sequential-stages concept and isn't
even parallelized yet (a `# TODO` in the library itself); `ProcessEnsemble` is an interface-merging
mixin, not a fan-out mechanism. `CompositeSpec.requires` — described earlier in this doc as an
"unused extension seam" — **that characterization was incomplete, corrected here**: `requires` has
a real, closed, actively-used meaning (`requires.processes`/`requires.types` — a build-time
"what must be registered" declaration, consumed by `viva_superpowers/composite_spec.py`, used by 3
real composites across 3 repos). Adding an unrelated `requires.dispatch` key would conflate two
different concerns under one name — not the right extension point.

### A real fork in the design space — two legitimate paths, not one

**Path A — generalize the hand-rolled AWS Batch chain into the compose subsystem.** Scoped in
detail by two research streams. What's already reusable as-is, zero new code: `_submit_mnp` (with
`depends_on`), `_ensure_mnp_job_def`, `_SubmitJobPacer`, the retry client/strategy, the campaign-DB
schema (`HpcRun.chain_n_generations`/`chain_final_job_ids` — confirmed generic, plain int/JSONB
list, no vEcoli shape), and critically `run_pbg.py --composite-id <id> --overrides <json>` (the
generic runner `_seed_generation_command` already targets) — `ComposeSimulationServiceRay` already
holds a real `SimulationServiceRay` instance (`self._ray`) and can call `self._ray._submit_mnp(...,
depends_on=[...])` today, mechanically, with no new imports. What's genuinely gap, four concrete
places:
1. Where `n_seeds`/`n_generations`/composite identity are sourced from — viva-api's own ORM +
   a hardcoded `V2ECOLI_BATCH_BASELINE_COMPOSITE_ID` constant, instead of the resolved composite's
   own declared `parameters` (`batch_baseline` already declares both).
2. The override-key vocabulary (`cache_dir`, `base_seed`, the checkpoint triad) is hardcoded as
   literal strings, even though these ARE the composite's own real parameter names — needs to read
   them off the resolved spec, not hardcode them.
3. **Real structural gap**: the compose subsystem has no per-commit image concept at all. Legacy
   resolves a per-commit image via `SimulatorVersion.git_commit_hash`; compose uses one static image
   tag for the whole deployment. Item 35 dispatches against a pinned commit — this needs real work,
   not just redirection.
4. ParCa is hardcoded as a non-optional first DAG node — needs to become optional/pluggable.

Additionally needed either way: new seed/generation-equivalent fields end-to-end on
`ComposeSimulationRequest`/the router, a place to persist per-instance job ids (extend `ComposeHpcRun`
or reuse the existing generic `HpcRun` table), and a fan-in poller (`JobScheduler.update_chain_campaigns`
is hard-typed to concrete `SimulationServiceRay` today — needs generalizing or a compose-side
sibling on `ComposeJobMonitor`, which currently has zero chain-related methods).

For a composite to self-describe which of its own parameters play the instance-count/stage-count/
resume-handoff roles (there's no framework-level introspection for this today — `batch_baseline`'s
`n_seeds`/`n_generations` naming is organic convention, not declared metadata), the clean small
addition is a new `CompositeSpec` sibling field — NOT a `requires` overload, a plain new field
following the exact extension pattern every other field there already uses:

```yaml
dispatch:
  topology: chain
  instances: n_seeds
  stages: n_generations
  resume: {stage_index_param: initial_generation_index,
           carry_state_in_param: initial_carry_state_path,
           carry_state_out_param: daughter_state_out_path}
```

**Path B — route through the already-live Nextflow-on-Batch backend.** viva-api already operates a
full, working, second backend (`ComputeBackend.BATCH = "batch"  # AWS Batch via Nextflow`,
`simulation_service_k8s.py`'s K8s-Job-runs-Nextflow-head pipeline) — currently serving
vEcoli-private only. process-bigraph's own `nextflow.py` (438 lines, compiles a Composite's step
graph into a real Nextflow DSL2 document) plus `plumbing.py`'s `Mix`/`Collect`/`Combine`/`GroupBy`/
`Join` dataflow Steps (explicitly designed so composite documents can express Nextflow-channel-style
dataflow "while still running natively in the process-bigraph engine") together mean a composite
COULD declare its N×G shape declaratively and have Nextflow's own native semantics execute it — zero
bespoke chain-dispatch code needed on viva-api's side at all. Architecturally the purest fit — reuses
an entire already-built, already-proven backend rather than extending a hand-rolled one. Real
unknown: whether v2ecoli/sms-ecoli's composites render correctly through `nextflow.py` today is
unverified — this backend currently only serves vEcoli-private, and connecting it to the v2ecoli
model family is its own, unscoped investigation.

### Recommendation: Path A now, Path B flagged as a real future direction, not dropped

Path A is the smaller, already-well-scoped, lower-unknown option — the four gaps above are concrete
and each individually bounded, all sitting on top of already-shared, already-proven primitives. Path
B is architecturally more elegant (zero new dispatch code, reuses a bigger existing surface) but
carries a real, currently-unscoped unknown (does v2ecoli render through `nextflow.py` at all?) that
would need its own investigation phase before any time estimate could be trusted. Recommend Path A
for the actual migration, with Path B recorded here — not silently dropped — as the direction worth
revisiting once Path A is live and there's room to ask whether the hand-rolled chain should
eventually be replaced by the Nextflow-native expression of the same shape.
