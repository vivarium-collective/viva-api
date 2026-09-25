# Plan: split viva-api into a reusable core and the SMS service

> **Living document.** This is the migration plan; the architecture it moves between is in
> [`architecture-core.md`](architecture-core.md) (Part 1 current, Part 2 target, Part 3 the
> seam-by-seam delta table).
>
> **Maintenance rule.** Every PR that implements a step updates the [status ledger](#status-ledger)
> here, and — if it changes structure — `architecture-core.md` in the same PR. A change of
> plan is a dated entry in the [decision log](#decision-log), never a silent rewrite.
>
> **Standing rules.** Nothing merges without Jim's explicit go-ahead, per PR. Merge commits
> only. Every phase goes to dev (`sms-api-stanford-test`) before prod (`sms-api-stanford`),
> is verified by a marker grep on the newest pod, and is exercised through `atlantis`, not curl.

## 1. Goal

Two services out of one:

1. **Core** — domain-neutral, **standalone for every service it provides**: containerized
   process-bigraph runs on Ray or Nextflow, script tasks in containers, interactive pbg
   environments (env workers and the relay), environments / build recipes / build jobs,
   the dataset registry, task provenance, job events and tracing. Reused by other projects;
   eventually hosted publicly for the vivarium-collective viva-* ecosystem.
2. **SMS service** — every existing viva-api endpoint for sms-ecoli, v2ecoli and vEcoli,
   and, through the core, selected third-party simulators wrapped in process-bigraph.

Every phase is independently shippable, keeps all existing URLs and database ids stable,
and has a rollback. There is no big-bang step.

## 2. Decisions

| # | Decision | Date | By |
|---|---|---|---|
| D1 | **Packaging:** same repo, new top-level package `viva_core/`, import-linter enforced; extract to its own repo and PyPI later. | 2026-09-18 | Jim |
| D2 | **Integration:** library first, then HTTP. SMS mounts the core routers in-process and calls core as a library; later core also runs as its own Deployment and SMS switches to an HTTP `CoreClient` behind the same Protocol. | 2026-09-18 | Jim |
| D3 | **Database:** same database, separate `core` schema with its own Alembic chain and version table. The link to core jobs lives on the SMS side. Ids preserved. | 2026-09-18 | Jim |
| D4 | **Backends:** core supports AWS Batch, K8s **and SLURM**. | 2026-09-18 | Jim |
| D5 | **Scope:** the `/datasets` registry (#661) and task provenance (#656) are core. | 2026-09-18 | Jim |
| D6 | **Environments:** core owns environments, build recipes and build jobs. The SMS `simulator` table and `/core/v1/simulator/*` stay in SMS and map onto core environments; repo guardrails stay SMS-side. | 2026-09-18 | Jim |
| D7 | **Standalone:** core must run without `viva_api` for every service it provides. | 2026-09-18 | Jim |
| D8 | **Core CLI:** an independent CLI targeting only core, built on the OpenAPI client generated from the core spec. In the plan for standalone operation; not required up front. | 2026-09-18 | Jim |
| D9 | **BioModels and the curated COPASI / Tellurium simulators move to core** (not SMS), and are eventually factored back out into the reproducible-biology hosted-services application. | 2026-09-18 | Jim |
| D10 | **Core's default path is *select or build* an acceptable environment, then run.** The reproducibility application on core accepts a composite and either selects a known compatible environment or builds one from the composite's dependencies. A barebones environment (built-ins only) is rarely useful; an uber-container does not scale. Extends D6: an environment is a **spec** (explicit `repo@commit` + recipe, or derived from a composite), an **environment** (spec hash *and* image digest, status, build job, what it provides) and a **resolver**. "Default path" is reserved for this. | 2026-09-20 | Jim |
| D11 | **Simulators are write-once provenance; a marked-temporary simulator is the only exception.** A simulator record, its container image and its image tag are the provenance of every simulation that ran on them. With one exception, **all of them are write-once, immutable and never deleted** — on every site, not only those that predate this work. The exception is a simulator that says so about itself: `temporary`, with a `label` naming who or what made it and its **own marked image tag** (`tmp-<commit>-<nonce>`, never `<commit>`). A temporary simulator may be overwritten or removed, and it is **marked back to the end user** — in the API, in the CLI, TUI and GUI lists, and left out of every "latest" or default choice — so nobody takes it for an authoritative one. A standing rule of the final design, not only of the migration. | 2026-09-20 | Jim |
| D12 | **`viva_core` carries no `Any`.** `disallow_any_explicit` and `disallow_any_unimported` are on for `viva_core.*` as a mypy per-module override, on top of `strict`. JSON-shaped values are a `JsonValue` alias or a `TypedDict`; an untyped third-party client is wrapped behind a typed Protocol or given stubs. Because the override is a **package glob**, every module that moves into `viva_core` in P3 / P4 / P5 comes under it the day it moves, so code arrives in core `Any`-free or does not arrive. The same ban covers `viva_api.simulation.dispatch` — by module now, as `ray.*` once the strategies have landed. The rest of the repository is ratcheted by count, not banned. | 2026-09-20 | Jim ("I especially want viva_core.* with strong mypy coverage within this initiative") |
| D13 | **ptools is SMS's, and private.** Pathway Tools and its PGDBs are licensed material (Jim, 2026-09-21: "ptools is a private repo due to licensing concerns and belongs to the sms-api not viva-core"). Everything of it — `Dockerfile-ptools`, `assets/ptools/`, the ptools analyses, the `sms-ptools` image and Deployment, the PTools page — stays on the **SMS** side of the split and **stays private**. Core is meant to be public, with public images, so this boundary is a legal one, not only an architectural one: nothing of ptools enters `viva_core/`, the core runtime image, or any package that is or may become public; and **before anything is made public, what it contains is listed first**. Guards: `ptools` is a forbidden term in core's constructs (`tests/core/test_core_is_standalone.py`); the public runtime image may `COPY` only from `viva_core/runtime/` (`tests/core/test_runtime_entrypoint.py`) |

## 3. The issues, in one page

Detail and evidence are in `architecture-core.md` Part 1. The ones that shape the plan:

1. **The generic Batch engine is inside the E. coli service.** `simulation_service_ray.py`
   (5,020 lines) holds job-definition cloning, MNP and container submit, the pacer, status,
   cancel and logs. Compose-on-Ray calls four of its private methods; `/tasks` *is* four
   methods on it.
2. **No image parameter anywhere.** Tasks, compose and env-worker all resolve
   `<ecr>/v2ecoli:<commit>`.
3. **`hpcrun` is one table doing four jobs** — generic job record, SMS foreign-key hub,
   chain-campaign state, trace root — and `compose_hpcrun` duplicates it with a different
   status enum and colliding, client-visible ids.
4. **One process owns everything.** One `init_standalone`, a lifespan that requires the SMS
   scheduler, router-setter wiring (an import cycle), a `JobScheduler` that mixes generic
   polling with campaign logic, an in-memory relay pinned to one pod, an unscoped boot
   sweep, and compose dispatch in a `BackgroundTask` that strands rows.
5. **One Alembic chain over two metadatas**, a fresh `upgrade head` that does not work
   (#637), `create_all` at every boot, and reconciler fingerprints that span both domains
   with no schema filter.
6. **`viva_api/common/` is not neutral** — about half of it is SMS application code.
7. **Module paths are deployment API** (the migration Job command, `importlib.resources`
   staging of `run_pbg.py` / `render_nf.py`, the uvicorn CMD).
8. **Everything assumes one origin** — clients, workbench, ALB single-owner paths, overlay
   patches positional against Deployment `api`.
9. **The name `core` is taken** — `/core/v1` is SMS simulator build and ParCa.
10. **Auth is a seam, not a boundary**, and three surfaces execute caller-supplied code.
11. **In-flight work touches the tables being split** — #661 adds a fourth `jobref_*`
    column and a `dataset` table with three SMS producer foreign keys.

## 4. Phases

Each phase: a handful of PRs, a version bump per the release protocol (including the
`<ns>-db-migration` overlay tag), dev then prod.

### P-1 — The plan-core PR (#679)

`docs/architecture-core.md` + `docs/plan-core.md`, and a pointer in `CLAUDE.md`. Docs only.

### P0 — Guardrails

Make the ground safe before moving anything.

- #661 merged as-is by its owning session (see §5).
- import-linter in `make check`, **report-only**.
- Fix `set_messaging_service` (`dependencies.py` — it assigns a local; `get_messaging_service()` always returns None).
- Reconciler `table_schema` filter.
- Fix #637 **per the decision already on the issue (Option 1: make the chain honest)** — two
  guarded revisions INSERTED before `d3f9a1c72b84`: `b9e1d5a3c7f2` creates the nine tables only
  `create_all` ever created; `c3f7a1e5b9d4` reshapes the three baseline tables the models
  outgrew. Guarded by the empty-Postgres test the decision asked for **and** a
  chain-vs-`create_all` parity test. (This plan first proposed `create_all` + stamp; withdrawn —
  only Option 1 also makes bare `alembic upgrade head` work.)
- `DB_CREATE_ALL` (default `true`; **`false` on both Stanford sites**) — the app creates nothing at
  startup there, which is what freezes the fingerprint list. Safe only since #637: the chain can
  now build every table itself. With the net gone, startup reads the database's Alembic revision,
  logs an ERROR naming the remedy when it is not at head (without crashing the pod — a rolling
  deploy can start a pod moments before the Job finishes), and `/health` reports it; smoke Tier 0
  gains a `database` check that fails a deploy on it.
- Replace positional kustomize JSON patches with named strategic-merge patches; verify
  `kustomize build` output is byte-identical before and after.
- Stop `ComposeJobMonitor` and the relay `TaskRunner` at shutdown; scope
  `fail_unfinished_tasks` with an `owner_instance` column.

Deploy: app + migration Job. Rollback: previous tag. Risk: low.

### P1 — Move the already-clean modules

Sliced by what each module imports, because core may not import `viva_api` — so anything
that reads `viva_api.config` cannot move until that read is gone.

- **P1a (first PR).** The `viva_core/` package, its packaging (wheel, image `COPY`, mypy,
  coverage), the **enforced** `core-is-standalone` contract, `tests/core/`, and the modules
  with no `viva_api` imports at all: `common/models.py` → `viva_core/models.py`;
  `common/messaging/` → `viva_core/infra/messaging/`; `common/events_env.py` →
  `viva_core/events/`; `common/hpc/{job_service,k8s_job_service,models,nextflow_weblog}.py`
  → `viva_core/backends/`.
- **P1b.** `viva_core/settings.py` — core's first, deliberately small, settings object —
  then `storage/{file_paths,file_service,file_service_s3,file_service_gcs,file_service_qumulo_s3,gcs_aio}`
  → `viva_core/storage/`, `ssh/ssh_service` → `viva_core/infra/ssh/`,
  `hpc/{slurm_service,nextflow_trace}` → `viva_core/backends/`.
  **One definition, one object.** `CoreSettings` defines the storage and path-prefix fields;
  `viva_api.config.Settings` *inherits* them (138 fields before and after, checked field by
  field against `main`). A standalone core builds `CoreSettings` from the environment; an
  embedding application registers a provider so core reads the application's **own object**
  — `viva_api/__init__.py` does that lazily, so reaching a moved module through its old path
  is enough. That matters here because `config.py` loads its dotenv files into the process
  environment at import: a second object built at another moment could disagree.
  The `config` ⇄ `file_paths` import cycle is gone (the lazy import it forced is now a normal
  one).

**The shim.** Old import paths keep working through a *self-replacing stub* left at each old
path: it imports the new module, keeps a `TYPE_CHECKING`-only star import so mypy still
resolves the old name, and assigns the new module into `sys.modules[__name__]`. The old name
is therefore the **same module object** — `mock.patch("old.path.X")` patches the one real
attribute, `isinstance` agrees across names, module state does not fork, and the `sms_api`
redirect keeps working on top of it. (The plan first said "a table-driven meta-path finder";
that cannot satisfy mypy, which needs a file at the old path.) A test reaching for a name the
star import does not carry — a private name, or one left out of `__all__` — imports it from
the new path. In-repo importers are **not** mass-rewritten: ~120 files import these modules
and other sessions have branches open against them; they migrate as they are touched.

**Re-homed out of P1**, each for a reason found when the imports were read:
`dispatch_validation.py` stays in SMS — it is domain code (`V2ECOLI_SKIP_CACHE_VERIFY`, the
ParCa-cache rules), not infrastructure; `api/{auth,oidc}.py` read `config` and move with the
settings split (P3); `simulation/chrome_trace.py` imports `simulation.models` and moves with
the events store (P4 / P7); `hpc/local_task_service.py` binds `hpcrun` rows and moves in P2.3.

Deploy: app only — **and the image must contain `viva_core`** (`Dockerfile-api` copies
source trees one by one; `tests/test_deploy_config.py` now asserts every shipped package is
copied). Verify: full `pytest`, `make check`, and on dev the marker grep is
`/app/viva_core/models.py` existing on the newest pod. Risk: low.

### P2 — Break up `simulation_service_ray.py`, and extract the backends

`simulation_service_ray.py` was **5,019 lines**; its one class, `SimulationServiceRay`, was
4,305 of them with ~75 methods and eleven concerns. (Every count in this document is against
that 5,019.) It was extended, mechanism by mechanism, into the home of the simulation dispatch
paths; it should have been broken up on the SMS side long before a core existed (Jim,
2026-09-19). Extracting the generic Batch engine alone would leave a ~4,000-line class with
every mechanism still in it — so P2 is a **decomposition**, of which the core extraction is
the first cut.

**Choose the shape by what the thing *is*, not by how its code is arranged** (the 2026-09-20
audit; Jim's test for build: *a common requirement every dispatch mechanism uses, that works
one way regardless of mechanism*):

| Domain role | What belongs | Shape |
|---|---|---|
| common capability, works one way | image build; the Batch seam; tasks; the three ParCa **cache jobs** | composed service, handed its collaborators |
| pure domain specification | config interpretation; the analysis spec (modules, memory sizing); ParCa **commands and cache URIs**; image paths; run tags | module of pure functions |
| dispatch mechanism | **ensemble**, chain, multi-node composite, mbp-tracked, Nextflow — *five*, not four | strategy object |
| shared compute pattern | `common/analysis_dag.py`, already shared by the Ray service and `compose` | stays where it is |

A **mixin** is none of these: it is a file split, and at runtime still one object. It was the
right tool for nothing that remains (see the audit entry in the decision log).

Where each concern went, or goes:

| Concern | ~lines | Destination | State |
|---|---|---|---|
| Batch engine — job definitions, MNP + container submit, pacer, status, cancel, logs | 550 | **core** `backends/batch.py` (`BatchJobClient`, takes no settings) | done, cut 2 |
| SMS's half of the Batch seam (settings, queue choice, this application's env entries) | 400 | SMS `dispatch/batch_layer.py`: today a base class, **to become a composed `service.batch`** | cut 3; recomposed in PR 5 |
| `/tasks` — submit, upload, dispatch, status, logs | 190 | SMS `dispatch/tasks.py` (`TaskService`) **now**; core `tasks/` in P4b, when the `task` table has a `JobStore` and the image an environment | service, #714 |
| Image build | 200 | SMS `dispatch/build.py` (`ImageBuilder`) **now**; a core `repo-recipe` build recipe in P5. Not core yet because `batch_build.py` reads this application's settings, names its jobs, and is shared with `SimulationServiceK8s` | service, #714 |
| Config interpretation | 211 | SMS `dispatch/config_interpretation.py` (pure) | done, cut 1 |
| ParCa and caches | 520 | SMS: commands + cache URIs → a pure module; the three cache jobs → `ParcaService`. ParCa is **not** submitted one way (container in mbp and chain, MNP in ensemble and composite), so each mechanism submits its own | **done** (PR 4): `dispatch/parca_spec.py` + `ParcaService`; `RayParcaMixin` is gone |
| Analysis | 460 | SMS: `dispatch/analysis_spec.py` (pure: modules, memory sizing, shared with Nextflow and the K8s path); each mechanism's analysis **submitter travels with that mechanism**; the pattern stays `common/analysis_dag.py` | PR 3 |
| Dispatch — ensemble (the router's inline ParCa + simulation MNP pair; 216 of its 308 lines) | 336 | SMS `ray/strategies/ensemble.py` | PR 10 |
| Dispatch — chain (per seed x generation, lineage, campaign result, cancel) | 848 | SMS `ray/strategies/chain.py` | PR 11 |
| Dispatch — Nextflow head (render, params, session, head job, reap) | 464 | SMS `ray/strategies/nextflow.py` | PR 8 |
| Dispatch — multi-node composite on Ray | 346 | SMS `ray/strategies/composite.py` | PR 9 |
| Dispatch — "mbp tracked" | 225 | SMS `ray/strategies/mbp_tracked.py` | PR 7 |
| **The router** `submit_ecoli_simulation_job` | ~20 of code | stays in the facade: precedence among the five, nothing else | PR 10 |
| The facade — status, cancel routing, config template, repo discovery | 142 | stays: it *is* `SimulationServiceRay` | — |

**The hazard that dictates the staging.** Tests patch this module's *names*: **298** string
patches. The **205** of `get_settings` are *positional* — a name patch reaches only the module
it names, so code moved to another file silently runs with the developer's **real settings**
(region, queues, bucket) while its test still passes. The **93** of `boto3.client` are not:
`patch("<module>.boto3.client")` resolves to the shared `boto3` module and replaces `client`
globally, so they survive a move. (An earlier version of this section said both were
positional; measuring P2.0b showed otherwise.) What *does* let a unit test reach AWS is patch
**lifetime** — a background task outliving its `with patch(...)` block — which is what the
P2.0a guard caught. So:

- **P2.0 — make it safe to move (no code moves).**
  (a) An autouse test guard: creating a real boto3 client or session in a unit test fails
  loudly; the genuine integration tests opt out with a marker.
  (b) One seam: `viva_api/simulation/dispatch/_seams.py` owns `get_settings` and `boto3`; every
  Ray-service module reads them through it at call time; the 298 patch strings are retargeted
  there mechanically, in one PR, while the code is still in one place. **Done:** bypass the
  seam and 82 of 267 tests fail; restore it and all pass. `tests/simulation/test_ray_seams.py`
  is the ratchet — no Ray-service module may import either name itself (including
  `from …_seams import get_settings`, which looks like using the seam and defeats it).
  (c) Smoke **Tier 2** and **Tier R** (§8): this is the first phase that can break dispatch.
- **P2.1 — carve, one concern per PR.** Cuts 1–5 are done (table below). The first plan for
  the rest was "every SMS piece becomes a **mixin** now, and the mixins become strategy
  objects in P2.2". The 2026-09-20 audit measured the class and found that plan had no basis
  for what remains, so **P2.2 is absorbed here** and the remaining mechanisms go **straight to
  strategy objects**:
  - no dispatch mechanism calls into another; only the router fans out
  - the helpers thought to be shared are not (`_sim_command`: ensemble only;
    `_seed_generation_command`, `_seed_lineage_command`: chain only). What is shared is
    `stage_runner`, `_record_run_with_companions`, `chain_base_tags`, and the Batch and ParCa
    layers
  - no test patches a method by string. Tests reach mechanisms by direct call on a real
    instance and by `patch.object` on Batch-layer seams. (The 298 name patches were
    `get_settings` and `boto3`; P2.0 dealt with those.)

  **What a strategy owns in this phase: command builders, submit, and its narrow AWS-side
  helpers** (`get_chain_campaign_result`, `cancel_chain_campaign`, `reap_cancelled_campaign`).
  Not progress and not cancel routing: progress lives in `job_scheduler.py`
  (`_advance_chain_campaign`, `_advance_parca_gate`, `_advance_seed_generations`,
  `_finalize_campaign`, `_advance_multi_node_job`, `_advance_nextflow_head`, ~600 lines keyed
  on `HpcRun` columns), and cancel is routed by `job_id.backend` in the facade and by
  `chain_final_job_ids` in the handler. Those halves join the strategies in **P6**. Deferred,
  not dropped.

  **Rules that stand from cuts 1–5.** No re-exports for module-level functions: a moved
  function's importers point at its new home in the same PR (mypy strict refuses the implicit
  re-export). Constants a carved module needs live in leaf modules with no imports
  (`image_paths.py`). Ratchet: `test_no_method_is_defined_twice_across_the_carved_classes`.
  A cut that only moves is proven byte-identical; a cut that rewires is proven by a
  differential run (section 9).

  | cut | concern | PR | lines out of the service file | state |
  |---|---|---|---|---|
  | 1 | config interpretation → `simulation/dispatch/config_interpretation.py` | #705 | 211 (5,019 → 4,815) | merged 2026-09-19 (`0d71e2a6`) |
  | 2 | Batch engine → `viva_core/backends/batch.py` (`BatchJobClient`) — a **delegation**, not a move; see the decision log | #706 | 234 (4,815 → 4,581) | merged 2026-09-19 (`d5f965a4`) |
  | 3 | tasks → `dispatch/tasks.py` (`RayTasksMixin`), on two prerequisites every later mixin shares: `dispatch/image_paths.py` (in-image path constants, a leaf) and `dispatch/batch_layer.py` (`BatchLayer`, the service's delegations to the engine) | #707 | 597 (4,581 → 3,984) | merged 2026-09-19 (`4173ca26`) |
  | 4 | build → `dispatch/build.py` (`RayBuildMixin`); the constructor and `_submit_image_uri` join `BatchLayer` | #712 | 144 (3,984 → 3,840) | merged 2026-09-20 (`8229315a`) |
  | 5 | ParCa and the caches → `dispatch/parca.py` (`RayParcaMixin`): cache URIs, the ParCa / new-gene / variant / upstream commands, their three submit methods, per-seed founder-cache staging | #713 | 512 (3,840 → 3,328) | merged 2026-09-20 (`1f90dd04`) |
  | — | **build and tasks become composed services** (`ImageBuilder`, `TaskService` behind a `TaskDispatch` Protocol), not mixins — see the decision log, 2026-09-20 | #714 | service file 3,328 → 3,361 (two delegations added) | merged 2026-09-20 (`a10ac6cf`) |

  The rest, in the order the audit settled (one PR each; nothing merges without Jim's say-so):

  | PR | what | why here |
  |---|---|---|
  | 1 | **docs truth** (#719) | both living documents made true before more work |
  | 2 | smoke: `sim-mbp` and an opt-in `build` check (#720); then deploy the merged-but-undeployed build cuts (**C2**) | no check builds an image or exercises mbp, and both are about to be rewired |
  | 3 ✅ | pure `dispatch/analysis_spec.py` + the static-guard glob fix; the two analysis submitters stay in the class until their mechanisms move. **#715 is closed, not reshaped:** it was never merged, so on `main` the submitters had never left the class and `job_scheduler.py` still calls them there — there is nothing for a `service.analysis` shim to delegate to, and none was added | analysis is a spec + a pattern + per-mechanism glue, not one service |
  | 4 ✅ | ParCa: commands and cache URIs → `dispatch/parca_spec.py` (pure); `ParcaService` (`service.parca`) holds only the three cache jobs; `_stage_seed_override_caches` back in the class (the multi-node composite's) | half of it is pure; only the cache jobs work one way |
  | 5 ✅ | **`BatchLayer` stops being a base class** and becomes a composed `service.batch`, behind two small SMS Protocols, `ContainerSubmitter` and `MnpSubmitter`, replacing `TaskDispatch` / `AnalysisDispatch`. `local` and `k8s` are constructor arguments of the strategies that need them, never Protocol members | done **first**, so every strategy is handed a real object; done last, each strategy would be rewired twice (~80 call sites, ~24 `patch.object`) |
  | 6 ✅ | `compose` is **handed** a Batch layer (its own `ComposeBatch` Protocol; `dependencies.py` provides `BatchLayer()`), not a whole `SimulationServiceRay()` — and not an import of the layer either, which would only have renamed the edge | one of the three broken `compose-is-domain-free` edges goes (9 → 8 broken edges in all) |
  | 6a ✅ | **typed boto3**: add `types-boto3[batch,s3,logs,ecr]` as a dev dependency and type the client factory (`BatchJobClient`'s `client_factory`, `BatchLayer.client()`, the `_seams` boto3 seam) (Jim, 2026-09-20: "inject into the plan soon") | the Batch engine — the one module everything submits through, and the first thing in core — is the **least** precisely typed code being restructured (72.7 % of its expressions; the repo is 92.7 %), because an untyped `boto3` makes the client `Any` and everything it returns `Any`. Before the strategies, so mypy is a real net for the five PRs that move the code that calls it |
  | 6b ✅ | **no `Any` in core (D12)**: per-module overrides turning on `disallow_any_explicit` + `disallow_any_unimported` for **`viva_core.*`** (55 explicit, 9 unimported — the kubernetes client in `backends/k8s_job_service.py`) and for the dispatch package's **existing modules, listed by name** (18 sites). JSON `Any` → a `JsonValue` alias or `TypedDict`s | right after 6a, which removes the biggest single cause. `viva_core.*` as a glob, so it is a standing rule for everything that later moves in. The dispatch package **by name, not `ray.*`**: PRs 7–11 move ~2,200 lines (and 28 `Any`) from the service file into that package, and a ban on `ray.*` would make every strategy PR change annotations in the code it moves — which breaks the AST-identity proof that makes those PRs reviewable. Widened to `ray.*` in PR 12 |
  | 7 ✅ | strategy: **mbp-tracked** → `dispatch/mbp_tracked.py` (`MbpTrackedStrategy(batch)`, `mbp_tracked_command`); the run-record helper → `dispatch/run_records.py` | smallest (225 lines); first use of the shape |
  | 8 ✅ | strategy: **Nextflow** → `dispatch/nextflow.py` (`NextflowStrategy(batch, k8s, stage_runner=)`; needs `k8s`; `reap_cancelled_campaign` travels with it); then **C3** | 464 lines (it was 548 with its module-level constants) |
  | 9 ✅ | strategy: **multi-node composite** → `dispatch/multi_node.py` (`MultiNodeCompositeStrategy(batch, stage_runner=)`, + its analysis submitter, its founder-cache staging, its vCPU lookup); `PBG_RUNNER_ENV` → the leaf `dispatch/runner_env.py` | 346 lines (it was 615 with its analysis, staging and lookup) |
  | 10 ✅ | strategy: **ensemble** → `dispatch/ensemble.py` (`EnsembleStrategy(batch, stage_runner=)`, `sim_command`), extracted from the router's tail | the router is now precedence and a hand-over: 12 statements (93 lines with the docstring and the comments that say why the order is what it is) |
  | 11 ✅ | strategy: **chain** → `dispatch/chain.py` (`ChainStrategy(batch, local)`, + its analysis submitter, the two seed commands, the analysis command, `chain_base_tags`); then **checkpoint C**. **Not done, and the row was wrong to promise it:** "delete the facade shims" — nine one-call delegates remain because the scheduler, a handler, the capability probe and the integration tests ask the *service*, and the audit itself put the scheduler off until P6. They are named and pinned by a test (they may only shrink). `scripts/prove_ray_carve_is_move_only.py` goes with the checkpoint-C ledger PR, not here: deleting it in the PR it proves would make the claim unrunnable | largest (1,002 lines with its analysis) and it bills real money, so last |
  | 12 ✅ | **the dispatch package is `Any`-free (D12, second half)**: the override is `viva_api.simulation.dispatch.*` — a glob, like core's — and `NOT_YET_BANNED` is gone. The 26 sites the strategies brought: the three dispatch blocks are `TypedDict`s (`MbpDispatch`, `MultiNodeDispatch`, `NextflowDispatch`), JSON that is only read is `Mapping[str, object]`, chain's optional client is a `BatchClient`, the Nextflow head is a `V1Job`. **Annotation-only, and shown to be:** with every annotation erased, three units differ from `main`, all inert (two `cast`s, a tuple that became a constant, `str()` of a `str`) | a type change, kept out of the five move PRs on purpose; after checkpoint C, so it is judged against a deployed, smoke-tested carve |
- **P2.2 — absorbed into P2.1** (2026-09-20). There are no mixins to turn into strategies.
- **P2.3 — the environment model and its *select* half** (no database, no build). D10 says
  core's default path is *select or build an environment, then run*; this is the select half.
  `viva_core/environments/`: `EnvironmentSpec` (explicit `repo@commit` + recipe | derived
  dependency set), `Environment`, and an `EnvironmentResolver` Protocol with one
  implementation that resolves an **explicit** spec to an image. It replaces four independent
  derivations of the same image from the same two settings (`dispatch/batch_layer.py:72`,
  `compose/simulation_service_ray.py:91`, `compose/env_worker_service.py:114`,
  `simulation_service_k8s.py:486`). With PR 5 and PR 6 of the carve, that is the smallest slice
  that runs a third party's pbg-wrapped simulator through core.

  **Sequence** (one concern per PR, as in P2.1; nothing merges without Jim's say-so):

  | # | PR | why here |
  |---|---|---|
  | 2.3a ✅ | **the model and the one resolver**: `viva_core/environments/` — `ExplicitSpec` / `DerivedSpec`, `spec_hash`, `Environment`, the `EnvironmentResolver` Protocol, `RegistryEnvironmentResolver`. Pure: no settings, no network, **no caller changed** | the vocabulary first, reviewable on its own; a test pins that the resolver says what each of the four derivations says |
  | 2.3b ✅ | **the four derivations ask the one resolver**: `viva_api/common/site_environments.py` builds it from the settings it is *handed* (each caller already holds them, through its own seam, and a test that patches that seam must be what it sees). The Batch layer's `image_uri` / `submit_image_uri`, compose's pinned tag, the env worker's `image_for_commit` and the K8s analysis Job ask it. The behaviour decision was **taken out**, not taken: an unset account still yields the malformed host it always did (deferred list) | a rewiring: differential against `main` over 432 cases, mutation-checked twice |
  | 2.3c ✅ | **the core runtime image**: `Dockerfile-core-runtime` (370 MB: Python slim, the engine at the commits the science image runs, `viva-emitters`, the AWS CLI) + **core's own container entrypoint** (`viva_core/runtime/batch-container-entrypoint.sh`, installed at the `/opt/` path every container job definition calls) + its first contract test + `build-core-runtime.yml` (dispatch-only, write-once tags) + `CORE_RUNTIME_IMAGE`, registered as the resolver's `runtime_image`. **Container-shape jobs only**: no multi-node Ray entrypoint yet. **Not pushed, not deployed** | infrastructure, and the image must exist before anything can select it |
  | 2.3d-1 ✅ | **a task may name an environment**: `TaskRunRequest.environment` (a *registered* environment by name; `"runtime"` today), mutually exclusive with `commit`; an uploaded task then runs in the runtime image and resolves no simulator; a repo-path script is refused (400), a site with no runtime image refuses (501). `atlantis task run --upload … --environment runtime`; `atlantis smoke run --task-environment runtime` for `task` / `task-fail`. **The image is pushed** (`ghcr.io/vivarium-collective/viva-core-runtime:0.1.0`, `sha256:356405729d9b…`), **private**, and dev does not name it yet | tasks first: the runtime image serves container-shape jobs only |
  | 2.3d-2 ✅ | **Batch can pull the image**: Jim made the ghcr package public (2026-09-21); **one trial job on dev's container queue pulled it from inside the VPC and ran under core's entrypoint** — nonce in its log, proof file staged out to S3 (job `58aaf734`). `CORE_RUNTIME_IMAGE` is set in dev's `api.env`. **Not deployed**: it takes effect with the next API roll (checkpoint D) | an org-package visibility change: Jim's, not a line in a code PR |
  | 2.3d-3 ✅ | **a compose run may name an environment**: `environment="runtime"` runs the composite as **one container** in the runtime image — no commit, no ParCa cache, no Ray cluster, no chained analysis; the same runner, command and results prefix, so status and results read as for any compose run. Checked in the router *before* dispatch (the submission is a background task): unknown name or mixed with `simulator_id` / `num_nodes` / `analysis_options` → 422; no runtime image → 501. `atlantis compose run --environment runtime`; smoke's option is now **`--environment`**, for `task`, `task-fail` **and `compose`**. **Env workers: not done, on purpose** (see the log) | the row said "compose's container path"; that path turned out to be the chained *science analysis*, so this is a new, smaller shape instead |
  | **D** ✅ | **Checkpoint D** (0.9.152, 2026-09-21, tag `v0.9.152`): bump, deploy (the API roll is what makes `CORE_RUNTIME_IMAGE` take effect), Tier 0 + 1 with `--environment runtime --require-aws`, and a like-for-like queue-to-running time for `task` and `compose` on both images | the deploy that judges all of P2.3 |

  Then **the core runtime image**: a small reference environment that is not any
  application's science image — Python slim + process-bigraph + pbg-emitters + the Batch
  container entrypoint (stage-in / stage-out contract) + the env-worker module. A few hundred
  MB, built by CI from this repo (`Dockerfile-core-runtime`), pushed to its own ECR/ghcr
  repository, versioned with `viva_core`. It is the environment the resolver selects for a
  composite that needs nothing beyond the built-ins — the "barebones" end of D10's line, and
  rarely the useful one. *Why it still matters:* tasks, compose and env workers take no image
  parameter today, so even a plumbing smoke test pulls a **5.74 GB** (compressed) science
  image. Measured on dev: a Tier 1 task spends **250–320 s** in queue + scale-up + pull and
  **2 s** running. With it, Tier 1 smoke and `tests/core/` get a real non-application
  environment, and the public core a default that carries no domain code.

  **Deliberately not here** (each was listed in two or three phases before the audit):
  promoting a dispatcher Protocol into core, the K8s / SLURM / LOCAL adapters behind
  `JobBackend`, `JobStore` (P4b only), a `CoreContainer` (P3), build → core (P5). On D4: the
  K8s and SLURM adapters are **deferred with a trigger, not dropped** — they land no later
  than P5, when `compose` (which already runs on SLURM and Batch) moves onto the core seam and
  becomes the second consumer; until a second backend implements it, a core `JobBackend` would
  quietly be Batch-shaped, so it is not declared final before then. Before any promotion, the
  four E. coli keywords on `_submit_container` (`expect_new_genes`, `expect_bundle_overrides`,
  `require_clean_chain`, `lineage_debug_division`) fold into a generic env contribution.

SLURM: contract tests on recorded sbatch / squeue fixtures, labelled *unverified live*;
location changes, behaviour does not. Deploy: app only. Verify: `pytest tests/simulation
tests/compose`; `atlantis task`, `compose`, `simulation` end to end on dev. **Risk: high** —
a 5k-line file edited by concurrent sessions; announce a freeze while each PR is open.

### P3 — Settings, DI, app factory

`CoreSettings` / `SmsSettings` (same env names; `get_settings()` stays as a facade);
`CoreContainer` / `SmsContainer`; `create_core_app()`, mounted by `viva_api/api/main.py` at
`/compose/v1`, `/env-worker/v1` and the new `/viva/v1`; router setters deleted (the cycle
goes); env-worker models lifted out of the router; the lifespan no longer requires the SMS
scheduler. Two OpenAPI specs, with the SMS spec the union until P8 so the drift tests stay
green. **The core spec existing is what unblocks the generated core client (D8).**

**Order inside P3 (2026-09-20):** `create_core_app()` and a **test that boots it** come first.
Only then the explicitly phased move of the `compose` package (5,526 lines; its SMS ties sit
in one file) and of env-worker (its models and service logic lifted out of the 1,170-line
router). Moving 5.5k lines into a package nothing boots is unverifiable. `dependencies.py`
(691 lines of module globals and setters pushed into routers) is what the containers replace.

**Sequence** (one concern per PR, as in P2.1 and P2.3; rows after 3a are a proposal, each refined when
its code is read — that is how 2.3c and 2.3d-3 turned out different from their rows):

| # | PR | why here |
|---|---|---|
| 3a ✅ | **`create_core_app()` and a test that boots it**: `viva_core/container.py` (`CoreContainer`), `viva_core/api/` (one router under `/viva/v1`; `create_core_app()`), the boot test (core alone, the application unimportable), and the SMS app **including** that router with its own container (`viva_api/core_wiring.py`). First route: `POST /viva/v1/environments/resolve` — the one service core has | the plan's own order: nothing moves into a package nothing boots |
| 3b ✅ | **env-worker: the 22 models out of the router**, in place → `viva_api/compose/env_worker_schemas.py` (1,170 → 978 lines). **The "service logic" half of this row did not exist**: with the models gone the file is 30 routes and 16 helpers, none over 36 lines of code, and the helpers are HTTP mapping — turning the worker's four ways of saying no into status codes — which is a router's job. The service logic was already in `compose/env_worker_service.py`, `env_worker_relay.py` and the task runner. What still ties the router to SMS is its **two setters, `get_settings` and `viva_api.api.auth`** — 3d's and 3e's | a router that holds models cannot be moved: everything that wants a model has to import the HTTP layer |
| 3c ✅ | **compose's two forbidden imports are gone, and `compose-is-domain-free` is ENFORCED**: the science analysis SMS chains onto a compose run moved out of `compose/simulation_service_ray.py` into `viva_api/simulation/compose_analysis.py` (`ComposeAnalysisChainer`), handed to compose as a **hook** (`AfterSubmit`) by the composition root. **The contract is narrower than the row's title**: compose still imports the application's *wiring* — `viva_api.config` (6 modules), `viva_api.dependencies` (4 lookups), `common/site_environments`, `common/storage/data_layout` — which is 3d's and 3e's work, listed in the log | the precondition for moving it, checkable on its own |
| 3d-1 ✅ | **compose's ParCa staging is a hook** (`StageInputs`): the last *domain* knowledge inside compose (`_parca_staging`, and the `compose_parca_cache_dir` setting it reads) moves to `viva_api/simulation/compose_staging.py`; the composition root hands it in. A compose service with no hook stages nothing | a setting named after ParCa cannot become a core setting (the vocabulary guard), so this goes before the settings move |
| 3d-2 ✅ | **the 14 settings compose and env-worker read are `CoreSettings` fields** (same names, same environment variables — SMS's `Settings` inherits them), and the four compose modules on the Batch / K8s path read them through core's accessor. Two of the 14 carry an **application default** (`env_worker_workspace_path`, `ray_ecr_repository`): core declares them empty and SMS redefines only the default, under a named allow-list in the settings guard. Proof: `Settings.model_fields` identical before and after, all 146 (name, annotation, default). **Not moved:** `slurm_log_base_path` (an import cycle — `viva_core.storage.file_paths` imports `viva_core.settings`), so `compose/hpc_utils.py` still reads the application's settings; and the SLURM compose service, which takes SMS's `Settings` whole (see the log) | settings first: nothing can move while it imports `viva_api.config` |
| 3d-3 ✅ | **compose's four `viva_api.dependencies` lookups arrive as constructor arguments**: the file service (`files=`), SMS's simulator registry (a third hook, `EnvironmentKeyOf`, filled by `viva_api/simulation/compose_simulators.py`), and the SLURM SSH sessions, twice (`slurm_ssh=`, a provider asked at the moment of use — a site without SLURM never has them). A parsed guard: nothing under `viva_api/compose/` imports `viva_api.dependencies`, lazily or not | services second |
| 3d-4 | **compose and env-worker move into `viva_core`** — in the order the three gates allow (measured per module, 2026-09-21: domain terms in constructs · `Any` · imports of the application). **The SLURM compose service stays in SMS** (Jim, 2026-09-21; see the log) | the move, once each module is movable |
| 3d-4a ✅ | **the two modules that pass every gate today**: `compose/env_worker_service.py` → `viva_core/env_worker/service.py`; `common/site_environments.py` → `viva_core/environments/site.py`. Byte-identical apart from four import lines; the old names are self-replacing shims (the P1a pattern) | leaves first; `site` is what the env-worker service and the Batch compose service both need |
| 3d-4b-1 ✅ | **the `Any`-free pass** over the compose modules with no domain terms — `database_service`, `job_monitor`, `env_worker_relay`, plus `tables_orm` and `env_worker_schemas` (none of their own) — and **the D12 ban is ON for all five, by name**, until the move puts them under core's glob. `render_nf` waits: it loads `run_pbg`, and goes with it | D12 is a glob: a module arrives `Any`-free or not at all |
| 3d-4b-2 ✅ | **`models.py` and `container_def.py` move first** — the order 3d-4b-1 wrote was wrong: four of the five `Any`-free modules import `compose/models.py`, and two of them `container_def.py`, so the two leaves go first. `viva_core/compose/{models,container_def}.py`; `ComputeBackend` → `viva_core/models.py` (re-exported by `viva_api.config`); the default allow-list → `viva_api/simulation/compose_allow_list.py` (it names one application's stack); the recipe is **handed** the runner's text instead of reading `run_pbg.py` at import | the leaves; what the five depend on |
| 3d-4b-3 ✅ | **the five move, and the abstract compose service with them** — all under `viva_core/compose/` (`database_service`, `tables_orm`, `job_monitor`, and `service.py` for the abstract `ComposeSimulationService`) and `viva_core/env_worker/` (`relay`, `schemas`); not `db/` and `services/` as the row first said — those packages are P4's and 3e's, and one file each would name a structure that does not exist yet. The by-name D12 entries are gone: core's glob covers them. `viva_api/compose/simulation_service.py` keeps the SLURM class and re-exports the base | the move itself |
| 3d-4c-1 ✅ | **the Batch compose service moves** → `viva_core/compose/simulation_service_ray.py` (the class keeps its name; the `ray` misnomer is its own rename PR). Its two ties: the S3 layout — two pure primitives in `viva_core/storage/layout.py` that SMS's `data_layout` is now built ON, and `s3_work_bucket` / `s3_output_prefix` on `CoreSettings` (the prefix's default is the application's) — and the runner it read from the application's package at import, now HANDED IN (`runner_source=`), like the recipe's | the last `viva_api` import in the file was the layout |
| 3d-4c-2 ✅ | **the runner and `render_nf` are core's; what SMS's model needs of the runner is a staged sibling** — `viva_core/compose/run_pbg.py` (generic; `Any`-free by four narrow Protocols; a `RunnerHooks` seam) and `render_nf.py`; the two v2ecoli pieces move verbatim to `viva_api/compose/runner_hooks.py`, uploaded beside the runner and copied beside it into every job (`stage_runner_commands`, one place), named by `PBG_RUNNER_HOOKS`. Jim chose this over shipping hooks in the science image (needs a v2ecoli PR + a rebuild, and every existing simulator would lack them). Both `runner_source=` seams are gone: core stages its own runner; compose is handed `runner_hooks=`. **Changes every mechanism's command → checkpoint E must run Tier 2 in full** | the largest file, on both counts |
| 3d-4d-1 ✅ | **the compose handlers are core's** (`viva_core/compose/handlers.py`: the generic run, the curated run, the allow-list check, the dispatch) — minus the two that were SMS's: `hooks_source` → `viva_api/simulation/compose_hooks.py`, `run_compose_v2ecoli` → `viva_api/simulation/compose_curated.py`. The two ids a run is known by leave the SLURM path module for `viva_core/compose/ids.py` | the handlers before the routes that call them |
| 3d-4d-2a ✅ | **the compose router is core's** — `viva_core/api/routers/compose.py` (16 routes, 15 of them), served by `create_core_app()` at `/viva/v1/compose` and by SMS at `/compose/v1`, unchanged: same 102 operations, same ids, same schemas. `/curated/ecoli` is `viva_api/api/routers/compose_sms.py` at the same prefix. The route knew one backend's transport (SSH for SLURM's zip): now `ComposeSimulationService.results_archive()`, the SLURM service's own. The S3 tar streaming is `viva_core/storage/s3_streaming.py`, file service and prefix handed in. BioModels → `viva_core/contrib/sysbio/` (D9), `Any`-free. Setters stay (3e replaces them); report-only broken edges 6 → **5** | the compose half of the routes |
| 3d-4d-2b ✅ | **the env-worker router is core's** — `viva_core/api/routers/env_worker.py` (30 routes, verbatim but for one summary string), served by `create_core_app()` at `/viva/v1/env-worker` and by SMS at `/env-worker/v1`, unchanged (102 operations, same ids, same schemas). With it, **identity is core's**: `viva_core/api/auth.py` + `oidc.py` (`Any`-free) and their eight settings (`oidc_*`, `identity_header`, `owner_instance`) on `CoreSettings`. The `dependencies.py → routers` contract is now **kept**; report-only broken edges 5 → **4** | the env-worker half; 3d-4 is complete |
| 3e ✅ | **containers replace the setters**: `CoreContainer` gains `compose: ComposeServices` (db, default backend, monitor, file service, the application's allow-list default) and `env_worker: EnvWorkerServices` (service, task db, runner); core's routes ask `current_container()`, the provider registered the way the settings provider is — `create_core_app()` registers the container it holds, SMS registers `core_wiring.core_container` at import and builds the two groups in its lifespan. Gone: `set_compose_services`, `set_env_worker_service`, `set_env_worker_task_service`, and the relay's process-wide `runner`. A test pins that no router module carries a setter and that a core with no services answers by name. The lifespan clause of this row ("no longer requires the SMS scheduler") is 3f's | the wiring follows the code |
| 3f | **settings split finished; two OpenAPI documents** (the SMS one still the union). **Checkpoint E** | what unblocks the generated core client (D8) |

Deploy: app only. Risk: medium.

### P4 — Provenance, in core shape

- **P4a.** One additive "owner-ref expand" revision on the SMS chain: `hpcrun.owner_kind`,
  `owner_id`, `output_uri`; `dataset.producer_job_id`, `owner_kind`, `owner_id`, `trace_id`,
  backfilled from the three FKs; dual-write. `dataset_registry`, `dataset_walk` and the
  ingest hook move to `viva_core/{datasets,events}` behind `OwnerResolver`,
  `ArtifactClassifier`, `WalkSource`. `/viva/v1/datasets`; `/api/v1/datasets` becomes a facade.
- **P4b.** #656 implemented **directly in core shape** in `viva_core/tasks`: a task is a job
  with `owner_kind = 'task'` (no `jobref_task_id`, no `dataset.task_id`); snapshot mode;
  `task_script`; the CD2 importer. It runs over the `JobStore` Protocol with an interim
  `HpcRunJobStore`, so P7 swaps only the store.

Deploy: app + migration Job. Rollback: columns are additive; the previous image works.

### P5 — Environments, builds, compose decoupling, durable dispatch

**The *build* half of D10 — where select-or-build becomes whole.** Select-or-build already
exists three times in this codebase and its siblings, each one partial
(`architecture-core.md` §2.3a has the table). P5 unifies them rather than adding a fourth:

- a `core.environment`-shaped table (in `public` until P7) carrying **both** identities — the
  **spec hash** (what was asked for) and the **image digest** (what was built; a recipe hash
  is not an image identity when the recipe runs `apt upgrade` and resolves pins at build
  time) — plus status, the build job and what it **provides**. A real unique key, which
  neither existing table has (`simulator` has none; `compose-api` races on insert).
- a `BuildRecipe` registry; build jobs as core jobs. Two generic recipes, plus the
  application's own: **`repo-recipe`** (clone a repo at a commit and run its own build
  script — today's Ray build *and* `SimulationServiceK8s`'s, which removes that duplication;
  the SMS simulator build path delegates to it, rows and `/core/v1/simulator/*` unchanged) and
  **`python-deps`** (synthesize an image from a dependency set — today's
  `compose/container_def.py`, emitting Apptainer for SLURM and an OCI image for Batch, which
  cannot build per-composite at all today).
- **deriving the spec from the composite:** port and *finish*
  `pbest.dependency_resolution.determine_dependencies` — it parses the
  `python:pypi<pkg[ver]>@module.path` address protocol and checks an allow-list, then returns
  empty lists and is called nowhere. The allow-list becomes one enforced core setting rather
  than three carried-and-dropped copies.
- **built environments are write-once (D11), with the marked-temporary exception already in
  place** (`simulator.temporary / label / image_tag`, `SimulatorVersion.environment_key`).
  P5 generalises it: a successful build is never replaced; a rebuild is a new row with a new
  image digest and a new tag; `force` means "build another", not "overwrite this one"; no
  lifecycle rule may expire an environment a run points at. The registries can become
  tag-IMMUTABLE now that even temporaries never reuse a tag. Still missing, and P5's: the
  image **digest** recorded with the build, and a purge for temporaries.
- from the SMS path, what the compose path lacks: **a FAILED build is retried**, not treated
  as "already built". And `run_pbg.py` stays **out of the hashed definition**, so editing the
  runner does not invalidate every environment.
- "satisfies" starts as an exact spec-hash match and may grow to "a registered environment
  whose `provides` covers the requirement" — how curated environments (COPASI, Tellurium) and
  SMS simulators get selected without being rebuilt (open question 9).

Compose's `analysis` writes, ParCa staging and `analysis_options` move behind an SMS
post-completion hook. `BackgroundTask` dispatch → lease-based dispatch with orphan reconcile
(the #414 pattern). `/curated/ecoli` → SMS. Prerequisite: a settings-as-arguments cut of
`batch_build.py`. This is also where the K8s and SLURM adapters land (see P2.3).

BioModels and curated COPASI / Tellurium (D9) → `viva_core/contrib/sysbio`, URLs unchanged.
The modules (`compose/biomodels_service.py`, `biomodel_documents.py`) already import nothing
from the rest of `viva_api`, so they can move as early as P1; the routes follow here, once
compose itself is core. While moving: lift the document building and the synchronous
BioModels fetches out of the router into the service, and add the import-linter contract —
`contrib.sysbio` may use only core's public service API and `CoreClient`; core proper never
imports `contrib`. That contract is what makes the later factoring-out (to the
reproducible-biology hosted-services application, after P10) a lift rather than a refactor.

**Standalone gate:** from here `tests/core/` covers every core service with no hooks
registered. Risk: medium.

### P6 — Scheduler split

`viva_core/services/job_monitor.py` takes the generic passes and folds in
`ComposeJobMonitor`; an SMS `CampaignSubscriber` consumes the `job_transition` outbox for
chain campaigns and analysis fan-in. Still one process. Verify: a multi-generation chain
campaign on dev; gating latency against the baseline.

**Before the split (2026-09-20):** `JobScheduler` takes the *concrete* `SimulationServiceRay`
(`job_scheduler.py:66`) and the handlers `isinstance`-check it six times
(`handlers/simulations.py`). Narrow Protocols replace both first. **And this is where a
dispatch strategy gets its other half:** the per-mechanism progress code in the scheduler
(~600 lines) and the cancel routing in the facade and the handler move onto the strategies
P2.1 created with build + submit only.

### P7 — The `core` schema

`ALTER TABLE hpcrun SET SCHEMA core; RENAME TO job` — metadata-only, ids and sequences
preserved. Three releases:

| Step | Action | Rollback |
|---|---|---|
| (a) | Move the table; add an auto-updatable view `public.hpcrun` so the previous image keeps working. | drop the view, move back |
| (b) | Drop the view; create the real `public.hpcrun` extension table (PK = FK to `core.job.id`); copy the SMS columns. **Only when no chain campaign is RUNNING.** | previous tag + reverse copy |
| (c) | Drop the SMS columns from `core.job`; convert enums to VARCHAR + CHECK via `lower(status::text)`. Deferred to here because the old image binds `::jobstatusdb`. | **snapshot only** — soak first |

Then events / spans, `task`, `env_worker_task`, `environment`, `dataset` and the compose
registry move; `compose_hpcrun` merges **last**, with new ids and a `legacy_compose_id`.
Second Alembic chain in `viva_core/db/migrations` (`version_table_schema = "core"`, schema
filters in both `env.py`); per-service fingerprints; **one migration Job, same command**,
core chain then SMS chain from an ordered manifest; (c) refuses to run if (b) has not.
`HpcRunJobStore` → `SqlJobStore`. `/viva/v1/jobs/{id}/events`.

Deploy: migration Job each step, RDS snapshot before each. **Risk: high.**

### P8 — Clients

- `E2EDataService` gains `core_base_url` (defaults to `base_url`).
- The DTOs clients need move out of server internals; import-linter contract: `app` ↛
  `viva_api.dependencies`, `viva_api.simulation`.
- Drift and parity tests become per-spec.
- **The standalone core CLI (D8).** `make core_client` generates `viva_core/client/generated`
  from the core spec; `viva_core/cli` is built **only** on it — no hand-rolled URLs, no
  server imports — and covers environments and builds, composites, tasks, workers, datasets,
  jobs and events. It is part of `tests/core/` (run against the hook-less core app) and
  ships in core's own distribution at extraction. `atlantis`'s generic verb groups may then
  delegate to it. Can start any time after P3; must exist by P9.

### P9 — Second Deployment

Same image, `uvicorn viva_core.api.app:app`.

| Step | Action |
|---|---|
| (a) | Core runs dark — API only, no pollers; reuses service account `batch-submit`, so no IRSA change. |
| (b) | `CORE_MODE=remote`: SMS uses `HttpCoreClient`, drops its in-process core routers and pollers, reverse-proxies the core prefixes. Core at replicas = 1 owns the relay; workbench `PROXY_BASE` → `http://core:8000`; the `hpcrun` → `job` FK becomes a soft reference. **Rollback = a flag flip.** |
| (c) | sms-cdk: target group, retarget the `/compose` and `/env-worker` rules, add `/viva`, security-group rule, CfnOutput; `cdk deploy`; remove the proxy. |

### P10 — Public readiness, then extraction

Per-route authn/authz (building on `plan-authentication.md` phase 4), `created_by`
enforced, quotas, image and repo allow-lists defaulting to deny, the three code-execution
surfaces gated, K8s labels parameterised, `VIVA_CORE_` env prefix. Then extract to its own
repo and PyPI distribution, with the core CLI.

### Out of scope, on purpose

- **`SimulationServiceK8s`** (`simulation_service_k8s.py`, 624 lines: the upstream-vEcoli
  K8s + Nextflow path). It is the **default backend on both Stanford sites**, it duplicates
  the image-build commands, and it holds both standalone-analysis entry points
  (`submit_standalone_analysis`, `submit_ray_native_analysis`). The P2 carve does not touch
  it. It shares the pure `analysis_spec` module once that exists (PR 3); its build becomes a
  `repo-recipe` in P5, which is what removes the duplication; `scripts/qualification_test.sh`
  stays its check, and `atlantis smoke` does not cover it. (Decided 2026-09-20. Before the
  audit the plan did not mention it at all.)
- **Production.** Prod is on **0.9.78**; dev is at 0.9.150. A catch-up is *not part of this
  work* (Jim, 2026-09-20): the plan only records the gap and what closing it needs — an RDS
  snapshot, `db_reconcile --analyze` against prod, the migration Job across every revision in
  between (section 7a, risk 3), then smoke Tier 0 + 1 and Tier 2 including the cancel checks.
  Prod still has #709 (a cancelled run leaves its ParCa job running).

## 5. Sequencing against in-flight work

- **#678 (BioModels consolidation, merged 2026-09-18):** already on `main`; it shrank the
  surface to `identifiers`, `metadata` and one `/biomodels/run`, which is the shape that
  moves (D9). New BioModels work should keep to the rule in P5 — no imports from SMS
  packages — so nothing has to be untangled at the move.

- **#661 (datasets): merge as-is, first.** It is large, conflicting with `main`, and owned
  by another session; reshaping it before merge costs more than relocating it after. P4a
  generalises it additively; P7 moves it.
- **#656 (task provenance): do not build it on `hpcrun.jobref_task_id`.** Build it once, in
  core shape, in P4b. If it becomes urgent, P2.0–P2.1's Batch-engine and tasks cuts + P4 can run ahead of the rest of P2 and P3.
- **`plan-remove-slurm-fsx-stanford-test.md`** removes SLURM from a *site*; D4 keeps SLURM
  as a *core backend*. They do not conflict, but the SLURM code must survive that removal
  as `viva_core/backends/slurm.py`.

## 6. Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | The P7 table move strands or corrupts job state | three-step expand / contract, compat view, snapshot, drain rule, precondition guard |
| 2 | Concurrent sessions editing the Ray service and `tables_orm.py` | move-only PRs, short-lived branches, announced freezes, #661 first |
| 3 | A prod big-jump across several migrations | ordered manifest, `--analyze` first, a runbook naming minimum intermediate tags |
| 4 | The old image's enum binds break against VARCHAR | conversion deferred to P7(c) |
| 5 | Chain-gating latency once core is HTTP | outbox cursor at 2 s, idempotent handlers, measured in P6 before P9 |
| 6 | Relay is single-replica; a second process clobbers tasks | `owner_instance` scoping in P0, replicas = 1, one poller-owner role flag |
| 7 | SLURM untestable from a laptop | fixture contract tests, explicit *unverified live* label, move but do not change |
| 8 | `compose_hpcrun` ids collide and are client-visible | merge last, `legacy_compose_id` |
| 9 | Shim fragility (`python -m`, resource staging, patch targets) | aliasing finder, `db_reconcile` stays a real module, a test asserting staged file bytes |
| 10 | Kustomize patch conversion silently changes env | diff of rendered manifests |
| 11 | Fingerprint drift across two chains | `create_all` off first |
| 12 | Drift tests fight two specs | SMS spec is the union until P8 |

## 7. Open questions

None blocks the P2.1 carve. Each has the phase that needs its answer; none has been decided,
though later text had started to assume some of them.

| # | Question | Needed by | Leaning |
|---|---|---|---|
| 1 | Is `/viva/v1` the right public prefix for core? | P3 | yes, root prefix configurable |
| 2 | May migrated compose rows get new job ids (old one kept in `legacy_compose_id`)? | P7 | yes; P7 and §7a already assume it |
| 3 | `/api/v1/tasks`: a permanent SMS facade, or deprecated in favour of `/viva/v1/tasks`? | P4b | facade |
| 4 | How urgent is #656 — take the fast lane to P4? | P4b | not urgent until someone says so |
| 5 | Core's bus: Redis + the outbox (and delete the dead `compose_nats_*` settings), or NATS? | P5 | Redis + outbox |
| 6 | Does the public hosted core get its own database? | P9 | — |
| 7 | The core CLI's name (`viva`?), and whether `atlantis` delegates its generic verbs to it | P8 | — |
| 8 | **Does core depend on `pbest`** for the address parser and the recipe generator (`compose-api` already imports its types), or carry its own copy? | P5 | depend, if `pbest` stays domain-neutral: a third copy is how the three partial implementations happened |
| 9 | **What does "compatible" mean** for selecting an environment, beyond an exact spec hash: covering `provides`? version ranges? a curated list only? | P5 | exact match first; `provides`-covers second |
| 10 | **viva-api#742 (Eran, 2026-09-21): push the split to "one object, one run, one record" — and one surface?** Six ranked changes and five questions for Jim. Two strategies are drafted for the two of them to decide between: [`strategy-core-incremental.md`](strategy-core-incremental.md) (Eran's `viva_core` end state, the SMS contract unchanged for the scope of this refactor — to contain risk, not as a commitment to permanence) and [`strategy-core-direct.md`](strategy-core-direct.md) (no facades; the callers change during the refactor, with Eran's design time and the added risk accepted). #776 (the `JobStore` seam) is consistent with both | before P4a | **open — Jim and Eran** |

## 7a. Migrations: proper, tested, and honest about reversibility

Every schema change is an Alembic revision on the chain that owns the table (the SMS chain
until P7, then SMS or `core`), with a fingerprint marker while `create_all` still
bootstraps. Nothing merged through 0.9.145 contains a migration.

**Standing rule for every revision in this plan:** a real `downgrade()`, and a
Postgres-container test that runs **upgrade → downgrade → upgrade** and checks the schema
each way. (Today only one migration test exercises a downgrade.) A revision that cannot meet
that says so in its docstring *and* is listed below, gets its own deploy checkpoint, and an
RDS snapshot comes first.

| Phase | Change | Reversible? |
|---|---|---|
| — | existing chain: 14 of 16 revisions | yes — real downgrades |
| P0 second wave | #637: `b9e1d5a3c7f2` (creates nine tables), `c3f7a1e5b9d4` (reshapes three baseline tables) — inserted, guarded | `b9e1…` yes, but its downgrade **drops the tables and their data** — only ever right on a database this chain built. `c3f7…` downgrade is a deliberate no-op: the baseline's shape is one the application cannot use. Both are no-ops on every existing (`create_all`) database |
| — | `a1c3e5f7b9d2`, `44335812e447` (and #661's `b2f6d8e0a4c7`): enum `ADD VALUE` | **no** — Postgres cannot drop an enum label; the downgrade is a documented no-op. Benign: an unused label |
| P0 second wave | `e7b3c9a1d5f2`: `env_worker_task.owner_instance` + index | yes — and proven: upgrade → downgrade → upgrade round-trips to an identical schema |
| D11 (checkpoint B2) | `f4c8a2e6d0b3`: `simulator.temporary` (NOT NULL, default false), `label`, `image_tag` | yes — additive with defaults, so the previous image keeps working; round-trip proven the same way. A downgrade loses the marker on any temporary simulator made since, which would then read as authoritative: purge them first |
| P4a | owner-ref columns on `hpcrun` / `dataset`, backfilled, dual-written | yes — old columns stay authoritative |
| P4b | `task_script` table; task = job with `owner_kind = 'task'` | yes. **No `TASK` enum label is added** — the job kind rides in the new VARCHAR column, precisely so this stays reversible |
| P5 | `environment` table | yes — dropping it loses only rows created since |
| P7 (a) | `SET SCHEMA`, rename, compat view | yes — metadata only |
| P7 (b) | `public.hpcrun` extension table + column copy | yes, with a reverse copy, while idle — `core.job` still holds the columns |
| P7 (c) | drop SMS columns from `core.job`; enums → VARCHAR + CHECK | **one-way in practice.** A data-preserving `downgrade()` is written (the data lives on in `public.hpcrun`; statuses cast back by `upper()`), but no older *image* runs against the result. Rollback = snapshot. Soak (b) first |
| P7, last | merge `compose_hpcrun` into `core.job` with new ids | **not reversible once new rows exist** — migrated rows rebuild from `legacy_compose_id`; rows created afterwards have no old id to return to |

## 8. Deploy checkpoints

Merging is not deploying. A phase is *proven* only where the thing under test exists only in
a deployment: the image actually containing a package, configuration as the pod reads it, the
reconciler against a real RDS, shutdown under a real rolling restart, dispatch against real
Batch, routing through the real ALB.

**Rule: one kind of plumbing change per deploy** — image / configuration / dispatch path /
startup wiring / database / routing — so a regression on dev bisects to one cause.

| # | After | Why it needs a deploy | Prove on dev |
|---|---|---|---|
| A ✅ 0.9.145, 2026-09-18 | P0 first wave + P1a | new top-level package in the image; reconciler probes; shutdown order | `current_schema()` is `public`; migration Job classifies MANAGED; pod boots; `/app/viva_core/models.py` on the newest pod; EUTE smoke via `atlantis`; `vwb smoke`; one rolling restart's logs |
| A2 ✅ 0.9.146, 2026-09-19 | P1b + the `run_pbg` fix (#689) | configuration plumbing — how the storage settings reach the file services — kept apart from P2.1's dispatch change (one kind per deploy) | Tier 0 + Tier 1; `compose` flips FAIL → PASS; `atlantis simulation outputs` (the S3 file service end to end); marker `/app/viva_core/settings.py` |
| B ✅ 0.9.147, 2026-09-19 | P0 second wave + #661 | `create_all` off and the FRESH path changed — how every database bootstraps | alone; `--analyze` per site; migration Job; boot against an already-migrated DB |
| C1 ✅ 0.9.148, 2026-09-20 | P2.1 cuts 1–3 + the #709 fix (#710) | the first **dispatch** checkpoint, taken early: the Batch engine now lives in core and every submit goes through it; cancel now stops a run's ParCa job | Tier 0 + 1 + 2, including `sim-cancel` and `chain-cancel`, which must flip FAIL → PASS; markers `/app/viva_core/backends/batch.py` and `cancel_companion_jobs` |
| B2 ✅ 2026-09-20 (migration Job from 0.9.149; API still 0.9.148) | the write-once marker (D11): migration `f4c8a2e6d0b3` adds `simulator.temporary / label / image_tag` | a **database** change, so on its own before C2 (one kind per deploy). `main` also carries undeployed dispatch changes that #722 sits on top of, so B2 and C2 cannot be two images; they are **one image (0.9.149) and two deploys**: B2 runs only the migration Job and leaves the API on 0.9.148, which the additive migration allows; C2 rolls the API | `--analyze`, then the migration Job; Tier 0 + Tier 1 against the **old** API on the new schema (`database` still says "at head `e7b3c9a1d5f2`": `/health` reads the revision **at startup** and the pod was not restarted, so between B2 and C2 that check is stale, not evidence); `force` → 409 is checked at C2, when the code that refuses it is running; on dev only, mark simulator 214 (the unmarked smoke artifact of 2026-09-20) temporary by hand |
| C2 ✅ 0.9.149, 2026-09-20 | cuts 4–5, build + tasks as services (#712–#714), PR 2's smoke checks, and #722's code (write-once, the marker) | the image build and the task path were rewired and are merged but undeployed | Tier 0 + 1 + 2, `sim-mbp`, and the opt-in **`build`** check: a real image build of a **marked-temporary** simulator, which Tier 2 then runs on |
| C3 ✅ 0.9.150, 2026-09-21 | PRs 3–8 (analysis spec, ParCa split, the composed Batch layer, `compose` on it, the mbp-tracked and Nextflow strategies), 6a, and the **#730 fix — the one behaviour change in the set** | every submit now goes through a composed object; two mechanisms are strategies; a multi-node composite starts receiving `RAY_SHARDS_DEFAULT` | Tier 0 + 1 + 2; `compose`, `sim-mbp`, `sim-nextflow`, `nextflow-cancel` especially; Tier 0 `capabilities` still lists `container-jobs` (PR 5); the three #730 checks in the deferred list |
| C ✅ 0.9.151, 2026-09-21 | PRs 9–11 (composite, ensemble, chain strategies); the end of P2.1 | the last three mechanisms, chain among them | Tier 0 + 1 + 2, **plus a real 2 x 2 chain campaign** and `chain-cancel`: chain bills real money and fakes share their author's blind spots |
| D ✅ 0.9.152, 2026-09-21 | P2.3 | one resolver replaces four image derivations; the core runtime image | workbench through the relay; `vwb smoke`; `atlantis worker`, `task`, `compose` on the new image |
| D2 ✅ 0.9.153, 2026-09-21 | #761 (a build adopts an existing image), P3a–c, #759 | **the build script is the one that runs against write-once tags** (sms-cdk#55 made the repository `IMMUTABLE` before this); core's router is served under `/viva/v1` | Tier 0 + 1 + 2 **with `--build`**: the temporary-simulator build is the check that matters — it is the adopt script against the immutable repository |
| E ✅ 0.9.154 → **0.9.155**, 2026-09-22/25 | P3d-1 … 3d-4d-1 (+ #779): compose and env-worker in `viva_core`; **the runner is core's with SMS's hooks staged beside it** — every mechanism's job contract changed | the staged-hooks contract, live, on all five simulations and compose | 0.9.154: Tier 2 20/2/2 — the 2 failures one bug (the Nextflow head staged the shim, #777); 0.9.155: `sim-nextflow` + `nextflow-cancel` 2/2, Tier 1 13/0, runtime 13/0. The originally planned E (settings split, lifespan, app factory) is now **checkpoint E2**, after 3e/3f |
| E2 | P3e, P3f | settings split finished, new wiring and lifespan, app factory | alone; diff redacted effective settings and the OpenAPI spec old pod vs new |
| F | P4a, then P4b | additive migration with dual-write | SQL check that both column sets agree; `atlantis dataset` |
| G | P5 | durable compose dispatch | kill the pod mid-dispatch; the row must be reconciled, not stranded |
| H | P6 | scheduler split | multi-generation chain campaign; gating latency vs baseline |
| I | P7 a, b, c — each separately | the table move | rehearse on a restored copy of the prod DB; RDS snapshot; (b) only with no campaign RUNNING; soak between steps |
| J | P9 a, b, c — each separately | second Deployment and ALB routing | dark, then the flag flip (rollback = flip back), then `cdk deploy` |

**The smoke suite (`atlantis smoke run`, `make smoke`).** Every checkpoint is proven with the
same command rather than by hand. A check passes only on an observed **effect** — a nonce
read back from a task's log, the number a composite must compute — never on a status; SKIP
is reported separately from PASS and says why; `--json-out` is the record a release links.

| Tier | Cost | What it proves |
|---|---|---|
| 0 | seconds, free, read-only | `/version` = `/health`; every spec operation is served; capabilities; the relay is routed and live (JSON 404, not the gateway's HTML); the database-backed list endpoints; an events read |
| 1 | minutes, cents | one tiny real dispatch per mechanism: a container **task** (uploaded script; the nonce *and* the `sim_data_refs` it was given must come back in its log); **`task-fail`** (a script that exits 3 must be reported FAILED, with proof in its log that it ran); **`task-repo`** (a script already in the image, by repo path — the other entry point); opt-in **`build`** (`--build`: build a **marked-temporary** simulator — its own record, `temporary` with a label, its own image tag `tmp-<commit>-<nonce>` — and pass only when the **registry** shows *that* tag arrive after the check began; the API alone would answer COMPLETED. It cannot touch an authoritative simulator (D11), and it stops if the server does not mark the simulator. ~15 min; it runs before Tier 2, which then runs on the image it built); a relayed env **worker** (a K8s Job) + a task on its task tier, always stopped; a five-step **composite** that must return 1.1^5; opt-in: a standalone **analysis** (`--simulation-id`), a **BioModels** run (`--biomodel`) |
| 2 | tens of minutes, dollars | **one real simulation per dispatch mechanism**, submitted the way a real client selects each and run **concurrently**: `sim-default` (1 seed x 1 generation), `sim-chain` (2 x 2 — more than one generation is what selects chain dispatch; every seed must have succeeded), `sim-nextflow` (`extra_params.nextflow_dispatch`; every traced task completed), `sim-composite` (`extra_params.multi_node_dispatch`), `sim-mbp` (`extra_params.mbp_dispatch`: the reference variant of `run_mbp_tracked.py` for one simulated minute — the mechanism exists so that output *survives the container*, so that is what is asserted). Each must show **output**, not just COMPLETED. Three more **cancel** what they submit — `sim-cancel` (the run's ParCa job, then its own), `chain-cancel` (a 2 x 2 campaign cancelled in its ParCa phase, where no seed has a job yet) and `nextflow-cancel` (head Job deleted; tasks stopped by Nextflow's hook or the scheduler's reaper) — and assert on **AWS Batch itself**, with the operator's own read-only credentials, that no job carrying the run's experiment id is still active: the API cannot be the witness, because the cancel handler writes CANCELLED to its own row whether or not anything stopped. Without AWS access they SKIP, before submitting anything. Not covered: the upstream K8s + Nextflow path (`scripts/qualification_test.sh` stays the check for that) |
| R (`--tier 3`) | minutes | a task is put in flight, the deployment is restarted with the operator's own `--restart-command` (`scripts/smoke_restart_k8s.sh`), `/version` must be unchanged and the task must still resolve with its output. Status that lives only in a pod's memory fails this. The shutdown order in the terminated pod's log is still read by hand |

Required: **A** = 0 + `task`. **A2** = 0 + 1 + an outputs download. **B** = 0 + 1. **C1**, **B2** = 0 + 1. **C1**, **C2**, **C3**, **C** = 0 + 1 + 2 (P2.1 is the first change that
can break dispatch — Tier 2 and R are built before it). **D** = 0 + 1 (`worker`, `task`
especially). **E** = 0 + 1 + R. **F** = 0 + 1. **G** = 0 + 1 + R. **H** = 0 + 1 + 2. **I**, **J** = all.

**Prod cadence.** Dev takes every checkpoint. Prod may skip code-only ones but follows dev on
every **database** checkpoint (B, F, I) after a soak — letting migrations pile up for prod is
the big-jump risk (§6 #3).

**Before each deploy:** check for pod-local in-flight work (`hpcrun` rows on the `local`
backend, relay workers, unsettled `env_worker_task`) — a restart drops those. Batch and K8s
jobs survive; polling resumes with the new pod.

## 9. Verification, every phase

`make check` (twice — the first run reformats), `uv run pytest`, the import-linter
contracts, the `create_all`-vs-migrations parity test (from P0), `db_reconcile --analyze` on
each site before any migration, a `kustomize build` diff for every overlay change. On dev
through the tunnel: `atlantis simulator latest`, `simulation run … --poll`,
`simulation outputs`, `task run|status|logs`, `compose …`, `worker …`, `dataset list|get`;
`uv run vwb smoke` from the workbench checkout for the env-worker and compose consumers;
the marker grep on the **newest** pod. For P6 and P9, a multi-generation chain campaign with
gating latency compared to the baseline.

**Methods this work has taught, promoted here from the decision log:**

- **Classify by domain role before choosing a shape** (P2's role table). Twice a shape was
  picked from how the code was arranged — a build mixin, an analysis service — and twice the
  question "what *is* this?" gave a different answer.
- **Pure move → byte-identical proof.** `scripts/prove_ray_carve_is_move_only.py` (deleted at
  checkpoint C, when the carve it proved was deployed; `git show v0.9.151:scripts/prove_ray_carve_is_move_only.py`
  brings it back) compared
  every method of the hierarchy by source with `origin/main` and exits 1 on a differing,
  lost, added or doubly-defined method. For a strategy PR the body moves verbatim with
  `self.` rewritten to the dispatcher, and the script compares **after that substitution**,
  so most of the byte-level proof survives a rewiring.
- **Rewiring → a differential run, and its zero counts only after a mutation.** Old and new
  side by side, recording fakes, every outward call and return compared. Then (a) count how
  many cases run to completion versus raise, and (b) mutate the new code and watch
  differences appear. #714's first "0 differences" was vacuous for three case families: the
  fakes raised the same error on both sides.
- **The live proof is the mechanism's own smoke check**, with a baseline taken *before* the
  change it judges. A check asserts the effect, not the status; for cancel that means asking
  AWS Batch, because the API's answer is the row it just wrote.
- **Static guards glob, never list.** `test_dispatch_events_identity.py` had already missed a
  dispatch once because "the module was simply not scanned"; it now scans `simulation/dispatch/*`.
- **Tests move with the code.** `tests/simulation/test_ray_backend.py` (5,866 lines, 200
  instantiations) is split per mechanism in the same PR as each strategy.
- **Type coverage: `strict` is not "no `Any`".** Measured 2026-09-20 on `main`: `strict = true` over
  339 files, 0 errors — but strict does not set `disallow_any_explicit`, `_unimported`,
  `_decorated` or `_expr`. Turning them on reports 1,028 explicit `Any` (482 of them in
  tests; 55 in `viva_core`, 18 in `simulation/dispatch`), 91 decorated, 13 unimported (mostly
  `viva_core/backends/k8s_job_service.py`: the kubernetes client has no stubs). Of the 546
  outside tests, 39 % are JSON-shaped (`dict[str, Any]`). Explicit `Any` understates it:
  by mypy's any-expression report `viva_core/backends/batch.py` is 72.7 % precise, against
  100 % for `dispatch/parca.py` and `dispatch/parca_spec.py`, because `boto3` is untyped. Sequence
  PR 6a fixed that one cause: `backends/batch.py` 72.7 % → **100 %**, `simulation/batch_build.py`
  74.6 % → 100 %, `simulation_service_ray.py` 96.4 % → 98.0 %, the repository 92.70 % → 92.99 %.
  PR 6b then put the ban on: `viva_core` 92.72 % → **99.17 %** precise (539 → 62 imprecise
  expressions), `simulation/dispatch` 92.92 % → **99.84 %** (135 → 3), the repository 92.70 % → 93.44 %.
  **Decided (D12):** `viva_core` carries no `Any` — sequence PR 6b
  turns on `disallow_any_explicit` + `disallow_any_unimported` for `viva_core.*` (a glob: a
  standing rule for everything that moves into core later) and for the dispatch package's
  existing modules; PR 12 widened that to `ray.*` once the strategies had landed (the five
  strategy modules: 34 imprecise expressions → **4**, all in the Nextflow head's kubernetes
  calls; `simulation/dispatch` 99.33 % → **99.87 %** precise; `viva_core` unchanged at 99.17 %). Still
  only proposed: ratcheting the rest of the repository by count, like the import edges.
  Not worth doing repo-wide at once (482 of the 1,028 are in tests).
- **Import edges ratchet.** Report-only edges still broken: **8** (measured with `lint-imports`
  at P2.1 PR 6; it was 12 at the audit and 9 before that PR). The count never rises, and a
  PR that touches one burns it down.
- **Deploy early and often within one kind of change** (C1, C2, C3): a Tier 2 failure then
  has two or three suspects, not ten.
- **Have the design attacked before building it.** An independent review overturned three
  claims of this audit's own first draft (what a strategy owns; recompose the Batch layer
  first, not last; ParCa is only half a service).

## Deferred, tracked

One list, so nothing lives only in a decision-log aside. None of these is part of the core
split; each has an owner-less issue or a named moment.

| Item | Where | When |
|---|---|---|
| The env-worker Job names one deployment: label `app: sms-api`, service account `batch-submit`, module path `/app/vivarium-workbench/…` — now inside core (found at 3d-4a; moved unchanged) | `viva_core/env_worker/service.py` | become settings with today's values as the application's defaults; before a second application runs env workers |
| An image build had never been exercised by any smoke tier, and the build path was rewired in cut 4 and #714 (merged, undeployed) | **done**: the opt-in `build` check (#720) | run it at checkpoint C2 |
| `mbp_dispatch` had no smoke check | **done**: `sim-mbp` (#720) | baseline before the mbp-tracked strategy (PR 7) |
| compose on Ray / Batch accepts `extra_pip_deps` and never installs them | #716 | refuse now, or honour in P5 |
| compose on SLURM: a FAILED container build suppresses every later rebuild | #717 | folded into the P5 resolver; live in `compose-api` |
| 153 simulation runs stuck RUNNING on dev | #718 | — |
| `force=true` on a built simulator | #721 | **guarded** (409) by the write-once PR; deploys at B2 |
| The `v2ecoli` and `vecoli` ECR repositories are tag-MUTABLE, and dev and prod share one registry (D11) | #721 | turn on tag immutability (sms-cdk) once B2 is on both sites |
| Temporary simulators accumulate: each is a row and ~5.7 GB in ECR, and nothing removes them | — | a purge command, with P5 |
| Simulator 214 on dev (`d01dc07`) is an unmarked smoke artifact from before the marker existed | dev | mark it temporary by hand at B2; its image keeps the commit's tag |
| The dataset walk re-lists every simulation forever (~$5–6 / month / site); walking terminal simulations once a day would cut it ~10x | decision log, 2026-09-19 | P4a, when the walker moves to core |
| Draft #670 conflicts with P1's move of `gcs_aio.py`; a resolution was offered | #670 | when its author picks it up |
| RDS snapshot `pre-0-9-147-checkpoint-b-20260919t1955z` | dev | delete once 0.9.148 has soaked |
| ~~**#730**, the *deployed* half~~ **done at C3** (2026-09-21): the smoke composite job `6b34a40f…` carries `RAY_SHARDS_DEFAULT=32` in its `RAY_JOB_CMD` (16 vCPU × 2 nodes; at C2: absent); the API log has 0 `Could not determine per-node vCPUs` warnings since the roll (at C2: one per dispatch); `sim-composite` completed | dev | a many-seed A/B of throughput would be a separate, billable measurement; not planned |
| ~~The package `viva_api/simulation/ray/` is named after one orchestration framework~~ **done for the package and its four helper classes** (2026-09-21, the rename PR): it is `viva_api/simulation/dispatch/`, and `RayBatchLayer` / `RayParcaService` / `RayTaskService` / `RayImageBuilder` lost the prefix. **Still named after Ray, on purpose:** `SimulationServiceRay` and `simulation_service_ray.py`, `ComposeSimulationServiceRay`, the `test_ray_*` files — they are what `ComputeBackend.RAY` selects — and the persisted or deployed names themselves (`ComputeBackend.RAY`, `JobBackend.RAY`, `JobId.ray`, `RayLayout`, the `ray_*` settings, the queue names). Ray runs inside only two of the five mechanisms (ensemble, multi-node composite); Nextflow orchestrates itself; mbp-tracked and chain are plain container jobs (Jim, 2026-09-21) | viva-api | one decision, later: rename the persisted names (a data and config migration) or keep them and rename nothing else. The service class moves with that decision, not before |
| `GET /api/v1/simulations/{id}/status` answers **500**, with a traceback in the log, for an id that does not exist (seen at C3 and again at D2, each time from a diagnostic probe) | viva-api | 404; pre-existing, not from this work |
| RDS snapshot `pre-0-9-149-checkpoint-b2-20260920t1433z` | dev | C2 passed 2026-09-20; delete once it has soaked |
| ~~Smoke `sim-chain` downloads the whole chain output~~ **done** (2026-09-21): it lists the run's output in S3 instead — the prefixes come from what the run's own finished Batch jobs declare (`*_OUT_S3`), since the API says nothing about where a run wrote. Checked against C2's real chain run: 53 objects and 2 seed summaries, the same as the download, in 5 s instead of ~30 min. Falls back to the download without AWS access | `app/smoke.py` | an API that LISTS a run's outputs (names and sizes) would let any client do this, and is worth having for users who should not have to download GBs to see what is there — not planned |
| Temporary simulators 214 and 215 and the images `tmp-d01dc07-b64227[-submit]` on dev / in the shared ECR | dev | the purge for temporary simulators (not built yet); until then they stay, marked |
| `/health` reports the database revision **as read at startup**, so smoke's `database` check cannot see a migration applied under a running pod (seen at B2) | viva-api | read it per request, or label it `db_revision_at_startup` |
| ~~`atlantis smoke` probes AWS Batch once, at startup, and reports a failure only where the affected checks print — behind the chain, an hour in~~ **done** (2026-09-21, the smoke-probe PR): the probe is tried twice; what a failed probe changes is printed **before the run** (`NOTE … sim-cancel will SKIP`, `sim-chain will DOWNLOAD`); `--require-aws` stops there with exit 2, which is what a deploy checkpoint wants; and a Tier 2 verdict is reported the moment it lands instead of in check order, so nothing queues behind the chain | `app/cli.py` (`smoke_run`), `app/smoke.py` | use `--require-aws` for every checkpoint from D on |
| `AwsRunOutputLister` finds no prefix for a multi-node run: it reads a job's container environment, and an MNP job keeps `RAY_OUT_S3` under `nodeProperties`. Harmless today (only `sim-chain` lists) | `app/smoke.py` | read `nodeProperties.nodeRangeProperties[].container.environment` too, then let `sim-default` and `sim-composite` list instead of download |
| Every simulation row's `last_updated` is the API pod's boot time: `Simulation.last_updated` defaults to `str(datetime.datetime.now())`, evaluated once at import (`simulation/models.py`) | viva-api | `default_factory`; its own small PR, with a test that two rows made a second apart differ |
| The **generic** container entrypoint lives in the science repositories, and their copies have **diverged**: v2ecoli's (2026-08-19) is application-free; sms-ecoli's (2026-09-12) imports its model's cache verifier inside the stage-in step. Core now has its own copy (2.3c), which no science image uses | `v2ecoli/docker/`, `sms-ecoli/docker/`, `viva_core/runtime/` | give core's entrypoint one hook (run `/opt/post-stage-hook` if it exists) so an application keeps its verification **without forking the contract**, then have the science images `COPY` core's script. Needs the science repos' owners |
| The core runtime image serves **container-shape** jobs only. The multi-node Ray entrypoint (`ray-batch-entrypoint.sh`, 330 lines) exists only in the science repositories and has model-specific steps inside it (`V2E_BUILD_UPSTREAM_PARCA`) | `viva_core/runtime/` | a core copy with those steps behind a hook. **Less urgent since 2.3d-3**: a composite that needs only the built-ins runs there as one container; the Ray shape matters when a curated environment wants a cluster |
| ~~Where Batch pulls the runtime image from~~ **decided and proven** (2026-09-21): ghcr, anonymously — Jim made `viva-core-runtime` public, and a trial job on `smsvpctest-ray-standalone` pulled `:0.1.0` from inside the VPC and ran. No ECR mirror | deployment | **prod is a separate VPC**: repeat the one-job trial there before prod names the image. If ghcr is ever unreachable from a Batch VPC, the fallback is still an ECR mirror |
| `process-bigraph` (at `55b70676`) imports `requests` without declaring it; the science image has it by accident. Pinned explicitly in `viva_core/runtime/requirements.txt` | upstream | an upstream issue / one-line PR to process-bigraph's dependencies |
| **Two sites, one registry, one tag per commit** — *in progress (2026-09-21)*. Dev and prod have their own RDS and share ECR `v2ecoli`, whose tags were `MUTABLE`; a second site building a commit the first already built **replaced** its image. **It had happened seven times** (seven untagged 5.7 GB originals, 2026-08-09 … 09-08; no lifecycle policy, so all are still there). **Done:** `IMMUTABLE` tags + `ecr:DescribeImages` for the build role — **sms-cdk#55 merged and deployed from dev's stack, 2026-09-21 18:26Z** (Jim: "deploy and merge sms-cdk#55"); the repository is shared, so **this is live for prod too**. **Done in code, not deployed:** the build step **adopts** an existing `<key>` / `<key>-submit` instead of rebuilding (viva-api#761) — until it is on a site, that site's build of an existing tag is *refused at the push* (loud, not silent) and a half-failed build cannot be retried. **Not done:** prod's `smscdk-build-batch` stack (a no-op for the repository; adds prod's `DescribeImages`); the **image digest on the simulator record** (needs a column: P5's `core.environment`); the upstream `vecoli` repository (per-arch intermediate tags; `SimulationServiceK8s` is out of scope until P5); re-tagging the seven originals so they cannot be lost | the shared ECR repository; `dispatch/build.py`; sms-cdk `BuildBatchStack` | deploy order: viva-api (adopt) on dev → sms-cdk from dev's stack → watch one build → prod's stack later (a no-op for the repository) |
| ~~With `ECR_ACCOUNT_ID` unset, every image reference is the malformed `.dkr.ecr.<region>.amazonaws.com/<repo>:<key>`~~ **done** (2026-09-21, Jim's call): an explicit spec is **refused by name** (`EnvironmentResolverNotConfigured`: "ecr_account_id is unset (ECR_ACCOUNT_ID) …"; 501 through core's route), before a job definition is registered or a job submitted. What needs no registry — core's health route, the runtime image — is unaffected. The suite now runs as a configured site (`tests/conftest.py`) | `viva_api/common/site_environments.py` | — |
| ~~`/viva/v1` is served by the pod and not routed by the ALB~~ **done on dev** (found at D2, 2026-09-21; sms-cdk#56 merged `c82d48c` and deployed the same day — one additive `ListenerRule`, 21 s; all three core requests then answered JSON through the tunnel). Smoke Tier 0 `core` now guards it | sms-cdk `internal-alb-stack.ts` | **prod**: deploy `smscdk-internal-alb` before prod serves `/viva/v1` |
| The dispatch blocks `mbp_dispatch` and `multi_node_dispatch` are **declared** (`TypedDict`s, PR 12) but not **validated** at the API boundary beyond `task_env`; `nextflow_dispatch` is checked for two rules only. A wrongly-typed value reaches the container command line | `common/dispatch_validation.py`, `handlers/simulations.py` | a behaviour change (requests that work today could be refused), so its own PR; the `TypedDict`s are the spec to validate against |
| `CLAUDE.md` still says backend selection is by `deployment_namespace` and that tests use SQLite | `CLAUDE.md` | any docs PR |
| `job_scheduler.py` (1,370 lines), `handlers/simulations.py` (2,463), `routers/env_worker.py` (1,170), `dependencies.py` (691) have no detailed plan yet | P3, P6 | before those phases start |

## Status ledger

| Phase | PRs | Version | Dev | Prod | Notes |
|---|---|---|---|---|---|
| P-1 | #679 | — | — | — | merged 2026-09-18 (`21bd7296`); docs only |
| P0 (first wave) | #680 import-linter contracts · #681 `set_messaging_service` · #682 reconciler `current_schema()` · #683 shutdown stops pollers · #684 kustomize by-name patches | 0.9.145 | **2026-09-18** (checkpoint A) | — | **merged 2026-09-18** (`ca67b43f`, `2f6d73b1`, `f99caa02`, `7f3f6777`, `86c5f292`); combined `main` verified: `make check` ×2, 378 tests. Not yet deployed — #681–#683 change runtime code and go out with the next version bump; #680 and #684 change nothing that runs |
| P0 second wave | #637 fresh-database fix + parity test — #700 (`833fcc8f`) · `DB_CREATE_ALL` guard + startup schema check — #701 (`2c898d19`) · `owner_instance` column scoping the env-worker boot sweep — #702 (`2ab03b37`) | 0.9.147 | **2026-09-19** (checkpoint B) | — | all three merged 2026-09-19 — **P0 complete**; migrations `e3a9c1d70b62` → `e7b3c9a1d5f2` applied on dev |
| P1a | #686 `viva_core/` skeleton, enforced `core-is-standalone`, `tests/core/`, first nine modules | 0.9.145 | **2026-09-18** (checkpoint A) | — | merged 2026-09-18 (`8c9f8e78`); marker `/app/viva_core/models.py` confirmed on the newest pod |
| P1b | #691 `viva_core.settings` (`CoreSettings` + provider); `storage/*`, `infra/ssh`, `backends/{slurm_service,nextflow_trace}` moved; `config` ⇄ `file_paths` cycle gone | 0.9.146 | **2026-09-19** (checkpoint A2) | — | merged 2026-09-19 (`c9fa2bd5`); proven by an S3 outputs download on the live pod |
| P2.0 | (a) test guard vs real AWS — #693, merged 2026-09-19; (b) `_seams` + 298 patches retargeted — #696; (c) smoke Tier 2 + R | (b) touches the module, no behaviour change | — | — | (a) #693 and (b) #696 merged; (c) smoke Tier 2 + R — #698; all merged 2026-09-19 |
| D11 | write-once simulators + the marked-temporary exception: migration `f4c8a2e6d0b3`, `environment_key`, `force` guarded (409), the marker in all three clients, smoke `build` on a temporary simulator — #722 | — | — (checkpoint **B2**, a database deploy, before C2) | — | open |
| P2.1 | carve `simulation_service_ray.py` (5,019 → **628** lines; PR 11 took 960; PR 10 took 377; PR 9 took 648; PR 8 took 548; PR 7 took 252; PR 5 added 35 — the constructor and two delegates came over from the layer; PR 4 *added* 89: a 68-line composite-only helper came back from the mixin, plus the `parca` property and two facades). Cut 1 config interpretation — #705 · cut 2 Batch engine → `viva_core/backends/batch.py` — #706 · cut 3 tasks + `BatchLayer` — #707 · cut 4 build — #712 · cut 5 ParCa — #713 · build and tasks as composed services — #714 · the #709 cancel fix — #710 · smoke checks — #708. · analysis spec → `dispatch/analysis_spec.py` — PR 3 (#726) · ParCa split → `dispatch/parca_spec.py` + `ParcaService` — PR 4 (#727) · `BatchLayer` composed as `service.batch` — PR 5 (#728) · compose handed its Batch layer — PR 6 (#729) · typed boto3 — PR 6a (#731) · #730 fixed (#732) · the D12 ban — PR 6b (#733) · strategy: mbp-tracked — PR 7 (#734) · strategy: Nextflow — PR 8 (#735) · strategy: multi-node composite — PR 9 (#738) · strategy: ensemble — PR 10 (#740) · strategy: chain — PR 11 · the package `Any`-free — PR 12 · `simulation/ray/` renamed `simulation/dispatch/`. **All five mechanisms are strategies**, and the 2026-09-20 sequence is complete; #715 (analysis as a service) **closed, superseded by PR 3** | 0.9.151 carries the whole carve: every strategy (PRs 7–11), 6a/6b, and the #730 fix | **2026-09-21** (checkpoints C1, B2, C2, C3, C) | — | **done — the carve is deployed.** **Dev is 0.9.151 (checkpoint C, 2026-09-21, tag `v0.9.151`):** all five dispatch mechanisms run as strategy objects on a deployment. Merged after C and **not deployed** (no behaviour in them to deploy for; they ride checkpoint D): PR 12 (#744, annotations only) and the `dispatch/` rename (#745, names only). The service's nine scheduler delegates and its progress / cancel / staging methods stay until **P6**. **Next: P2.3**, the environment model and its *select* half |
| P2.2 | — | | | | **absorbed into P2.1** (2026-09-20): the mechanisms go straight to strategy objects |
| P2.3 | the environment model and its *select* half (D10): one resolver for four image derivations; then the core runtime image. 2.3a the model + `RegistryEnvironmentResolver` (`viva_core/environments/`, no caller changed) — #748 · 2.3b the four derivations ask it (`common/site_environments.py`) — #749 · 2.3c the core runtime image + core's container entrypoint (`Dockerfile-core-runtime`, `viva_core/runtime/`) — #750 · 2.3d-1 a task may name an environment (`TaskRunRequest.environment`) — #751 · 2.3d-2 Batch pulls the image; dev names it — #752 · 2.3d-3 a compose run may name an environment (one container) | | | | **done and deployed** — dev is 0.9.152 (checkpoint D, 2026-09-21, tag `v0.9.152`): the four image derivations ask one resolver, `CORE_RUNTIME_IMAGE` is live, a task and a compose run may name `environment="runtime"`. Measured on dev: `compose` 21.6 s in the runtime image against 495.9 s on the science image; a cold-fleet `task` starts in 106 s against 221 s. Prod: repeat the one-job trial pull from its VPC before it names the image |
| P3 | settings, DI, app factory. 3a `create_core_app()` boots alone; SMS includes core's router under `/viva/v1` | | | | **in progress** — 3a, 3b, 3c deployed at D2 (0.9.153); **3d-1 … 3d-4d-1 deployed at E (0.9.154/0.9.155, tag `v0.9.155`)**; 3d-1 done (compose's ParCa staging is a hook); 3d-2 done (the 14 settings compose and env-worker read are `CoreSettings` fields); 3d-3 done (compose is handed its services; it imports nothing of `viva_api.dependencies`); 3d-4a done (the env-worker service and the site resolver are in `viva_core`); 3d-4b-1 done (five compose modules `Any`-free, the D12 ban on for them by name); 3d-4b-2 done (`models` and `container_def` are in `viva_core`; `ComputeBackend` too); 3d-4b-3 done (the five and the abstract service are in `viva_core`: 3,025 lines under `viva_core/compose/` + `env_worker/`); 3d-4c-1 done (the Batch compose service is in `viva_core`; the layout primitives too); 3d-4c-2 done (the runner and `render_nf` are core's; SMS's hooks are a staged sibling — every dispatch command changed, undeployed); 3d-4d-1 done (the compose handlers are core's); 3d-4d-2a done (the compose router is core's, served at `/viva/v1/compose` and, unchanged, at `/compose/v1`; BioModels in `contrib/sysbio`); 3d-4d-2b done (the env-worker router and identity are core's) — **3d-4 complete**; 3e done (containers replace the setters: the routers carry none, `current_container()` is the one seam); next 3f (settings split finished, two OpenAPI documents, the lifespan), then **checkpoint E2**. **SLURM compose stays in SMS** until a SLURM site can test it |
| P4a | | | | | not started (checkpoint F) |
| P4b | | | | | not started (checkpoint F) |
| P5 | | | | | not started (checkpoint G) |
| P6 | | | | | not started (checkpoint H) |
| P7a / b / c | | | | | not started (checkpoint I) |
| P8 | | | | | not started (checkpoint —) |
| P9a / b / c | | | | | not started (checkpoint J) |
| P10 | | | | | not started (checkpoint —) |

## Decision log

> **Names.** `viva_api/simulation/ray/` became `viva_api/simulation/dispatch/` on 2026-09-21, and
> `RayBatchLayer` / `RayParcaService` / `RayTaskService` / `RayImageBuilder` lost the prefix. Entries
> dated before that are history and keep the names they were written with; everything above this
> heading uses the current ones.

- **2026-09-25** — **P3e: the container replaces the setters — one seam, registered like the settings provider.**
  Jim: "merge #785 and continue with 3e". Until now every subsystem router held module globals that
  `viva_api/dependencies.py` pushed into it at startup (`set_compose_services`,
  `set_env_worker_service`, `set_env_worker_task_service`), and the relay held a process-wide
  `runner` set the same way; `dependencies.py` imported router modules to do it. Now `CoreContainer`
  carries two service groups — `ComposeServices` (db, default backend, monitor, file service, and the
  APPLICATION's allow-list default; core's is empty) and `EnvWorkerServices` (service, task db,
  runner; each optional on its own, since a deployment may run workers without the task tier or the
  reverse) — and the routes ask `current_container()`. The provider is registered the way core's
  settings provider has been since P1b: `create_core_app()` registers the container it holds; SMS
  registers `core_wiring.core_container` at import, which builds a container per request from the
  settings of the moment and the groups `dependencies.py` built in its lifespan (so a request before
  the lifespan sees a container with no services and is answered by name, the status those routes
  always used). `dependencies.py` no longer imports a router (the contract that was report-only
  since P0 was kept in 3d-4d-2b and stays kept), and its shutdown finds the runner on the container.
  **Kept on purpose:** the routers' `_require_*` accessors, because 41 tests patch them by name and
  they are now one line each over the container; the relay's socket `registry`, which is process
  state by nature (it holds live sockets). **Proof:** `make check` clean twice; suite 2294 passed;
  a new test boots a core with an empty container and gets 500/503 by name on the compose and
  env-worker routes and 200 on health, and parses every router module for a `set_` function.
- **2026-09-25** — **P3d-4d-2b: the env-worker router is core's, and identity came with it — 3d-4 is complete.**
  Jim: "merge #784 and continue with 3d-4d-2b". The router (978 lines, 30 routes) had two ties the
  compose one did not: **who the caller is** (`viva_api/api/auth.py::require_caller` /
  `resolve_caller`, which read the identity header and verify an OIDC bearer through
  `viva_api/api/oidc.py`) and **which role this process is** (`owner_instance`, stamped on env-worker
  tasks). Both are core's questions — a route that destroys something must ask who is asking — so
  `auth` and `oidc` moved into `viva_core/api/` unchanged but for `oidc`'s five JSON `Any`s (a
  discovery document that is not an object is now a `TypeError` inside the same `except`, warned
  once as before), and their eight settings are `CoreSettings` fields. One string in the router named
  the model ("Translate a vEcoli-style config") — the vocabulary guard's hit — and is reworded; it is
  the only change in the served OpenAPI document (regenerated and compared: the same 102 operations,
  ids and schemas). `create_core_app()` serves both routers at core's prefix; SMS serves them where
  its callers expect. The `auth` module's `__all__` hides `MAX_IDENTITY_LEN` from the shim's
  star-import (the relay lesson), so the four tests that read it import from core by name; the
  overlay-env-vars guard now scans both packages, because the relay's advertise-host reader moved.
  **What 3d-4 leaves in `viva_api/compose/`:** the SLURM service and `hpc_utils` (Jim's decision,
  2026-09-21) and eleven shims. **Proof:** `make check` clean twice; suite 2292 passed; the
  `dependencies.py does not import the routers it wires` contract is now KEPT (4 kept, 3 broken);
  report-only broken edges 5 → 4.
- **2026-09-25** — **P3d-4d-2a: the compose router is core's, and the SMS surface did not move.**
  Jim: "merge #783 and continue with 3d-4d-2". `viva_api/api/routers/compose.py` (702 lines, 16
  routes) had four ties. **The results route knew one backend's transport**: for SLURM it opened an
  SSH session and `scp`'d the HPC-side zip; for Batch it streamed S3. The transport is the backend's:
  `ComposeSimulationService.results_archive(experiment_id) -> Path | None` (the SLURM service fetches
  its zip over the SSH provider it was handed in 3d-3; the Batch service returns `None`), and the
  route serves what the backend hands back or streams the run's prefix. **The S3 tar streaming**
  (`common/s3_streaming.py`) read `data_layout` and looked up the file service; the mechanism is
  `viva_core/storage/s3_streaming.py::stream_prefix_tar_gz(file_service, prefix, name)`, and the SMS
  module keeps its old names as wrappers for `handlers/simulations.py`. **The allow-list fallback**
  (`DEFAULT_COMPOSE_ALLOW_LIST`, SMS's science stack) and **the file service** are handed to the
  router with its services (`set_compose_services(..., default_allow_list=, files=)`); core's default
  is an empty list. **`/curated/ecoli`** names the model: it is `viva_api/api/routers/compose_sms.py`,
  a second router SMS mounts at the same `/compose/v1` prefix (its jinja template with it).
  `compose_containers_output_dir` joins `CoreSettings`. The setters (`set_compose_services`,
  `_require_*`) stay: 41 test patches reach them by name and 3e is the step that replaces them with
  the container; this step is the move. BioModels (`biomodels_service`, `biomodel_documents`) went
  first, into `viva_core/contrib/sysbio/` per D9, `Any`-free — libsedml's untyped objects are three
  small Protocols, the multi-model entry a `TypedDict`.
  **Served twice, one code:** `create_core_app()` includes the router at `/viva/v1/compose`; the SMS app
  at `/compose/v1`. The standalone boot test now sees the compose routes at core's prefix and none at
  the application's. **The SMS OpenAPI document:** regenerated and compared structurally — the same
  102 path+method pairs, the same operation ids, the same schemas; the diff is the stale version line,
  one description string already on `main`, and the order `/curated/ecoli` is emitted in.
  **Proof:** `make check` clean twice; suite 2283 passed; the report-only broken-edge count fell 6 → 5
  (`dependencies.py → routers.compose` is gone: the setter it imported is core's).
- **2026-09-25** — **Checkpoint E passed on dev (0.9.155, #782, tag `v0.9.155`): P3d is deployed, and the staged-hooks runner contract is proven on every mechanism.**
  Jim: "go, bump 0.9.155 and close checkpoint E". Two rolls, one checkpoint. **0.9.154** (#774,
  2026-09-22, `88094488`): P3d-1 … 3d-4c-2; Tier 0 8/8 (with the new `core` check), Tier 1 with
  `--build` (temporary simulator 217), runtime-image run 13/0, **Tier 2 20 passed / 2 failed / 2
  skipped** — `sim-default`, `sim-chain` (2/2 seeds × 2 generations), `sim-composite`, `sim-mbp`,
  `compose` on the science image and the three cancels all ran core's runner with `runner_hooks.py`
  staged beside it; the two failures were one bug, the Nextflow head staging the 16-line shim at
  `viva_api.compose.render_nf` instead of the script (see the 2026-09-22 entry; #777). **0.9.155**
  (#782, `f0263986`; carries #777, #775 and #779's `viva_emitters` rename in core's runner): markers on
  `api-849d7bbb88-dfzsb` (`render_nf_source` in core, `viva_core/compose/handlers.py`, no
  `pbg_emitters` in the runner); `sim-nextflow` **PASS** (simulation 1429: 2 tasks, all completed,
  1,677 s) and `nextflow-cancel` **PASS** (1428, a running head cancelled); Tier 1 13/0/3; runtime
  13/0/3. No migration in either roll; the workbench and ptools pods did not move.
  **What E was and was not:** the plan's row E named the settings split, the new lifespan and the app
  factory (3e/3f). What shipped as E is P3d — the package move and the one runtime contract change
  the move forced. That was the right thing to prove first, and it found a real bug; the original
  contents are now **checkpoint E2**, after 3e/3f. 0.9.154 is deliberately untagged: E is tagged on
  the version that passed in full.
  **Process, for the record:** #775, #778, #777 and #656 were merged by Eran (2026-09-23/25) as
  squash commits, outside the merge-commit rule; #777 he rebased and merged "to unblock the edge".
  The rebase broke one import in the new staged-scripts guard, fixed before merge.
  **Also on this deploy:** viva-api#780 — the 36 CD2 Run 3 backfill rows were stored `COMPUTING`
  with `result_uri` at the analyses root; set READY with `result_uri` at the module directory
  (2026-09-24); `/plots` and `/status` on Batch sites are #781.
- **2026-09-22** — **#742 is a decision for the two humans, and it now has two documents instead of a thread.**
  Eran's issue (2026-09-21 03:25Z) proposes taking the split to "one object, one run, one record" and
  asks Jim five D-series questions. Its 15-comment thread was conducted by sessions under both logins;
  Jim first saw it on 2026-09-22 ("This is Jim the human … Are you advocating breaking the api contract
  with workbench and ptools?"). Two strategy documents are drafted for review: **A, incremental** —
  Eran's `viva_core` end state (templates as the campaign primitive at P7, `core.job` as the record,
  one environment concept, one generic run surface) with the SMS contract unchanged for the scope of this refactor (the 29 workbench
  paths, the 3 PTools calls, the aliases) — held still to contain risk, not as a commitment to
  permanence, which Jim never intended; **B, direct** — the same core with no facades, every SMS
  surface dated and removed on a clock, with the coordinated workbench / PTools / atlantis releases and
  the prod catch-up that implies — acceptable to Jim if Eran takes on the added risk and the design
  of the final configuration. Open question 10; nothing in P0–P3d is undone by either. #776 fits both.
- **2026-09-21** — **Checkpoint D2 passed on dev (0.9.153, #762, tag `v0.9.153`): the build adopts, the tags are write-once, and core answers at `/viva/v1`.**
  Jim: "merge viva-api#761, and then get back on the plan. if we need a deploy now then do it". It was
  needed: sms-cdk#55 had already made `v2ecoli` `IMMUTABLE`, so until #761 was on a site a retry of a
  half-failed build could not work there. Image from `50f3fd8c`; only the api pod rolled; no migration.
  Markers on the newest pod (`api-5c54b8f7db-flxcb`): `image_exists` in `dispatch/build.py`;
  `viva_core/api/app.py`, `simulation/compose_analysis.py` and `compose/env_worker_schemas.py`
  present; `_RegistryNotConfigured` in the site resolver.
  **Smoke, `--tier 2 --build --require-aws`: 21 passed, 0 failed, 2 skipped** (`analysis` and
  `biomodels`, which need an argument). Tier 0 7/7. Tier 1: `task`, `task-fail`, `task-repo`, `worker`,
  `compose` (516 s on the science image), and **`build`: temporary simulator 216, 689 s, tags
  `tmp-d1aeba3-536591` and `tmp-d1aeba3-536591-submit` pushed 19:18:33Z and 19:18:48Z** — the generated
  script (which asks ECR first; a temporary simulator's tag is new, so there was nothing to adopt)
  built and pushed both into a repository that `describe-repositories` reports `IMMUTABLE`. Tier 2 ran **on that temporary simulator**: `sim-mbp`
  998 s, `sim-default` 1,624 s, `sim-nextflow` 2,088 s, `sim-composite` 2,330 s, `sim-chain` 2,583 s
  (2/2 seeds over 2 generations, 52 files in S3), and the three cancels asserted on AWS Batch (20 s,
  41 s, 340 s). A second run in the runtime image (`--environment runtime`): 3 passed, `compose` in
  31 s. **What the smoke did not prove** is the *adopt* branch itself — a build of a key whose image
  already exists — because a temporary simulator always has a new tag. That branch is proved by
  `tests/simulation/test_build_adopts_an_existing_image.py`, which runs the generated script against
  a fake registry; live, it will first run when a site registers a simulator the other already built.
  **Found by the deploy, fixed the same day: `/viva/v1` was served by the pod and answered by PTools.**
  The ALB routes by path prefix and had no rule for `/viva`, so every core request fell through to
  PTools' HTML 404 — and smoke's `routes` check passed, because it compares two OpenAPI documents and
  cannot see a gateway. sms-cdk#56 added the rule (one additive `ListenerRule`, deployed to
  `smsvpctest` in 21 s; prod's stack is not deployed), and #764 added a Tier 0 **`core`** check that
  calls `/viva/v1/health` through the front door and fails on an HTML answer; it passes on dev.
  **The API log since the roll:** one ERROR with a traceback, and it was mine — asking for the status
  of simulation 1402, which does not exist, answers 500 rather than 404. Already on the deferred list, since C3.
  **Merged during or just after the smoke, not deployed:** P3d-1, 3d-2, 3d-3 (#763, #765, #766), #764,
  P3d-4a (#767) and P3d-4b-1 (#768). One of them changes behaviour, in one place — a relay frame that is
  JSON but not an object is now a named error (#768); they ride the next checkpoint.
- **2026-09-22** — **Checkpoint E found it: a module shim is transparent to an import and opaque to a file read.**
  0.9.154's Tier 2: `sim-nextflow` FAILED at 171 s (simulation 1403) and `nextflow-cancel` with it.
  The head's log: `render_nf.py` downloaded at **559 bytes**, then `from viva_core.compose import
  render_nf as _moved` / `ModuleNotFoundError: No module named 'viva_core'`. `dispatch/nextflow.py`
  staged the compiler by reading `render_nf.py` as package data from `viva_api.compose` — which since
  3d-4c-2 is the 16-line self-replacing shim. Every importer of the moved modules was unaffected
  (the shim IS the module at runtime), which is what the shims are for; a **file read** of the old
  package gets the shim's text. The runner had already been switched to core's `runner_source()`;
  the compiler had not. Fix: `render_nf_source()` beside it in `viva_core/compose/runner_files.py`,
  and a guard that parses every staged text and fails on a shim
  (`tests/simulation/test_staged_scripts_are_the_real_files.py`) — run against `main` it reports
  "16 lines; shim: True". The other four Tier 2 paths (chain, multi-node, ensemble, compose) stage
  the runner and the hooks and passed; only Nextflow stages the compiler.
- **2026-09-22** — **P3d-4d-1: the compose handlers are core's; two of them were SMS's and go to SMS.**
  Written while checkpoint E's Tier 2 ran (Jim: "option (1)"). `compose/handlers.py` (293 lines) held
  the generic run, the curated run, the allow-list check and the dispatch — and two things that name
  one application: `hooks_source` (reads SMS's `runner_hooks.py`) and `run_compose_v2ecoli` (the
  `/curated/ecoli` run: the model's git origin, its container mode, 7 domain terms). Those two are
  `viva_api/simulation/compose_hooks.py` and `compose_curated.py`, beside the other things SMS hands
  to compose; the rest is `viva_core/compose/handlers.py`, unchanged. The two ids a run is known by
  (`correlation_id`, `experiment_id`) sat in `compose/hpc_utils.py` among the SLURM paths; neither is
  a path and every backend needs them, so they are `viva_core/compose/ids.py`, with the old names kept
  as aliases in `hpc_utils` (which stays, with the SLURM service). Shim at the old handlers name.
  **Proof:** `make check` clean twice; suite 2267 passed.
- **2026-09-21** — **P3d-4c-2: the runner is core's and generic; what the model needs of it is a staged sibling file.**
  `run_pbg.py` (1,165 lines) is the generic process-bigraph runner, staged into a job and run INSIDE
  the science image — where neither `viva_api` nor `viva_core` is installed. Two things in it were one
  model's: a 70-line context manager that makes v2ecoli's generator-declared ParquetEmitter write
  real, partitioned output to the job's results directory (it imports `v2ecoli` lazily), and the set
  of composite ids that are the batch-baseline Step. Core cannot carry them and the runner cannot
  import them from a package. **Where should they live?** Three answers were put to Jim: (a) a sibling
  file viva-api stages beside the runner — the pattern `render_nf` already uses to find `run_pbg`;
  (b) a module in the science image, named by env — the cleanest end state, but a v2ecoli PR, a new
  simulator build, and until then every existing simulator image lacks the hooks, so it cannot deploy
  alone; (c) defer. Jim chose (a).
  **The seam.** The runner declares `RunnerHooks` (`emitter_override`, `batch_baseline_composite_ids`,
  both optional) and loads them from a module named by `PBG_RUNNER_HOOKS`, importable from its own
  directory; unset means the generic runner; named-but-not-staged fails before the run, by name — a
  staging bug must not be a silent no-op three minutes into a pull. SMS's two pieces are
  `viva_api/compose/runner_hooks.py`, verbatim, stdlib-only at module scope (a test pins that for both
  files). **The staging** is written once: `viva_core/compose/runner_files.py` names the two files and
  the hooks' sibling URI; `dispatch/runner_env.py::stage_runner_commands` is the `aws s3 cp` pair every
  mechanism uses (chain ×2, multi-node, ensemble), and `PBG_RUNNER_ENV` carries `PBG_RUNNER_HOOKS`.
  `stage_runner` uploads both. The compose service (core) stages ITS runner from its own package and
  the application's hooks when handed some (`runner_hooks=`, replacing 3d-4c-1's `runner_source=`),
  and only on the application's image — a registered environment has no application in it. The
  SLURM recipe embeds core's runner and no hooks: that container is python-slim + process-bigraph,
  where the hooks' lazy imports fail and they were already a no-op. Nextflow's head stages the runner
  for `render_nf`'s resolver only; rendering never reaches the override.
  **Typing.** Four Protocols name what the runner asks of process-bigraph's objects (`Core`,
  `CompositeSpec`, `EventEmitter`, `Span`); the JSON walks take `object`; where a function only hands
  an object on or reads it by `getattr`, it takes `object`. Two narrowings became explicit: a `.pbg`
  file that is not a JSON object, and a `n_generations` that is not a number, each with the same
  outcome as before (refuse / skip the guard). One string in a refusal message named the model's
  issue tracker; reworded.
  **Blast radius, said plainly:** every mechanism's command gains one `aws s3 cp` and one env var, and
  every job now imports a second staged file. The unit tests pin the copy pair on all four commands
  and the compose one; the live proof is checkpoint E's Tier 2, all five simulations.
  **Proof:** `make check` clean twice; suite 2263 passed (12 new).
- **2026-09-21** — **P3d-4c-1: the Batch compose service moves; the S3 layout's two primitives go to core, pure.**
  Jim: "okay, please proceed". `compose/simulation_service_ray.py` had two ties left to the application.
  **The layout.** It called `data_layout.RayLayout.experiment_prefix` / `results_uri` and `s3_uri` — SMS's
  206-line layout module, whose reason to exist is that the writer, the reader and the downloader
  derive one location from one place. Compose needs only the two things every layout is built from:
  *one bucket, one prefix, one directory per run.* Those are `viva_core/storage/layout.py` —
  `s3_uri(bucket, key)`, `experiment_prefix(prefix, id)`, `results_uri(bucket, prefix, id)` — and
  `data_layout` is now built ON them (its `_bucket` / `_prefix` helpers are gone), so the two cannot
  drift; a test pins that they agree. **They are pure**, the bucket and prefix handed in. The first cut
  read `get_core_settings()` inside them, and 121 tests failed at once: SMS's tests patch
  `data_layout.get_settings` as their seam for the bucket, and a primitive that reads its own settings
  walks past that seam. Handing the values in keeps every existing seam what it was — the same rule
  the site resolver follows. `s3_work_bucket` and `s3_output_prefix` move to `CoreSettings`; the
  prefix's default (`vecoli-output`) names the science and is the application's, under the same
  allow-list as the two of 3d-2.
  **The runner.** The service read `run_pbg.py` from `viva_api.compose` as package data at import, as
  the recipe did before 3d-4b-2; it is handed a reader (`runner_source=`) the same way, from the
  composition root (`compose/handlers.py::runner_source`, now shared with the SLURM path), and
  refuses to stage a run without one. When the runner moves (3d-4c-2) core supplies its own default.
  The class keeps its name and file name: the `ray` misnomer is a separate rename, after the moves.
  **Proof:** `make check` clean twice; suite 2243 passed (3 new). The five compose test doubles for core
  settings gained the two fields, as they gained `batch_region` in 3d-2 — a `SimpleNamespace` names only
  what its test is about, so a new read is a test edit by design.
- **2026-09-21** — **P3d-4b-3: the five move, the abstract compose service with them, and two things a shim cannot carry.**
  Jim: "continue". `database_service`, `tables_orm`, `job_monitor` → `viva_core/compose/`;
  `env_worker_relay`, `env_worker_schemas` → `viva_core/env_worker/relay.py`, `schemas.py`. The job
  monitor's type for its registry is the abstract `ComposeSimulationService`, which lived in the file
  that also holds the SLURM implementation — the one file that stays. So the ABC is its own module,
  `viva_core/compose/service.py`, and `viva_api/compose/simulation_service.py` keeps the SLURM class
  and re-exports the base for its importers. **Homes, corrected from the row:** all under
  `viva_core/compose/` and `env_worker/`, not `db/` and `services/` — those packages belong to P4 (the
  `core` schema) and 3e (the containers), and creating each for one file would name a structure that
  does not exist yet. Every import inside the moved files that went through a `viva_api.common.*`
  shim is re-pointed at core directly; none of them imports the application any more.
  **Two things a self-replacing shim cannot carry.** (1) The relay declares `__all__`, and it leaves
  out its process-wide `runner` and `set_runner` on purpose; the shim's `TYPE_CHECKING` star-import
  honours `__all__`, so mypy — not the runtime — lost them for the router and the composition root.
  Both now import `viva_core.env_worker.relay` by its real name. (2) Three tests reached a private
  name (`_TERMINAL_COMPOSE_STATUSES`) or module state (`runner`) through the old path; re-pointed.
  Every other importer, string patch and `isinstance` is untouched.
  **One string.** `env_worker_schemas` described a field as "vEcoli-style config to translate" — the
  guard's one hit in the five, a description in a construct. It now says what the field is without
  naming a model. **The D12 by-name entries are gone** from `pyproject.toml`: the glob covers them.
  **Proof:** `make check` clean twice; suite 2237 passed. 3,025 lines now sit under `viva_core/compose/`
  and `viva_core/env_worker/`; what remains in `viva_api/compose/` is eight 16-line shims, the SLURM
  service (253), `hpc_utils` (42), and 3d-4c's four: `simulation_service_ray` (371), `run_pbg` (1,165),
  `render_nf` (303), `handlers` (290) — plus BioModels (358), 3d-4d's.
- **2026-09-21** — **P3d-4b-2: the leaves go first — `models`, `container_def`, and `ComputeBackend` are in core.**
  Jim: "continue with step 1". 3d-4b-1's row said "those five move" next; reading their imports said
  otherwise — four of the five import `compose/models.py`, two of them `container_def.py`, and a module
  cannot enter core ahead of what it imports. So the two leaves go first, and the row is corrected here
  rather than silently. What each one needed:
  **`models.py` (12 `Any`, one domain term).** Ten of the `Any`s were `dict[str, Any]` JSON fields and
  are `dict[str, object]`, which the OpenAPI schema renders identically (the spec diff is the version
  line and one docstring). Two were a `FlexData` / `Payload` bag and `as_payload()`, with no caller
  anywhere in the tree — deleted. `from_pb_outline` unpacked `**process` into a model; it now validates
  a mapping, and an entry that is not one is a `TypeError` by name instead of a failure inside `**`.
  The domain term was **`DEFAULT_COMPOSE_ALLOW_LIST`**: the packages a compose document may install,
  naming one application's science stack (`v2ecoli`, cobra, tellurium, …). It is the application's:
  `viva_api/simulation/compose_allow_list.py`, next to the other things SMS hands to compose (its
  analysis, its staging, its simulator registry); the composition root seeds it and the router falls
  back to it, as before. A standalone-gate test pins that core's models carry no default.
  **`ComputeBackend`** was `viva_api.config`'s; compose's request models name it, so it is
  `viva_core.models.ComputeBackend` beside `JobBackend`, and `viva_api.config` re-exports it — its
  nineteen importers are unchanged.
  **`container_def.py`** read `run_pbg.py` from `viva_api.compose` as package data **at import time**
  — a recipe that cannot be built without a file in another package. `build_pbg_def` now takes
  `runner_source=` and the SLURM handler, which is the one caller, reads and hands it over. That is
  also the shape P5 wants ("keep `run_pbg.py` out of the hashed definition"): the runner is an input
  to the recipe, not part of it. The runner itself stays where it is until 3d-4c.
  Both old names are self-replacing shims; every importer, patch and `isinstance` is untouched.
  **Proof:** `make check` clean twice (the vocabulary guard, the standalone import and the `Any` glob
  all pass with the two inside core); suite 2221 passed (5 new); OpenAPI spec regenerated — the only
  schema change is `ComputeBackend`'s description.
- **2026-09-21** — **P3d-4b-1: five compose modules are `Any`-free, and the ban says so — what each `Any` was hiding.**
  The move into core is refused by mypy for any module that says `Any` (D12), so the pass comes first
  and the ban is switched on **by name** for the five (`pyproject.toml`), exactly as the dispatch
  modules were named in PR 6b before PR 12 made them a glob. The escape-hatch guard now reads the
  override, so a module banned by name is scanned too, and a name that no longer exists fails the
  guard ("moved? then drop the name").
  Three of the `Any`s were hiding something:
  **(1) the job monitor's `sim_registry: dict[ComputeBackend, Any]`.** A first attempt typed it as the
  one method the monitor calls; mypy then refused `api/routers/compose.py:94`, which reads the same
  registry to honour a per-request `compute_backend` and needs the whole service. The monitor is, in
  fact, where the registry of compose services lives. It is typed as that, and said in a comment;
  giving the registry its own home is 3e's (containers replace the setters).
  **(2) `list_all_computes(...) -> Any`.** It returns processes, steps, or both, by argument — and its
  two callers annotate the narrower list. Three overloads on the abstract method and on its one
  implementation say what the `match` already did.
  **(3) the relay's frames.** `_recv` declared `dict[str, Any]` for whatever `json.loads` returned. A
  frame that is valid JSON but not an object (a list) reached `call` and failed there as an
  `AttributeError`; it now raises `WorkerUnavailable("malformed frame: …")`, the same fault as a frame
  that does not parse. **The one behaviour change in this PR**, and it has a test. A JSON-RPC `error`
  that is not an object is treated as an empty one, as `or {}` already did for `null`.
  The NATS client (`Any | None`, with a comment naming the class) is two small Protocols —
  `WorkerEventBus`, `WorkerEventMessage` — so core will not import `nats` to type a client no
  deployment passes today. `env_worker_schemas` had no `Any` of its own; its 22 class lines carry the
  guarded pydantic ignore, as core's schemas do.
  **Not in this pass:** `render_nf` (8) loads `run_pbg` and goes with it (3d-4c); `models` (12) and
  `run_pbg` (32) name the application as well; BioModels (19) is `contrib/sysbio`'s question (3d-4d).
  **Proof:** `make check` clean twice; suite 2203 passed.
- **2026-09-21** — **P3d-4: the SLURM compose service stays in SMS for now; and the move is four steps, ordered by measurement.**
  **The decision (Jim).** Asked what the SLURM compose service amounts to: about 320 SLURM-only lines
  in a 5,772-line package — `ComposeSimulationServiceHpc` (~215 lines: a `singularity build` job and
  the run job, over SSH), `hpc_utils.py` (42), the monitor's `squeue` branch (~30), one branch each in
  the handler and the results download. No unit test submits either job. Half of the run path is one
  model's: a `mode == "v2ecoli"` branch and `_write_v2ecoli_script`, 15 domain terms in constructs.
  Neither Stanford site deploys AWS PCS, and the site that has SLURM — UConn Health's on-premise K8s
  cluster and HPC cluster, the `sms-api-rke` overlays — is paused. Jim: *"we will eventually like SLURM
  as a core technology, but now it can wait."* So `ComposeSimulationServiceHpc` and `hpc_utils.py`
  **stay in `viva_api/compose/`** as that site's compose backend, registered from the composition root
  exactly as today; everything generic around them moves (the abstract `ComposeSimulationService`,
  the monitor with its SLURM branch — handed its SSH provider, so a core with no SLURM never calls it
  — and `container_def.py`, which is D10's `python-deps` recipe). **D4 is unchanged**: SLURM is a core
  backend, and `SlurmService`, the SSH sessions and the SLURM job models have been in `viva_core`
  since P1b. What waits is the *consumer*, until a SLURM site can prove the move — which also makes
  it the second backend that D4's trigger asks for before a core Protocol is called final.
  **The order.** Every module under `viva_api/compose/` was scored against the three gates core
  enforces — the vocabulary guard's own AST scan, a count of `Any`, and imports of the application
  that are not already shims over core. Two modules pass all three today; six need only the `Any`
  pass; two name the application in a construct; the runner is the largest on both counts. The
  sequence table carries the numbers. `hpc_utils.py` and the SLURM service are not in it.
  **3d-4a, in this PR.** `env_worker_service.py` and `site_environments.py` moved with `git mv`; a
  diff of each against its old self shows four import lines changed in the first and nothing in the
  second. The old names are self-replacing shims, so every importer, every string patch and every
  `isinstance` is untouched. Core's gates pass with them inside: standalone import (the application
  blocked), vocabulary, and mypy's no-`Any` glob.
  **Found inside the moved code, not changed (a move changes nothing):** the worker Job carries the
  label `app: sms-api`, the service account `batch-submit`, and the module path
  `/app/vivarium-workbench/…`. None is a domain term and the guard does not object, but each is one
  deployment's name inside core — deferred list.
  **Proof:** `make check` clean twice; suite 2209 passed.
- **2026-09-21** — **P3d-3: compose is handed its services — and one of the four was not a service but a third hook.**
  The row said "four lookups (database, file service, SSH session)". Reading them: the *database*
  lookup is `_resolve_commit`, which turns `ComposeSimulationRequest.simulator_id` into an image key by
  reading **SMS's `simulator` table** — the application's registry, not compose's. Handing compose a
  database service would have moved SMS's table into compose's vocabulary. So it is a hook, like the
  two before it: compose declares **`EnvironmentKeyOf`** ("the environment key of the application's
  simulator *n*"), `viva_api/simulation/compose_simulators.py::simulator_environment_key` answers it
  (the same five statements, moved), and a compose service with **no** registry refuses a
  `simulator_id` by name rather than running on the site-pinned image. The other three are plain
  arguments: `files=` (the file service exists before the compose subsystem is built, so the instance
  is handed over), and `slurm_ssh=` on both the job monitor and the SLURM compose service — a
  **provider**, asked at the moment of use, because on a site without SLURM the session service never
  exists and both objects are still built. Behaviour on every path is what it was; the errors name
  what was not handed in.
  **Guard:** `tests/compose/test_compose_is_handed_its_services.py` parses every module under
  `viva_api/compose/` and fails on any import of `viva_api.dependencies`, lazy or not. Run against
  the parent commit it finds exactly the four.
  **What still ties the package to `viva_api`, for 3d-4** (counted, 19 imports): `common.models`
  (`JobBackend`, `JobStatus`, `SSHTarget` — 5), `config` (`ComputeBackend` ×2, the SLURM service's
  `Settings`, `hpc_utils` — 4), `common.site_environments` (2), `common.hpc.*` (5) and
  `common.storage.file_paths` (2) — the last two groups are already shims over `viva_core` and only
  need re-pointing — and `common.storage.data_layout` (1), which is not in core and names the
  application's S3 layout. The report-only broken-edge count is unchanged at 6: none of the four
  lookups was one of those edges.
  **Proof:** `make check` clean twice; suite 2201 passed (6 new).
- **2026-09-21** — **P3d-2: fourteen settings move to core; two keep an application default; one cannot move yet.**
  The 13 settings compose and env-worker read on the Batch / K8s path, plus `batch_region` (mypy found
  it: `RegistrySettings`, the Protocol `site_resolver` takes, names it, and a `CoreSettings` handed to
  `environment_image` did not have it), are now declared on `CoreSettings`. `Settings` inherits them, so
  every environment variable and every reader of `get_settings()` is unchanged; the four compose modules
  (`simulation_service_ray`, `env_worker_service`, `job_monitor`, `database_service`) read through
  `get_core_settings`, which returns the application's object through the provider registered in P1b.
  **Two fields are generic with an application-shaped default.** `env_worker_workspace_path`
  (`/app/v2ecoli`) and `ray_ecr_repository` (`v2ecoli`) name one application's repository — the
  vocabulary guard refuses them as core defaults, rightly. Core declares both as `""` and SMS redefines
  the default only. The settings guard used to forbid any redefinition; it now allows exactly the names
  in `APPLICATION_SUPPLIES_THE_DEFAULT`, and only when core's default is empty and the annotation is
  the same — so a redefinition cannot change a type or silently shadow a real core default.
  **One does not move: `slurm_log_base_path`.** Its type is `HPCFilePath`, from
  `viva_core.storage.file_paths`, which imports `viva_core.settings` — declaring it there is an import
  cycle. It stays in SMS, so `compose/hpc_utils.py` keeps reading `viva_api.config`; breaking the cycle
  (the path type stops reading settings at import) is its own cut, listed for 3d-4.
  **Found while counting, not changed:** the *SLURM* compose service (`compose/simulation_service.py`)
  was not in the 14. It takes SMS's `Settings` whole, reads `slurm_partition` / `slurm_qos` /
  `slurm_node_list` and two `compose_*` paths, and carries a method named after one model
  (`_write_v2ecoli_script`). It runs only on the unsupported RKE target. Whether it moves to core with
  the rest of the package, or stays in SMS as that site's compose backend, is decided in 3d-4 — not by
  moving its settings now. `ComputeBackend` (imported by `compose/models.py` and `job_monitor.py` from
  `viva_api.config`) is the other remaining tie, also 3d-4's.
  **Proof:** a snapshot of `Settings.model_fields` — name, annotation, default, for all 146 — taken before
  and after the change is identical. `make check` clean twice; the suite passes.
- **2026-09-21** — **P3d-1: compose's ParCa staging is the second hook; and 3d is four steps, not one.** Jim: "get
  back on the plan". 3c listed what still ties compose to SMS; measuring it said in what order it can
  go. Compose and env-worker read **15 settings, none of them a `CoreSettings` field**, and one of the
  fifteen — `compose_parca_cache_dir` — **cannot become one**: core's vocabulary guard refuses a
  construct named after ParCa, and rightly, because what it configures (`_parca_staging`: "stage this
  simulator's ParCa cache into the job") is SMS's knowledge inside a composite runner. So before the
  settings move, that goes the way the chained analysis went in 3c: compose declares **`StageInputs`**
  — "`(stage_s3, stage_dir)` to sync in before the composite runs, or nothing" — and
  `viva_api/simulation/compose_staging.py::compose_parca_staging` fills it from the composition root.
  Staging itself is the container contract's and stays generic; *what* to stage is the application's.
  **Proof:** the method's 3 statements are the function's 3, AST-identical; the three staging tests
  exercise the function where it now lives; the submit test builds the service with the hook, as the
  composition root does; new: a compose service with **no** hook stages nothing even on a site whose
  settings name a cache directory. 3d is rewritten as 3d-1 … 3d-4 in the sequence: hook, settings,
  services, then the move.
- **2026-09-21** — **Two sites, one registry: a build adopts an existing image, and the tags become write-once.** Jim
  asked what happens when the test site and the prod site — each with its own VPC and RDS — push the
  same image name and tag, and whether the site should go into the name. **Checked before answering:**
  the shared `v2ecoli` repository's tags were `MUTABLE`, no build path asked whether a tag existed, and
  the repository held **seven untagged whole images (5.7–5.8 GB, 2026-08-09 … 2026-09-08)** — each an
  original whose tag a later push took. So it was not hypothetical: seven simulator records point at
  a tag that now names a different image than the one they built. No lifecycle policy exists, so the
  seven originals are still there, by digest. D11's write-once had been enforced per *database*.
  **Not the site in the tag.** That isolates the sites and gives up the most valuable thing a shared
  registry offers: promoting to prod the *exact image dev tested*. Instead the shared name becomes
  truly write-once and a site **selects before it builds** — D10, applied across sites:
  (1) **sms-cdk#55** — an `AwsCustomResource` sets `IMMUTABLE` on the repository (a custom resource
  because the repository is created by the first build, not by the stack; idempotent; **no
  `onDelete`**, so deleting a stack never makes provenance overwritable again) and the build role gains
  `ecr:DescribeImages`. `cdk diff` on dev: additive only, no compute environment touched.
  (2) **viva-api** — the generated build script asks ECR first and **adopts** `<key>` and
  `<key>-submit` independently: a commit another site built succeeds *on the same image* (one short
  Batch job, no clone, no build, no push); a build that failed half way is **finished, not restarted**
  (without this, write-once tags would strand it for ever: its first image can no longer be pushed);
  any answer other than "not found" is reported and treated as absent, so a permissions gap cannot
  turn every build into a silent no-op; a temporary simulator asks about its own tag only. The API
  needs no ECR permission of its own: the build job is the adopter, and it prints the digest it adopted.
  **Tested by running the script**, under `sh`, against fake `aws` / `docker` / `git`: fresh build,
  adoption, half-failed retry, access denied, temporary tag — mutation-checked.
  **Deliberately not done here:** the digest on the simulator *record* (a column and a migration —
  P5's `core.environment`, which has both identities); the upstream `vecoli` repository;
  `cdk deploy`, which is Jim's — **the repository is shared, so immutability switches on for prod
  (0.9.78) at the same moment**: from then a prod build that pushes an existing tag is refused rather
  than overwriting, which is the protection, and a prod retry of a half-failed build stays failed
  until prod runs this change.
  **Deployed the same day** (Jim: "deploy and merge sms-cdk#55" — ahead of the order recommended above,
  knowingly). Both sites' build queues were idle; sms-cdk#55 merged as `825c084`; `cdk diff` from a
  checkout pinned at the merge matched the PR (five additions, one policy change);
  `cdk deploy smsvpctest-build-batch --exclusively` took 58 s and touched nothing else. **Verified three
  ways:** the repository reports `IMMUTABLE`; the build role's policy carries `ecr:DescribeImages`; and a
  probe — a throwaway tag added to a *temporary* simulator's image (allowed: new tags are additive),
  then an attempt to move it to a different image — was refused with `ImageTagAlreadyExistsException …
  cannot be overwritten because the tag is immutable`. The probe tag and its manifest were removed
  again (276 images before and after; the temporary simulator's two images untouched). **The window
  this opens, until #761 is deployed:** on ~~dev (0.9.152)~~ (dev has #761 since D2, 0.9.153) and prod (0.9.78) a build that pushes an
  existing tag now *fails at the push* instead of overwriting — which is the protection, said loudly —
  and a half-failed build cannot be retried.
- **2026-09-21** — **Checkpoint D passed on dev (0.9.152, #755, tag `v0.9.152`): P2.3 is deployed, and the runtime image was measured.**
  Jim: "merge #753 and #754, then bump and deploy D". Image from `100f7996`, built after its test gate.
  `kubectl diff`, rendered from a worktree pinned at the merge commit (the stale-checkout trap of
  0.9.138), showed the api image and a new `api-config` ConfigMap carrying `CORE_RUNTIME_IMAGE`; only
  the api pod rolled (ptools and workbench are the pods of 09-16). No migration. Markers on the newest
  pod: 0.9.152; `common/site_environments.py` and `viva_core/environments/` present;
  `simulation/dispatch/` present and **`simulation/ray/` gone**; `_submit_in_environment` in compose;
  `CORE_RUNTIME_IMAGE` in the pod's environment; the dev-only stub packages unimportable.
  **Smoke, all with `--require-aws` after refreshing the session (the lesson of checkpoint C), three
  runs started at the same instant from a worktree with its own environment:** Tier 0 7/7. **Runtime
  image** (`--environment runtime`): `task`, `task-fail`, `compose` — 3 passed. **Tier 1 on the science
  image:** `task`, `task-fail`, `task-repo`, `worker`, `compose` — 5 passed. **Tier 2: 8 passed, 0
  skipped** — `sim-default`, `sim-chain` (2/2 seeds over 2 generations; **52 output files listed in
  S3**, the #737 path used for real for the first time), `sim-nextflow`, `sim-composite`, `sim-mbp`, and
  the three cancels asserted on AWS Batch. Every image those runs pulled was named by the ONE resolver
  of 2.3b. No missing-access notice, no SKIP; and each Tier 2 verdict printed the moment it landed —
  the cancels at 20 s, not behind the chain (#747). #730's after-state again: `RAY_SHARDS_DEFAULT=32` on
  the composite job. The API log since the roll: no traceback, no ERROR line.
  **The runtime image, like for like** (AWS Batch timestamps; both `task` jobs created at the same
  moment on the same cold fleet):

  | job | image | queue → start | run |
  |---|---|---|---|
  | `task` | runtime, 370 MB | **106 s** | 1.9 s |
  | `task` | science, 5.74 GB compressed | **221 s** | 2.0 s |
  | `task-fail`, fleet now warm | either | 6–7 s | 2 s |
  | `compose`, one container | runtime | 4 s | 5.4 s |

  The pull is worth about 115 s on a new instance and nothing on a warm one — what 2.3d-2 predicted.
  The large number is `compose`: **21.6 s end to end in the runtime image against 495.9 s on the science
  image in the same hour**, because it is one container instead of a multi-node Ray cluster formed to
  multiply by 1.1 five times. That is what 2.3d-3 was for.
  **Not exercised:** the row asked for "workbench through the relay" and `vwb smoke`. The relay path the
  workbench uses — start a worker, call it, stop it, on an image the new resolver named — is what
  smoke's `worker` check does, and it passed; the workbench UI itself was not driven, and `vwb smoke` is
  a local check of the other repository that a viva-api deploy does not change.
  **Then exercised, at Jim's suggestion ("there is a workbench service cli which could be used"):** the
  workbench's own code, from a laptop, through dev's relay. `vwb sync CovertLabEcoli/sms-ecoli@d67b0a7`
  materialized simulator 213's exact workspace; with `VIVARIUM_WORKBENCH_ENV_WORKER_PROXY_BASE` pointing
  at the tunnel, `vwb smoke --workspace` selected its `ProxyWorkerLauncher`, and viva-api 0.9.152
  created `env-worker-d67b0a7-w-…` on the image the ONE resolver named, held its socket and forwarded
  the call: **`env-worker ok — ping -> ['ok', 'uptime_s']`**, confirmed in the API log (relay start,
  call 200, stop). `workspace` ok too. **`server` FAILED, and it is not viva-api's:** the workbench's
  local `serve` answered `/health`, then `GET /` — the first page render of a real whole-cell workspace
  over a cold relayed worker — exceeded smoke's 10 s (the cold start `CLAUDE.md` Pitfall 6 measured at
  126 s). 2 of 3. Two things a hosted worker needs that a laptop sync does not give: the workspace
  must carry a `.viv-build.json` build stamp (written by hand into the scratch copy), and the timed-out
  check left one relayed worker running (stopped with `atlantis worker stop`). `vwb sync` also
  registers the workspace in `~/.pbg/workspaces.json`; that entry was removed afterwards.
  **Not idle:** P3a, P3b and P3c (#756–#758) and the unset-account refusal (#759) were written while D
  built, rolled and ran — none of them is in 0.9.152.
- **2026-09-21** — **An unset ECR account is refused by name.** Jim, after asking what that meant: "okay … make a
  PR refusing an unset ECR account by name." The default settings leave `ECR_ACCOUNT_ID` empty, and
  until now every image reference was then `.dkr.ecr.<region>.amazonaws.com/<repo>:<key>` — a malformed
  name nobody looked at, which became a Batch job definition, then a submitted job, then an image-pull
  failure ten minutes later that never mentions the setting; since P3a it was also handed to clients
  by `POST /viva/v1/environments/resolve`. 2.3b preserved it on purpose so that rewiring could be
  proven to say what the four derivations said; this is the behaviour change it deferred.
  **Narrower than "refuse when unset", and that is the design.** The site's resolver is built on every
  request to core's health route, and the runtime image is a full ghcr reference that needs no ECR
  account. So `site_resolver` never fails; with the account unset it returns a resolver that refuses
  **the one kind of request that needs the registry — an explicit spec** — and answers everything else
  as before. Core gained the name for it: `EnvironmentResolverNotConfigured` ("the request may be
  perfectly good; the deployment is missing a setting"), distinct from `EnvironmentNotResolvable`
  ("looked, found nothing"), and core's route maps it to **501** where the other is 404.
  **The tests said what it would cost, exactly:** 9 failed — the 8 predicted in 2.3b plus the one test
  whose name said this PR would flip it. The 8 were not fixed one by one: they fail because the suite
  ran as an *unconfigured* site, so `tests/conftest.py` now sets an obviously fake account
  (`000000000000`; the no-real-AWS guard blocks the network regardless), and the unset case has tests
  of its own that pass their own settings — at the resolver, at the Batch layer (nothing registered,
  nothing submitted), and through core's route (501, the setting named, no `.dkr.ecr.` in the body,
  health and the runtime image still answering). Dev and prod set the account: nothing changes there.
- **2026-09-21** — **P3c: the first hook. Compose stops importing SMS's science; the contract is enforced.**
  Jim: "merge #756 and #757, then start on 3c." `compose-is-domain-free` had two broken edges, both in
  `compose/simulation_service_ray.py`, both for one method: `_submit_analysis_job`, which chains the
  **simulator's** analysis onto a compose run (`common/analysis_dag`, and the simulator's in-image paths
  from `simulation/dispatch/image_paths`). That is SMS's business inside a composite runner. It is now
  `viva_api/simulation/compose_analysis.py::ComposeAnalysisChainer`, and compose declares what it
  offers instead: **`AfterSubmit`** — "a run was submitted; here is its job, its commit, the image it
  runs under" — which the composition root (`dependencies.py`) fills with the chainer, sharing the one
  Batch layer. This is the plan's hook mechanism (P5) arriving early and small, because it is the
  honest shape: a core with no application registers nothing, and a test pins that such a compose
  service submits the run and chains nothing, even when the request carries `analysis_options`.
  **Proof:** the method's 14 statements are the chainer's 14, AST-identical after the one respelling
  (`self._image_uri(commit)` → the `image` argument); same gate (`analysis_options`), same
  best-effort guard around it; the two chaining tests pass unchanged but for how they build the
  service (as the composition root does). `compose-is-domain-free` is **KEPT and in
  `ENFORCED_IMPORT_CONTRACTS`**: `make check` now fails on a new edge. Report-only broken edges 8 → 6.
  **What the contract does not cover — 3d's list.** The row said "compose imports nothing of SMS";
  the contract forbids the *domain* (`simulation`, `analysis`, `data`, handlers, simulator defaults,
  the analysis DAG), and that is now true. Compose still imports the application's **wiring**:
  `viva_api.config` in 6 modules (`get_settings`, `Settings`, `ComputeBackend`); `viva_api.dependencies`
  for 4 lookups (database, file service, SSH session — two of them lazy, inside functions, which is
  how they hid); `common/site_environments` (2); `common/storage/data_layout` (the results prefix and
  the ParCa cache URI — the second of which is domain, and wants the same hook treatment). The rest
  of its `viva_api.common.*` imports are already aliases of `viva_core` modules. None of that can move
  until settings and services arrive through a container — which is why 3d and 3e are one piece of
  work in two PRs, and why 3d starts from this list.
- **2026-09-21** — **P3b: the env-worker router's models move out; its "service logic" was never there.** Written
  while checkpoint D's smoke ran. The 22 pydantic models that sat between the routes of
  `api/routers/env_worker.py` are in `viva_api/compose/env_worker_schemas.py`, verbatim: **22 of 22
  class ASTs identical and in the same order, the router's other 58 statements untouched, and the
  OpenAPI document unchanged** but for the version string and two timestamps — which is the proof
  that matters, since these models *are* the API. The section comments that explain the endpoints
  stayed with the endpoints.
  **The row's other half was wrong**, and reading the code said so. "Service logic lifted out of the
  1,170-line router" assumed there was some. There is not: 30 routes and 16 helpers, the largest 36
  lines of code, and what the helpers do is the HTTP boundary's work — `_relay_call` maps a lost
  socket to 410 and a worker's refusal to 422; `_unwrap` and three `_fails_on_*` rules turn the
  worker's **four different ways of saying no** into statuses; `_refuse_if_over_tier_budget` asks the
  worker whether a study fits the tier and answers 422. Moving those into a "service" would put
  `HTTPException` in a service. The file was long because it is thirty endpoints with long, useful
  docstrings, not because it hid a service. What actually ties it to SMS is small and is wiring: two
  module-global setters, `get_settings`, and `viva_api.api.auth` — 3d and 3e.
- **2026-09-21** — **P3a: core boots, alone — written while checkpoint D deployed.** Jim: "while we wait for ci
  and then deploy and smoke tests, can we start working on the next step optimistically". Yes: `main`
  is not dev, and only *deploying* the next thing waits for D's verdict. The plan's order for P3 is
  "`create_core_app()` and a test that boots it come first"; this is that, and no more.
  `CoreContainer` (`viva_core/container.py`): settings, and one field per core service — `None` where a
  deployment does not provide it, and the route then answers **501, by name**. It replaces, for core,
  the module globals and setters of `dependencies.py`; each service that moves into core becomes a
  field. **One router, one prefix, the same paths either way** (`/viva/v1`; question 1 of section 7 was
  already answered "yes, configurable", and the prefix is a parameter): a standalone core serves it
  through `create_core_app()`, and an application **includes** it — not mounts: a mounted
  sub-application's lifespan never runs and its routes leave the application's OpenAPI document, which
  the plan keeps as the union until P8. The container is handed in as a **provider called per
  request**, so an application may build it from services that exist only after its lifespan ran; a
  test builds the container *after* including the router to pin that.
  **The first core route is the one service core has:** `POST /viva/v1/environments/resolve` — explicit
  or derived spec in, image + spec hash + digest out; 404 (never something close) when nothing answers;
  422 the caller's; 501 the deployment's. Small, but it makes the boot test mean something: **a fresh
  interpreter in which `viva_api` and `app` cannot be imported builds the app from `CoreSettings`
  alone, serves a request, writes its own OpenAPI document, and leaks no application module.**
  SMS side: `viva_api/core_wiring.py` builds the container from the settings of the moment and the
  site's resolver; a test checks that core answers what `environment_image` itself says, for a commit,
  a temporary tag and the `submit` variant. Tagged **"Viva Core"**, because SMS already has a router
  called `core` (`/core/v1/simulator/*`) that is older than the split and a different thing.
  **Seen on the way:** with `ECR_ACCOUNT_ID` unset, the new route hands a client the malformed
  `.dkr.ecr.<region>…` image name — the deferred-list item from 2.3b, now visible through an API.
- **2026-09-21** — **P2.3d-3: a compose run in the runtime image is one container — and env workers stay where they are.**
  Jim: "merge #752, then start on 2.3d-3." The plan row read "compose (its container path) and env
  workers may name an environment". **Both halves were wrong about the code, and reading it said so.**
  *Compose's container path is not a way to run a composite*: it is `_submit_analysis_job`, the chained
  **science analysis** (it reads the run's ParCa cache and runs the simulator's analysis modules). A
  composite always ran as a multi-node Ray job — and the runtime image carries no Ray entrypoint
  (deferred list). But the compose **command** is shape-agnostic: `aws s3 cp` the document and the
  runner, `python run_pbg.py <doc> -o <out> -n <steps>`. 2.3c had already run exactly that under the
  container entrypoint and got 1.1^5. So `environment="runtime"` submits **that same command as one
  container job**: no commit resolved, no ParCa cache staged, no cluster formed, no analysis chained,
  and no `PBG_CORE_BUILDER` (the workspace's core builder is a module of the workspace's image). What a
  client sees is unchanged — same runner, same results prefix, same status call — which is the point:
  it is D10's barebones case, a composite that needs only what process-bigraph ships, and it no longer
  costs a 5.74 GB pull and a Ray cluster to add 1.1 five times.
  **The refusals happen in the router, before dispatch**, because the submission itself runs in a
  background task where a refusal would reach nobody: unknown name → 422; combined with `simulator_id`,
  `num_nodes` or `analysis_options` (each of which is a simulator's) → 422; a site with no runtime
  image → 501.
  *Env workers are not done, on purpose.* An env worker serves a **workspace** — the simulator's
  repository at `/app/v2ecoli`, its composites and generators. The runtime image has no workspace, so
  an env worker there would start and have nothing to serve. "Env workers may name an environment"
  only means something once there is an environment that *is* a workspace and is not a simulator —
  a curated one (D10), which is P5's. Recorded here so it is not rediscovered as an omission.
  **Smoke:** `--task-environment` (merged hours ago in #751) became `--environment`, covering `task`,
  `task-fail` and now `compose`; `task-repo`, `worker` and Tier 2 always need a simulator's image.
  **Run for real, locally:** the published image, pulled from ghcr, ran the compose command shape with
  `PBG_REQUIRE_OUTPUT=1` and returned `level = 1.61051 = 1.1^5`. On Batch it is checkpoint D's to prove.
- **2026-09-21** — **D13: ptools is SMS's, and private.** Jim, right after making the `viva-core-runtime` package
  public: "just to be clear, ptools is a private repo due to licensing concerns and belongs to the
  sms-api not viva-core." Nothing had crossed the line, and it was **checked rather than assumed**: the
  public image's `/opt` holds one file from this repository (the entrypoint), its Dockerfile `COPY`s
  only `viva_core/runtime/requirements.txt` and that script, a search of the image finds no ptools,
  PGDB or BioCyc material, and `sms-ptools` is still private (the API says so; an anonymous manifest
  request answers 403). What changes is that the rule is now written down where the split is planned,
  because **core is headed for public and ptools can never be**: a decision (D13), and a guard — the
  runtime image's Dockerfile may `COPY` from the build context only out of `viva_core/runtime/`, so
  making that image public can never publish anything else in this repository. The lesson for the
  process, recorded with it: before *anything* is made public, list what it contains, first.
- **2026-09-21** — **P2.3d-2: Batch pulls the runtime image from ghcr — tried, not assumed.** Jim: "merge #751, I
  made the package public, try the pull." An anonymous manifest request now answers 200. **The trial
  used the code the API will use**, not a hand-written job: core's `BatchJobClient` derived
  `smsvpctest-ray-container-viva-core-runtime-0-1-0:1` from dev's base container job definition — the
  exact name `TaskService` derives for `environment="runtime"`, so the API will find and reuse that
  revision — and `submit_container` + `stage_out_env` submitted one job to `smsvpctest-ray-standalone`
  (job `58aaf734`, tagged `viva-core-runtime-trial`). **Result: SUCCEEDED, exit 0.** Its CloudWatch log
  shows core's entrypoint (`[batch-container] … starting`, the periodic and final output syncs,
  `exited rc=0`) around the workload's own line — the engine imported, Python 3.12.14, the nonce — and
  the proof file it wrote was **staged out to S3** with the job role's credentials and the image's own
  AWS CLI. So every part of the contract that the hermetic test fakes has now also run for real: the
  pull from ghcr inside the VPC, the entrypoint at the `/opt/` path the job definition calls, and
  `aws s3 sync` out.
  **Timing, honestly.** Created → started **202 s**, of which about 170 s RUNNABLE (the container
  fleet scaling up from zero) and **about 30 s STARTING** (pull + start; polled every 6 s); the workload
  itself ran 2.2 s. Against
  the 250–320 s measured for a Tier 1 task on the science image that is a modest gain, and it should
  be: on a cold fleet the wait is instance scale-up, which no image changes. What the image changes
  is the pull — 370 MB instead of 5.74 GB compressed — and that is the part that repeats on every new
  instance. A like-for-like number (same fleet state, both images) is checkpoint D's to take.
  `CORE_RUNTIME_IMAGE` is set in dev's `api.env` (not `shared.env`, which also rolls ptools); it takes
  effect with the next API roll. **Prod is another VPC**: the same one-job trial comes before prod
  names the image.
- **2026-09-21** — **P2.3d-1: a task may name an environment — and the image is pushed, but not yet pullable.**
  Jim: "merge #750, then start on 2.3d." 2.3d split in three when it met the infrastructure.
  **The API.** A request names a simulator's image by `commit`; it may now instead name a *registered*
  environment by `environment`. One name exists, `"runtime"` (the core runtime image). A **name**,
  not a spec object: D10's curated environments (COPASI, Tellurium) are further names, which is how a
  client asks for them; a derived spec is something the *server* computes from a composite, not a
  field a client fills in. Mutually exclusive with `commit` (422). An uploaded task in an environment
  resolves **no simulator at all** — not even "the latest commit" — and its job definition is derived
  for that image under a key that is not a commit (`<base>-viva-core-runtime-0-1-0`). Three refusals,
  each saying whose fault it is: an unknown name or both ways of naming an image → **422**; a repo-path
  script in an environment → **400** ("upload the script": the path is inside a simulator's image); a
  site with no `CORE_RUNTIME_IMAGE` → **501**, never the task run somewhere else. The CLI refuses the
  first two before it calls the server. `atlantis smoke run --task-environment runtime` runs `task`
  and `task-fail` there; `task-repo` always needs a science image. The `task` table still records no
  image at all, for a commit or for an environment — that is #656's (task provenance), not this.
  **The image.** `build-core-runtime.yml` ran from `main` (gate passed in CI; amd64 + arm64):
  `ghcr.io/vivarium-collective/viva-core-runtime:0.1.0`,
  `sha256:356405729d9b09ce3aa9a4e18b2f43a4dee207760d2bc45cf5a87434b80c9ace` — the first environment
  whose digest is written down the day it was built (D11's deferred "record image digests").
  **Where it stopped.** A new ghcr package is private: an anonymous manifest request answers 403
  where `sms-api`'s answers 200. Batch has no pull secret and GitHub has no API for an organisation
  package's visibility. Making it public, or standing up an ECR mirror, is not something to do on the
  way past — it is on the deferred list with both options, and dev's `CORE_RUNTIME_IMAGE` stays unset
  (so `environment="runtime"` answers 501 there) until one is chosen and a trial pull has worked.
- **2026-09-21** — **P2.3c: the core runtime image — and the entrypoint turned out not to be ours.** Jim:
  "merge #749, then do 2.3c, the core runtime image." The plan said the image carries "the Batch
  container entrypoint". Looking for it found the real state: **the stage-in / run / stage-out
  script is not in this repository at all.** It lives in the science repositories (`docker/`), the
  job definitions in sms-cdk call it by absolute path (`/opt/batch-container-entrypoint.sh`), and the
  two copies have **diverged** — v2ecoli's says "nothing in this script is workload-aware" and is;
  sms-ecoli's, three weeks newer, imports `v2ecoli.library.cache_version` inside the stage-in step.
  That is what happens to a generic contract kept in an application's repository, and it is the
  strongest argument yet for core owning it. So 2.3c is four things, not one:
  **(1) Core's own entrypoint**, `viva_core/runtime/batch-container-entrypoint.sh`: v2ecoli's copy at
  `fb4e091ce` (the last application-free one), its examples de-domained, plus the one generic check
  the newer copy had grown — a stage prefix that syncs *nothing* fails the job (`aws s3 sync` exits 0
  on an empty prefix). **(2) The contract's first test** (`tests/core/test_runtime_entrypoint.py`): the
  script run for real under `bash` against a fake `aws` that maps `s3://` to a directory — exit code
  propagated, inputs staged before the command, an empty stage refused before the command runs,
  outputs uploaded whether the command succeeds or fails, **a failed upload fails a job whose command
  succeeded** (mutation-checked), the report uploaded, and every variable `stage_out_env` writes is one
  the script reads. **(3) The image**, `Dockerfile-core-runtime`: two stages (git only in the first),
  Python 3.12 slim, the engine pinned to **the exact commits the deployed science image runs**
  (process-bigraph `55b70676`, the first with `process_bigraph.events`; bigraph-schema `8268aa14`),
  `viva-emitters[parquet]`, the AWS CLI. **370 MB** uncompressed against the science image's 5.74 GB
  compressed. **(4) Registration**: `CORE_RUNTIME_IMAGE` (a `CoreSettings` field, empty by default) is
  what `site_resolver` hands the resolver as `runtime_image`, so an empty `DerivedSpec` resolves to it
  where a site names one and is refused where it does not.
  **Proved by running it, which found a bug a static check would not:** built locally, the image ran
  the smoke composite through `viva_api/compose/run_pbg.py` under the entrypoint and returned
  `level = 1.61051 = 1.1^5`, with `events.jsonl` beside it. The first attempt failed — process-bigraph
  imports `requests` without declaring it, and `import process_bigraph` alone never reaches that
  module. It is pinned now, the Dockerfile's build-time check imports the module that needs it, and
  the upstream fix is on the deferred list.
  **What this is not.** Container-shape jobs only: the multi-node Ray entrypoint has model-specific
  steps inside it and needs its own de-domaining, so `compose` on the MNP shape cannot run here yet
  and 2.3d starts with tasks. **Nothing is pushed or deployed:** `build-core-runtime.yml` is
  dispatch-only, gated on the contract test, multi-arch, and **refuses an existing tag** (an
  environment is write-once, D11); running it, and where Batch pulls from (ghcr or an ECR mirror), are
  2.3d's — both on the deferred list with the science repositories adopting core's entrypoint.
- **2026-09-21** — **P2.3b: four derivations, one place — and a behaviour change taken out of it.** Jim:
  "merge #748, then start on 2.3b." `viva_api/common/site_environments.py` turns the site's three
  settings into core's `RegistryEnvironmentResolver`; the Batch layer (`image_uri`,
  `submit_image_uri`), compose (its site-pinned tag), the env-worker service and the K8s analysis Job
  ask it. **The settings are handed in, not read:** the dispatch package reads them through
  `_seams.get_settings`, compose and the env worker through `config.get_settings`, and hundreds of test
  patches sit on those names — a function that read settings itself would see none of them.
  **The decision that was in this PR is not in it any more.** Refusing an unset ECR account at once
  looked like a free improvement; the first run of the suite said otherwise: **8 tests fail**, because
  the default settings leave the account unset and those tests have been building
  `.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:<key>` all along without looking at it. That makes it
  a behaviour change with a blast radius, and this PR a rewiring that is proven by *saying what the
  four said*. So the malformed host is preserved on purpose, in one named function with the reason
  on it, pinned by a test whose name says it is temporary, and the refusal is on the deferred list as
  its own PR. (The env-worker service refused already, and still does.)
  **Proof.** A differential of all six call paths (two in the Batch layer, two in compose, the env
  worker, the K8s Job's container image) against `origin/main`'s source executed under another name,
  over a grid of account (set, unset) x region x repository x pinned tag (set, unset) x key (commit,
  temporary tag, 40-char sha): **432 cases — 240 return, 192 raise, the same value or the same
  exception and message as `main` in all but 96.** Those 96 are one input, put in the grid as a
  probe: an **empty repository setting**, for which `main` silently produced `<host>/:<key>` and core's
  resolver refuses. No configuration and no test sets one; it is the single declared difference,
  pinned by a test. **Mutation-checked twice:** dropping the `submit` variant gives 48 further
  differences, a wrong repository 192. A static guard counts hand-spelled registry hosts in
  `viva_api/`: two remain, both in `SimulationServiceK8s`'s upstream vEcoli path (another repository,
  `-amd64-submit`; out of scope until P5), and the number may only shrink.
- **2026-09-21** — **P2.3a: what an environment is, as code — and nothing calls it yet.** Jim: "merge #747,
  then start on P2.3." `viva_core/environments/`: the vocabulary of D10 and the one resolver there can
  be before a table exists. **Three choices worth recording.**
  **(1) The registry key is not part of the request.** Every one of the four derivations takes one
  string — called `commit` everywhere, and since D11 not always a commit: a marked-temporary
  simulator is tagged `tmp-<commit>-<nonce>`. So `ExplicitSpec` carries a `key` (what names the image)
  *and* a `commit` (what was asked for), and `spec_hash` covers the second, not the first: a temporary
  build of a commit and the authoritative one have the **same spec hash and different images**. That
  is D11's "a rebuild is a new environment" and the architecture's "two identities", as a test. A caller that holds only a key (an env worker is handed a tag) may leave the rest empty:
  resolvable, not rebuildable; its key stands in for the commit in the hash, so two such requests differ.
  **(2) An empty derived spec is a real request**, not a missing one: a composite that needs nothing
  beyond the built-ins. It resolves to the site's `runtime_image` when there is one (2.3c). A derived
  spec that *does* name dependencies is **refused** (`EnvironmentNotResolvable`, naming them) — nothing
  is registered to select from and this resolver cannot build. Never something close: running under
  other dependencies than were asked for is the failure the model exists to prevent.
  **(3) Only what can be known is declared.** `Environment` has `spec`, `image`, `image_digest`
  (`None`: a resolver that computes where an image lives does not know what was built). Status, the
  build job and `provides` wait for P5's table; declaring them now would be fields nothing can fill.
  Frozen dataclasses, not pydantic models: values, hashable, and `Any`-free without the pydantic
  class-line exemption (D12). The resolver is handed `registry` and `repository` — it is not ECR-shaped;
  `ecr_registry()` is a helper beside it. **Select, here, means "say where the image for this key
  lives"**, exactly as the four derivations did: whether it is there is still found out by the pull.
  Proof that it can replace them: `test_it_says_what_each_hand_rolled_derivation_said` compares the
  resolver with core's `ecr_image_uri`, with the f-string three sites use, and with the `-submit`
  suffix, for a commit, a second commit and a temporary tag. Core-only tests; mypy clean under the
  ban; the standalone and vocabulary guards pass. The sequence 2.3a–d is in P2.3's section.
- **2026-09-21** — **Smoke: a probe that decides what will be skipped says so when it decides.** Jim:
  "merge #746, then fix the smoke startup probe." Checkpoint C's defect, fixed in four parts. (1) The
  AWS probes (Batch, and the registry for `--build`) are **tried twice** — one failed call decided the
  shape of an hour. (2) What a failed probe changes about *this* run is **printed before the run**:
  which selected checks will SKIP, and that `sim-chain` will download instead of list. It existed only
  as each check's SKIP reason. (3) **`--require-aws`**: with such a notice, stop with exit 2 before
  anything is submitted — at a deploy checkpoint a SKIP is not a verdict. (4) **A Tier 2 verdict is
  reported the moment it lands**; the returned list and the JSON report stay in check order. Tier 2
  reported in check order (`pool.map`), so at C every verdict after `sim-default` sat behind the chain's
  download. Tested: a probe that fails once succeeds on the retry; a probe that keeps failing gives its
  reason; notices name only the selected checks; the CLI prints the notice before any verdict;
  `--require-aws` submits nothing, not even a check that needs no AWS; and the ordering test deadlocks
  by construction unless the fast SKIP is reported while the slow check is still running
  (mutation-checked: with submission-order reporting it fails). Tried live against dev: a nonexistent
  profile gives both notices and stops; real access says nothing.
  **The cause at C, read when the original run finally printed its SKIPs** (17 passed, 0 failed, 6
  skipped; its own verdicts for the five simulations match the ones read by hand):
  `TokenRetrievalError … Token has expired and refresh failed` — the operator's SSO session had lapsed
  at the moment the run started and was refreshed minutes later by another command. So the retry is
  the lesser part of this fix: three seconds do not renew a session. The notice and `--require-aws`
  are the cure — the run would have stopped in its first second, naming the token.
- **2026-09-21** — **Checkpoint C passed on dev (0.9.151, #743, tag `v0.9.151`): the carve is deployed.** Jim:
  "merge #741, then bump and deploy C". Image from `c5a1600d`; `kubectl diff` of the app overlay was one
  line (the api image); only the api pod rolled; no migration (the `-db-migration` tag was bumped to stay
  equal, the Job was not run). Markers on the newest pod: the service file is 628 lines,
  `_submit_chain_dispatch_background` is gone and `def _chain(self)` is there, the five strategy modules
  import, the dev-only stub packages do not.
  Tier 0 7/7 (100 of 100 routes; `chain-dispatch`, `chain-progress`, `container-jobs` advertised). Tier 1:
  `task`, `task-fail`, `task-repo`, `worker`, `compose` (1.1^5). **Tier 2, on simulator 213 — every
  mechanism, now a strategy object, ran a real simulation:** `sim-default` (ensemble; 82 files, 1 seed
  summary), `sim-chain` (a real 2 x 2 campaign: both lineage jobs SUCCEEDED on Batch, 52 objects and 2
  per-seed `summary.json` in S3), `sim-composite` (2 nodes; 14 objects, 378 MB), `sim-mbp` (its output
  survived the container: 5 objects), `sim-nextflow` (2 traced tasks, all completed); and the three
  cancels, each asserted on AWS Batch itself — `sim-cancel` (2 active jobs before, none after),
  `chain-cancel` (cancelled in its ParCa phase, the #709 case), `nextflow-cancel` (the scheduler's
  reaper stepped in). **#730, after, again:** the composite job's command carries `RAY_SHARDS_DEFAULT=32`
  and the API log has no vCPU warning.
  **How the verdicts were read, because the run did not go as designed.** The smoke client probes AWS
  Batch once, when it starts; that one call failed (the same call, with the same credentials, worked
  for the rest of the hour). The design then does the right thing twice and says so far too late: the
  three cancel checks SKIP before submitting anything, and `sim-chain` falls back to downloading 3.6 GB
  through the tunnel — but both are reported where those checks' lines print, behind the chain, an hour
  in. Found at minute 55 by noticing that no cancel simulation existed. Not waited for: the cancels
  were run on their own (`--only`, 3 passed in 5 minutes), and the chain, composite, mbp and Nextflow
  verdicts were read with the checks' own assertions (the same S3 lister, the same trace call) against
  the runs the main process had submitted. **So the checkpoint's evidence is complete, and the tool has a
  defect** (deferred list): a probe that decides what will be skipped must say so when it decides.
  Two smaller things seen in passing, both on the deferred list: the S3 lister finds nothing for a
  multi-node run (it reads a container's environment; an MNP job keeps it under `nodeProperties`), and
  every simulation row shows the same `last_updated` — the API pod's boot time.
  **Not idle:** PR 12 (#744) and the `dispatch/` rename (#745) were written, proven and merged while
  this ran, and the repository's merged branches and stale worktrees were cleaned up (Jim's ask).
  `scripts/prove_ray_carve_is_move_only.py` is deleted here, as PR 11's row said: the carve it proved is
  deployed, and `git show v0.9.151:scripts/prove_ray_carve_is_move_only.py` brings it back.
- **2026-09-21** — **The package is `simulation/dispatch/`, not `simulation/ray/`.** Jim, at C3: Ray is an
  orchestration framework that runs *inside* some jobs; Nextflow jobs use none of it; "maybe the parent
  concept should have a different name … just note this and carry on." Noted then, done now that the
  strategies have landed and no proof compares against `main` by path any more. Jim chose the name
  (`dispatch/` — what the package holds: the five mechanisms and what they share; over `batch_dispatch/`,
  which is only mostly true, and `mechanisms/`, which misnames the shared half) and the scope (**package +
  the four helper classes**: `BatchLayer`, `ParcaService`, `TaskService`, `ImageBuilder`).
  **Deliberately not renamed:** `SimulationServiceRay` and its file, `ComposeSimulationServiceRay`, the
  `test_ray_*` files — they are what `ComputeBackend.RAY` selects, and renaming them first would leave a
  `SimulationServiceBatch` chosen by an enum called `RAY`. They move with the one decision still open:
  the **persisted and deployed names** (`ComputeBackend.RAY`, `JobBackend.RAY`, `JobId.ray`, `RayLayout`,
  the `ray_*` settings, the queue names), which is a data-and-config migration, not a rename.
  "Ray" also stays wherever it is literally true (`RAY_SHARDS_DEFAULT`, the MNP entrypoint).
  **Proof that it is rename-only:** apply the name map (nine regular expressions, in the PR) to every
  Python file of `origin/main` and require the same AST as the branch, imports compared as a set (isort
  re-sorts `dispatch` where `ray` stood) — **661 files identical, 2 differ, both on purpose**: the
  package docstring, rewritten to say what the package is, and a constant and a test name in
  `tests/core/test_no_any_in_core.py`. Import-linter's broken report-only edges: 8 before, 8 after.
  Suite 2119 passed, unchanged. **One runtime-visible effect:** logger names follow module names, so log
  lines from these modules read `viva_api.simulation.dispatch.<module>`; nothing in the repository or the
  kustomize config filters on the old name (checked).
- **2026-09-21** — **PR 12: the ray package is `Any`-free, and the ban is a glob.** Jim: "start on PR 12
  while the smoke runs." The override now reads `viva_core.*` and `viva_api.simulation.ray.*`; the by-name
  list and `NOT_YET_BANNED` are gone, so a new module of the package is under the ban the day it appears
  and nobody has to remember to list it. Twenty-six sites, four kinds. **(1) The three dispatch blocks**
  (`mbp_dispatch`, `multi_node_dispatch`, `nextflow_dispatch`) were `dict[str, Any]` whose values flowed
  into typed parameters — the `Any` was hiding that nothing checks them. They are `TypedDict`s now, in the
  module of the mechanism that reads them. **That declares a contract; it does not enforce one:** the
  blocks arrive as JSON through the config's passthrough fields, and only `task_env` (all three) and
  `composite_id` / `resume_from` (Nextflow) are validated at the API boundary. Said in each docstring,
  and on the deferred list — enforcing it is a behaviour change (a request that works today could start
  being refused) and does not belong in a type PR. **(2) JSON that is only read** (`injected_processes`,
  `variants`, `config_overrides`, `exchange_fluxes`) is `Mapping[str, object]`, not `dict[str, object]`:
  `dict` is invariant, so a caller holding a `dict[str, list[str]]` could not pass it — the tests found
  that in nine places the moment the `Any` went. `resolve_task_env` and `_batch_domain_overrides` take
  a `Mapping` for the same reason. **(3) Two untyped clients:** chain's `batch_client` is a
  `BatchClient | None`, the Nextflow head Job is a `V1Job`, both under `TYPE_CHECKING` (stub packages
  are dev-only; `tests/core/test_typed_boto3.py`). **(4) The generator's own `params`** stay
  `dict[str, object]` — they are passed through unread but for two counts, read through `cast`s that say
  only "`int()` decides, as it always did".
  **Proof that it is annotation-only.** The strategy PRs were proven by AST identity, which an annotation
  change breaks by definition, so this PR's proof is the complement: erase every annotation (parameter,
  return, `x: T = v` → `x = v`, `typing` imports, `TYPE_CHECKING` blocks, `TypedDict` classes) from
  `origin/main` and from the branch, and compare what is left, per function, over the seven files
  touched (164 units). **Three differ, all inert:** two `cast`s around values handed to `int()` /
  `float()` / a function (identity at runtime), the tuple of three retry keys lifted into a
  `Literal`-typed constant (a `TypedDict` cannot be indexed by a plain `str`), and `str()` around a value
  built as an f-string two statements earlier. The ban is mutation-checked: an `Any` added to
  `ray/runner_env.py` — a module never listed by name — fails mypy. Suite 2119 passed, unchanged;
  `make check` clean.
- **2026-09-21** — **PR 11: `ChainStrategy` — the last mechanism. The class is its interface, five
  builders, two composed services, and a named list.** 5,019 lines → 628. Chain was twelve
  methods and 1,002 lines: four never touched `self` (the two seed commands, the analysis command,
  `chain_base_tags`) → functions; eight became the strategy's, verbatim but for respellings.
  `ChainStrategy(batch, local)`: `local` — the in-process task service that runs the long
  submission loop in the background — is a constructor argument of this strategy and of no
  Protocol, as `k8s` is of Nextflow's. `submit` (was `_submit_chain_dispatch_background`) is what
  the router calls. Progress and cancel (`get_chain_campaign_result`, `cancel_chain_campaign`,
  `cancel_companion_jobs`) stay on the service until P6, as the audit decided.
  **The plan row promised "delete the facade shims", and that was wrong.** Nine one-call delegates
  remain, because the scheduler (7 methods), a handler, the capability probe
  (`hasattr(service, "submit_chain_dispatch_job")`) and the integration tests ask the *service* —
  and the same audit put the scheduler off until P6. Rather than pretend, they are **named, with
  the reason each exists, and pinned by a test**: the service's public surface beyond its interface
  is `{parca, tasks}` + nine delegates + four pieces of progress / cancel / staging, and may only
  shrink. The delegates carry the strategy's full signatures (generated from the AST, not typed by
  hand), so the scheduler's calls stay type-checked — a `**kwargs` delegate would have cut mypy out
  of exactly the calls P6 has to change. A second test says no mechanism code is left on the class.
  **Proof.** Script: clean, and again mutation-tested — a changed environment inside a seed command
  and `git_commit_hash` for `environment_key` inside the campaign submit are reported; a
  *layout-only* change is not, which is the AST comparison doing what it is for. It learned one
  more shape: a method that became a function *and* stays as a delegate (`chain_base_tags`).
  Differential: 405 cases, 0 differences, 219 to completion — including the routed case with the
  **background submission run to the end** (the harness drains the event loop). Two false
  differences first, both from outside the chain and both only visible because the first differing
  *call* is printed: a wall-clock `end_time` the local task service stamps (masked), as `uuid4`
  was in PR 10. Wiring mutations: the router's hand-over dropping the correlation id → 44; the
  dispatch delegate dropping it → 44; the strategy built around its **own** `LocalTaskService` —
  its background work would be invisible to the service's status and cancel — → the unit test.
  **Tests that patch by module path, again:** the pacer tests patched
  `simulation_service_ray.asyncio.sleep`; a gate test wrapped `SimulationServiceRay
  .submit_chain_dispatch_job` while the code under test now calls the strategy's. Repointed.
  **The test split owed since PR 10:** six chain classes and two helpers (1,284 lines) →
  `test_ray_chain.py`; the eleven `sim_command` tests that sat at the end of the image-*build* test
  class → `TestSimCommand` in `test_ray_ensemble.py`. 187 test functions before, 187 after.
  `test_ray_backend.py`: 5,879 lines at the start of the strategies → 2,554.

- **2026-09-21** — **Is the ensemble an SMS concept or core?** (Jim, during PR 11.) As built, SMS: a ParCa
  prerequisite, the v2ecoli-vs-vEcoli engine choice, `parca_options`, the two-engine comparison —
  none of it can live in core. As a *shape*, core: N replicate runs of one composite, fanned out
  over a Ray cluster in one multi-node job, optionally gated on a prerequisite job. **Three things
  in the tree already reduce to that one shape** — stage the runner, ensure an MNP job definition
  for an image, submit `run_pbg.py <composite> --overrides …`, maybe `dependsOn` a prior job: the
  ensemble, the multi-node composite (near-duplicates; they differ in which composite and how the
  parameters are built, not in mechanism — the ensemble simply predates the generic one), and
  compose on Ray (the same without ParCa). An observation for P3 / P5, **not a decision**: core
  wants one primitive, "a composite on a multi-node job" (environment, composite id, params, node
  count, depends-on), with two thin SMS front-ends deciding *what* to ask of it and compose as the
  third client. Not merged during the carve on purpose: that would be a behaviour change, and a
  carve that also changes behaviour cannot be proven.

- **2026-09-21** — **PR 10: `EnsembleStrategy` — the mechanism that had no name.** It was not a method. It
  was the last 36 statements (217 lines) of the router, reached by falling through every other
  mechanism's check. Measured: from the code above it, it read three locals (`config`,
  `composite`, `n_generations`) and the three parameters, and of `self` it used the Batch layer
  (an `MnpSubmitter` and nothing else), `cache_s3_uri`, `stage_runner`, and `_sim_command` —
  which never touched `self`, and is the function `sim_command`. `EnsembleStrategy.submit` is
  those 36 statements verbatim but for respellings, after a three-statement preamble that derives
  the three locals *by the router's own statements*. The router is its old head plus one
  hand-over `return`. `V2ECOLI_BATCH_BASELINE_COMPOSITE_ID` (with its 40 lines of history)
  joined the leaf `ray/runner_env.py`: chain's commands and `sim_command` both need it.
  **The proof script grew a fourth kind of change, `EXTRACTED_TAILS`,** checked three ways: the
  old tail is the new method's body after docstring and preamble (AST + comment lines); every
  preamble statement is a statement of the old head; what stayed is the old head plus exactly the
  named hand-over. It passed on its first run, so it was mutation-tested before being believed —
  a changed preamble, a changed statement in the tail (`git_commit_hash` for `environment_key`:
  D11), a hand-over that drops the correlation id, a changed chain condition in the head — four
  out of four caught, each with its own message.
  Differential against `origin/main`: 375 cases, 0 differences, 141 to completion, through the
  router and strategy-direct-vs-old-router. Its first run said 24 differences, all in the one
  case that is *not* the ensemble (the chain's background submit returns a local task id made
  from `uuid4`); pinned, 0. Wiring mutations: the hand-over dropping the correlation id → 52;
  a runner stager that stages elsewhere → 9; its own layer → the unit test.
  **A test that went vacuous, and the pattern behind it:** three tests spy on
  `service.cache_s3_uri`. Two failed as they should. The third asserts the spy was **not**
  called (upstream vEcoli must ignore `cache_variant`) — and passed, because after the move
  nothing calls the *service's* method at all. A negative assertion on a spy is only as good as
  the spy's aim. Repointed at `parca_spec.cache_s3_uri` and mutation-checked: make the upstream
  path call it → the test fails again.
  **Tests did not move this time, and that is a debt, not a choice:** the ensemble's tests share
  a 473-line class with chain's routing tests, and `sim_command`'s tests sit inside a class about
  image builds. Splitting classes method by method is a different kind of edit from moving a
  block; it is done with PR 11, when chain leaves and what remains of that class is the router.

- **2026-09-21** — **Checkpoint C3 passed on dev (0.9.150, #736, tag `v0.9.150`).** Jim: "merge #735,
  then bump and deploy C3". Image from `3ecb7b25`; `kubectl diff` of the app overlay was one line
  (the api image); only the api pod rolled. No migration (the `-db-migration` tag was bumped to stay
  equal; the Job was not run). Markers on the newest pod: `ray/nextflow.py` and `ray/mbp_tracked.py`
  present, the fixed `jobDefinitions=[job_definition]` call present and the old one gone, the
  capability probe reading `service.batch`. **The dev-only stubs are not in the image** —
  `types_boto3_batch` and the Kubernetes stubs unimportable, the typed modules importing without
  them — which is the live form of 6a's guard.
  Tier 0: 7/7, with **`container-jobs` still advertised** (the `hasattr` probe PR 5 would have
  silenced) and all 100 routes served. Tier 1: `task`, `task-fail`, `task-repo`, `worker`, and
  `compose` — now through a Batch layer it is handed rather than a simulation service it builds.
  Tier 2, on the newest authoritative simulator (213): `sim-default`, `sim-composite`,
  `sim-nextflow`, `sim-mbp` completed; `sim-cancel`, `chain-cancel`, `nextflow-cancel` cancelled;
  `sim-chain` 2/2 seeds (`chain-progress` terminal, 52 objects and 2 per-seed summaries listed in S3).
  Verdicts read from the API and AWS per run; the smoke client's own tally line was not waited for —
  it was still downloading the chain's output with the pre-#737 code, which is the ceremony #737
  removes. **The two mechanisms that are now strategy objects ran on a deployment.**
  **#730, after:** `RAY_SHARDS_DEFAULT=32` in the composite job's command, no vCPU warning in the
  log, the run completed (deferred list has the before/after).
  **On the hour:** Jim asked whether an hour of smoke per deploy had to be dead time. Measured: the
  chain finished server-side 44 minutes into Tier 2; the *client* then spent the rest downloading
  its 3.6 GB through the tunnel. Three changes: PR 9 was written, proven and opened while Tier 2
  ran (`main` is not dev — only deploying N+1 waits for N's verdict); `sim-chain` now lists the
  run's output in S3 instead of downloading it (#737: 53 objects and 2 summaries in 5 s, the same
  answer the 30-minute download gave at C2), and was used to read this run's chain verdict the
  moment the server finished (52 objects, 2 summaries, 5.7 s); and only deploys wait on smoke at
  all — the carve has one left.
  One traceback in the pod's log is mine: a status probe of a simulation id that does not exist,
  which the API answers with a 500 (deferred list).
  **A mistake of mine, no loss:** to run that lister from its branch I used `git stash` +
  `checkout` + `stash pop` in a worktree that was clean. `stash` saved nothing, so `pop` applied
  the top of the repository's **shared** stash list — another session's work, 16 files. It
  conflicted, which is the only reason the entry was kept rather than dropped. Verified the stash
  intact and the main checkout's untracked originals present *before* resetting my own worktree;
  54 entries before and after. The rule is now: never `git stash` here — a throwaway worktree, or
  `git show branch:path`.
- **2026-09-21** — **PR 9: `MultiNodeCompositeStrategy` — the third strategy, written while C3's Tier 2
  ran** (Jim asked whether an hour per deploy had to be dead time; it does not — `main` is not dev,
  and only *deploying* N+1 has to wait for N's verdict). Six methods, 615 lines: three never
  touched `self` (the composite command, the analysis command, the founder-cache staging) →
  functions; three are the strategy's (`submit`, `submit_analysis`, `_mnp_node_vcpus`, with the two
  retry constants and the comment that tells #730's history). `batch` is a `MultiNodeBatch` = an
  `MnpSubmitter` for the run and its ParCa + a `ContainerSubmitter` for the analysis + the client
  for one read of a job definition. `submit_analysis` travels with the mechanism, as the audit
  decided for every mechanism's analysis submitter; the scheduler keeps reaching it through a
  delegate until P6. `PBG_RUNNER_ENV` and its long comment moved to a leaf, `ray/runner_env.py`:
  three mechanisms build commands from it and a strategy cannot import the service.
  Proof script: clean on its first run this time (`moved, but not as named: []`). Differential:
  316 cases, 0 differences, 176 to completion — and again a harness artefact first
  (`seed_overrides` is a mapping, not a list), visible only because the distinct raise messages
  are printed; with the real shape the S3 founder-cache staging runs (20 `copy_object`s
  compared). Every outward call tallied: job definitions, submits, the vCPU read and its retry
  sleeps, the staging copies, the analysis record. Wiring mutations: the router dropping the
  correlation id → 36; the analysis delegate truncating the environment key → 12; a runner
  stager that stages elsewhere → 180; its own layer → the unit test (and the no-real-AWS guard).
  A new test pins #730's fix at unit level from the strategy's side: 16 vCPUs × 2 nodes →
  `RAY_SHARDS_DEFAULT=32` in the command.
  **What moving code does to tests that patch by module path:** three tests patched
  `simulation_service_ray.time.…`. The linter removed `import time` from the service once its
  last user left, and the patch target stopped existing — for two *pacer* tests that were never
  about the service at all (the pacer lives in `viva_core/backends/batch.py`; they had been
  patching the global `time` module through whichever module happened to import it). Repointed
  at the module that owns the clock in each case.
  Tests moved with the code: 1,388 lines → `tests/simulation/test_ray_multi_node.py`;
  `test_ray_backend.py` is 5,465 → 4,076.

- **2026-09-20** — **PR 8: `NextflowStrategy` — the second strategy, and the first that is handed
  more than a submitter.** Measured: ten methods and a block of module-level constants. Five
  methods never touched `self` (two were `@staticmethod`s) → functions; five became the
  strategy's methods, verbatim but for respellings. The mechanism needs three things:
  `batch` — a `NextflowBatch` (**two image names and the engine; it submits nothing through
  the layer**, so it is handed no way to: the head is a K8s Job and the tasks are Nextflow's
  own); `k8s` — a constructor argument of this strategy and of no Protocol, as the audit
  said; and `stage_runner` — a callable, because staging the generic runner is shared with two
  other mechanisms and the scheduler and so stays the service's, late-bound so a test that
  swaps `service.stage_runner` is still what the strategy gets. `reap_cancelled_campaign`
  travels with the mechanism (it undoes what `submit` started); the scheduler reaches it
  through a one-line delegate until P6. `V2ECOLI_CORE_BUILDER` moved to the leaf constants
  module: the service's runner env and the head Job both need it, and a strategy cannot
  import the service.
  **The proof script earned its keep.** First run: `_nf_head_job … differs by more than the
  named respellings`. The old method imported the Kubernetes client *inside the function*; my
  new module imported it at the top, so the linter deleted the local import as a redefinition
  — silently, and harmlessly as it happens (kubernetes is already imported by the service),
  but "verbatim" is a claim, not a vibe. Restored. The script also learned `@staticmethod`
  (no `self` to put back) and delegates (`REWIRED` + `REWIRED_BODIES`: the delegate may differ,
  the body it left behind must be found unchanged where the table says).
  **The differential was vacuous twice before it was real** — and said so both times, which
  is the whole reason for counting completions: first every `submit` raised `AttributeError`
  (my fake simulation lacked `experiment_id`), then `TypeError` (my fake file service had the
  wrong `upload_file` signature). 0 differences each time, and 0 submits completed. Fixed:
  172 cases, 0 differences, **48 submits run to completion** (the whole `V1Job` serialised and
  compared, the staged file's content compared), 32 reaps, 92 raising identically for four
  legitimate reasons. Wiring mutations: the router dropping the correlation id → 8; the
  strategy not handed the K8s backend → 64; the delegate passing a different head name → 16;
  its own layer → caught by a unit test (and by the no-real-AWS guard, which fired when the
  orphan layer reached for boto3).
  The Nextflow tests were already their own file; 27 references respelled by script, two
  router-precedence tests repointed at `NextflowStrategy.submit`. Module-level constants and
  helpers checked byte-identical separately, since the method proof does not cover them.

- **2026-09-20** — **PR 7: the first dispatch strategy, `MbpTrackedStrategy` — and the shape the
  other four will take.** Measured first: the mechanism is two methods. `_mbp_tracked_command`
  never touched `self` → the function `mbp_tracked_command`. `_submit_mbp_tracked_dispatch`
  used four things of the service: the Batch layer, `cache_s3_uri` (already a function),
  its own command, and `_record_run_with_companions` — which also never touched `self`, and is
  shared with the ensemble path, so it is a function in `ray/run_records.py` rather than
  something a strategy would have to be handed the service to reach.
  **The shape:** `MbpTrackedStrategy(batch: ContainerSubmitter)` — handed what it submits
  through and nothing else; `submit(...)` is the old method's body, verbatim but for four
  respellings. A test runs it on a 15-line object that is *only* a `ContainerSubmitter`: if it
  reached for the service's database, its other backends or a sibling mechanism, that would
  not be enough. The service builds it per call (`_mbp_tracked()`), the router calls `submit`.
  **Deliberately not yet:** a `DispatchStrategy` Protocol. The five mechanisms take five
  different things (a dispatch block, a Nextflow block, nothing), and an interface is better
  read off five real strategies than guessed from one; adding `applies()` later is additive.
  Progress and cancel routing stay where they are until P6, as the audit decided.
  **Proof.** `prove_ray_carve_is_move_only.py` is now machinery plus per-cut tables
  (`BECAME_FUNCTIONS`, `BECAME_STRATEGY_METHODS` with their respellings, `RESPELLED_IN_SERVICE`,
  `NEW`), so PRs 8–11 edit tables, not code: `moved, but not as named: []`, `differing / lost /
  added: []`. Three mutations *inside the moved code* each caught (a dropped `depends_on`, an
  emptied companion list, a different script in the command). Differential against
  `origin/main`, directly and **through the router**: 252 cases, 0 differences — 90 run to
  completion (842 outward calls recorded), 162 raise identically (no simulator, no variant, an
  unset queue, a variant cache that is not staged). Wiring mutations: the router dropping the
  correlation id → 30 differences; the strategy ignoring the cache variant → 20; the strategy
  built around its own `RayBatchLayer()` → 0, as predicted, and caught by the shared-layer unit
  test, now extended to the strategy.
  **Guards that had to learn about strategies:** the events-identity guard pinned dispatch
  methods by bare name, and every strategy's entry point is called `submit` — five bare
  `submit`s would pin nothing, so a method in the package is pinned as `<module>.<name>`
  (`mbp_tracked.submit`). D12's guard did its job on its first day: the new module carries the
  `dict[str, Any]` it had in the service, so it is named in `NOT_YET_BANNED` with the reason;
  `run_records.py` has no `Any` and went straight under the ban.
  **Tests moved with the code:** the two mbp test classes (413 lines, unchanged but for six service constructions the command
  tests no longer need) are
  `tests/simulation/test_ray_mbp_tracked.py`; `test_ray_backend.py` is 5,879 → 5,467 lines.

- **2026-09-20** — **PR 6b: D12 is on. `viva_core.*` and the ray package's modules are type-checked
  with `disallow_any_explicit` + `disallow_any_unimported`.** 74 sites: 55 real, 19 not ours.
  **The 19, and a limit worth knowing:** under `disallow_any_explicit` mypy reports *every*
  subclass of pydantic's `BaseModel` / `BaseSettings` on its `class` line — pydantic's own
  `__init__(self, **data: Any)` is part of the class. Reproduced in three lines; neither the
  `pydantic.mypy` plugin (which also adds 31 errors repo-wide) nor its `init_typed` option
  removes it. So those `class` lines carry `# type: ignore[explicit-any]`, and **a guard test
  makes that the only thing the ignore may be used for** (it resolves model classes by AST,
  including subclasses of a model; mutation: the ignore on a plain function → fails).
  `warn_unused_ignores` retires each one by itself when upstream fixes it.
  **The 55, by rule rather than by taste:** a value passed through unread is `object` (what
  pydantic does with `object` is what it does with `Any`: same JSON schema, same validation,
  same dump — checked, and the generated OpenAPI spec is unchanged); a payload that is *read*
  is a `TypedDict` of the fields read (GCS objects; the Batch request shapes from the stubs,
  so `submit_job(**kwargs)` is now checked against the API); a third-party return typed
  `dict[str, Any]` is `cast` at the boundary, which says what it is and changes nothing at
  runtime; duck-typed settings read with `getattr` are `object`; and three `**kwargs: Any`
  that **no caller had ever passed anything to** were deleted rather than retyped
  (`MessagingService.connect` ×2, `SSHSession.scp_upload`). `with_events_env(**kwargs: Any)`
  now spells out `events_env`'s keywords, so a misspelled one is a type error.
  **Kubernetes is typed too:** `kubernetes-stubs-elephant-fork` 36.0.3 (the client is 35.0.0)
  as a dev dependency, `kubernetes.*` out of `ignore_missing_imports` — all 9 unimported `Any`
  in core gone. `client.rest.ApiException` is respelled `client.ApiException`: the stubs do
  not export the former; at runtime they are the same class (asserted before changing 13
  sites). Two findings, neither a bug: `pod.metadata` may be `None` by the stubs (guarded:
  returns `None` instead of an `AttributeError` nobody would catch), and
  `Configuration.set_default` exists but is missing from the stubs (one `attr-defined` ignore).
  The ray package is listed **by name**; a second guard fails when a module appears there that
  is neither listed nor named in `NOT_YET_BANNED` — so each strategy PR has to say, in a test,
  that its new module is not under the ban yet, and PR 12 has a list to empty.
  Behaviour: the GCS listing converts `size` with an explicit `int(...)` where pydantic coerced
  the decimal string silently; everything else is annotations, casts and deletions.
  Bans bite (mutation: an explicit `Any` in `backends/batch.py` and in `ray/parca_spec.py` →
  mypy fails; a module outside the ban is untouched).

- **2026-09-20** — **#730 fixed, before 6b (Jim: "fix #730 before 6b").** The lookup now passes the
  definition the way the API takes it, `describe_job_definitions(jobDefinitions=["<name>:<rev>"])`,
  and tells two failures apart that the blanket `except Exception` had merged: the *service*
  said no or nothing yet (`ClientError`, an empty result: retried, quietly) and the *call* is
  wrong (`ParamValidationError`: a programming error: not retried, `logger.exception`, and
  still `None`, because a sizing nicety must not fail a dispatch). It was the second kind,
  retried as the first, that hid this for a month. The comment that cited an
  eventual-consistency incident "confirmed live" is rewritten to say what was actually
  happening. **What changes for a run:** `RAY_SHARDS_DEFAULT` = vCPUs × nodes reaches the job
  for the first time; process-bigraph's `RayProtocolRuntime` sizes its actor pool from it,
  where until now it fell back to the head's `os.cpu_count()` — one node's worth of actors
  on an N-node job. v2ecoli's own docs say viva-api "already computes this correctly"; it
  never had. It is a throughput change, not a results change (lineages are independent); I
  have not measured past campaigns and make no claim about them.
  **The fakes were the accomplice, so they changed too.** The three shared Batch fakes in
  `test_ray_backend.py` now validate `DescribeJobDefinitions`, `SubmitJob` and
  `RegisterJobDefinition` keywords against botocore's service model, offline. Turning that
  on across the suite refused 41 calls — all one test-double artefact (a `MagicMock`
  `cost_team_tag` becoming a Batch tag), fixed in the settings double — and then **nothing
  else: no other tested path makes a Batch call AWS would refuse.** The strict `xfail`
  committed in 6a flipped as designed and became five tests (the read, the form of the call,
  no sleep on success, a retried service error, a loud un-retried wrong call).
  **Before/after.** Before, from AWS, read-only, on C2's own `sim-composite` job: job
  definition `VCPU: 16`, 2 nodes, and no `RAY_SHARDS_DEFAULT` in `RAY_JOB_CMD`; one WARNING
  per dispatch in the API log. After needs a deployment and is three checks in the deferred
  list, attached to C3. The smoke composite is one seed, so it can confirm the variable and
  not a speed-up.

- **2026-09-20** — **PR 6a: the AWS clients are typed — and the first thing the types found was a
  bug that has been live since 2026-08-24.** `types-boto3[batch,s3,logs,ecr]` as a dev
  dependency; `boto3`/`botocore` removed from `ignore_missing_imports`; `BatchJobClient`'s
  factory is `Callable[[], BatchClient]`, `RayBatchLayer.client()` returns a `BatchClient`,
  job objects are `JobDetailTypeDef`. Annotations only, all under `if TYPE_CHECKING:` —
  because the API image is built `--no-default-groups`, the stubs **do not exist in
  production**, and an unguarded import would pass every test and fail at import time in
  the pod, in the module everything submits through. Two guards: an AST scan of
  `viva_core` / `viva_api` / `app`, and a subprocess that imports the five typed modules
  with every stub package made unimportable (it first proves the blocker blocks). Mutation:
  one unguarded import in the engine → both fail.
  Typing the factory turned 0 errors into 11. Nine were annotations and test fakes (the
  fakes are now cast in one named helper rather than the engine being widened back to
  `Any`). **One was real: #730.** `_mnp_node_vcpus` calls
  `describe_job_definitions(jobDefinitionName=…, revision=…)`; the API has no `revision`.
  botocore refuses it before any network traffic, `except Exception` logs that at DEBUG and
  retries it as if it were eventual consistency — which is what the comment above it says it
  is, "confirmed live" — and the method has returned `None` on every multi-node composite
  dispatch since it landed, after 3 s of sleeping. Reproduced offline; and the WARNING is in
  dev's log for C2's own `sim-composite` run. Every unit test used a `MagicMock` client,
  which accepts any keyword. **Not fixed here, on purpose:** the fix makes a dispatch start
  receiving `RAY_SHARDS_DEFAULT`, i.e. changes its shard count — a behaviour change to a
  dispatch does not ride in on a typing PR. The call is byte-for-byte what it was, behind a
  targeted `# type: ignore[call-arg]` that names the issue; its acceptance test is committed
  ahead of it as a strict `xfail`, using a fake that **validates its keywords against
  botocore's own service model, offline** (checked: with the one-line fix applied it returns
  16 and strict-xfail fails the run, as designed). That validating fake is the general answer
  to "MagicMock accepts anything" and is worth reusing in 6b.
  The stubs are 1.43.x against a 1.40.61 runtime: newer than what runs. Acceptable for four
  services whose API surface only grows; a stub-only method would be caught by the
  validating fake, not by mypy.

- **2026-09-20** — **Typed boto3 injected into the sequence as PR 6a** (Jim: "inject `Add
  types-boto3[batch,s3,logs,ecr] as a dev dependency and type the client factory` into the
  plan soon"). It came out of his question about the state of strict coverage with no explicit
  `Any`; the measurement is in section 9. Placed before the strategies on purpose: PR 5 leaned
  on mypy as the net for 145 respellings, and the net has a hole exactly where the Batch client
  is. **The `Any` ban followed minutes later and is D12** (Jim: "inject … per-module overrides
  for viva_core.* and viva_api.simulation.ray.* … into the plan after the types-boto3 task —
  use your judgement where it goes but I especially want viva_core.* with strong mypy
  coverage within this initiative"). My judgement on placement: **6b, straight after 6a, for
  `viva_core.*` as a glob** — the earlier the ban is on, the less core code is ever written
  with an `Any`, and a glob makes it the rule for every later move into core (P3, P4, P5)
  without anyone remembering it. **The ray package by module name in 6b, as `ray.*` only in
  PR 12:** the five strategy PRs are reviewable because a moved method is AST-identical to
  what it was, and an annotation change is an AST change; a `ray.*` ban before them would
  force one into every move. The third follow-up (ratchet the rest by count) stays proposed.

- **2026-09-20** — **PR 6: compose is handed its Batch layer.** `ComposeSimulationServiceRay` built a
  whole `SimulationServiceRay()` — an E. coli simulation service with its scheduler-facing
  surface and two other backends — to call five Batch methods and one status lookup on it.
  **The plan row was wrong in a way worth recording:** "compose uses `RayBatchLayer`
  directly" would have replaced the import of `viva_api.simulation.simulation_service_ray`
  with an import of `viva_api.simulation.ray.batch_layer`. The contract forbids compose →
  `viva_api.simulation` *at all*, so the broken edge would have been renamed, not removed,
  and the ledger would have claimed a burn-down that `lint-imports` did not show. Checked
  before writing code. What removes the edge is dependency inversion: compose declares
  `ComposeBatch` (six members, only the keywords it passes) and the composition root,
  `dependencies.py`, hands it `RayBatchLayer()`. Measured: 9 → 8 broken edges; compose's
  remaining two (`common.analysis_dag`, `ray.image_paths`) both belong to its analysis leg.
  `get_job_status` no longer borrows the simulation service's three-backend version: a
  compose job id is always a Batch job id, so it asks the layer for that one status. It had
  **no test of its own**; it has one now. Differential against `origin/main`, every Batch
  state plus an unknown id plus image resolution: 13 cases, 0 differences, outward calls
  included. **My first mutation was equivalent and the harness correctly said 0:** I changed
  the `.get(..., UNKNOWN)` default, which is dead code because every `JobStatus` is mapped.
  A zero from a mutation is a question about the mutation before it is a verdict on the
  harness; a real one (the lookup takes the wrong job) gives 10 of 13. The layer is
  stateless, so compose gets its own instance rather than sharing the Ray service's.
  A unit test pins the removed edge, since the contract itself is report-only.

- **2026-09-20** — **PR 5: the Batch layer is composed — `service.batch` — and the class inherits
  nothing from `simulation/ray/`.** Measured first: 13 members, 43 call sites in the service,
  19 in the package, 5 in compose, ~100 lines of tests. Decisions:
  *one instance, built in the constructor* — not a property that builds one per access,
  because `patch.object(service.batch, "submit_container")` has to reach every consumer, and
  because PR 6 (compose) and the strategies are handed that object;
  *public names* (`submit_container`, `image_uri`, `engine()`, `client()` …) — a private
  method called across an object boundary is a lie about what is private;
  *`_results_s3_uri` deleted, not moved* — it was a one-line wrapper of
  `data_layout.RayLayout.results_uri` and had nothing to do with Batch; the task service is
  handed a `results_uri` callable instead of learning this application's data layout, which
  is what it will be handed in core;
  *two delegates kept on the service*, `get_batch_job_statuses` / `_details`, because the
  scheduler and a handler ask the service (until P6) — a test pins that set at exactly two;
  *Protocols*: `ContainerSubmitter`, `MnpSubmitter`; `TaskDispatch` and `ParcaDispatch` are
  gone (`TaskBatch` extends `ContainerSubmitter` with the three status/log questions a task
  asks). `_local` and `_k8s` stay the service's own constructor arguments.
  **What the type checker could not see, and what caught it instead.** mypy strict was the
  net for the 145 scripted respellings (8 errors on the first pass, all real). Three things it
  is blind to: (1) **a `hasattr` capability probe** — `capabilities.py` advertised
  `container-jobs` iff the service had `_submit_container`; after the move every deployment
  would have silently stopped advertising it (smoke Tier 0 would have said so, at C3). Every
  existing test used a fake, which a rename does not touch. Fixed, and a new test runs the
  probes against the **real** class; mutation: restore the old probe → 3 tests fail.
  (2) **a wrong member with the same signature** — `image_uri` ↔ `submit_image_uri` at a call
  site type-checks; the proof script catches it (`differing: ['_awsbatch_nf_params']`).
  (3) **a consumer built around its own `RayBatchLayer()`** — identical against real AWS,
  blind to a patch; invisible to a differential by construction, so it is a unit test
  (mutation → it fails).
  **Proof.** The proof script was rewritten around the claim that fits a rewiring: *every
  difference is one of a short list of named changes* — 9 renames, the respellings undone
  and compared as AST + comment lines (17 service methods), 3 rewired, 1 gone, 2 delegates —
  `differing / lost / added: []`. Differential against `origin/main` with recording fakes at
  the boto3 seam: **465 cases, 0 differences** (130 `submit_container` and 130 `submit_mnp`
  option combinations over 5 settings variants, job definitions, statuses, details, cancel,
  log groups, and the task and ParCa services end to end; 104 + 26 `RuntimeError` on an
  unset queue, identically). My first run of it was **invalid and said so loudly** — 410
  differences — because BSD `sed` has no `\b` and the "old" copy imported the new layer;
  rebuilt in Python, and the old class's MRO is now asserted before the run. Mutations caught:
  10, 5, 18 differences; the fourth (own layer) 0, as predicted, and caught by the unit test.
  The obsolete guard "no method is defined twice across the carved classes" (it asserted the
  service *inherits* from the package) became its opposite: it inherits nothing from it.

- **2026-09-20** — **PR 4: `RayParcaMixin` dissolved — the last mixin.** Measured first: of its ten
  methods, **six never touched `self`** (the two cache URIs, the four command builders), three
  are the cache jobs, and one (`_stage_seed_override_caches`) is an S3 helper with exactly one
  caller. So the split followed the roles instead of inventing them:
  the six → functions of `ray/parca_spec.py`, called through the module
  (`parca_spec.parca_command(...)`) so a test can wrap one and see every caller; the three
  jobs → `RayParcaService(dispatch)` behind a three-member `ParcaDispatch` Protocol, reached as
  `service.parca`; the helper → back into the class, verbatim, next to its caller. I had
  written "only chain uses it" in the new module's docstring from memory; the AST said
  `_submit_multi_node_composite`. Corrected before it was committed — the reason to measure.
  `submit_parca_job` stays on the class as the facade `SimulationService` requires, and
  `cache_s3_uri` stays as a one-line delegate because the scheduler and the handlers ask the
  *service* for it (that ends in P6). No other shim: handlers call `service.parca.…`.
  **Proof, by kind.** *Method → function:* `scripts/prove_ray_carve_is_move_only.py` now checks
  it — same AST once `self` is dropped, same comment lines — and **names** every non-identical
  change (6 became functions, 2 rewired, 2 to a service, 1 new) so that anything unnamed
  fails. Four dispatch methods changed only in how they spell a ParCa call; the script undoes
  that spelling and compares AST + comments (the formatter re-flowed one statement), and a
  mutation — one extra argument at one of those call sites — turns `differing: []` into
  `['submit_ecoli_simulation_job']`. *The rewiring:* a differential against `origin/main` with
  recording fakes, **1,322 cases, 0 differences** — 1,024 `parca_command` argument
  combinations (768 return, 256 raise `ValueError` on a bad `new_genes` name, identically),
  and 138 cache-job submissions recording 506 outward Batch calls (42 raise `RuntimeError` on
  an unset queue, identically). Four mutations, each caught: `variant` dropped from
  `cache_s3_uri` (92 differences), the new-gene job staging from the wrong slot (60), the
  ParCa job named by commit instead of `environment_key` — the D11 property — (4), a
  hard-coded `--cpus` (776).
  A side effect worth keeping: 35 tests no longer construct a `SimulationServiceRay` at all.
  The file grew by 89 lines in this PR; the class lost a base.

- **2026-09-20** — **PR 3: the analysis specification, and #715 closed rather than reshaped.**
  `ANALYSIS_SCALES`, `APPLICABLE_ANALYSES`, the three sizing constants, `analysis_modules_for`
  and `analysis_memory_class` moved to `ray/analysis_spec.py` — 91 lines, byte-identical
  (functions, constants *and* their comments compared by source against `origin/main`; the
  class hierarchy's 69 methods unchanged per `prove_ray_carve_is_move_only.py`). No re-export:
  the service, the Nextflow handler, `scripts/cd2_nextflow_dispatches.py` and two test
  modules import from the new home.
  The plan row said "reshape #715 … keep `service.analysis` as a delegating shim". Starting
  from `main` that turned out to be unnecessary: #715 never merged, so the submitters never
  left the class and the scheduler still calls `submit_campaign_analysis` /
  `submit_multi_node_analysis` on it. A shim would have delegated to itself. #715 is closed
  with a pointer here; what was right in it (the pure functions, the glob fix) is this PR,
  and what was wrong (grouping two mechanisms' submitters by a shared word) is not carried.
  **The static guard now globs `viva_api/simulation/ray/*.py`** instead of listing files, and
  the "known dispatch paths" pin reads the whole package. Checked with a mutation: a new
  file in the package containing a `resolve_task_env` call without `with_events_env` fails
  the guard (1 failed, 14 passed), and is gone again. Before this, that file would not have
  been scanned at all — which is exactly how this guard missed a dispatch once already.

- **2026-09-20** — **Checkpoint C2 passed on dev (0.9.149, tag `v0.9.149`): the API roll.** Jim:
  "merge #724 and deploy C2". `kubectl diff` of the app overlay at `79fb0b21` against the live
  cluster was one line (the api image); apply rolled the api pod only (workbench and ptools
  pods unchanged, 4 d old). Markers on the newest pod: `environment_key` in `models.py`,
  `SimulatorIsWriteOnce` in the handler and the router, `ray/build.py`, `ray/parca.py`,
  `ray/tasks.py` present. `/health`: 0.9.149, `db_at_head=true` at `f4c8a2e6d0b3` — a fresh
  reading this time, because the pod restarted.
  **The marker reaches the user:** `atlantis simulator list` now prints 214 as "TEMPORARY -- a
  test artifact, NOT an authoritative simulator", and the smoke `task` checks went back to the
  newest *authoritative* simulator (213, `d67b0a7`) where at B2 they had picked 214.
  **Smoke, Tier 0 + 1 + 2 with `--build --build-commit d01dc07`: 21 passed, 0 failed, 2 skipped**
  (`analysis`, `biomodels`: opt-in). `build` made temporary simulator **215**
  (`atlantis-smoke 6dfe2521`, image `tmp-d01dc07-b64227` + `-submit`) in 588 s on the rewired
  build service — 702 s on the old path (214), same commit. All eight Tier 2 checks then ran
  **on 215**: every Batch job, the ParCa cache and the job definitions carried the marked tag
  (`ray-parca-tmp-d01dc07-b64227-…`, `mbp-parca-tmp-…`, `v2ecoli-ray-build-tmp-…`), so the
  temporary simulator wrote nothing into an authoritative namespace. `sim-mbp` 1,005.9 s vs
  1,005 s baseline. The three cancels PASS, `chain-cancel` in the ParCa phase.
  **Provenance, measured:** digests and push times of `:d01dc07`, `:d01dc07-submit` and
  `:d67b0a7` in ECR were recorded before the build and are identical after it.
  **`force` → 409 was NOT probed live, on purpose:** a `?force=true` against a real simulator
  is the overwrite if the refusal is somehow not running. It is proven by the marker on the
  pod plus `tests/simulation/test_simulators_write_once.py`.
  Two tracebacks in the pod log during the run are one event and mine: a diagnostic `curl -m
  240` of the chain's output, cut off client-side (`BrokenPipeError` in `s3_streaming`). That
  diagnostic was needed because `sim-chain` looked hung for 30 minutes; it was downloading
  (deferred list).
- **2026-09-20** — **Checkpoint B2 passed on dev: the D11 migration, and nothing else.** Image
  0.9.149 built from `79fb0b21` (#723). RDS snapshot
  `pre-0-9-149-checkpoint-b2-20260920t1433z`; `--analyze` from the new image (a one-off Job:
  the migration Job's manifest with `--apply` swapped) said MANAGED at `e7b3c9a1d5f2`, 19 of 20
  markers, one pending revision; the migration Job ran `e7b3c9a1d5f2 -> f4c8a2e6d0b3`. Only the
  `-db-migration` overlay was applied: the API pod is the same one, 0.9.148, 0 restarts, no
  `UndefinedColumn` / `ProgrammingError` in its log.
  **Provenance check, before and after:** 205 simulator rows both times, and an md5 over
  `id:commit:repo:branch` of every simulator with id ≤ 213 (everything created before
  2026-09-19) is identical — `39b92a7f…`. Every existing row reads `temporary=false, label=NULL,
  image_tag=NULL`, so `environment_key` is the commit, exactly as before.
  **Simulator 214** (`d01dc07`, created 2026-09-20 by the unmarked `build` baseline) was marked
  `temporary=true` with a label by a single guarded `UPDATE … WHERE id=214 AND
  git_commit_hash='d01dc07' AND created_at >= '2026-09-19' AND temporary=false` that rolls back
  unless it matches exactly one row. Its `image_tag` stays NULL: its image *is* `:d01dc07`.
  Smoke Tier 0 + 1 against 0.9.148 on the new schema: **11 passed, 0 failed, 4 skipped**
  (`routes` — client 0.9.149 vs server 0.9.148; `build`, `analysis`, `biomodels` — opt-in).
  **Two things the old API cannot do, which is why C2 should not wait long:** it does not
  return `temporary`, so every client still shows 214 as authoritative (the smoke `task` checks
  picked it: "ran on d01dc07"); and `force` still overwrites. Both are closed by the API roll.
  **A flaw found in the `database` check:** it passed with "at head `e7b3c9a1d5f2`" *after* the
  database had moved to `f4c8a2e6d0b3`, because `/health` reports the revision read at startup.
  Harmless in a normal deploy (the pod restarts after the Job) but it means the check cannot
  see a migration applied under a running pod. Deferred list.
- **2026-09-20** — **B2 and C2 are one image and two deploys.** I had promised B2 "on its own,
  before C2". By the time #722 merged, `main` also held cuts 4–5 and #714, undeployed, and
  #722 is written on top of them (it edits `ray/build.py` and `ray/parca.py`, which exist
  only after those cuts) — so no image can contain the migration without the dispatch
  changes. The one-kind-per-deploy rule is kept by separating them in time: **B2** applies
  only the `<ns>-db-migration` overlay (RDS snapshot, `--analyze`, the Job) and leaves the API
  on 0.9.148; the migration is additive with defaults, so the old code is untouched by it —
  which is also the rollback story. **C2** then applies the app overlay. A lesson for the
  ledger: a database PR should merge *before* undeployed code it does not depend on, or
  not be written on top of it.
- **2026-09-20** — **D11 made real: write-once simulators and the marked-temporary exception.**
  Jim sharpened the rule three times in one afternoon — preserve what predates 2026-09-19;
  a test simulator should be a *clearly marked temporary* record, image and tag; it must be
  *marked back to the end user*; and finally, **everything else is write-once, immutable,
  never deleted**. Built as one PR, and deployed as its own database checkpoint (**B2**)
  before C2. **The key idea is one property**, `SimulatorVersion.environment_key` =
  `image_tag or git_commit_hash`: every place the Ray path keyed an environment on the
  commit (image URI, job-definition suffix, ParCa cache, build job name — eleven call sites)
  now keys on that. For an authoritative simulator it *is* the commit, so nothing changes;
  for a temporary one it is `tmp-<commit>-<nonce>`, so everything a test simulator writes —
  including its **ParCa cache**, which would otherwise have overwritten the commit-keyed
  cache the authoritative simulator stages from — lands in its own namespace. Only the
  `git checkout` still uses the commit. **Server:** a temporary upload is always a *new*
  record with a fresh tag (never reused, never the answer to an authoritative request, any
  number per commit); `force` against a built or building authoritative simulator is
  refused (`SimulatorIsWriteOnce` → 409) while a FAILED build is still retried; temporaries
  are Ray-path only, because only that builder can push a tag that is not the commit.
  **Clients:** one shared `simulator_marker`; the CLI list prints it, the TUI has a Kind
  column, the GUI table leads with it, and the three CLI run commands warn before
  submitting on a temporary simulator. **Smoke:** `build` makes a temporary simulator,
  refuses to continue if the server does not mark it, asserts on the *marked* tag in ECR,
  and hands the simulator to Tier 2; no default ever picks a temporary one. A guard test
  pins that no HTTP route can delete or rewrite a simulator. The baseline `build` run on
  the old path (dev 0.9.148) passed — simulator **214**, 702 s — and left the one unmarked
  test artifact this rule was written to prevent; it is marked by hand at B2.
- **2026-09-20** — **D11: simulators are provenance. I was stopped one command short of
  breaking that.** The first `build` check forced a rebuild of an *existing* simulator, and I
  was about to run it on simulator 212 because it was five days old and looked unused. Jim:
  the existing simulators are the provenance of last week's deliverable simulations. A forced
  rebuild pushes a **different** image under the same `v2ecoli:<commit>` tag (the builds are
  not reproducible), so "this result came from that image" silently stops being true.
  Nothing was overwritten — the command was refused before it ran. What I found when I then
  looked: both image repositories (`v2ecoli`, `vecoli`) are **tag-MUTABLE**; they have **no
  lifecycle policy**, so nothing expires (good); and there is **one ECR registry for dev and
  prod**, so an overwrite "on dev" is an overwrite on prod. And the product itself offers the
  overwrite: `POST /core/v1/simulator/upload?force=true`. The rule (D11): everything created
  before 2026-09-19 is preserved through the refactor and retained after it; a rebuild is a
  new environment identity, never the same tag; only what the refactor's own testing created
  may be overwritten. In the target design this is what P5's two identities are for — the
  spec hash says what was asked, the **image digest** says what ran — and `force` becomes
  "build a new environment", not "replace this one". Filed as #721: the present-day hazard.
- **2026-09-20** — **PR 2: the two smoke checks the carve needs before it goes on.** `sim-mbp`
  (Tier 2) and an opt-in `build` (Tier 1). Design points for `build`: (1) like the cancel
  checks it asserts on the **outside world**, here the image registry: the status endpoint
  alone would answer COMPLETED, so the check passes only when ECR shows `<repository>:<commit>`
  arrive *after* the check began. (2) **It never rebuilds (D11).** It builds a commit that has
  no simulator record and no image tag — the branch HEAD, or `--build-commit` — without
  `force`; a commit that is already a simulator, or already a tag, is a SKIP. The image tag
  is the commit alone, so *any* simulator at that commit owns the tag. A unit test pins that
  `force=True` does not appear in the smoke module at all. (3) It sits in Tier 1, which
  finishes before Tier 2 starts, so the simulations then run on the image it just built —
  the strongest proof a rewired build path can get. (4) No registry access ⇒ SKIP before
  anything is touched. (The recipe pushes only `<sha>`; the comments saying it also moves
  `:latest` are stale — it would need `-t`.)
  **Baseline, dev 0.9.148 (before PR 7 rewires the path):** `sim-mbp` PASS in 1,005 s —
  simulation 1361, `baseline-reference-multigen`, 5 output files; almost all of it the ParCa
  job the run waits on.
- **2026-09-20** — **Plan audit after cut 6, at Jim's request** ("a good time to double-check
  our plan given all we have learned"). Method: three read-only explorations — what is left
  in the class, these two documents against themselves, how far `viva_core` really is — and
  then an independent reviewer asked to attack the draft revision. Findings and decisions:
  (1) **"Mixins first, strategies in P2.2" had no basis for what remains.** Measured: no
  mechanism calls another; the helpers assumed shared are not; no test patches a method by
  string. So the mechanisms go **straight to strategy objects** and P2.2 is absorbed. There
  are **five**, not four: 216 of the router's 308 lines are an inline ParCa + simulation path,
  now named the **ensemble** path. (2) **Shapes are chosen by domain role.** Jim's test for a
  service — a common requirement every mechanism uses, that works one way regardless of
  mechanism — fits build and the three ParCa cache jobs. It does not fit analysis, which is a
  domain specification + a compute pattern already shared in `common/analysis_dag.py` +
  per-mechanism glue; `RayAnalysisService` (#715) grouped by the word, and is to be reshaped.
  ParCa's commands and cache URIs are pure functions, not a service either. (3) **The
  reviewer overturned three claims of my own draft:** a strategy cannot own progress or
  cancel yet (they live in the scheduler and in routing code; they join in P6); recompose
  `RayBatchLayer` *first*, not last, or every strategy is rewired twice; ParCa is only half a
  service. (4) **D10**, from Jim's reply to the first draft: core's default path is *select
  or build an acceptable environment, then run* — the pattern of `../compose-api` and
  `../pbest`. The plan's `EnvironmentRef` was an exact coordinate; neither document described
  deriving an environment from a composite or selecting a compatible one. Select-or-build
  already exists three times, each partial (table in `architecture-core.md` §2.3a), and the
  derivation half in `pbest` parses addresses and then returns empty lists. "Default path" is
  reserved for this. (5) **Scope, decided:** `SimulationServiceK8s` is out of the carve; a
  prod catch-up is not part of this work. (6) **Order of core work:** the environment model
  and its select half, then an app that boots, then package moves, then the build half. D4's
  K8s and SLURM adapters are deferred with a trigger (no later than P5), not dropped.
  (7) **These documents had drifted**: three different line counts, a ledger saying "nothing
  deployed", P2.2 describing mixins that do not exist, `EnvironmentRef` / `JobStore` /
  build → core each in two or three phases, seven open questions with no owner, and no
  mention at all of the default backend on both Stanford sites. #719 makes them true;
  #716, #717 and #718 were filed from what the audit found.
- **2026-09-20** — **Checkpoint C1 passed on dev (0.9.148, #711, tag `v0.9.148`).** `kubectl
  diff` = one line (the api image); the apply alone rolled it, the workbench did not roll;
  both markers on the newest pod; `/health` at head `e7b3c9a1d5f2`. Smoke **Tier 0 + 1: 12
  PASS / 0 FAIL** (2 opt-in skips) — the three task checks and `compose` ran through the
  Batch engine in core. **Tier 2: 7 / 7 PASS** — `sim-default` (82 output files),
  `sim-chain` (2/2 seeds over 2 generations), `sim-nextflow`, `sim-composite`, and the three
  cancels. **`sim-cancel` and `chain-cancel` flipped FAIL → PASS**, 921 s and 915 s of
  waiting down to 45 s each: Batch showed `ray-parca-…` *and* `ray-sim-…` active before the
  default-path cancel and none after; the chain campaign was cancelled in its ParCa phase and
  its one job was gone. Posted on #709. Cuts 1–3 are proven on a deployment, not only in unit
  tests. Prod is untouched (0.9.78) and still has #709.

- **2026-09-18** — D1–D8 (above). Two course corrections the same day: the `simulator`
  table was briefly planned to move into core, then kept in SMS once environments, build
  recipes and build jobs were confirmed sufficient (D6); "core is standalone" was made an
  explicit principle with an enforcing test suite rather than an implication (D7).
- **2026-09-18** — P0 split into two waves. #661 is still open and conflicting, and three
  P0 items add or test migrations on the single Alembic chain; starting them now would give
  the chain two heads. First wave (no schema change, no overlap with #661 beyond one
  non-adjacent hunk in `db_reconcile.py`): #680–#684. Second wave after #661 merges.
  First result from #680: 1 contract kept (env workers + relay — now **enforced**), 5
  broken, 9 direct edges — the work list for P1–P5.
- **2026-09-20** — **Build and tasks are services, not mixins** (Jim asked "why have a
  RayBuildMixin rather than a build service"; there was no good reason). What the three
  build methods take from `self`: `_local`, and each other. Nothing of the Batch layer. It
  inherited `RayBatchLayer` to reach one attribute — and its destination is core
  (`backends/build.py` + a recipe), where a class that inherits an SMS class cannot go.
  Tasks are the same case with one more collaborator. So: `RayImageBuilder(local)` with
  `submit` / `build_command` / `run`, behind `SimulationServiceRay.submit_build_image_job`
  (kept: it is part of the `SimulationService` interface, and `handlers.simulators`
  *inspects its signature*, so the keywords are spelled out); and `RayTaskService(dispatch)`
  where **`TaskDispatch` is a Protocol of the seven things a task asks of whatever runs
  container jobs for it** — the seam P4b needs, written down now. The router calls
  `simulation_service.tasks.…`; no four-method facade was kept on the big class. An earlier
  entry rejected a Protocol for typing a *mixin's* `self` (it would restate an 18-keyword
  signature); here it declares only the nine keywords tasks pass, and mypy checks
  `RayTaskService(self)` against the real class at the one place it is constructed.
  **Proof:** not byte-identical, so a differential — `origin/main`'s mixins against the
  services, recording fakes for Batch, S3, CloudWatch, the task table, the local task
  service and `batch_build`: 148 cases (build command × 8 flag combinations × 2 private
  commits × 2 settings; run; submit; repo-path and uploaded tasks × settings × filenames;
  status and logs × job states), **0 differences**, 114 run to completion and 34 are the
  four legitimate errors, identical on both sides. **The harness was wrong first**: its
  initial "0 differences" was vacuous for three families — both sides raised the same
  error from my own fakes — and only the mutation check (a changed job name went
  undetected) showed it. Fixed, then mutations are caught (18 and 32 differences). The
  lesson is in the method, not the code: *a differential's zero means nothing until a
  mutation makes it non-zero.* New: `tests/simulation/test_ray_services.py` drives both
  services with no `SimulationServiceRay` at all — `FakeDispatch` is thirty lines.
  ParCa and the dispatch mechanisms stay mixins until P2.2.
- **2026-09-20** — **P2.1 cut 5: ParCa and the caches** (#712 merged first, on Jim's say-so).
  Ten methods → `ray/parca.py`; 76 methods compared, 0 differ (42 service / 14 layer / 7
  tasks / 3 build / 10 ParCa). **A rule for the remaining cuts:** the dispatch mixins (chain,
  multi-node composite, mbp-tracked, analysis) all call `cache_s3_uri` and `_parca_command`.
  They will **inherit `RayParcaMixin`**, not find those in the base layer — pushing every
  shared method down into `RayBatchLayer` would rebuild the god class one level lower; a
  mixin that needs ParCa says so in its bases. The proof each cut has claimed is now a
  script, `scripts/prove_ray_carve_is_move_only.py` (exit 1 on any differing, lost, added or
  doubly-defined method), so a reviewer runs the claim instead of reading it; delete it
  when P2.1 ends. #710's fix sits partly in code this cut did **not** move
  (`_record_run_with_companions`, `cancel_companion_jobs` stay with `cancel_job`, which
  belongs to the facade), so the fix and the move never touched the same lines.
- **2026-09-19** — **P2.1 cut 4: build**, started while 0.9.148 deployed (Jim). Three methods
  (`submit_build_image_job`, `_build_command`, `_run_build`) → `ray/build.py`. The mixin
  needs `self._local`, so the **constructor moved into `RayBatchLayer`** — every later mixin
  needs `_local` or `_k8s` too — and `_submit_image_uri` joined `_image_uri` there (Nextflow
  uses it, not only build). **Proof:** 76 methods across the four classes compared by
  source with `origin/main`: 0 differ, none twice (52 service / 14 layer / 7 tasks / 3
  build). Eight test patches aimed at `simulation_service_ray.batch_build.…` were retargeted
  to `viva_api.simulation.batch_build.…`: they reached the functions through a name the
  service module no longer binds (same object, so same effect). **Not done here, on
  purpose:** the plan's table sends build to core (`backends/build.py` + an SMS recipe).
  That is a design change, not a move — `batch_build.py` reads this application's settings,
  imports `boto3` directly, names this application's jobs (`v2ecoli-ray-build-…`) and is
  shared with `SimulationServiceK8s`. It goes with the recipe registry in P2.3 / P5. And
  no smoke check builds an image: run one by hand before this cut deploys.
- **2026-09-19** — **Checkpoint C split: C1 now (0.9.148), C when the carve is done** (Jim:
  "bump and deploy 0.9.148"). `main` was four dispatch changes ahead of dev — cuts 1–3 and
  #710. Deploying now rather than after cut 10 means a Tier 2 failure has three suspects
  instead of ten, the #709 compute leak stops, and the two cancel checks get their
  FAIL → PASS. It does not break "one kind of plumbing change per deploy": all four are the
  same kind (dispatch). #710 merged as `495fdd2e` with a fifth smoke check, `chain-cancel`,
  whose pre-fix baseline FAILED exactly as predicted — a chain campaign cancelled in its
  ParCa phase stopped nothing (`ray-parca-… RUNNING, terminated: false`, 900 s on).
- **2026-09-19** — **#709 fixed before cut 4, at Jim's direction** (#708 merged first). Its
  extent was wider than the one case the smoke check hit: **three** dispatch paths submit a
  ParCa job ahead of the job they return (default, multi-node composite, mbp-tracked) and
  none recorded it; and a **chain campaign** cancelled during its ParCa phase terminated
  nothing at all, because the campaign's own job id *is* the ParCa job and no seed has a
  current job yet. The fix uses what exists: `hpcrun.external_job_ids` (#414) holds the jobs
  a run owns besides the one it tracks; `insert_hpcrun` takes them; the default and
  mbp-tracked paths now record their own row under the caller's correlation id — the
  pattern chain and composite already use, and what the handlers' insert-if-absent guard
  keys on, so still one row per run; cancel terminates the companions **first** (Batch
  keeps a terminated job `PENDING` until its dependency ends), then the tracked job. No
  schema change. **The partial-cache question, decided:** terminating ParCa mid-write adds
  no state the system does not already produce — the image's entrypoint syncs the cache
  dir to the shared key every 30 s *during every ParCa run*, a failed or reclaimed ParCa
  leaves the same, the stage-in's `verify_cache_version` is the existing guard, and the
  default path rebuilds ParCa on every submit. Only the run's *own* ParCa job is
  terminated, never another run's. After this deploys, `sim-cancel` is an expected **PASS**;
  until then it stays an expected FAIL. This lands in code the ParCa cut (5) will move —
  which is the argument for fixing it first: the carve then moves correct code.
- **2026-09-19** — **The new checks' first live run (dev 0.9.147, pre-carve) found a real
  bug: #709.** `task` PASS (304 s, cold start; nonce and `sim_data_refs` read back),
  `task-fail` PASS (12 s), `task-repo` PASS (22 s), `nextflow-cancel` PASS (216 s — and it
  was **the scheduler's reaper** that stopped the task, `dispatch.reaped` = 1, so
  `terminate_matching`'s path is genuinely exercised by this check), **`sim-cancel` FAIL**.
  The failure is the deployment's, not the check's: cancelling a default-path simulation
  terminates the simulation's Batch job only. The ParCa job submitted alongside it is
  never recorded, so nothing can stop it — it ran to SUCCEEDED, 757 s, all after the API
  had answered CANCELLED — and the terminated simulation job sat `PENDING` behind its
  `dependsOn` for the whole time. **This predates the core split** (nothing of P2.1 is
  deployed), so at checkpoint C `sim-cancel` is an *expected* FAIL until #709 is fixed; the
  checkpoint's question for it is "same failure as the baseline", not PASS. The check was
  then improved by what it found: a failure now keeps its evidence, and names each
  lingering job's state and what it is waiting on (`… PENDING (terminate accepted), waiting
  on ray-parca-… RUNNING`) — the first run caught the bug only through the zombie, since a
  ParCa job's name carries the commit, not the experiment id. The fix belongs with the
  ParCa cut (P2.1 cut 5) or before it; `hpcrun.external_job_ids` already exists for this.
- **2026-09-19** — **Smoke grew four checks before checkpoint C** (Jim asked whether the
  smoke tests were rich enough for the paths P2.1 touches; they were not). Nothing in P2.1
  has been deployed, so no smoke check has run against it yet; and mapping cut 2's surface
  onto the existing checks showed every *happy* path covered and three kinds of branch
  covered by nothing: **cancel** (`terminate`, `terminate_matching` — the most restructured
  code in cut 2, and its failures are swallowed by the scheduler's `except`), **failure
  reporting**, and the **repo-path task** entry point + env passthrough. Added:
  `task-fail`, `task-repo` (Tier 1), `sim-cancel`, `nextflow-cancel` (Tier 2), and a
  `sim_data_refs` read-back on `task`. Design points: (1) the cancel checks look at **AWS
  Batch**, not the API — `cancel_simulation` sets the row to CANCELLED unconditionally and
  `/status` reads the row, so a status assertion would pass with every job still running;
  the run's unique experiment id is in a default job's *name* and a Nextflow task's
  *command*, so one token finds both. (A ParCa job has it in neither — it is named for the
  commit — so the lister also reads the `ExperimentId` cost tag; added with `chain-cancel`.) (2) No AWS access ⇒ SKIP before anything is
  submitted, per the suite's rule that SKIP is not PASS. (3) A cancel check that gives up
  still cancels what it submitted. (4) `task-repo` runs `scripts/build_cache.py --help`:
  the image has no script that echoes its environment, and relying on `script="-c"` would
  make a smoke check depend on the absence of input validation P10 must add. Deliberately
  not added: a process-swap simulation (smoke cannot see the submitted command, so it could
  only assert status; the functions are byte-identical with ~20 unit tests), the
  large-memory queue (no site provisions one), and an image build (~20 min, multi-GB —
  to be run once, by hand, before cut 4 deploys).
- **2026-09-19** — **P2.1 cut 3: tasks, and the shape every later cut uses.** #705 and #706
  merged on Jim's say-so; I had asked whether the settings-free engine and the no-re-export
  rule were acceptable before building eight more cuts on them, and the answer was "merge,
  then start on cut 3". Tasks could not move alone: a mixin calls `_submit_container`,
  `_image_uri` and `_results_s3_uri`, which lived in the class that would inherit the mixin.
  So this cut is three pure moves — `ray/image_paths.py` (ten path constants, no imports),
  `ray/batch_layer.py` (`RayBatchLayer`: the twelve delegation methods + `_rand_suffix`),
  `ray/tasks.py` (`RayTasksMixin`: seven methods + `_safe_task_name`). **Proof:** all 74
  methods of the class compared with `origin/main` by source — 0 differ, none lost, none
  defined twice (55 service / 12 layer / 7 tasks); module functions and constants likewise.
  Rejected: typing the mixin's `self` with a Protocol — it restates signatures that already
  exist. Side effect worth having: `compose` now imports its two constants from
  `image_paths`, not from the 4,000-line service module. Tasks stay SMS for now, as a mixin:
  the `task` table and the image they run in are still SMS; P4b gives them a `JobStore`
  and an `EnvironmentRef` and moves them to `viva_core/tasks`. After this cut the only
  direct `self._batch()` calls left are `get_task_logs` (now in `tasks.py`) and
  `_mnp_node_vcpus`.
- **2026-09-19** — **P2.1 cut 2: the Batch engine is in core** (`viva_core/backends/batch.py`:
  `BatchJobClient`, `SubmitJobPacer`, `BatchJobDetail`, `stage_out_env`, `ecr_image_uri`).
  Three decisions. (1) **The engine takes no settings** — every queue, base job definition
  and prefix is an argument, and the boto3 client comes from a factory. Not a style choice:
  205 tests isolate the service by patching `_seams.get_settings`, core may not import
  `viva_api`, so an engine that read `get_core_settings()` for itself would run with the
  developer's real queues while those tests passed — the exact hazard P2.0 was built
  against. `SimulationServiceRay._batch_jobs()` builds the engine per call around
  `lambda: self._batch()`, so a swapped `service._batch` still reaches it. (2) **What stayed
  in SMS**, because it is a decision about *this* deployment or *this* science: which queue
  (standalone vs placement-group; large-memory), the "setting not provisioned" guards, the
  strain / clean-chain / lineage-debug entries `_stage_out_env` appends after core's generic
  half, and the predicate that says which Batch jobs belong to a Nextflow campaign
  (`terminate_matching(matches=…)`). This is the plan's "four strain kwargs become opaque
  `extra_env`", done as *append to the list* so the env order is unchanged. (3) **Method
  names did not move**: `_submit_mnp`, `_submit_container`, `_ensure_*_job_def`,
  `_image_uri`, `get_batch_job_statuses/details` keep their signatures and delegate, because
  ~110 test references and `compose`'s four private-method calls go through them. `compose`
  switches to `BatchJobClient` directly in P2.3. **Proof:** 2,178 differential cases (every
  combination of stage-in / dependency shape / tags / retry / strain / task_env × node count
  × memory class × settings variants, plus definitions, statuses with 234 ids, details, log
  group, terminate with pagination) — **0 differences**; mutating one env value in core
  produced 864, so the harness sees what it should. **Two observable differences**, both
  logs: the engine's messages are logged under `viva_core.backends.batch`; and "Submitted
  container job … to <queue>" now names the queue the job actually went to — it used to
  print `ray_container_queue` even when the job was routed to the large-memory queue.
  Left for later cuts: `get_task_logs` (tasks, cut 3) and `_mnp_node_vcpus` (MNP, cut 9)
  still call `self._batch()`; `batch_build.py` carries its own copy of the DescribeJobs
  chunk size (build, cut 4).
- **2026-09-19** — **P2.1 cut 1: config interpretation.** Five module-level functions
  (`_is_upstream_vecoli`, `strain_from_config`, `injected_processes_from_config`,
  `_thread_injected_processes_into_params`, `_batch_domain_overrides`; 211 lines) moved to
  `viva_api/simulation/ray/config_interpretation.py`, byte-identical. Chosen first because
  they are pure functions of a config — no settings, no AWS, no database, so the seam is not
  even in play — and no test patches any of them by name. Two things the cut showed. (1) The
  plan said the service file "ends as the facade **and re-exports**"; mypy strict rejected
  the implicit re-export on the first run, and ruff removed `strain_from_config` from the
  re-import because the service file **never used it** — it lived there only because
  everything did. So importers move with the function (`job_scheduler`,
  `scripts/cd2_nextflow_dispatches.py`, two imports in `test_ray_backend.py`), and the plan
  now says so. (2) The one observable difference: the "nested … overrides the config's own
  flat …" warning is now logged under `viva_api.simulation.ray.config_interpretation`
  instead of `…simulation_service_ray`. Nothing in the repo keys on that name (tests, overlays), but a saved log query would.
- **2026-09-19** — **Checkpoint B passed on dev (0.9.147, #703, tag `v0.9.147`)** — the first
  deploy in this work to change the schema. RDS snapshot first
  (`pre-0-9-147-checkpoint-b-20260919t1955z`); a read-only `--analyze` run **from the new
  image** as a one-off Job (the migration Job's manifest with `--apply` swapped for
  `--analyze`) showed *managed*, exactly the three pending markers unchecked; the Job then ran
  `b2f6d8e0a4c7`, `c9a1e3f5b7d2`, `e7b3c9a1d5f2` in 9 s. All rows preserved; the old pod stayed
  healthy against the new schema; new pod creates nothing at startup and logs the schema at
  head; smoke Tier 0 7/7 (`database` PASS, 100/100 operations) and Tier 1 3/3 — **`compose`
  PASS live for the first time**, closing the loop on #689 and #695. First task under the new
  code is stamped `owner_instance = api`. The walk registered 7,141 dataset rows in ~10 min.
- **2026-09-19** — What 24x7 polling costs (Jim asked). Measured inputs: the walk is a fixed
  25 S3 LISTs a minute whatever the number of simulations (~1.1–1.2 M a month → **about $5–6
  a month per site** at $0.005 per 1,000); the event ingester is **zero when idle** (it skips
  rows with no event in 10 minutes); `DescribeJobs` and CloudWatch reads are free APIs; RDS is
  plain Postgres on a `db.t3.medium` with gp2 — no per-query or per-I/O charge; both VPCs have
  an S3 **gateway endpoint**, so none of it crosses the NAT. Noise beside the always-on
  infrastructure. Worth doing anyway: the walk re-LISTs finished simulations forever, whose
  outputs cannot change — walking terminal simulations daily would cut it ~25x.
- **2026-09-19** — Found on dev, not caused by this work: **153 simulation rows stuck RUNNING**,
  started 2026-04-03 … 2026-09-11, none with a single event. They cost nothing (the pollers
  skip them) but make "how many runs are active" meaningless. Not yet filed.
- **2026-09-19** — **One way to run a composite** (Jim asked how composites are specified; the
  answer was "two ways, on two endpoints"). Recorded in `architecture-core.md`: core takes an
  `EnvironmentRef` plus exactly one of a `document` or a `composite{id, params}`; execution
  (multi-node, Nextflow head, single container) is a field of that request, not an endpoint.
  The SMS `multi_node_dispatch` / `nextflow_dispatch` shapes become facades over it. Lands
  with P5, where the environment registry arrives.
- **2026-09-19** — `owner_instance` is a **role**, not a pod name: the boot sweep runs in the
  *next* incarnation of the same role, which has to recognise its predecessor's rows, and a pod
  name changes on every restart. Rows with no owner (written before the column) are swept too,
  or the first boot after the migration would strand them for good. This is the first revision
  the parity test checked on arrival — and it caught nothing, which is the point.
- **2026-09-19** — `DB_CREATE_ALL` guard. Not fatal by design: a mismatch is an ERROR log and a
  `/health` field, not a crash, because a rolling deploy can start a pod just before the
  migration Job finishes and that must not become an outage; the smoke `database` check is what
  fails the deploy. Turned off for BOTH Stanford sites in config — prod picks it up at its next
  deploy, where its database (at `b4d7e9c02a15`) must go through the migration Job first, as it
  must anyway.
- **2026-09-19** — #637, three corrections in one. (1) It had been **closed by accident** the
  moment #661 merged, though #661's own text says `upgrade head` "still fails on an empty
  database"; re-verified failing on `main` and reopened. (2) **A decision was already on the
  issue** (Eran, 2026-09-13: Option 1, guarded `CREATE TABLE`s + an empty-Postgres negative
  control), and it differs from what this plan proposed — the plan is corrected to follow it.
  (3) **Making the chain run exposed that it built the wrong schema**: a chain-vs-`create_all`
  comparison found 20 differences, all in three baseline tables (`hpcrun` without
  `correlation_id`; `simulation` and `worker_event` in a shape no real database has — checked
  on dev). A second guarded revision reconciles them; parity is now exact but for one
  irreversible, unused enum label. Neither inserted revision has a reconciler fingerprint, on
  purpose: no marker for them can be false below its revision on a `create_all` database.
- **2026-09-19** — **Tier 2 and Tier R passed live on dev (0.9.146).** Tier 2, four simulations
  submitted together: `sim-default` sim 1350 (29 min; 82 output files, 1 seed summary),
  `sim-chain` sim 1347 (52 min; 2/2 seeds succeeded over 2 generations), `sim-nextflow` sim
  1348 (31 min; 2 traced tasks, all completed), `sim-composite` sim 1349 (33 min; 14 output
  files) — **52 min wall clock, ~146 min if run serially.** Tier R: task 4 in flight, api pod
  replaced, `/version` unchanged, task still resolved with its output; new pod 0 restarts,
  0 errors. This is the pre-carve baseline for checkpoint C: every dispatch path is known
  good on 0.9.146, so a Tier 2 failure after P2.1 is P2.1's. One thing the live run showed:
  `kubectl rollout status` returned in 3.6 s, before the old pod was gone — the restart
  helper now also waits for the old pods to be deleted.
- **2026-09-19** — Smoke Tier 2 + R built (P2.0c). Tier 2 is **per dispatch mechanism**, because
  that is the unit P2.1 can break; the four are selected exactly as the router selects them
  (`nextflow_dispatch` → `mbp_dispatch` → `multi_node_dispatch` → more than one generation =
  chain → default). `mbp_dispatch` has no check yet. They run concurrently — their cost is
  AWS Batch time, not the client's. Polling now rides through up to 180 s of the API being
  unreachable: an hour-long poll will meet a port-forward restart or the tunnel's 70-minute
  lifetime, and neither is the deployment failing.
- **2026-09-19** — P2.0b corrected a claim of mine. I had written that the `boto3` patches
  were positional too. They are not: all 93 are `patch("….boto3.client")`, which patches the
  shared module globally. Only the 205 `get_settings` patches are positional. The seam is
  still right (one place to patch, and it is what lets P2.1's modules share it), but the AWS
  exposure during a carve is real *settings*, plus patch lifetime — not unpatched clients.
- **2026-09-19** — **Checkpoint A2 passed on dev (0.9.146, #694).** `kubectl diff` = one line;
  migration Job *managed*, 16/16, no-op; marker `/app/viva_core/settings.py`; inside the pod
  `get_core_settings() is get_settings()` with the S3 bucket and region populated; startup 0
  errors / 0 warnings; smoke Tier 0 6/6; Tier 1 `task` PASS, `worker` PASS; **P1b proven** by
  `atlantis simulation outputs 1344` — 82 files streamed out of S3 through the new settings
  path; **#689 proven** — compose simulation 9 COMPLETED with `emitter_history.json` in its
  results and `level = 1.61051 = 1.1^5`. The `compose` *check* itself then crashed
  (`BadZipFile`): the Ray path serves a gzipped tar, which the client saved as `.zip` and the
  check read as a zip. Both fixed (format sniffed); the check verified against that real
  payload. Second run, second real finding.
- **2026-09-19** — Checkpoint **A2** inserted (Jim asked whether to deploy now; yes): P1b is a
  *configuration* change and P2.1 a *dispatch* change, so letting P1b ride checkpoint C broke
  the one-kind-per-deploy rule this plan set. A2 = 0.9.146 = P1b + the `run_pbg` fix.
- **2026-09-19** — P2.0(a), the no-real-AWS test guard, **found a live one on its first full
  run**: `test_submit_build_returns_local_job` issued a real `batch.SubmitJob`. Not a missing
  patch — a patch-LIFETIME race: `submit_build_image_job` starts the build as a background
  task and returns, the test left its `with patch(...)` block, and the pending task then ran
  against the real `batch_build.submit_batch_build`. With a logged-in profile that is a real
  image-build submission from `pytest`; without credentials the task's own error handler
  swallowed it, so nobody saw. The guard therefore also FAILS A TEST AT TEARDOWN for any
  refused call, because a swallowed refusal must not read as a pass.
- **2026-09-19** — **P2 rewritten as a decomposition** (Jim: the module was extended for
  several dispatch mechanisms and should have been broken up even on the SMS side). Measured:
  one class, 4,305 lines, ~75 methods, eleven concerns, four dispatch mechanisms behind one
  299-line router; 78 commits to the file in 30 days, none in an open PR today. The staging
  is dictated by the tests, not the code: 298 patches of the module's `get_settings` / `boto`
  names would silently stop applying to moved code. Mixins first (pure moves), strategies
  second. Same disease, not yet planned in this detail: `job_scheduler.py` (1,276 lines, P6),
  `common/handlers/simulations.py` (2,418), `routers/env_worker.py` (1,168, P3).
- **2026-09-19** — P1b: core gets settings by **inheritance + provider**, not a copy. Rejected:
  a second `BaseSettings` reading the same variables (import-order dependent, because
  `config.py` loads dotenv files at import), and passing settings into every constructor
  (touches every call site while other sessions have branches open). This is the seam P3
  widens — `CoreSettings` grows, `Settings` shrinks.
- **2026-09-19** — **Core runtime image pulled forward to the front of P2b** (now P2.3) (Jim). Trigger:
  the first live Tier 1 run — 320 s of cold start for a 2 s task, because the only image a
  task can run in is the 5.74 GB science image. The stopgap (a small image pushed under a
  tag in the `v2ecoli` repository) was considered and rejected: it pollutes the science
  repo's tags and bakes in the `/app/v2ecoli` path contract.
- **2026-09-19** — First live Tier 1 run on dev (0.9.145): `task` PASS (nonce read back; 320 s
  cold start, 2 s run), `worker` PASS (K8s Job started, read, task-tier task completed,
  worker stopped), **`compose` FAIL — a real, pre-existing bug the suite found on its first
  run**: `run_pbg.py` excludes in-memory emitters from output redirection by comparing the
  whole address tail to `"RAMEmitter"`, but under the workspace core only the dotted name
  (`process_bigraph.emitter.RAMEmitter`) resolves, so the emitter is "redirected", the
  history fallback is skipped, and `PBG_REQUIRE_OUTPUT` fails the job. Any generic
  in-memory composite fails on compose-on-Ray (since #276, 2026-08-25). Fixed in #689;
  reproduced locally (original rc=1, fixed rc=0).
  Also seen: a five-step toy composite provisions **3** multi-node instances and stages the
  whole ParCa cache first — both go away with P2.3's runtime image (then called P2b) and P5's decoupling.
- **2026-09-19** — `atlantis smoke` added (Tier 0 + Tier 1), at Jim's prompt: before it, the
  only checks against a *deployment* were the vEcoli qualification script, the analysis-read
  node tests and hand inspection; `make e2e` pointed at a test class that no longer exists
  and passed by collecting nothing. Tier 2 and Tier R are built before checkpoint C.
- **2026-09-18** — **Checkpoint A passed on dev (0.9.145, #687, tag `v0.9.145`).**
  `kubectl diff` = one line (api image); migration Job: *managed*, 16/16 markers, no-op;
  `viva_core` in the image and old/new module names identical inside the pod; the new
  shutdown order seen on a 0.9.145 pod (second rolling restart — the first only shows the OLD
  pod's behaviour); 91 live operations; `atlantis task run --wait` COMPLETED with its output
  confirmed in the log; new pod 0 restarts, 0 errors. Not run: a full simulation, a compose
  run, `vwb smoke`. Found, pre-existing: one `env_worker_task` row with status
  `WEIRD_UNKNOWN` and no `ended_at`, which the boot sweep does not settle.
- **2026-09-18** — §7a added at Jim's question: migrations are proper Alembic revisions with
  tested downgrades; three planned changes are not really reversible (enum labels; P7(c) in
  practice; the `compose_hpcrun` id merge) and are named. P4b no longer adds a `TASK` enum
  label, to stay reversible.
- **2026-09-18** — Deploy checkpoints added (§8), at Jim's prompt: merged ≠ deployed, and
  several changes can only be proven live. Rule: one kind of plumbing change per deploy.
  Checkpoint A = 0.9.145 on dev, taken BEFORE P1b. Pre-flight on dev: `current_schema()` is
  `public` (search_path `"$user", public`, user `postgres`, no other schema), DB already at
  head `e3a9c1d70b62`, no pod-local work in flight.
- **2026-09-18** — P1 sliced (P1a / P1b) and four modules re-homed, after reading each
  candidate's imports: core may not import `viva_api`, so `viva_api.config` readers wait.
  The shim became a self-replacing stub file instead of a meta-path finder, because mypy
  needs a file at the old path. Details under P1.
- **2026-09-18** — #679 and the P0 first wave (#680–#684) merged, on Jim's say-so per PR.
  #683 had been stacked on #681; it was retargeted to `main` after #681 merged and went in
  last. Still open from P0: the second wave, waiting on #661.
- **2026-09-18** — D9: BioModels and the curated simulators go to core, not SMS, and are
  later factored out into the reproducible-biology hosted-services application. The earlier
  draft already placed them in `viva_core.contrib.sysbio`; what changed is that the
  destination is now named, and the "public API only" contract is a requirement of the move
  rather than a nicety.
