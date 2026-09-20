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
| SMS's half of the Batch seam (settings, queue choice, this application's env entries) | 400 | SMS `ray/batch_layer.py`: today a base class, **to become a composed `service.batch`** | cut 3; recomposed in PR 5 |
| `/tasks` — submit, upload, dispatch, status, logs | 190 | SMS `ray/tasks.py` (`RayTaskService`) **now**; core `tasks/` in P4b, when the `task` table has a `JobStore` and the image an environment | service, #714 |
| Image build | 200 | SMS `ray/build.py` (`RayImageBuilder`) **now**; a core `repo-recipe` build recipe in P5. Not core yet because `batch_build.py` reads this application's settings, names its jobs, and is shared with `SimulationServiceK8s` | service, #714 |
| Config interpretation | 211 | SMS `ray/config_interpretation.py` (pure) | done, cut 1 |
| ParCa and caches | 520 | SMS: commands + cache URIs → a pure module; the three cache jobs → `RayParcaService`. ParCa is **not** submitted one way (container in mbp and chain, MNP in ensemble and composite), so each mechanism submits its own | mixin today (cut 5); split in PR 4 |
| Analysis | 460 | SMS: `ray/analysis_spec.py` (pure: modules, memory sizing, shared with Nextflow and the K8s path); each mechanism's analysis **submitter travels with that mechanism**; the pattern stays `common/analysis_dag.py` | PR 3 |
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
  (b) One seam: `viva_api/simulation/ray/_seams.py` owns `get_settings` and `boto3`; every
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
  | 1 | config interpretation → `simulation/ray/config_interpretation.py` | #705 | 211 (5,019 → 4,815) | merged 2026-09-19 (`0d71e2a6`) |
  | 2 | Batch engine → `viva_core/backends/batch.py` (`BatchJobClient`) — a **delegation**, not a move; see the decision log | #706 | 234 (4,815 → 4,581) | merged 2026-09-19 (`d5f965a4`) |
  | 3 | tasks → `ray/tasks.py` (`RayTasksMixin`), on two prerequisites every later mixin shares: `ray/image_paths.py` (in-image path constants, a leaf) and `ray/batch_layer.py` (`RayBatchLayer`, the service's delegations to the engine) | #707 | 597 (4,581 → 3,984) | merged 2026-09-19 (`4173ca26`) |
  | 4 | build → `ray/build.py` (`RayBuildMixin`); the constructor and `_submit_image_uri` join `RayBatchLayer` | #712 | 144 (3,984 → 3,840) | merged 2026-09-20 (`8229315a`) |
  | 5 | ParCa and the caches → `ray/parca.py` (`RayParcaMixin`): cache URIs, the ParCa / new-gene / variant / upstream commands, their three submit methods, per-seed founder-cache staging | #713 | 512 (3,840 → 3,328) | merged 2026-09-20 (`1f90dd04`) |
  | — | **build and tasks become composed services** (`RayImageBuilder`, `RayTaskService` behind a `TaskDispatch` Protocol), not mixins — see the decision log, 2026-09-20 | #714 | service file 3,328 → 3,361 (two delegations added) | merged 2026-09-20 (`a10ac6cf`) |

  The rest, in the order the audit settled (one PR each; nothing merges without Jim's say-so):

  | PR | what | why here |
  |---|---|---|
  | 1 | **docs truth** (#719) | both living documents made true before more work |
  | 2 | smoke: `sim-mbp` and an opt-in `build` check (#720); then deploy the merged-but-undeployed build cuts (**C2**) | no check builds an image or exercises mbp, and both are about to be rewired |
  | 3 | reshape #715: pure `ray/analysis_spec.py` + the static-guard glob fix; the two analysis submitters stay in the class until their mechanisms move; `service.analysis` kept as a delegating shim for `job_scheduler.py` | analysis is a spec + a pattern + per-mechanism glue, not one service |
  | 4 | ParCa: commands and cache URIs → a pure module; `RayParcaService` holds only the three cache jobs | half of it is pure; only the cache jobs work one way |
  | 5 | **`RayBatchLayer` stops being a base class** and becomes a composed `service.batch`, behind two small SMS Protocols, `ContainerSubmitter` and `MnpSubmitter`, replacing `TaskDispatch` / `AnalysisDispatch`. `local` and `k8s` are constructor arguments of the strategies that need them, never Protocol members | done **first**, so every strategy is handed a real object; done last, each strategy would be rewired twice (~80 call sites, ~24 `patch.object`) |
  | 6 | `compose` uses `RayBatchLayer` directly, not a whole `SimulationServiceRay()` | one of the three broken `compose-is-domain-free` edges goes |
  | 7 | strategy: **mbp-tracked** | smallest (225 lines); first use of the shape |
  | 8 | strategy: **Nextflow** (needs `k8s`; `reap_cancelled_campaign` travels with it); then **C3** | 464 lines |
  | 9 | strategy: **multi-node composite** (+ its analysis submitter) | 346 lines |
  | 10 | strategy: **ensemble**, extracted from the router (+ `_sim_command`) | the router shrinks to ~20 lines of precedence |
  | 11 | strategy: **chain** (+ its analysis submitter; needs `local`); delete the facade shims and `scripts/prove_ray_carve_is_move_only.py`; **checkpoint C** | largest (848 lines) and it bills real money, so last |
- **P2.2 — absorbed into P2.1** (2026-09-20). There are no mixins to turn into strategies.
- **P2.3 — the environment model and its *select* half** (no database, no build). D10 says
  core's default path is *select or build an environment, then run*; this is the select half.
  `viva_core/environments/`: `EnvironmentSpec` (explicit `repo@commit` + recipe | derived
  dependency set), `Environment`, and an `EnvironmentResolver` Protocol with one
  implementation that resolves an **explicit** spec to an image. It replaces four independent
  derivations of the same image from the same two settings (`ray/batch_layer.py:72`,
  `compose/simulation_service_ray.py:91`, `compose/env_worker_service.py:114`,
  `simulation_service_k8s.py:486`). With PR 5 and PR 6 of the carve, that is the smallest slice
  that runs a third party's pbg-wrapped simulator through core.

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
- **Production.** Prod is on **0.9.78**; dev is at 0.9.149. A catch-up is *not part of this
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
| C3 | PRs 3–8 (analysis spec, ParCa split, the composed Batch layer, `compose` on it, the mbp-tracked and Nextflow strategies) | every submit now goes through a composed object; two mechanisms are strategies | Tier 0 + 1 + 2; `compose`, `sim-mbp`, `sim-nextflow`, `nextflow-cancel` especially |
| C | PRs 9–11 (composite, ensemble, chain strategies); the end of P2.1 | the last three mechanisms, chain among them | Tier 0 + 1 + 2, **plus a real 2 x 2 chain campaign** and `chain-cancel`: chain bills real money and fakes share their author's blind spots |
| D | P2.3 | one resolver replaces four image derivations; the core runtime image | workbench through the relay; `vwb smoke`; `atlantis worker`, `task`, `compose` on the new image |
| E | P3 | settings split, new wiring and lifespan, app factory | alone; diff redacted effective settings and the OpenAPI spec old pod vs new |
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
- **Pure move → byte-identical proof.** `scripts/prove_ray_carve_is_move_only.py` compares
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
  dispatch once because "the module was simply not scanned"; it now scans `simulation/ray/*`.
- **Tests move with the code.** `tests/simulation/test_ray_backend.py` (5,866 lines, 200
  instantiations) is split per mechanism in the same PR as each strategy.
- **Import edges ratchet.** Twelve report-only edges are broken today; the count never
  rises, and a PR that touches one burns it down.
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
| RDS snapshot `pre-0-9-149-checkpoint-b2-20260920t1433z` | dev | C2 passed 2026-09-20; delete once it has soaked |
| Smoke `sim-chain` downloads the whole chain output (~3.6 GB uncompressed) through the SSM tunnel at ~0.35 MB/s: about 30 of its 77 minutes at C2 were the download, long after the server was done | `app/smoke.py` | assert on a listing plus the per-seed `summary.json` files instead of the full archive |
| Temporary simulators 214 and 215 and the images `tmp-d01dc07-b64227[-submit]` on dev / in the shared ECR | dev | the purge for temporary simulators (not built yet); until then they stay, marked |
| `/health` reports the database revision **as read at startup**, so smoke's `database` check cannot see a migration applied under a running pod (seen at B2) | viva-api | read it per request, or label it `db_revision_at_startup` |
| `CLAUDE.md` still says backend selection is by `deployment_namespace` and that tests use SQLite | `CLAUDE.md` | any docs PR |
| `scripts/prove_ray_carve_is_move_only.py` | — | delete in PR 11 |
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
| P2.1 | carve `simulation_service_ray.py` (5,019 → 3,361 lines so far). Cut 1 config interpretation — #705 · cut 2 Batch engine → `viva_core/backends/batch.py` — #706 · cut 3 tasks + `RayBatchLayer` — #707 · cut 4 build — #712 · cut 5 ParCa — #713 · build and tasks as composed services — #714 · the #709 cancel fix — #710 · smoke checks — #708. Remaining: PRs 1–11 of the 2026-09-20 sequence; #715 (analysis as a service) is open and **to be reshaped** as PR 3 | 0.9.149 carries cuts 1–5, #710, #714, #722 | **2026-09-20** (checkpoints C1, B2, C2) | — | **in progress.** Everything merged is deployed to dev. Next: PR 3 (reshape #715) |
| P2.2 | — | | | | **absorbed into P2.1** (2026-09-20): the mechanisms go straight to strategy objects |
| P2.3 | the environment model and its *select* half (D10): one resolver for four image derivations; then the core runtime image | | | | not started (checkpoint D) |
| P3 | | | | | not started (checkpoint E) |
| P4a | | | | | not started (checkpoint F) |
| P4b | | | | | not started (checkpoint F) |
| P5 | | | | | not started (checkpoint G) |
| P6 | | | | | not started (checkpoint H) |
| P7a / b / c | | | | | not started (checkpoint I) |
| P8 | | | | | not started (checkpoint —) |
| P9a / b / c | | | | | not started (checkpoint J) |
| P10 | | | | | not started (checkpoint —) |

## Decision log

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
