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

### P-1 — This PR

`docs/architecture-core.md` + `docs/plan-core.md`, and a pointer in `CLAUDE.md`. Docs only.

### P0 — Guardrails

Make the ground safe before moving anything.

- #661 merged as-is by its owning session (see §5).
- import-linter in `make check`, **report-only**.
- Fix `set_messaging_service` (`dependencies.py` — it assigns a local; `get_messaging_service()` always returns None).
- Reconciler `table_schema` filter.
- Fix #637: FRESH = `create_all` on all bases + stamp heads, guarded by a
  `create_all`-vs-migrations parity test.
- `DB_CREATE_ALL=false` guard, set in the overlays — this is what freezes the fingerprint list.
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

`SimulationServiceRay` is one class of **4,305 lines and ~75 methods** holding eleven
concerns. It was extended, mechanism by mechanism, into the home of *four* simulation
dispatch paths; it should have been broken up on the SMS side long before a core existed
(Jim, 2026-09-19). Extracting the generic Batch engine alone would leave a ~4,000-line class
with all four mechanisms still in it — so P2 is a **decomposition**, of which the core
extraction is the first cut.

| Concern | ~lines | Destination |
|---|---|---|
| Batch engine — job definitions, MNP + container submit, pacer, status, cancel, logs | 550 | **core** `backends/batch.py` (`BatchJobClient`) |
| `/tasks` — submit, upload, dispatch, status, logs | 190 | **core** `tasks/` |
| Image build — `submit_build_image_job`, `_build_command`, `_run_build` | 200 | **core** `backends/build.py` + an SMS recipe |
| ParCa and caches (ParCa, new-gene, variant, upstream, seed-override staging) | 520 | SMS `simulation/ray/parca.py` |
| Dispatch 1 — multi-node composite on Ray | 470 | SMS `ray/strategies/mnp.py` |
| Dispatch 2 — chain dispatch (per seed x generation, lineage, campaign result, cancel / reap) | 900 | SMS `ray/strategies/chain.py` |
| Dispatch 3 — Nextflow head (render, params, session, head job) | 460 | SMS `ray/strategies/nextflow.py`, on core's Nextflow backend |
| Dispatch 4 — "mbp tracked" | 220 | SMS `ray/strategies/mbp_tracked.py` |
| Analysis — container, campaign, multi-node | 460 | SMS `ray/analysis.py` |
| **The router** — `submit_ecoli_simulation_job`, one 299-line method choosing among the four | 299 | SMS `ray/service.py`, reduced to selecting a strategy |
| Config interpretation (`strain_from_config`, `injected_processes_from_config`, …) | 200 | SMS `ray/config_interpretation.py` |

**The hazard that dictates the staging.** Tests patch this module's *names*: **309** string
patches, **205** of `simulation_service_ray.get_settings` and **93** of
`simulation_service_ray.boto`. Code moved to another file looks those names up in *its own*
module, so the patches stop reaching it — silently. A naively moved `_submit_container` runs
with real settings and a **real boto client** while its test still passes. The suite has no
guard against reaching AWS today. So:

- **P2.0 — make it safe to move (no code moves).**
  (a) An autouse test guard: creating a real boto3 client or session in a unit test fails
  loudly; the genuine integration tests opt out with a marker.
  (b) One seam: `viva_api/simulation/ray/_seams.py` owns `get_settings` and `boto`; this file
  reads them through it; the 298 patch strings are retargeted there mechanically, in one PR,
  while the code is still in one place — so the PR proves the patches still bite (break the
  seam, the tests must fail).
  (c) Smoke **Tier 2** and **Tier R** (§8): this is the first phase that can break dispatch.
- **P2.1 — carve, move-only, one concern per PR**, leaves first: config interpretation →
  Batch engine (**straight into `viva_core`** as `BatchJobClient`, composed, not inherited) →
  tasks → build → ParCa → analysis → Nextflow → mbp-tracked → MNP → chain. The SMS pieces
  start as **mixins** of `SimulationServiceRay`: `self.` keeps working, private-method names
  that tests and `compose` reach for (`_parca_command` ×34, `_seed_generation_command` ×15,
  `_submit_container`, `_submit_mnp`, …) stay valid, and each PR is a pure move that a reviewer
  can verify by diff. `simulation_service_ray.py` ends as the facade and re-exports.
- **P2.2 — mixins become strategies.** A `DispatchStrategy` Protocol (`applies`, `submit`,
  `cancel`, `progress`); each mechanism an object with explicit dependencies (`BatchJobClient`,
  layout, settings) instead of `self`; `submit_ecoli_simulation_job` shrinks to a router.
  Tests move from patching module globals to passing fakes — the smell P2.0 only contained.
- **P2.3 — the rest of the backends** (was P2b), **starting with the core runtime image**:

  - **The core runtime image.** A small reference environment that is not
  any application's science image: Python slim + process-bigraph + pbg-emitters + the Batch
  container entrypoint (stage-in / stage-out contract) + the env-worker module. A few hundred
  MB, built by CI from this repo (`Dockerfile-core-runtime`), pushed to its own ECR/ghcr
  repository, versioned with `viva_core`.
  *Why first:* tasks, compose and env workers take no image parameter today — all three
  hard-wire `<ecr>/v2ecoli:<commit>` — so even a plumbing smoke test pulls a **5.74 GB**
  (compressed) science image. Measured 2026-09-19 on dev: a Tier 1 task spent **320 s** in
  queue + instance scale-up + image pull and **2 s** running. The explicit `EnvironmentRef`
  below is what lets a request name this image; the image is what gives it something small
  to name. Once it exists: Tier 1 smoke runs on it by default, `tests/core/` gets a real
  non-application environment, and the public core has a default environment that carries no
  domain code. (Instance scale-up from zero remains; only the pull shrinks.)
  - **Then:** K8s, SLURM and LOCAL adapters behind `JobBackend`; `batch_build.py` →
  `backends/build.py`; `LocalTaskService` over a `JobStore` Protocol; compose stops calling
  Ray privates; tasks and env-worker take an explicit `EnvironmentRef` with SMS supplying
  the defaults; a minimal `CoreContainer`.

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

`core.environment`-shaped table (in `public` until P7), the `BuildRecipe` registry, build
jobs as core jobs; the SMS simulator build path delegates to them (rows and
`/core/v1/simulator/*` unchanged). Compose resolves images through `EnvironmentRef`; its
`analysis` writes, ParCa staging and `analysis_options` move behind an SMS post-completion
hook. `BackgroundTask` dispatch → lease-based dispatch with orphan reconcile (the #414
pattern). `/curated/ecoli` → SMS.

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

1. Is `/viva/v1` the right public prefix for core?
2. May migrated compose rows get new job ids (old one kept in `legacy_compose_id`)?
3. `/api/v1/tasks`: a permanent SMS facade, or deprecated in favour of `/viva/v1/tasks`?
4. How urgent is #656 — take the fast lane (the Batch-engine and tasks cuts of P2.1, then P4)?
5. Core's bus: Redis + the outbox (leaning this way, and deleting the dead `compose_nats_*`
   settings), or NATS?
6. Does the public hosted core get its own database? It would confirm the soft-reference
   policy early.
7. The core CLI's name (`viva`?), and whether `atlantis` delegates its generic verbs to it
   or stays independent.

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
| — | `a1c3e5f7b9d2`, `44335812e447` (and #661's `b2f6d8e0a4c7`): enum `ADD VALUE` | **no** — Postgres cannot drop an enum label; the downgrade is a documented no-op. Benign: an unused label |
| P0 second wave | `env_worker_task.owner_instance` | yes — drop column |
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
| B | P0 second wave | `create_all` off and the FRESH path changed — how every database bootstraps | alone; `--analyze` per site; migration Job; boot against an already-migrated DB |
| C | P1b + P2.0–P2.1 | core's first settings object; the Batch submit path moved | every dispatch path: Ray MNP sim, container analysis, task, compose, image build, Nextflow head |
| D | P2.2–P2.3 | strategies; env-worker and task image resolution | workbench through the relay; `vwb smoke`; `atlantis worker`, `task` |
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
| 1 | minutes, cents | one tiny real dispatch per mechanism: a container **task**; a relayed env **worker** (a K8s Job) + a task on its task tier, always stopped; a five-step **composite** that must return 1.1^5; opt-in: a standalone **analysis** (`--simulation-id`), a **BioModels** run (`--biomodel`) |
| 2 | tens of minutes, dollars — *not built yet* | a Ray multi-node simulation with analysis; a 2x2 chain dispatch; a Nextflow head; the vEcoli qualification script; an image build |
| R | *not built yet* | a pod restart with a Tier 1 job in flight: status still resolves; the terminated pod's log shows the shutdown order |

Required: **A** = 0 + `task`. **B** = 0 + 1. **C** = 0 + 1 + 2 (P2.1 is the first change that
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

## Status ledger

| Phase | PRs | Version | Dev | Prod | Notes |
|---|---|---|---|---|---|
| P-1 | #679 | — | — | — | merged 2026-09-18 (`21bd7296`); docs only |
| P0 (first wave) | #680 import-linter contracts · #681 `set_messaging_service` · #682 reconciler `current_schema()` · #683 shutdown stops pollers · #684 kustomize by-name patches | 0.9.145 | **2026-09-18** (checkpoint A) | — | **merged 2026-09-18** (`ca67b43f`, `2f6d73b1`, `f99caa02`, `7f3f6777`, `86c5f292`); combined `main` verified: `make check` ×2, 378 tests. Not yet deployed — #681–#683 change runtime code and go out with the next version bump; #680 and #684 change nothing that runs |
| P0 (after #661) | #637 FRESH fix + `create_all`-vs-migrations parity test · `DB_CREATE_ALL` guard · `owner_instance` column scoping the env-worker boot sweep | | | | not started — each adds or tests a migration, so they wait for #661 to keep the chain at one head |
| P1a | #686 `viva_core/` skeleton, enforced `core-is-standalone`, `tests/core/`, first nine modules | 0.9.145 | **2026-09-18** (checkpoint A) | — | merged 2026-09-18 (`8c9f8e78`); marker `/app/viva_core/models.py` confirmed on the newest pod |
| P1b | #691 `viva_core.settings` (`CoreSettings` + provider); `storage/*`, `infra/ssh`, `backends/{slurm_service,nextflow_trace}` moved; `config` ⇄ `file_paths` cycle gone | rides checkpoint C | | | merged 2026-09-19 (`c9fa2bd5`), not deployed |
| P2.0 | (a) test guard vs real AWS — **this PR**; (b) `_seams` + retarget 298 patches; (c) smoke Tier 2 + R | test-only | — | — | (a) open; (b), (c) not started |
| P2.1 | carve `simulation_service_ray.py`, one concern per PR (Batch engine → core) | | | | not started |
| P2.2 | mixins → `DispatchStrategy` objects; router | | | | not started |
| P2.3 | core runtime image; K8s / SLURM / LOCAL adapters; `EnvironmentRef` | | | | not started |
| P3 | | | | | |
| P4a | | | | | |
| P4b | | | | | |
| P5 | | | | | |
| P6 | | | | | |
| P7a / b / c | | | | | |
| P8 | | | | | |
| P9a / b / c | | | | | |
| P10 | | | | | |

## Decision log

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
  whole ParCa cache first — both go away with P2b's runtime image and P5's decoupling.
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
