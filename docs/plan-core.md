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

`common/{ssh,messaging}`, `storage/file_service*`, `gcs_aio`, `hpc/*`, `common/models.py`,
`events_env.py`, `dispatch_validation.py`, `api/{auth,oidc}.py`, `simulation/chrome_trace.py`
→ `viva_core/`. Old names keep working through a table-driven `sys.modules`-aliasing finder
(the `sms_api` shim pattern) — not `import *` stubs, which break patch targets. Break the
`config` ⇄ `file_paths` cycle. Flip the core ↛ viva_api contract to **enforcing**. Start
`tests/core/`.

Deploy: app only. Verify: `pytest tests/common tests/api tests/core`, import-linter. Risk: low.

### P2 — Backend extraction

- **P2a.** `BatchJobClient` → `viva_core/backends/batch.py`; `SimulationServiceRay`
  delegates. Move-only PRs.
- **P2b.** K8s, SLURM and LOCAL adapters behind `JobBackend`; `batch_build.py` →
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
  core shape, in P4b. If it becomes urgent, P2a + P4 can run ahead of P2b / P3.
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
4. How urgent is #656 — take the P2a → P4 fast lane?
5. Core's bus: Redis + the outbox (leaning this way, and deleting the dead `compose_nats_*`
   settings), or NATS?
6. Does the public hosted core get its own database? It would confirm the soft-reference
   policy early.
7. The core CLI's name (`viva`?), and whether `atlantis` delegates its generic verbs to it
   or stays independent.

## 8. Verification, every phase

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
| P-1 | this PR | — | — | — | docs only |
| P0 (first wave) | #680 import-linter contracts · #681 `set_messaging_service` · #682 reconciler `current_schema()` · #683 shutdown stops pollers (stacked on #681) · #684 kustomize by-name patches | — | — | — | open, unmerged; none needs a deploy on its own |
| P0 (after #661) | #637 FRESH fix + `create_all`-vs-migrations parity test · `DB_CREATE_ALL` guard · `owner_instance` column scoping the env-worker boot sweep | | | | not started — each adds or tests a migration, so they wait for #661 to keep the chain at one head |
| P1 | | | | | |
| P2a | | | | | |
| P2b | | | | | |
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
- **2026-09-18** — D9: BioModels and the curated simulators go to core, not SMS, and are
  later factored out into the reproducible-biology hosted-services application. The earlier
  draft already placed them in `viva_core.contrib.sysbio`; what changed is that the
  destination is now named, and the "public API only" contract is a requirement of the move
  rather than a nicety.
