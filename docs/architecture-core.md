# Architecture: viva-api today, and the core / SMS split it is moving to

> **Living document.** Part 1 describes the code as it is, Part 2 the architecture we are
> moving to, and Part 3 tracks every seam between the two. The migration itself — phases,
> ordering, risks, status — is in [`plan-core.md`](plan-core.md).
>
> **Maintenance rule.** Any PR that implements a step of `plan-core.md` and changes
> structure updates this file in the same PR: move the item from Part 2 to Part 1, and
> flip its row in Part 3. A refinement of the target is recorded as a dated entry in the
> decision log of `plan-core.md`, not as a silent rewrite here.
>
> **Snapshot.** Part 1 was surveyed at `main` `8c597736` (2026-09-18) and the endpoint
> counts re-checked at `c0ea8c0b` (after #678, the BioModels consolidation). §1.3, §2.3 and
> Part 3 were re-audited at `a10ac6cf` (2026-09-20; plan-core decision log). Line numbers
> drift; module and function names are the durable reference.

## Why this document exists

viva-api began as the E. coli whole-cell API. It has since grown three capabilities that
are not about E. coli at all, and that other viva-* projects want:

- running containerized process-bigraph composites on Ray or Nextflow (`/compose/v1`),
- running a script inside a container (`/api/v1/tasks`),
- interactive requests to a process-bigraph Python environment (`/env-worker/v1`, with the relay).

Two more are close behind: a registry of the datasets runs actually write
([`plan-data-provenance.md`](plan-data-provenance.md), PR #661) and provenance for tasks
(`plan-task-provenance.md`, PR #656).

The goal is two services:

1. a **core** that is domain-neutral, **standalone for every service it provides**, reusable
   by other projects, and eventually hosted publicly for the vivarium-collective ecosystem;
2. an **SMS service** that keeps every existing endpoint for sms-ecoli, v2ecoli and vEcoli,
   and uses the core to drive third-party simulators wrapped in process-bigraph.

---

# Part 1 — Current architecture

## 1.1 One app, one process

`viva_api/api/main.py` builds a single `FastAPI(title="sms-api")`. No `root_path`; the
image runs `uvicorn viva_api.api.main:app` on port 8000. The only middleware is CORS.

| Prefix | Router | Operations | Side |
|---|---|---|---|
| `/api/v1` | `routers/sms.py` | 29 | SMS |
| `/api/v1/tasks` | `routers/tasks.py` | 4 | generic mechanism, SMS implementation |
| `/core/v1` | `routers/core.py` | 8 | **SMS** — simulator build and ParCa. The name is historical. |
| `/compose/v1` | `routers/compose.py` | 16 | mostly generic |
| `/env-worker/v1` | `routers/env_worker.py` | 30 | generic |
| `/`, `/home`, `/health`, `/version` | `main.py` | 4 | shared |
| `/ws` | marimo ASGI mount (`app/ui/*`) | — | SMS notebooks |

91 operations in the generated spec. `sms` and `tasks` share the `/api` prefix, so a path
prefix does not separate them.

**Lifespan.** `dependencies.init_standalone()` builds every singleton in one pass — file
service, the simulation-service registry, Postgres (and `create_db`), SSH, SLURM, Redis
messaging, the `JobScheduler`, the compose subsystem, the env-worker task tier and relay
`TaskRunner`, the `EnvWorkerService`. The lifespan then starts `JobScheduler` polling every
5 s and **raises if the scheduler is missing**, so the app cannot boot without the SMS
scheduler.

**Dependency injection is module globals.** Handlers call getters in
`viva_api/dependencies.py` inline. The compose and env-worker routers hold their services
in router-module globals, set by setters that `dependencies.py` calls *by importing the
routers* — so wiring depends on routers and routers on wiring.

## 1.2 Endpoint inventory, by which side of the split it belongs to

### `/api/v1` — SMS (29)

Simulations (submit, list, get, status, chain-progress, events, tasks, cancel, log, data,
tags, discovery, workspace, observables), ParCa caches (`new-gene-cache`, `variant-cache`),
analyses (submit, list, get, status, log, plots, data, per-simulation listing, figures).
Models in `simulation/models.py` and `analysis/models.py`; logic in
`common/handlers/{simulations,analyses}.py`.

Two of these expose engine-generic infrastructure under an SMS URL:
`/simulations/{id}/events` and `/simulations/{id}/tasks` read the `hpcrun_event` /
`hpcrun_span` store and the Nextflow trace — neutral by design, but keyed on SMS rows, so
compose and task runs cannot use them.

### `/core/v1` — SMS (8)

`capabilities`, `simulator/{latest,versions,status,upload}`,
`simulation/parca{,/versions,/status}`. `simulator/upload` is gated by the `RepoUrl`
allow-list (`verify_simulator_payload`), routed by `config.compute_backend_for_repo`, and
takes vEcoli-specific build parameters (`stage_private_fork`, `vecoli_private_commit`,
`include_submit_image`). `capabilities` is one registry mixing both sides' names.

### `/api/v1/tasks` — generic mechanism, SMS implementation (4)

`POST /tasks`, `POST /tasks/upload`, `GET /tasks/{id}/status`, `GET /tasks/{id}/logs`.
Implemented by `TaskService` (`simulation/dispatch/tasks.py`, a composed service since #714),
reached as `simulation_service.tasks`; the router still type-checks for
`SimulationServiceRay`. **There is no image or repo parameter**: the image is always
`<ecr>/{ray_ecr_repository = "v2ecoli"}:<commit>`, paths sit under `/app/v2ecoli`, the
input field is `sim_data_refs`, and an omitted commit resolves through the default E. coli
repo. The `task` table is in the SMS ORM base. Status is refreshed only when read.

### `/compose/v1` — mostly generic (16)

Run a composite (`simulation/run`, `run-document`), status (single and batch), results,
document, build status, the registry (`simulators`, `processes`, `steps`), curated
templates (`copasi`, `tellurium`, `ecoli`), and BioModels (`identifiers`, `metadata`,
`run`).

Where it reaches into SMS:

- `simulator_id` resolves against the SMS `simulator` table.
- `ComposeSimulationServiceRay` is **handed** a Batch layer (its own `ComposeBatch` Protocol;
  `dependencies.py` provides SMS's `BatchLayer`). Until P2.1 PR 6 it instantiated a whole
  `SimulationServiceRay` to call five Batch methods and one status lookup on it.
- It stages a ParCa cache (`RayLayout.parca_cache_uri`) and imports `V2ECOLI_DIR`,
  `ANALYSIS_OUT_DIR`.
- `analysis_options` chains a v2ecoli analysis job and **writes a row into the SMS
  `analysis` table** (via `common/analysis_dag`, `simulation_id = NULL`).
- `/curated/ecoli` is E. coli; `compose_pbg_core_builder` defaults to `v2ecoli.core:build_core`.
- Dispatch runs in a FastAPI `BackgroundTask`: the row says RUNNING before anything is
  submitted, and a pod death strands it. There is no orphan reconcile for compose.

**How a composite and its environment are specified today — two models, on two endpoints:**

| | the composite | the Python environment |
|---|---|---|
| `/compose/v1/simulation/run` (upload) and `/run-document` (JSON body) | **supplied by the client** — a process-bigraph document (or an OMEX). Its processes are *addresses* (`local:<registered name>`) resolved inside the job by a core built from `compose_pbg_core_builder`; the composite itself need not be in the image | optional **`simulator_id`** → a commit → `<ecr>/v2ecoli:<commit>`. Omitted, every compose run on the site uses one image pinned by `compose_ray_image_tag`. `extra_pip_deps` adds packages, gated by `compose_allow_list`. (The SLURM path instead *builds* a Singularity image from the packages the document needs — `compose_simulator`, keyed by the definition's hash) |
| `/api/v1/simulations` with `extra_params.multi_node_dispatch` / `nextflow_dispatch` | **already in the container** — the request carries a `composite_id` (e.g. `v2ecoli.composites.lineage_ray_batch`) plus `params` overrides; `run_pbg.py --composite-id … --overrides …` builds the document from a composite generator the image registers | **`simulator_id`, required** — it decides both which code and which composites exist |

Three weaknesses follow. `simulator_id` points at the SMS `simulator` table (the code calls it
"the LEGACY simulator registry"). The default environment for a document is a site-wide pinned
science image. And "run this document" and "run the composite this image knows, with these
overrides" are one idea split across two endpoints — one of them an SMS endpoint.

BioModels (consolidated by #678 into `identifiers`, `metadata` and one `/biomodels/run` that
runs one or more models through one or more simulators) and the COPASI / Tellurium templates
are **not SMS**. They are the seed of a separate application — reproducible-biology hosted
services — and today they are already a clean consumer: `compose/biomodels_service.py` and
`compose/biomodel_documents.py` import nothing from the rest of `viva_api`, and submit runs
through the ordinary compose path. The router still builds the process-bigraph document
inline and fetches from BioModels synchronously in the handler.

### `/env-worker/v1` — generic (30)

Dial-back workers, relayed workers (`/relay/workers`, `/call`, and the named capability
routes), and the durable task tier (`/tasks`). Code in `compose/env_worker_service.py`
(depends only on `K8sJobService` and settings) and `compose/env_worker_relay.py`; request
and response models are defined inside the 1,168-line router.

Leaks: the image is built from `ray_ecr_repository`; `env_worker_workspace_path` defaults
to `/app/v2ecoli`; `composite-state/from-config` translates a vEcoli-style config;
`study_precheck` is workbench vocabulary. It carries the **only authorization rule in the
API** — a task can be cancelled only by the caller who created it.

## 1.3 Service and compute layer

**Backend selection** is `COMPUTE_BACKEND`, plus which backends are configured
(`k8s_job_namespace` → `SimulationServiceK8s`; `ray_mnp_queue` → `SimulationServiceRay`;
`slurm` → `SimulationServiceHpc`), plus a per-request lookup by repo URL
(`config.compute_backend_for_repo`, which hard-codes the `RepoUrl` enum).
`deployment_namespace` is informational only.

**The generic Batch engine lived inside the E. coli service; since P2.1 cut 2 it is in core.**
`simulation/simulation_service_ray.py` was 5,019 lines with the engine inside it. The engine
is now `viva_core/backends/batch.py` — `BatchJobClient` (`ensure_mnp_job_definition`,
`ensure_container_job_definition`, `submit_mnp`, `submit_container`, `job_statuses`,
`job_details`, `describe_job`, `job_definition_log_group`, `terminate`, `terminate_matching`),
`SubmitJobPacer`, `BatchJobDetail`, `stage_out_env`, `ecr_image_uri`. It takes **no
settings**: every queue and base job definition is an argument and the boto3 client comes
from a factory. SMS's half of the seam — read settings through `_seams`, choose the queue, add this
application's env entries — is `BatchLayer` (`simulation/dispatch/batch_layer.py`):
`image_uri`, `submit_image_uri`, `ensure_container_job_def`, `ensure_mnp_job_def`,
`submit_container`, `submit_mnp`, `resolve_log_group`, `get_batch_job_statuses` / `_details`,
`engine()`, `client()`. A base class from cut 3 until P2.1 PR 5; now **composed**: one
instance, `service.batch`, for the service's lifetime, handed to everything that submits.
Consumers take the narrowest of two SMS Protocols, `ContainerSubmitter` and `MnpSubmitter`.
The service keeps `get_job_status` / `cancel_job` (its own interface) and two delegates,
`get_batch_job_statuses` / `_details`, because the scheduler still asks the service (until P6).

**What `SimulationServiceRay` is as of 2026-09-20** (`SimulationServiceRay(SimulationService)` —
it inherits nothing from `simulation/dispatch/`; the file is **628** lines, from 5,019 — its interface, five strategy builders, two composed services, and a named, test-pinned list of delegates the scheduler still needs):

| Piece | Where | Shape |
|---|---|---|
| config interpretation | `dispatch/config_interpretation.py` | pure functions |
| in-image paths | `dispatch/image_paths.py` | constants, no imports |
| analysis specification (which modules, which memory class) | `dispatch/analysis_spec.py` | pure functions; shared with the Nextflow handler and `scripts/cd2_nextflow_dispatches.py` |
| SMS's half of the Batch seam | `dispatch/batch_layer.py` (`BatchLayer`; Protocols `ContainerSubmitter`, `MnpSubmitter`) | composed object, `service.batch` |
| where ParCa caches live, and the commands that build them | `dispatch/parca_spec.py` | pure functions (one reads two ParCa settings through `_seams`); every mechanism calls them |
| the three ParCa cache jobs | `dispatch/parca.py` (`ParcaService`, takes a `ContainerSubmitter`) | composed service, reached as `service.parca` |
| image build | `dispatch/build.py` (`ImageBuilder`) | composed service |
| tasks | `dispatch/tasks.py` (`TaskService`, `TaskBatch` Protocol) | composed service |
| recording a dispatch's own run row | `dispatch/run_records.py` | a function (it never used `self`); shared by the mechanisms that submit a job ahead of the one they return |
| dispatch mechanism: **mbp-tracked** | `dispatch/mbp_tracked.py` (`MbpTrackedStrategy(batch)`, `mbp_tracked_command`) | **strategy object**, handed a `ContainerSubmitter` and nothing else — the first of five |
| dispatch mechanism: **Nextflow** | `dispatch/nextflow.py` (`NextflowStrategy(batch, k8s, stage_runner=)`, `NextflowBatch` Protocol, five functions, the resource defaults) | **strategy object**; handed two image names + the engine, the K8s backend, and a runner stager. Owns `reap_cancelled_campaign`; the service keeps a delegate for the scheduler |
| dispatch mechanism: **multi-node composite** | `dispatch/multi_node.py` (`MultiNodeCompositeStrategy(batch, stage_runner=)`, `MultiNodeBatch` Protocol, three functions) | **strategy object**; owns its analysis submitter, its founder-cache staging and the vCPU lookup that sizes `RAY_SHARDS_DEFAULT`; the service keeps `submit_multi_node_analysis` as a delegate for the scheduler |
| dispatch mechanism: **ensemble** (ParCa MNP job, then the simulation MNP job; the single-generation run and the two-engine comparison) | `dispatch/ensemble.py` (`EnsembleStrategy(batch, stage_runner=)`, `sim_command`) | **strategy object**, handed an `MnpSubmitter` and a runner stager. It was the router's fall-through tail, not a method |
| dispatch mechanism: **chain** (ParCa, then one container job per seed per lineage, chained by `dependsOn`; and the campaign's analysis) | `dispatch/chain.py` (`ChainStrategy(batch, local)`, four functions) | **strategy object**, handed a `ContainerSubmitter` and the in-process task service that runs its submission loop in the background. Not a Ray mechanism |
| the runner environment (`PBG_RUNNER_ENV`) and the baseline composite id | `dispatch/runner_env.py` | leaf constants, shared by three mechanisms |
| what is left on the class | `simulation_service_ray.py` | `SimulationService`'s interface (9 methods), the router, five strategy builders, `parca` / `tasks`, nine one-call delegates for the scheduler (until P6), and four pieces of progress / cancel / staging (`get_chain_campaign_result`, `cancel_chain_campaign`, `cancel_companion_jobs`, `stage_runner`) |

**Five dispatch mechanisms, none of which calls another** (measured 2026-09-20). The router
`submit_ecoli_simulation_job` is 308 lines, of which ~20 are routing and **216 are a fifth,
unnamed mechanism done inline**: the *ensemble* path (one ParCa MNP job, then one simulation
MNP job that depends on it). Precedence: `nextflow_dispatch` → `mbp_dispatch` →
`multi_node_dispatch` → (no composite and more than one generation ⇒ chain) → ensemble.
Sizes: chain 848 lines, Nextflow 464, multi-node composite 346, ensemble 336, mbp-tracked
225. What they share is small: `stage_runner`, `_record_run_with_companions`,
`chain_base_tags`, and the Batch and ParCa layers. Per-mechanism *progress* is not in this
class at all — it is ~600 lines of `job_scheduler.py` keyed on `HpcRun` columns — and
*cancel* is routed by `job_id.backend` here and by `chain_final_job_ids` in the handler.

**`SimulationServiceK8s`** (`simulation/simulation_service_k8s.py`, 624 lines) is a second,
separate service: the upstream-vEcoli path, a Nextflow head as a K8s Job whose tasks run on
Batch. It is the **default backend on both Stanford sites** (`COMPUTE_BACKEND=batch`; the Ray
service is registered beside it and takes the v2ecoli / sms-ecoli repos). It has its own
image-build commands, duplicating the Ray builder's, and both standalone-analysis entry
points. It shares `batch_build.py` with the Ray builder.

| Dispatch path | Submitted as | Status | SMS content |
|---|---|---|---|
| SLURM (`SimulationServiceHpc`) | sbatch over SSH, Singularity image | `JobScheduler` squeue/scontrol | all of it |
| K8s + Nextflow (`SimulationServiceK8s`) | DooD Batch builds, then a K8s Job `nf-<exp>` | resolved on read | command, `ECOLI_SOURCES`, sim-data paths |
| Ray multi-node (`_submit_mnp`) | Batch MNP, `dependsOn` chain | `describe_jobs` + scheduler pass | the stage-out env |
| Batch container (`_submit_container`) | single-container Batch job | same | the stage-out env |
| Chain dispatch | one job per seed per generation | `update_chain_campaigns` on `hpcrun.chain_*` | all of it |
| Nextflow head from Ray | `compose/render_nf.py` → K8s Job head | trace CSV via `common/hpc/nextflow_trace.py` | composite id, caches |
| Image build | DooD Batch job driven by `LocalTaskService` | `reconcile_local_tasks` | recipe and repo names |

`run_pbg.py` and `render_nf.py` are read with
`importlib.resources.files("viva_api.compose")` and staged to S3 for jobs — **the module
path is part of the deployment contract.** `run_pbg.py` is generic except a v2ecoli parquet
emitter override and a baseline-id list.

**`JobScheduler`** (`simulation/job_scheduler.py`, 1,370 lines) runs seven passes every 5 s.
The generic ones — SLURM poll, local-task reconcile, event ingest, Nextflow-head trace, MNP
status — are tangled with chain-campaign gating and analysis fan-in, and the constructor
takes a `SimulationServiceRay`. `ComposeJobMonitor` is a second, parallel 30 s loop with its
own status enum.

## 1.4 Package structure

*Since P1a* there is a second top-level package, `viva_core/` (`models`, `infra/messaging`,
`events/events_env`, `backends/{job_service,k8s_job_service,models,nextflow_weblog}`; since
P1b also `settings`, `storage/*`, `infra/ssh`, `backends/{slurm_service,nextflow_trace}`; since
P2.1 `backends/batch` — the first core module that was *extracted* rather than moved; since
P2.3a `environments/{model,registry_resolver}` — the first that is *new*: D10's vocabulary and the
one resolver, which nothing calls yet), which
imports nothing from `viva_api` or `app`. The old `viva_api.common.*` paths for those modules
are self-replacing stubs — same module object under both names. The graph below is otherwise
unchanged: everything still imports them through the old names.

```
app (cli / tui / gui) ──► common.handlers, config, simulation, common.storage
        ▲                        │
        └────────────────────────┘   cycle: handlers import app.cli_theme / app_data_service

api.routers.{sms,core,tasks} ──► simulation, analysis, common.*, dependencies
api.routers.compose          ──► compose
api.routers.env_worker       ──► compose
        ▲
dependencies ──► api.routers.{compose,env_worker}, simulation, compose, common.*   (inverted)

compose            ──► simulation (ray.image_paths only, since P2.1 PR 6), dependencies, common.*
compose.env_worker ──► common.hpc, config                                          (clean)
simulation         ──► common.*, dependencies, analysis, compose (resources + schema_diff)
common.handlers    ──► simulation (18 imports), analysis, app, dependencies
```

**`viva_api/common/` is not neutral.** Truly generic: `ssh/`, `messaging/`,
`storage/file_service*`, `storage/gcs_aio`, `hpc/{slurm_service,k8s_job_service,job_service,nextflow_trace,nextflow_weblog,models}`,
`models.py`, `events_env.py`, `dispatch_validation.py`. SMS application code under a generic
name: `handlers/*` (3,300 lines), `simulator_defaults.py`, `storage/data_layout.py`,
`analysis_dag.py`, `gateway/utils.py`, `utils.py`, `capabilities.py`, `s3_streaming.py`.

Cycles: `config` ⇄ `common.simulator_defaults`; ~~`config` ⇄ `common.storage.file_paths`~~ (gone with P1b);
`dependencies` ⇄ `api.routers`; `common.handlers` ⇄ `app`; `simulation` ⇄ `compose`.

## 1.5 Persistence

Two declarative bases, **one engine, one database, the `public` schema**.

`Base` (`simulation/tables_orm.py`): `simulator`, `parca_dataset`, `simulation`, `hpcrun`,
`hpcrun_event`, `hpcrun_span`, `worker_event`, `analysis`, `task`.
`ComposeBase` (`compose/tables_orm.py`): `compose_simulator`, `compose_packages`,
`compose_simulator_to_package`, `compose_bigraph_compute`, `compose_allow_list`,
`compose_simulation`, `compose_hpcrun`, `compose_worker_event`, `env_worker_task`.

```
 simulator ◄── parca_dataset ◄── simulation ◄── analysis.simulation_id (nullable)
     ▲              ▲                ▲
     └─ jobref_simulator_id          │
                    └─ jobref_parca_dataset_id
                                     └─ jobref_simulation_id
                               [ hpcrun ] ◄── hpcrun_event, hpcrun_span, worker_event
 task (no FKs)

 compose_packages ◄── compose_simulator_to_package ──► compose_simulator ◄── compose_simulation
 compose_allow_list                              compose_hpcrun ◄── compose_worker_event (CASCADE)
 env_worker_task (no FKs)
```

**`hpcrun` is one table doing four jobs**: the generic job record (type, correlation id,
backend job id, status, error, attempt, `external_job_ids`); the hub of the SMS foreign
keys (`jobref_*`); the chain-campaign state machine (`chain_*`, `stage`, `generation`); and
the trace root (`trace_id`, `events_cursor`, …). The foreign keys are never joined in SQL —
everything is stitched in Python through `HpcRun.ref_id` — so cutting them costs only
referential integrity. The real coupling is the scheduler dereferencing `ref_id`.

**`compose_hpcrun` duplicates `hpcrun`**, with an 11-value status enum against 7, a unique
`correlation_id` against an indexed one, and ids that are client-visible (`hpcrun_id`) and
collide numerically with `hpcrun.id`. `env_worker_task.status` is deliberately VARCHAR — the
recorded lesson is that `create_all` and Alembic disagree about Postgres enum labels.
`worker_event.mass` (NOT NULL) is an E. coli observable baked into a generic event row.

**Migrations.** One linear Alembic chain; `alembic/env.py` targets only `Base.metadata`, yet
three revisions hand-write compose DDL. A fresh `alembic upgrade head` does not work (#637):
no revision creates `analysis` or most `compose_*` tables. Every real database works
because the app runs `create_all` on both bases at every boot.
`simulation/db_reconcile.py` classifies a database (FRESH / MANAGED / LEGACY / INCONSISTENT)
by walking `LEGACY_FINGERPRINTS` — one marker per revision, spanning both domains, with no
`table_schema` filter.

**Ids leave the database**: `hpcrun.id` and `simulation.id` appear in correlation ids, SLURM
directory names and client-visible ids. S3 is keyed by `experiment_id` strings and commits.

**Other state.** Redis is one pub/sub channel (`worker.events`), nothing durable. Compose
was written for NATS; those settings are dead. Process-local state that shadows the
database: the relay's socket registry and per-worker queues, `LocalTaskService`'s task
maps, hit-only correlation-id caches. The boot sweep `fail_unfinished_tasks` is unscoped —
a second process booting would fail the first one's tasks (scoping it needs a column;
P0 second wave). *Changed by P0 (#683):* shutdown now stops the scheduler, the
`ComposeJobMonitor`, the relay `TaskRunner` and the relay sockets **before** the engine is
disposed; previously the last three were never stopped.

Tests use Postgres testcontainers with `create_db`; they never run Alembic except in the
dedicated migration tests.

## 1.6 Configuration

One flat `Settings` class, no `env_prefix`. Generic infrastructure keys (`storage_*`,
`postgres_*`, `slurm_*`, `redis_*`, `batch_*`, `ray_*` queues, `k8s_job_namespace`,
`events_*`, `env_worker_*`, `oidc_*`, `compose_*`) are interleaved with SMS keys (`hpc_*`
paths, `vecoli_config_dir`, `biocyc_*`, `ecoli_sources_*`, `ray_parca_*`). Several are
generic in role and SMS in default: `ecr_repository = "vecoli"`,
`ray_ecr_repository = "v2ecoli"`, `s3_output_prefix = "vecoli-output"`,
`env_worker_workspace_path = "/app/v2ecoli"`.

## 1.7 Packaging and deployment

One distribution, `viva_api`, whose wheel ships `viva_api`, `sms_api`, `app` and `tests`;
one flat dependency list mixing server, clients (marimo, textual) and science
(biosimulators-utils, libsbml, …). `sms_api/__init__.py` is a meta-path finder redirecting
`sms_api.X` to `viva_api.X`, which keeps `python -m sms_api…` Jobs alive after the rename —
**the precedent for moving modules that are deployment API.**

CI builds one image, `ghcr.io/vivarium-collective/sms-api:<ver>`, gated on the full test
suite and mypy. See [`DEPLOY.md`](DEPLOY.md) for the image version schemes.

Kubernetes: Deployment and Service `api` on 8000, service account `batch-submit` (Jobs,
ConfigMaps, pod logs — **not Services**, which is why the relay exists), ConfigMap
`api-config` from `api.env` + `shared.env`. Overlays patch the `api` Deployment with
**positional** JSON patches (`remove …/env/5`). The `alembic-migrate` Job runs
`python -m viva_api.simulation.db_reconcile --apply` from a tag that must equal the app's.

Routing is by path prefix at the ALB, defined in `../sms-cdk/lib/internal-alb-stack.ts`:
`/openapi.json`, `/home`, `/docs`, `/ws`, `/api`, `/core`, `/health`, `/version`,
`/compose` (priority 82) and `/env-worker` (84) go to `api`; `/workbench` and
`/bigraph-loom` to the workbench; **everything else to PTools**, so an unrouted path is an
HTML 404. A new routed service needs a target group, rules, a security-group rule and a
`cdk deploy`. `ingress.yaml` on the Stanford overlays is dead code.

*Changed by P0 (#684):* the Stanford overlays now strip the SLURM/SSH wiring with a
strategic-merge `$patch: delete` keyed by name; `tests/test_deploy_config.py` forbids
JSON-patching a list by index in a supported overlay. `sms-api-eks` and `sms-api-rke-dev`
still carry one positional remove each.

## 1.8 Clients and consumers

CLI, TUI and GUI all go through `app/app_data_service.py` (`E2EDataService`): hand-rolled
httpx, about 60 literal URLs, **one `base_url`**. The CLI verb groups already follow the
seam — SMS: `simulator`, `simulation`, `parca`, `analysis`; generic: `task`, `compose`,
`composite`, `worker`. The clients import server internals (`simulation.models`,
`common.handlers.simulations`, `dependencies`). The generated client is used only by tests;
`test_no_route_drift.py` pins the data-service URLs to the single spec.

External: vivarium-workbench uses one base URL for all four prefixes and reaches env-worker
at `http://api:8000`; the PTools page (`sms.js`) calls only `/api/v1` simulations and
analyses.

## 1.9 Auth

`api/auth.py` (`resolve_caller`, `require_caller`) and `api/oidc.py` are an identity
**seam, not a security boundary**, used only by env-worker. No route requires a credential.
Three surfaces execute caller-supplied code: `/tasks/upload`, the relay `/call`, and
compose `extra_pip_deps` (guarded only by `compose_allow_list`). No API keys, quotas or
tenancy columns beyond `env_worker_task.created_by`. See
[`plan-authentication.md`](plan-authentication.md).

---

# Part 2 — Target architecture

## 2.1 Principle: core is standalone for every service it provides

`viva_core` runs **without `viva_api` installed or deployed**: its own FastAPI app
(`viva_core.api.app:app`), settings, container, pollers, database schema and Alembic chain
(working from an empty database), OpenAPI spec and auth seam. The SMS service is one
consumer among several. It adds facades, defaults, guardrails and hooks; it never supplies
something core needs in order to function.

Enforced by: an import-linter contract (`viva_core` imports neither `viva_api` nor `app`); a
`tests/core/` suite that boots `create_core_app()` with **no hooks registered**, against a
core-only schema, and exercises every service end to end with stub backends; and a CI job
that installs only core's dependency set. Hooks default to no-ops: a core job with no
registered owner, classifier or transition hook still completes, is traced, and registers
its datasets.

No domain terms in core identifiers — the same rule process-bigraph follows.

## 2.2 Package layout

```
viva_core/
  settings.py  container.py  models.py
  api/        app.py (create_core_app), auth.py, oidc.py,
              routers/{jobs,environments,tasks,composites,workers,datasets,events,capabilities}
  services/   job_service, job_monitor, task_service, compose_service,
              dataset_service, environment_service, hooks
  backends/   base (JobBackend), batch, k8s, slurm, local, nextflow, build
  db/         orm (CoreBase, schema "core"), stores, reconcile, migrations/
  events/     events_env, ingest, chrome_trace
  env_worker/ service, relay, models
  compose/    run_pbg, render_nf, container_def, allow_list, registry
  tasks/      snapshot, …
  datasets/   registry, walk
  storage/    file services, s3 helpers, CoreLayout
  infra/      ssh, messaging
  client/     protocol (CoreClient), inprocess, http, generated/ (OpenAPI client from the core spec)
  cli/        the standalone core CLI, built only on client/generated
  contrib/sysbio/    BioModels + curated COPASI / Tellurium — in core for now, built as a consumer of
                     core's public API so it can be factored out later (see 2.3)
```

## 2.3 The abstractions

**`JobBackend`** — one Protocol, one generic `Job` record.

```python
class JobBackend(Protocol):
    kind: str   # "batch-mnp" | "batch-container" | "k8s" | "slurm" | "local"
    async def submit(self, spec: JobSpec) -> JobHandle: ...
    async def status(self, handles: Sequence[JobHandle]) -> dict[str, BackendStatus]: ...
    async def cancel(self, handle: JobHandle) -> None: ...
    def logs(self, handle: JobHandle, *, tail: int | None) -> AsyncIterator[str]: ...
```

`JobSpec` carries image, command, env, resources, stage-in/out, trace env, labels and an
`OwnerRef`. A Nextflow head is a `k8s` submission composed by `backends/nextflow.py`. The
SMS strain arguments become an opaque `extra_env`. SLURM is a supported core backend.

**Environments: select or build, then run (decision D10).** The application this core is for
accepts a **composite** and either selects a known compatible environment or builds one from
the composite's dependencies. So an environment is more than an exact coordinate:

- **`EnvironmentSpec`** — what is *needed*. Either **explicit** (`repo_url`, `commit`, a
  recipe name: an SMS simulator, a third party's pbg-wrapped simulator) or **derived** (a
  dependency set — packages with source and version — read from the composite's process
  addresses plus any declared extras, each one checked against the allow-list).
  `EnvironmentRef{repo_url, commit, variant}` is the explicit form.
- **`Environment`** — what *exists*. Two identities, because they are different things: the
  **spec hash** (what was asked for) and the **image digest** (what was built). Plus status,
  the build job, and what it **provides** (packages and versions; process addresses).
- **`EnvironmentResolver.resolve(spec or composite) → Environment`** — *select* an
  environment that satisfies the spec, else *build* one through a `BuildRecipe`, and have the
  run wait on it. "Satisfies" starts as an exact spec-hash match.

Tasks, compose and env-worker resolve images only through it — the interoperability point
for third-party simulators. The three positions one could take are points on one line, not
three designs: the **core runtime image** serves a composite that needs only the built-ins
(rarely a useful one); **curated environments** (COPASI, Tellurium, an SMS simulator) are
registered ones the resolver may select; an environment **built per spec** is the default
when nothing registered fits. An uber-container is what you get when the derived spec is
ignored and everything is installed every time.

**Built environments are write-once provenance (decision D11).** A simulator record, its
image and its tag are what every simulation that ran on them points back to. So an
environment, once built, is **immutable and never deleted**: not overwritten, force-rebuilt,
re-tagged or expired. A rebuild is a *new* environment — new image digest, new tag — and the
two identities above are what make that expressible: the spec hash says what was asked for,
the digest says what actually ran.

The one exception is an environment that **says so about itself**: `temporary`, with a
`label` naming who made it and its own marked image tag, `tmp-<commit>-<nonce>`. It may be
overwritten or removed, and it is marked back to the end user everywhere — API, CLI, TUI,
GUI — and left out of every "latest" or default choice. **This part exists today** on the
SMS `simulator` table (`temporary`, `label`, `image_tag`; `SimulatorVersion.environment_key`
keys the image, the job definitions, the ParCa cache and the build job, so a temporary
simulator's writes never enter the authoritative one's namespace), and `force` against a
built simulator answers 409. What is still only care: both ECR repositories are
tag-mutable, dev and prod share one registry, and the image digest is not recorded.

A `BuildRecipe` registry maps a name to a command/env builder and its allowed parameters.
Core ships two generic ones — **`repo-recipe`** (clone a repo at a commit, run its own build
script) and **`python-deps`** (synthesize an image from a dependency set; Apptainer for
SLURM, OCI for Batch) — and an application registers its own. A build is an ordinary core
job. **The SMS `simulator` table and `/core/v1/simulator/*` stay in SMS** (D6), each row an
explicit spec built by `repo-recipe`; the repo allow-list, default repo and repo-to-backend
map stay SMS guardrails.

### 2.3a Select-or-build as it exists today: three times, each partial

| Where | Environment identity | Select | Build | What is missing |
|---|---|---|---|---|
| SMS `upload_simulator` (`common/handlers/simulators.py`) | repo + branch + commit; no unique constraint | exact match | the repo's **own** build script, as a Batch DooD job (`dispatch/build.py`; `simulation_service_k8s.py`) | nothing is derived. It is the only one that **retries a FAILED build** |
| `viva_api/compose` on SLURM (`compose/handlers.py`, `container_def.py`) | md5 of a **synthesized** Apptainer definition: python-slim + process-bigraph + `extra_pip_deps` + the embedded `run_pbg.py` | by hash (`compose_simulator.singularity_def_hash`, unique) | SLURM `singularity build`; the run waits in process, ~30 min at most | the dependencies are the caller's `extra_pip_deps`, **never read from the document**; a FAILED build suppresses every rebuild (#717); editing `run_pbg.py` changes every hash |
| `compose-api` + `pbest` (sibling repos) | md5 of the generated definition | by hash, then pull a published image, else build | a `pbest` Jinja recipe (uv + micromamba; Docker → Apptainer via spython) | `pbest.dependency_resolution.determine_dependencies` parses `python:pypi<pkg[ver]>@module.path` addresses against an allow-list, **then returns empty lists, and nothing calls it**. The live path installs the whole biosimulations registry every time: a 1.3 GB uber-container |
| `viva_api/compose` on Ray / Batch (`compose/simulation_service_ray.py`) | one site-wide pinned tag, or an SMS `simulator_id` | none | none | `extra_pip_deps` are allow-list-checked, hashed into a row and **never installed** (#716) |

Also true of all of them: the allow-list is enforced only in `viva_api/compose` (a
bidirectional substring match); "compatible" never means more than an exact hash; a recipe
hash is not an image identity, since the definitions run `apt upgrade`, fetch `latest` and
resolve pins at build time; and the process registry that would map an address to a package
exists as tables (`compose_package`, `compose_bigraph_compute`) whose only writer has no
caller. The target unifies these; it does not add a fifth.

**The core runtime image.** Core ships a small reference environment of its own — Python
slim, process-bigraph, pbg-emitters, the Batch container entrypoint and the env-worker
module; a few hundred MB — so that nothing core does *requires* an application's image. It
is what the resolver selects for a composite needing only the built-ins, the default for
smoke tests and a standalone or public core, the environment `tests/core/` runs real jobs
in, and the proof that the entrypoint contract (stage-in, run, stage-out, exit code) is
core's and not the science image's. Today there is no such thing: every task, composite and
env worker runs in `<ecr>/v2ecoli:<commit>` (5.74 GB compressed), an image derived four
separate times from the same two settings.

**Storage.** Core owns generic prefixes; every job records an `output_uri`. `RayLayout` and
`NextflowLayout` stay in SMS, which passes URIs in.

**Hooks and the outbox.** `on_job_transition`, `OwnerResolver` (trace baggage → owner),
`ArtifactClassifier` and `WalkSource` (dataset walker), `run_pbg` emitter plugins, namespaced
capabilities. Every transition is also written to a durable `core.job_transition` outbox, so
the in-process hook and the later HTTP feed read one stream.

**One way to run a composite.** `POST /viva/v1/composites` takes an **environment** and
**exactly one** description of the composite:

```jsonc
{
  "environment": {"repo_url": "…", "commit": "…", "variant": "…"},   // an EnvironmentRef; omitted = the core runtime image
  // exactly one of:
  "document":  { "state": { … } },                                     // the composite itself, as a process-bigraph document
  "composite": {"id": "pkg.module.generator", "params": { … }},        // a composite the ENVIRONMENT registers, plus overrides
  "steps": 5, "num_nodes": 1, "extra_packages": [ … ], "owner": { … }
}
```

Both are the same operation — build a composite inside an environment, run it, record what it
wrote — so they are one request, one job record and one results shape. `document` needs only
the *classes* in the environment; `composite.id` needs the environment to register the
generator, which is why the environment is part of the request rather than a site default.
The two SMS dispatch shapes (`multi_node_dispatch`, `nextflow_dispatch`) become SMS facades
that fill in `composite` and the SMS defaults; `/compose/v1/simulation/run` and
`/run-document` stay as aliases that fill in `document`. How the job is *executed* (a
multi-node Ray job, a Nextflow head, a single container) is a separate field of the request —
the execution axis — not a different endpoint.

**BioModels and the curated simulators — in core now, leaving later.** They move to core
with the rest of compose (`viva_core/contrib/sysbio`, mounted by default at the existing
`/compose/v1/biomodels/*` and `/compose/v1/curated/{copasi,tellurium}` URLs), and never to
SMS. Their eventual home is the reproducible-biology hosted-services application, a second
consumer of core alongside SMS. Two rules keep that extraction a lift rather than a
refactor: `contrib.sysbio` uses **only** core's public service API and `CoreClient` — the
same surface an external application would have — and core proper never imports it (an
import-linter contract). Its science dependencies (libsedml, libsbml, biosimulators-utils,
biomodels) are an extra, so a minimal core does not need them.

**`CoreClient`.** A Protocol with an in-process and an HTTP implementation. SMS touches core
only through it and the hook registries.

## 2.4 URLs

| Prefix | Owner | Note |
|---|---|---|
| `/viva/v1/{jobs,environments,tasks,composites,workers,datasets,events}` | core | new canonical surface; root prefix configurable |
| `/compose/v1`, `/env-worker/v1` | core | permanent aliases on the same routers |
| `/api/v1/tasks`, `/api/v1/datasets`, `/simulations/{id}/datasets`, `/analyses/{id}/datasets` | SMS facade | inject SMS defaults, call `CoreClient` |
| `/api/v1/*` (the rest), `/core/v1/*`, `/ws` | SMS | unchanged |

## 2.5 Database

Same database, new **`core` schema**, its own Alembic chain and version table.

- `core.job` — the generic record: identity (`id`, `kind`, unique `correlation_id`,
  `created_by`), owner (`owner_kind`, `owner_id` TEXT), backend (`backend`, `job_id_ext`,
  `external_job_ids`, `environment_id`, `output_uri`), status as **VARCHAR + CHECK**,
  tracing columns, and durable-dispatch columns (`dispatch_state`, `dispatch_owner`,
  `dispatch_lease_at`).
- `core.job_event`, `core.job_span`, `core.job_transition`, `core.worker_event`
  (`observables`, nullable), `core.task`, `core.task_script`, `core.env_worker_task`,
  `core.environment`, the compose registry, `core.dataset`.
- **`public.hpcrun` remains, as the SMS extension row** — primary key = foreign key to
  `core.job.id`. Ids are preserved, the `jobref_*` and `chain_*` query shapes survive, and
  the link lives on the SMS side. It becomes a soft reference before the second Deployment.
- `core.dataset` is the #661 table without the three SMS producer foreign keys: producer is
  `producer_job_id` and/or an opaque `(owner_kind, owner_id)`. The SMS facade derives
  `simulation_id` / `analysis_id` / `parca_dataset_id` for its DTO. No foreign key points
  from core to SMS.

One migration Job, same command, running the core chain then the SMS chain.

## 2.6 Runtime topology

Library first: the SMS app mounts the core routers in-process and calls core services
through `InProcessCoreClient` — URLs unchanged, no ALB work. Then the same image also runs
as a second Deployment (`uvicorn viva_core.api.app:app`): core owns the pollers, the relay
(single replica, or connection affinity) and its prefixes; SMS switches to `HttpCoreClient`,
first proxying the core prefixes, then handing them to the ALB.

## 2.7 A standalone core CLI

Core ships its own command-line client, independent of `atlantis`: it targets **only** the
core API and is built **only** on the OpenAPI client generated from the core spec — no
hand-rolled URLs and no imports from server internals. That makes the generated client a
real, exercised product surface (today it is used only by tests) and gives a standalone
core deployment a first-class tool: environments and builds, composites, tasks, workers,
datasets, jobs and their events.

Working name `viva` (entry point in `viva_core/cli`); the name is an open question in
`plan-core.md`. `atlantis` remains the SMS product CLI; its generic verb groups (`task`,
`compose`, `composite`, `worker`, `dataset`) can later delegate to the same code so the two
do not drift. Not required up front — it lands once the core spec exists (P3) and is part of
what "standalone" means by the time core is deployed on its own (P9).

## 2.8 Public-hosting posture

Per-route authn/authz on the existing seam, `created_by` on every core record, quotas,
image and repo allow-lists defaulting to deny, the three code-execution surfaces closed or
gated, K8s labels parameterised, `VIVA_CORE_` env prefix. Then extraction to its own repo
and PyPI distribution.

---

# Part 3 — Delta table

Status: `planned` → `in progress` → `done (PR, version)`. Phases refer to `plan-core.md`.

| # | Seam | Current | Target | Phase | Status |
|---|---|---|---|---|---|
| 1 | Import boundary | none enforced | import-linter: core ↛ viva_api, app | P0 / P1 | in progress — seven contracts; **enforced:** `core-is-standalone` (P1a, transitive) and env workers + relay (#680); report-only: 5 (**12** direct edges as of 2026-09-20: compose 3, `dependencies.py` → routers 4, server → `app` 3, `config` 1, `local_task_service` 1; a ratchet — the count never rises) |
| 2 | Generic modules | under `viva_api/common`, `api/` | `viva_core/{infra,storage,backends,events,api}` + aliasing shim | P1 | in progress — P1a: `models`, `infra/messaging`, `events/events_env`, `backends/{job_service,k8s_job_service,models,nextflow_weblog}` moved; old paths are self-replacing stubs. P1b: `storage/*`, `infra/ssh`, `backends/{slurm_service,nextflow_trace}` moved |
| 3 | Batch engine | private methods of `SimulationServiceRay` | `viva_core/backends/batch.py` (`BatchJobClient`, composed) | P2.1 | in progress — cut 2: the engine exists in core, settings-free, with its own tests (`tests/core/test_batch_backend.py`); `SimulationServiceRay` delegates through `_batch_jobs()`; `compose` still reaches it via the service's private methods (→ P2.3); not yet behind the `JobBackend` Protocol |
| 4 | Backends | three unrelated shapes in `viva_core/backends/` (`batch.py`, `k8s_job_service.py`, `slurm_service.py`) sharing only `JobStatus` / `JobId` | `JobBackend` adapters: batch, k8s, slurm, local | P5 (was P2.3) | planned — **deferred with a trigger**: a core Protocol with one implementation would quietly be Batch-shaped, so it waits for its second consumer (`compose` on the core seam) and is not final before a second backend implements it |
| 5 | Image resolution | ~~`<ecr>/v2ecoli:<commit>`, derived four separate times from the same settings~~ one place: `viva_api/common/site_environments.py` builds core's `RegistryEnvironmentResolver` from the settings it is handed, and the Batch layer, compose, the env-worker service and the K8s analysis Job ask it | one `EnvironmentResolver`; the *select* half of D10 (§2.3) | P2.3 | **done for the select half** (2.3a the model, 2.3b the rewiring; a guard fails on a fifth hand-rolled derivation). Still by hand, knowingly: the upstream vEcoli path of `SimulationServiceK8s` (another repository, `-amd64-submit`; out of scope until P5). Open: refuse an unset ECR account by name (deferred list); the runtime image (2.3c) |
| 6 | Settings | one flat `Settings` | `CoreSettings` + `SmsSettings`, same env names | P1b / P3 | in progress — P1b: `viva_core.settings.CoreSettings` holds the storage + path-prefix fields; `Settings` inherits them; the application registers a provider so core reads its object. P3 moves the rest |
| 7 | Wiring | module globals, router setters, one `init_standalone` | `CoreContainer` + `SmsContainer`, `create_core_app()` | P3 | planned |
| 8 | OpenAPI | one spec | core spec + SMS spec (SMS = union until P8) | P3 / P8 | planned |
| 9 | Datasets | #661, SMS-shaped, three producer FKs | `viva_core/datasets`, owner refs, SMS facade | P4a | planned |
| 10 | Task provenance | not implemented (#656) | built in core shape: task = core job | P4b | planned |
| 11 | Environments and builds | select-or-build exists three times, each partial (§2.3a): SMS `simulator` (repo + commit, the repo's own recipe), `compose_simulator` (hash of a synthesized definition), `compose-api` / `pbest` (derivation written, unwired) | `core.environment` with two identities (spec hash, image digest) and `provides`; `BuildRecipe` registry (`repo-recipe`, `python-deps`); build jobs; the spec **derived from the composite**; one enforced allow-list; FAILED builds retried | P5 | planned |
| 12 | Compose → SMS reach-ins | simulator table, `analysis` rows, ParCa staging | SMS post-completion hook | P5 | planned |
| 13 | Compose dispatch | `BackgroundTask`, strandable | lease-based dispatch + orphan reconcile | P5 | planned |
| 14 | BioModels / curated | in compose router (`compose/biomodels_service.py`, `biomodel_documents.py`) | `viva_core.contrib.sysbio`, core-public-API only; later factored out to the reproducible-biology application. `/curated/ecoli` → SMS | P1 (modules) / P5 (routes) → extraction after P10 | planned |
| 15 | Monitoring loops | `JobScheduler` + `ComposeJobMonitor` | core `job_monitor` + SMS `CampaignSubscriber` on the outbox | P6 | planned |
| 16 | Job table | `hpcrun` + `compose_hpcrun`, `public` | `core.job` + `public.hpcrun` extension row | P7 | planned |
| 17 | Events and spans | keyed on SMS `hpcrun` | `core.job_event` / `job_span`, `/viva/v1/jobs/{id}/events` | P7 | planned |
| 18 | Alembic | one chain, `create_all` at boot, #637 | two chains, `create_all` off, FRESH works | P0 / P7 | planned |
| 19 | Clients | one `base_url`, server imports | `core_base_url`, DTOs out of server internals | P8 | planned |
| 20 | Deployment | one `api` Deployment | second Deployment, then ALB rules | P9 | planned |
| 21 | Standalone gate | — | `tests/core/` with no hooks, core-only deps | P1 → P5 | in progress — P1a: every core module imports with `viva_api`/`app` blocked; AST scan for domain terms in core constructs; old-name/new-module identity. App boot + per-service tests arrive with P3–P5 |
| 22 | Auth and tenancy | seam only | enforced, quotas, allow-lists | P10 | planned |
| 23 | Core CLI | none; generated client is test-only | standalone CLI on the generated core client | P8 | planned |
| 24 | Runtime image | every task / composite / env worker runs in `<ecr>/v2ecoli:<commit>` (5.74 GB) | a small core runtime image, the default `EnvironmentRef`; Tier 1 smoke and `tests/core/` run on it | P2.3 (first piece) | planned |
| 25 | SMS Ray service | one file of 5,019 lines; one class holding eleven concerns and **five** dispatch mechanisms (four named, plus the router's inline ensemble path), none of which calls another | `simulation/dispatch/`: a facade + a ~20-line router; **one strategy object per mechanism** (ensemble, chain, multi-node composite, mbp-tracked, Nextflow), each owning command builders + submit and handed a composed Batch seam; pure modules for config interpretation, the analysis spec, ParCa commands / URIs; composed services for build, tasks, the ParCa cache jobs. A strategy's progress and cancel halves join it in P6 | P2.0 → P2.1 (P2.2 absorbed) | **done for dispatch** (P2.1 PRs 1–11, 2026-09-21): 5,019 → 628 lines; five strategy objects; the router is 12 statements. **Still to come in P6:** each strategy's progress and cancel halves, and the nine delegates the scheduler keeps on the service until then |
| 26 | Specifying a composite | two models on two endpoints: a client-supplied **document** on `/compose/v1`, a `composite_id` already in the image on `/api/v1/simulations` `extra_params`; environment = SMS `simulator_id` or a site-wide pinned image | one request on `/viva/v1/composites`: an `EnvironmentRef` + exactly one of `document` / `composite{id, params}`; execution is a field, not an endpoint | P5 | planned |
| 27 | `SimulationServiceK8s` (upstream vEcoli: Nextflow head as a K8s Job) | 624 lines; the **default backend on both Stanford sites**; its own image-build commands (duplicating the Ray builder's) and both standalone-analysis entry points; shares `batch_build.py` | unchanged by the carve; shares the pure `analysis_spec` module; its build becomes a `repo-recipe` | P5 | **out of scope until P5**, by decision (2026-09-20); `scripts/qualification_test.sh` stays its check |
