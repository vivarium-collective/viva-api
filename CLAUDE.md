# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

SMS API (Simulating Microbial Systems API, also known as Atlantis API) is a FastAPI-based REST API for designing, running, and analyzing whole-cell simulations of E. coli using the vEcoli model. The API supports two compute backends: **SLURM** (on-prem HPC at UCONN CCAM) and **K8s + AWS Batch** (GovCloud, Stanford deployment). Backend selection is automatic based on `deployment_namespace` in config.

## Architecture

### Core Components

- **FastAPI Server**: REST API hosted at `https://sms.cam.uchc.edu/`
- **SLURM Integration**: Submits and monitors jobs on HPC clusters via SSH
- **Singularity/Apptainer**: Containerized vEcoli simulator execution
- **PostgreSQL**: Stores simulation metadata, job records, and parca datasets
- **Marimo UI**: Web-based client interfaces

### Directory Structure

```
viva_api/
├── api/           # FastAPI routes and generated OpenAPI client
│   ├── routers/   # Route handlers: gateway, core, antibiotics, biofactory, inference, variants
│   ├── client/    # Auto-generated OpenAPI client (do NOT edit manually)
│   └── spec/      # Generated OpenAPI spec
├── analysis/      # Analysis job orchestration (post-simulation)
├── common/        # Shared utilities
│   ├── hpc/       # SLURM service, job models
│   ├── ssh/       # SSH session management (asyncssh)
│   ├── storage/   # File path abstractions (HPCFilePath), FileService (GCS, S3, Qumulo)
│   ├── gateway/   # Gateway I/O and models
│   └── messaging/ # MessagingService (Redis-backed)
├── data/          # Data services and BioCyc integration
├── simulation/    # Simulation service, database service, job scheduler, models, ORM tables
├── config.py      # Settings via pydantic-settings
└── dependencies.py # Dependency injection (SSH, DB, file, messaging, simulation services)

tests/
├── integration/   # HPC workflow tests (require SSH access)
├── fixtures/      # Pytest fixtures (centralized, imported via conftest.py)
│   └── configs/   # Sample JSON config files for testing
├── api/           # Route/handler tests
├── common/        # Service-level tests
├── data/          # Data service tests
└── simulation/    # Simulation logic tests

artifacts/         # Debug output directory (gitignored)
                   # Contains captured sbatch scripts and config snapshots
```

### Request Flow

API requests hit FastAPI routers (`viva_api/api/routers/`) which depend on services injected via `viva_api/dependencies.py`. The dependency module manages global singletons for SSH sessions, database connections, file storage, messaging, and the simulation service.

### Key Services

- **SimulationService** (`simulation/simulation_service.py`): Orchestrates the full HPC workflow (build, parca, simulate). Uses SlurmService + SSHSessionService.
- **AnalysisService** (`analysis/analysis_service.py`): Post-simulation analysis job orchestration.
- **DatabaseService** (`simulation/database_service.py`): SQLAlchemy async ORM for simulation metadata. Tables in `tables_orm.py`.
- **SlurmService** (`common/hpc/slurm_service.py`): SLURM job submission/monitoring. All methods take `ssh: SSHSession` as first arg.
- **SSHSessionService** (`common/ssh/ssh_service.py`): asyncssh connection pooling. Session reuse is critical for polling loops.
- **FileService** (`common/storage/`): Abstraction over GCS, S3, and Qumulo S3 storage backends.
- **JobScheduler** (`simulation/job_scheduler.py`): Coordinates multi-step HPC workflows.

### Compute Backend Dispatch

Backend selection is determined by `deployment_namespace` in `viva_api/config.py`:
- **SLURM** (default): `sms-api-rke`, `sms-api-rke-dev` — UCONN CCAM on-prem HPC
- **K8s + AWS Batch**: `sms-api-stanford`, `sms-api-stanford-test` — GovCloud

The dispatch happens in `dependencies.py` at startup: `SimulationServiceHpc` for SLURM, `SimulationServiceK8s` for K8s.

Config filenames are also namespace-aware via `viva_api/common/simulator_defaults.py`:
- `SimulationConfigPublic` (CCAM/RKE deployments)
- `SimulationConfigPrivate` (Stanford deployments)
- `SimulationConfigFilename` is dynamically set based on `PUBLIC_MODE`

### Three Client Interfaces

The API has three client entrypoints that implement the same EUTE workflow:
- **CLI** (`app.cli`): `uv run atlantis <command>` — Typer + Rich, Memphis theme
- **TUI** (`app.tui`): `uv run atlantis tui` — Textual app, animated logo banner
- **GUI** (`app.gui`): `uv run atlantis gui` — Marimo notebook, Memphis CSS theme

The Atlantis logo (E. coli capsule + flagella squigglies) is defined in:
- `app/cli_theme.py` — CLI Rich markup
- `app/tui.py` — TUI with animated green↔purple gradient (`_animated_banner()`)
- `app/gui.py` — GUI with HTML/CSS + SVG flagella

### HPC Workflow Pipeline

1. **Build Image**: Clone vEcoli repo, build Singularity container
2. **Run Parca**: Parameter calculator creates simulation dataset
3. **Run Simulation**: Execute vEcoli simulation via SLURM
4. **Run Analysis**: Post-process simulation outputs

### File Paths

- `HPCFilePath`: Abstraction for remote HPC paths with `.remote_path` and `.local_path()` methods
- Key settings paths: `hpc_image_base_path`, `hpc_parca_base_path`, `hpc_repo_base_path`, `simulation_outdir`, `analysis_outdir`

### Generated Code

`viva_api/api/client/` is auto-generated from the OpenAPI spec. Regenerate with `make api_client`.

## Development

### Setup

```bash
uv sync                    # Install dependencies (includes dev + docs groups)
uv run pre-commit install  # Set up pre-commit hooks (ruff lint + format, JSON formatting)
```

### Configuration

Environment variables loaded from `assets/dev/config/.dev_env`:
- `SLURM_SUBMIT_HOST`, `SLURM_SUBMIT_USER`, `SLURM_SUBMIT_KEY_PATH`: SSH access
- `POSTGRES_*`: Database connection
- `HPC_*`: HPC filesystem paths

### Key Commands

```bash
make check                 # Full quality check: lock consistency, pre-commit, mypy, deptry
make test                  # Run all tests with coverage
make gateway               # Start local dev server (port 8888, auto-reload)
make spec                  # Regenerate OpenAPI spec
make api_client            # Regenerate spec + OpenAPI client library
make e2e                   # Run full simulation end-to-end test

uv run pytest              # Run all tests
uv run pytest -x           # Stop on first failure
uv run pytest tests/path/test_file.py::TestClass::test_method -v -s  # Single test
uv run pytest tests/integration/test_hpc_workflow.py -v              # Integration tests (need SSH)
```

### After Making Significant Changes

After making significant code changes, run these steps in order:

1. `make spec` - regenerate OpenAPI spec (if API routes/models changed)
2. `make api_client` - regenerate client library (if spec changed)
3. `make check` - lint and type check
4. `uv run pytest` - run all unit tests
5. `uv run pytest tests/integration/test_hpc_workflow.py -v` - integration tests (requires SSH)

### Database migrations

Schema changes go through **Alembic** (`alembic/versions/`), but the deployment
applies them with a **self-diagnosing reconciler**, not bare `alembic upgrade head`.

Why: the app also bootstraps schema with `Base.metadata.create_all` at startup
(`tables_orm.create_db`), which creates missing objects but never *alters*
existing ones and never writes `alembic_version`. A database the app touched
before Alembic ends up un-stamped and possibly drifted, and `alembic upgrade
head` then runs from base and fails re-`CREATE`-ing existing tables.

- **Add a migration**: `uv run alembic revision -m "…"` (or hand-write one; see
  `c1a2b3d4e5f6_add_tags_to_simulation.py`). Set `down_revision` to the current
  head. Prefer PG-safe, idempotent ops.
- **The reconciler** (`viva_api/simulation/db_reconcile.py`, run by the
  `alembic-migrate` Job) classifies any DB and acts idempotently:
  - **FRESH** (no tables, no `alembic_version`) → `upgrade head` from base
  - **MANAGED** (`alembic_version` present) → `upgrade head` (no-op if current)
  - **LEGACY** (tables, no `alembic_version`) → `stamp <matched>` → `upgrade head`
  - **INCONSISTENT** (non-linear fingerprint) → refuse, print manual steps
  The LEGACY match walks a small **frozen** fingerprint table (one schema marker
  per pre-adoption revision) in `LEGACY_FINGERPRINTS`.
- **Fingerprint maintenance contract**: while `create_all` still bootstraps prod
  DBs, **add a new fingerprint marker whenever you add a migration** (a create_all
  DB advanced past the top marker would otherwise stamp stale and re-apply an
  already-made migration). Guarding `create_all` off in prod freezes the list —
  the intended end state.
- **Before deploying to a site**: run the read-only report first, then apply.
  Each site's RDS may sit at a different un-stamped point.
  ```bash
  # read-only report (in a pod or locally; uses SQLALCHEMY_DATABASE_URL or POSTGRES_*)
  uv run python -m viva_api.simulation.db_reconcile --analyze   # or scripts/db_analyze.py
  # apply (this is what the migration Job runs)
  kubectl delete job alembic-migrate -n <ns> --ignore-not-found   # Jobs are immutable
  kubectl apply -k kustomize/overlays/<ns>-db-migration
  kubectl wait --for=condition=complete job/alembic-migrate -n <ns> --timeout=300s
  ```
  The `<ns>-db-migration` overlay's `sms-api` `newTag` must contain the new
  migration — **keep it in sync with the app overlay's tag** (see version-sync
  checklist).

## Testing

### Integration Tests

`tests/integration/test_hpc_workflow.py` tests the full HPC workflow:
- Requires SSH access (skipped if `SLURM_SUBMIT_KEY_PATH` not set)
- Tests are idempotent - check for existing HPC artifacts before running
- Uses `TEST_EXPERIMENT_ID = "test_integration"`

### Fixtures

Key fixtures centralized in `tests/fixtures/` and imported via `tests/conftest.py`:
- `database_service`: SQLite test database
- `simulation_service_slurm`: HPC simulation service
- `slurm_service`, `ssh_session_service`: SLURM and SSH fixtures
- `file_service_gcs`, `file_service_s3`, `file_service_qumulo`: Storage backend fixtures
- `simulator_repo_info`: Default vEcoli repo config
- `configs/`: Sample JSON config files for deserialization tests
- Uses testcontainers for Postgres, Redis, MongoDB
- pytest-asyncio for async test support

## Common Patterns

### SSH Session Reuse (for polling loops)

```python
async with get_ssh_session_service().session() as ssh:
    while not done:
        status = await service.get_status(job_id, ssh=ssh)
        await asyncio.sleep(10)
```

### Database Operations

```python
async with database_service.session() as session:
    result = await session.execute(query)
```

### SLURM Job Submission

```python
slurm_service = SlurmService()
async with get_ssh_session_service().session() as ssh:
    job_id = await slurm_service.submit_job(
        ssh,  # Required first argument
        local_sbatch_file=local_path,
        remote_sbatch_file=remote_hpc_path,
    )
    # Reuse session for polling
    status = await slurm_service.get_job_status_squeue(ssh, job_ids=[job_id])
```

## Tooling

- **Linting/Formatting**: ruff (line length 120). Pre-commit runs ruff lint + ruff format.
- **Type checking**: mypy with strict mode. Excludes: `viva_api/api/client/`, `app/ui/`, `notes/`, `scratchpads/`.
- **Python**: 3.12.9 (pinned exact).
- **Package manager**: uv with hatchling build backend.


## Release Protocol

Follow this exact sequence to cut a release:

1. **Include the version bump in the feature/fix branch** before merging:
   - `viva_api/version.py` — `__version__ = "X.Y.Z"`
   - `pyproject.toml` — `version = "X.Y.Z"`
2. **Single PR to `main`** — contains all changes + version bump. Merge.
3. **Tag the merge commit**:
   ```bash
   git checkout main && git pull
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```
4. **Create GitHub Release** from the tag with release notes:
   ```bash
   gh release create vX.Y.Z --title "vX.Y.Z — <summary>" --notes-file <notes.md>
   ```
5. **Build + deploy** (if deploying):
   ```bash
   gh workflow run build-and-push.yml --ref main -f version=X.Y.Z
   ```
   Then bump `newTag` in all kustomize overlays and apply.

**Version sync checklist** (when bumping version):
- `viva_api/version.py`
- `pyproject.toml`
- `kustomize/overlays/sms-api-stanford-test/kustomization.yaml` (sms-api only — keep sms-ptools at 0.5.9)
- `kustomize/overlays/sms-api-stanford/kustomization.yaml`
- `kustomize/overlays/sms-api-rke/kustomization.yaml`
- `kustomize/overlays/sms-api-rke-dev/kustomization.yaml`
- **`kustomize/overlays/<ns>-db-migration/kustomization.yaml`** — the migration
  Job runs the reconciler from *this* image tag, so it MUST contain any new
  migration. Keep it equal to the matching app overlay's `sms-api` tag. (These
  historically drifted — e.g. stanford-test was pinned to an old tag — because
  they were omitted from this checklist.)

## Notes

### Full end-user E2E workflow (E.U.T.E: End User Tooling Experience)

#### We consider the "full end-user end-to-end workflow (a.k.a: E.U.T.E: End User Tooling Experience) to be: *(build -> get build status -> workflow(parca --> simulation --> analyses)) -> get workflow status -> get simulation data (download)*
We seek to have the Atlantis CLI (`app.cli`) to do this workflow, which again should be:

```
1. <GET> /core/v1/simulator/latest
2. -> <POST> /core/v1/simulator/upload
3. -> <GET> /core/v1/simulator/status (perhaps poll?, whatever is in the atlantis cli)
4. (once done) -> <POST> /api/v1/simulations
5. -> <GET> /api/v1/simulations/{id}/status (again, perhaps poll? Whatever is sleek and a good ux)
6. -> (once done) <POST> /api/v1/simulations/{id}/data (saved to a specified outdir, which for our testing/debugging can be a dir at ./debug)
7. -> (optional) <POST> /api/v1/simulations/{id}/analysis (re-run specific analysis modules on existing output)
```


#### Development Flow State for EUTE

*WHEN TESTING THE SMS_API's EUTE, MAKE SURE to use the atlantis cli (app.cli).* IN FACT, this is the iterative dev loop i want to get in: we use the cli to test end-user-facing e2e workflows (that is, the
"product" itself, one that stakeholders and clients alike will use: must be sleek, easy to use, yet robust and informative, and most importantly useful/novel enough to where it would be perferred to use the cli over any other
arbitrary external client that may call the api...I will then want to ensure that the same working functionality is exposed/present in the tui (basically, the entrypoint to the rest api defined in viva_api has 3
entrypoints/clients (other than direct http requests to the endpoints themselves): a. the marimo notebooks found in app/ui/..., b. the cli (atlantis) found in app.cli, c. the tui found at app.tui. With that said, it is
imperative that the aforementioned a, b, and c are implementations of the same thing (the full e2e end-user workflow calling the restapi endpoonts as mentioned), but within different media...ie: cli app, marimo gui (app mode in
marimo), and tui (textual-based tui) all expose/provide the same functionality, just in those different formats. Let's fully make this happen! If youre in, say "I dig ya broski: let's cook!", then make it happen babbbby!

### Stanford-Test Deploy Loop (K8s + AWS Batch)

The iterative fix → deploy → test cycle for the `sms-api-stanford-test` namespace:

```bash
# 1. Commit and push the fix (THIS MUST HAPPEN BEFORE STEP 2 — see "Pitfall 1" below)
git add <files> && git commit -m "fix: ..." && git push origin atlantis-cli

# 2. Build and push Docker image via GitHub Action
#    The GH Action checks out the ref at the REMOTE branch tip, not your local
#    working tree. If your fix isn't pushed, the action builds the old code.
gh workflow run build-and-push.yml --ref atlantis-cli -f version=<VERSION>
gh run watch $(gh run list --workflow=build-and-push.yml --limit 1 --json databaseId -q '.[0].databaseId')
# NOTE: The action builds only sms-api now (scripts/build_action.sh). The
# Nextflow/vEcoli-submit image is NOT built here — it's a runtime, per-commit
# build (base = that commit's vecoli:<sha>), so it was removed from the loop and
# Dockerfile-nextflow deleted (it always failed "BASE_IMAGE blank"). Success =
# "Built and pushed service api". DO NOT bump the sms-ptools newTag to match api;
# ptools is intentionally pinned to 0.5.9 (no newer sms-ptools image on ghcr.io).

# 3. Apply + roll Stanford-test
kubectl kustomize kustomize/overlays/sms-api-stanford-test | kubectl apply -f -
kubectl rollout restart deployment/api -n sms-api-stanford-test
kubectl rollout status  deployment/api -n sms-api-stanford-test

# 4. Start the external tunnel for local access
AWS_PROFILE=stanford-sso AWS_DEFAULT_REGION=us-gov-west-1 \
  ../sms-cdk/scripts/ptools-proxy.sh -s smsvpctest
#    (keep this terminal open)

# 5. Verify the running pod actually has your fix
POD=$(kubectl get pod -n sms-api-stanford-test -l app=api \
  --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n sms-api-stanford-test $POD -- grep -c <marker> \
  /app/viva_api/<path/to/changed/file>.py
#    ^ Replace <marker> with something unique to your fix (an identifier,
#      a log string, etc.). If this returns 0, step 2's build didn't pick up
#      your commit. Stop. Do not proceed.

curl -s http://localhost:8080/version   # sanity check the tunnel

# 6. Test E2E via Atlantis CLI (NOT curl)
uv run atlantis simulator latest --repo-url https://github.com/CovertLabEcoli/vEcoli-private --branch master
uv run atlantis simulation run test1 <SIMULATOR_ID> --generations 1 --seeds 1 --poll
uv run atlantis simulation outputs <SIM_ID> --dest ./debug
```

**Version sync:** When bumping version, update ALL of:
- `viva_api/version.py`
- `pyproject.toml`
- `kustomize/overlays/sms-api-stanford-test/kustomization.yaml` (the `sms-api` image entry only — leave `sms-ptools` pinned to 0.5.9)
- `kustomize/overlays/sms-api-rke/kustomization.yaml`
- `kustomize/overlays/sms-api-rke-dev/kustomization.yaml`

Prefer bumping the tag to reusing the same one — a new tag is the unambiguous signal that a new image must exist on ghcr, and eliminates all "did the rollout actually pull new bits?" confusion.

**Alternative: Local build** (faster, no GH Action wait):
```bash
./kustomize/scripts/build_and_push.sh   # reads version from viva_api/version.py
```

**Helper script:** `scripts/deploy-namespace.sh` wraps steps 1–6 (mostly) for the
three-namespace fleet. Read it before running it — it's been the source of
several subtle bugs (e.g. an earlier VERSION-variable ordering bug silently
built image tags with empty version strings).

---

#### Pitfalls the team has hit (documented here so nobody re-hits them)

**Pitfall 1 — GH Action builds the REMOTE branch, not your working tree.**
`gh workflow run ... --ref atlantis-cli` runs the action against whatever the
remote branch points at when it starts. If you forgot to `git push` before firing
the action, the build will produce an image with your OLD code — and
`imagePullPolicy: Always` will faithfully pull that old code onto the new pod.
Symptom: you deployed, pods rolled, `/health` works, but the fix isn't there.
Verification step 5 above catches this — always grep a marker unique to your
fix inside `/app/viva_api/...` on the live pod before declaring victory.

**Pitfall 2 — Ephemeral storage eviction on large downloads.**
The `api` pod mounts `/app/.results_cache` as an `emptyDir` with
`ephemeral-storage: requests=4Gi, limits=12Gi` (see `kustomize/base/api.yaml`).
Those numbers are sized against the `m6i.large` node's ~16.9 GiB allocatable —
do **NOT** raise them without bumping `diskSize` in `../sms-cdk/lib/eks-stack.ts`
first, or you'll overcommit and trigger eviction cascades under load. If a real
workload exceeds the 12 GiB cache ceiling, the correct answer is **task 11**
(stream S3 → tar response without ever touching disk), not raising the limit.

**Pitfall 3 — The Stanford-test ingress.yaml is DEAD CODE.**
`kustomize/overlays/sms-api-stanford-test/kustomization.yaml` has
`#  - ingress.yaml` commented out of its `resources:` list. Stanford-test uses
`target-group-binding.yaml` to attach pods directly to a CDK-managed ALB. Any
edit to that `ingress.yaml` file is a no-op. The real ALB config (including
`idleTimeout`) lives in `../sms-cdk/lib/internal-alb-stack.ts` on the
`aws-batch-manual` branch and requires `cdk deploy` to change.

**Pitfall 4 — The ALB target group occasionally flakes to `Target.Timeout`.**
After sustained outbound traffic (e.g. many S3 downloads), the ALB's view of
the api pod can transition to `unhealthy: Target.Timeout` even though the pod
is fine. The pod itself remains reachable via cluster-internal paths
(`kubectl port-forward`, other pods, /health probes from inside the pod all
work). When this happens, any CLI request via `ptools-proxy.sh → ALB` will
hang. **Workaround:** bypass the ALB entirely with `kubectl port-forward`:

```bash
# In a dedicated terminal, kill the ptools-proxy first
kubectl port-forward -n sms-api-stanford-test deployment/api 8080:8000
# Then point the CLI at the same local port
uv run atlantis simulation outputs 44 --base-url http://localhost:8080
```

Port-forward routes through the kube-apiserver HTTP/2 tunnel — slower than the
ALB path (~1–3 MiB/s sustained) but 100% reliable. Good enough for a single-user
CLI workflow. The proper long-term fix is task 11 (streaming responses keep the
connection chatty, which sidesteps the ALB conntrack staleness entirely).

**Pitfall 5 — `kubectl port-forward` and multi-request handlers don't mix.**
If a client-side code path opens a SECOND TCP connection to the port-forwarded
local port while a large async stream is already in flight over the same
kubectl port-forward session, the second connection can get RST'd by the
HTTP/2 multiplexer. We hit this once in `E2EDataService.submit_stream_output_data`
(fixed by parsing the filename from `Content-Disposition` instead of making a
second `GET /simulations/{id}` call). Rule of thumb: inside an
`async with client.stream(...)` block, don't make additional HTTP calls.

# PRIORITY

Implement that which is laid out in ./PLAN.md, if not already done.
