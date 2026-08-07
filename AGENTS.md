# AGENTS.md — sms-api (Atlantis API)

FastAPI REST API for E. coli whole-cell simulations (vEcoli model). Two compute backends: **SLURM** (UCONN CCAM) and **K8s + AWS Batch** (GovCloud/Stanford).

## Critical reading

Start with `CLAUDE.md` in the repo root — it's the authoritative source with full architecture, deploy loops, and pitfalls. This file only adds what an agent is likely to miss.

## Commands (exact)

```bash
uv sync                    # install deps (includes dev + docs groups)
uv run pre-commit install  # setup pre-commit hooks

make check                 # lock consistency → pre-commit → mypy → deptry (run BEFORE pushing)
make test                  # pytest with coverage
make spec                  # regenerate OpenAPI spec
make api_client            # spec + generate client library
make gateway               # dev server on :8888 with --reload
make e2e                   # full simulation E2E test (needs live infra)

uv run pytest tests/path/test_file.py::TestClass::test_method -v -s  # single test
uv run pytest -x           # stop on first failure
uv run pytest -s -m <marker>
```

**After significant changes, run in order:** `make spec` → `make api_client` → `make check` → `uv run pytest`

## Generated code — NEVER edit by hand

- `viva_api/api/client/` — auto-generated OpenAPI client via `openapi-python-client`
- `viva_api/api/spec/` — auto-generated OpenAPI spec from route introspection
- Regenerate both with `make api_client`

## Architecture

- **Entrypoints**: `viva_api/api/main.py` (FastAPI server), `app/cli.py` (Typer CLI, entry `atlantis`), `app/tui.py` (Textual TUI), `app/gui.py` (Marimo GUI)
- **All three clients** implement the same EUTE workflow calling REST endpoints. Prefer CLI for testing (`uv run atlantis <command>`).
- **Backend dispatch**: `viva_api/config.py` — `compute_backend` setting. SLURM for `sms-api-rke*`, Batch for `sms-api-stanford*`.
- **Services wired** in `viva_api/dependencies.py` via global singletons (SSH, DB, file, messaging, simulation).

## Testing quirks

- `tests/conftest.py` MUST set `COMPUTE_BACKEND` and `PUBLIC_MODE` env vars **before** any `viva_api` imports (module-level code reads them at import time).
- Integration tests (`tests/integration/`) need SSH access; auto-skipped if `SLURM_SUBMIT_KEY_PATH` not set.
- Uses testcontainers for Postgres, Redis, MongoDB fixtures.
- Centralized fixtures live in `tests/fixtures/`, re-exported via `tests/conftest.py`.

## Environment

- Env file: `assets/dev/config/.dev_env` (contains real credentials — do not commit changes)
- Python 3.13, uv package manager, hatchling build backend
- ruff line-length 120, mypy strict mode (excludes: `viva_api/api/client/`, `app/ui/`, `notes/`, `scratchpads/`)

## Version bump checklist (6 files, all must match)

1. `viva_api/version.py`
2. `pyproject.toml`
3. `kustomize/overlays/sms-api-stanford-test/kustomization.yaml` (sms-api only; keep sms-ptools at 0.5.9)
4. `kustomize/overlays/sms-api-stanford/kustomization.yaml`
5. `kustomize/overlays/sms-api-rke/kustomization.yaml`
6. `kustomize/overlays/sms-api-rke-dev/kustomization.yaml`

## Common gotchas

- **ALB timeout flake**: After sustained downloads, ALB marks pod unhealthy. Workaround: `kubectl port-forward` bypasses the ALB.
- **GH Action builds remote, not local**: Always `git push` before `gh workflow run build-and-push.yml`. Verify fix on the live pod.
- **ephemeral storage**: 12Gi limit on `/app/.results_cache` — don't raise without coordinating with CDK `diskSize`.
- **No second HTTP call inside `async with client.stream(...)`**: The kubectl port-forward HTTP/2 mux will RST the second connection.
- **Stanford-test ingress.yaml is dead code**: Real ALB config is in CDK stack on `aws-batch-manual` branch.
- **Alembic migrations** live in `alembic/versions/` for DB schema changes.

## Deployment naming conventions

This project exposes two distinct REST API deployments, each with multiple aliases:

| Deployment | Also called | Compute backend | K8s namespace(s) | Base URL |
|------------|-------------|-----------------|-------------------|----------|
| **Academic API** | CCAM, HPC, SLURM, public API, sms-api-rke | SLURM via SSH | `sms-api-rke`, `sms-api-rke-dev` | `https://sms.cam.uchc.edu` |
| **GovCloud API** | AWS, Batch, Stanford, govcloud | AWS Batch via Nextflow | `sms-api-stanford`, `sms-api-stanford-test` | internal (via ptools-proxy) |

- The **academic API** is the publicly-accessible production deployment at UCONN. It uses on-prem SLURM and the HPC filesystem (Qumulo S3). This is where all BioModels/compose functionality runs and where external users (ptools stakeholders, BioModels owners, SEUs) interact.
- The **govcloud API** is a restricted deployment in AWS GovCloud for the Stanford billing group. It uses AWS Batch and S3. Requires SSO + tunneling.

Internal shorthand: "the academic api" = RKE namespace. "the govcloud/batch api" = Stanford namespace.

When reading code/config: `sms-api-rke*` → SLURM backend, `sms-api-stanford*` → AWS Batch backend.

## OpenCode Local-LLM Agents

Powered by Ollama + deepseek-coder-v2. Zero cloud dependency, zero cost. Config in `opencode.json` + `.opencode/agent/`.

```bash
opencode                           # launch with atlantis-dev (default primary)
opencode --agent atlantis-dev      # full-stack dev: features, fixes, refactoring
opencode --agent architect         # read-only design review, pattern checks, PR review
opencode --agent tester            # test writing, fixture patterns, mock strategies
opencode --agent security-auditor  # OWASP/injection/secret audit (read-only)
opencode --agent deploy            # deploy loop, version bump, rollout, pitfalls
opencode -m ollama/llama3.1        # lighter model for quick questions
```

Re-optimize config after large PRs: invoke `/bootstrapper` skill inside opencode.

## Active plan files

- `TODO_40.md` — ✅ BioModels integration via compose subsystem (academic api only) — **verified complete** (62 tests, `make check` clean, all 6 endpoints + 6 CLI commands + OpenAPI client regenerated)
- `BIOMODELS_GUIDE.md` — user-facing CLI guide for BioModels owners (created alongside TODO_40 verification). Commands default to `https://sms.cam.uchc.edu` (sms-api-rke). Compose biomodels always routes through SLURM — compose has no Batch implementation.
- `SECURITY_UPDATES.md` — CVE-2026-31431 "DirtyFrag" response for AWS GovCloud infra

## Deploy loop (rke — academic API, biomodels target)

```bash
git push && gh workflow run build-and-push.yml --ref <branch> -f version=X.Y.Z
# wait for build, then:
kubectl kustomize kustomize/overlays/sms-api-rke | kubectl apply -f -
kubectl rollout restart deployment/api -n sms-api-rke
# verify on pod:
kubectl get pods -n sms-api-rke
kubectl logs -n sms-api-rke deployment/api --tail=50
# test via CLI:
uv run atlantis compose biomodels-ids
uv run atlantis compose biomodels-meta BIOMD0000000001
```

## Deploy loop (stanford-test)

```bash
git push && gh workflow run build-and-push.yml --ref <branch> -f version=X.Y.Z
# wait for build, then:
kubectl kustomize kustomize/overlays/sms-api-stanford-test | kubectl apply -f -
kubectl rollout restart deployment/api -n sms-api-stanford-test
# tunnel: AWS_PROFILE=stanford-sso AWS_DEFAULT_REGION=us-gov-west-1 ../sms-cdk/scripts/ptools-proxy.sh -s smsvpctest
# verify on pod, then test via atlantis CLI
```
