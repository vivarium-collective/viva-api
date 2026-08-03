# Plan: Remove PCS/SLURM and FSx from Stanford Deployments

## Context

The Stanford deployments (`sms-api-stanford-test` and `sms-api-stanford`) use AWS Batch via Nextflow for simulation compute. SLURM, SSH, and FSx Lustre infrastructure were configured but unused. This plan removes them and modernizes the config management.

## Status: COMPLETE (except Phase 4)

All phases completed 2026-04-12/13 except Marimo notebook fixes.

CDK cleanup completed for both `smsvpctest` (stanford-test) and `smscdk` (stanford) — see `../sms-cdk/docs/remove-pcs-fsx-plan.md`.

## Phase 1: Kustomize Cleanup — DONE

Removed from both `sms-api-stanford-test` and `sms-api-stanford` overlays:

| File | What It Was |
|------|-------------|
| `fsx-pcs-root-pv.yaml` | FSx Lustre PersistentVolume |
| `fsx-pcs-root-pv.yaml.template` | Template for above |
| `vivarium-home-pvc.yaml` | PVC bound to FSx PV |
| `storageclass-gp3-retain.yaml` | StorageClass for EBS |
| `secret-ssh.yaml` | SSH key for SLURM login node |

Updated `kustomization.yaml` for both overlays:
- Removed resource references to deleted files
- Removed `vivarium-home-pvc` volumeMount patch
- Kept `haproxy-ssh` at 0 replicas (comes from shared base)
- Added JSON patch to remove SSH env var, volume mounts, and volumes from base `api.yaml`

Manually cleaned up orphaned K8s resources on both clusters:
- Deleted FSx PVCs (required finalizer patch — FSx filesystem no longer exists)
- Deleted FSx PVs and NFS PVs
- Force-deleted zombie api pods stuck on missing volume mounts

## Phase 2: Config Cleanup — DONE

Removed from both `shared.env` files:
- All `SLURM_*` variables (host, user, key path, known hosts, partition, node list, QOS, log/base paths)
- All `HPC_*` path variables (image, parca, repo, sim base paths)
- `SIMULATION_OUTDIR`, `ANALYSIS_OUTDIR` (FSx paths)
- `HPC_SIM_CONFIG_FILE`

Added mandatory config settings to all overlays:
- `COMPUTE_BACKEND=batch` (stanford, stanford-test) or `COMPUTE_BACKEND=slurm` (RKE, local)
- `PUBLIC_MODE=false` (stanford) or `PUBLIC_MODE=true` (RKE)

Batch queue names updated to CDK-prefixed versions (e.g., `smsvpctest-vecoli-task-arm64`).

## Phase 3: Code Fixes — DONE

### 3a. Analysis Endpoint Backend Guard
**File:** `viva_api/api/routers/sms.py` (renamed from `gateway.py` on `atlantis-cli`)

Added `ComputeBackend.SLURM` check on `POST /analyses` and `GET /analyses/{id}/status`. Returns 501 with message pointing to `POST /api/v1/simulations/{id}/analysis` (the K8s-native alternative).

### 3b. Parca Endpoint Backend Guard
**File:** `viva_api/api/routers/core.py`

Added `ComputeBackend.SLURM` check on `POST /simulation/parca`. Returns 501 — parca runs inside the Nextflow workflow on Batch.

### 3c. Base api.yaml Volume Mounts
Both overlays add a JSON patch that removes (highest index first):
- `env/5` (SLURM_SUBMIT_KEY_PATH)
- `volumeMounts/0-2` (vivarium-home-pvc, slurm-submit-key-file, ssh-known-hosts)
- `volumes/0-2` (same)

### 3d. ComputeBackend Enum (additional)
Added `ComputeBackend` StrEnum (`"slurm"` | `"batch"`) in `config.py`. All backend dispatch uses the enum — no string literals to misspell.

### 3e. Mandatory Config Settings (additional)
`COMPUTE_BACKEND` and `PUBLIC_MODE` are now required. App fails at startup with a clear error if not set. Removed all namespace-name-based inference (`_K8S_NAMESPACES`, `PUBLIC_MODE = NAMESPACE == "sms-api-rke"`).

## Phase 4: Marimo Notebooks — NOT DONE

### 4a. `app/ui/explore.py`
Still imports `SimulationDataServiceFS` which reads from local/mounted filesystem. Breaks on Batch backend.

**Options:**
1. Short term: skip notebook on Batch (backend check at top)
2. Medium term: create `SimulationDataServiceS3`
3. Long term: unify behind `FileService` abstraction

### 4b. `app/ui/configure.py`
Imports `ConfigServiceHpc` which doesn't exist. Broken regardless of backend.

**Fix:** Remove broken import and dead code, or create the module.

## Phase 5: secrets.sh Cleanup — DONE

Rewrote `secrets.sh` for both overlays. Removed:
- Login node instance ID lookup (PCS deleted)
- SSH known_hosts ConfigMap generation (SSM to login node)
- SSH sealed secret generation
- FSx PV YAML generation from template

Added CloudFormation-based lookups for all Batch infrastructure:

| Resource | Stack | Output Key |
|----------|-------|------------|
| Batch task queue names | `{prefix}-batch` | `Amd64TaskQueueName`, `Arm64TaskQueueName` |
| Build queue names | `{prefix}-build-batch` | `Amd64BuildQueueName`, `Arm64BuildQueueName` |
| Build job definition | `{prefix}-build-batch` | `DindBuildJobDefinitionName` |
| Git secret ARN | `{prefix}-build-batch` | `GitSecretArn` |
| IRSA role ARN | `{prefix}-batch` | `BatchSubmitRoleArn` |
| S3 bucket name | `{prefix}-shared` | `SharedBucketName` |
| Redis endpoint | `{prefix}-shared` | `RedisEndpoint` |
| DB credentials | `{prefix}-shared` | `DbSecretArn` → Secrets Manager |
| Target group ARNs | `{prefix}-internal-alb` | `ApiTargetGroupArn`, `PtoolsTargetGroupArn` |

No hardcoded ARNs, queue names, or bucket names remain. Everything is resolved from CloudFormation at `secrets.sh` run time using the `STACK_PREFIX` from `secrets.dat`.

## Phase 6: CDK Cleanup — DONE

Completed for both deployments. See `../sms-cdk/docs/remove-pcs-fsx-plan.md`.

| Deployment | Prefix | Status |
|------------|--------|--------|
| stanford-vpc-test | `smsvpctest` | All stacks deployed 2026-04-12 |
| stanford | `smscdk` | All stacks deployed 2026-04-13 |

Stanford production also required:
- DB migration (`job_id_ext`, `job_backend` columns added to `hpcrun`)
- Alembic version table created
- IAM fix: `DindExecutionRole` needed `secretsmanager:GetSecretValue` for `vecoli-github-pat`

## Verification

- [x] Pod starts without FSx/SSH mounts (both deployments)
- [x] `atlantis simulator latest --force` succeeds (both)
- [x] `atlantis simulation run --run-parca --poll` submits and runs (both)
- [ ] `atlantis simulation outputs` downloads results
- [ ] 501 on `POST /analyses` and `POST /simulation/parca` (code deployed, not yet tested via CLI)

## What Stays

| Component | Why |
|-----------|-----|
| PostgreSQL (RDS) | Simulation metadata, job records |
| Redis (ElastiCache) | Messaging |
| EKS cluster | API pods, Nextflow head jobs |
| AWS Batch (ARM64 + AMD64) | Simulation tasks (Spot + on-demand fallback) |
| S3 bucket | Workflow data, simulation outputs |
| ECR | Container images |
| ALB + Target Groups | API access |
| GHCR secret | Pull sms-api/ptools images |
| batch-submit SA + IRSA | AWS access from pods |
