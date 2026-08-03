# AWS Batch Workflow Architecture Analysis

> Analysis date: 2026-03-31 (updated after Stage 2 implementation)
> Branch: `aws-batch-full` (from `main`)
> Goal: Run multiple concurrent Nextflow workflow runs with AWS Batch as the task execution substrate.

## Table of Contents

1. [Repository Findings](#1-repository-findings)
2. [Current Architecture Summary](#2-current-architecture-summary)
3. [Existing CDK Infrastructure](#3-existing-cdk-infrastructure)
4. [Option A: Kubernetes Job per Workflow Run (Recommended)](#4-option-a-kubernetes-job-per-workflow-run-recommended)
5. [Option B: Pet EC2 Launcher Service](#5-option-b-pet-ec2-launcher-service)
6. [Testability Comparison](#6-testability-comparison)
7. [Recommendation](#7-recommendation)
8. [Detailed Implementation Plan](#8-detailed-implementation-plan)
9. [Open Questions / Assumptions](#9-open-questions--assumptions)
10. [Suggested First Slice](#10-suggested-first-implementation-slice)

---

## Constraints

- Application runs on EKS (AMD64 nodes).
- Workflows are implemented with Nextflow.
- Nextflow submits its process/task execution to AWS Batch (ARM64/Graviton Spot instances).
- AWS Batch must remain the task execution substrate.
- Current prototype uses a dedicated EC2 instance (ARM64) for the Nextflow head/launcher and Docker image builds.
- Workflows are containerized; S3 is the sole shared storage (no shared filesystem).
- vEcoli uses `fsspec` with S3 URIs directly for simulation outputs.
- Expected concurrency: 5-10 concurrent workflow runs.
- All runs come from a single trusted source.
- Main pain points: cost, throughput, operational simplicity.
- Cancel is required.
- Resume/retry do not need to be first-class features right now (but S3 work dirs make resume easy to add later).
- The vEcoli task container image must be ARM64 to match Batch compute. This image is built on-the-fly from a specific git commit on an ARM64 EC2 instance, then pushed to ECR.

---

## 1. Repository Findings

### Where workflow submission is initiated

- **API entry point**: `viva_api/api/routers/gateway.py` -- `POST /simulations` calls `run_simulation_workflow()`
- **Handler orchestration**: `viva_api/common/handlers/simulations.py` -- `run_simulation_workflow()` (line 135) coordinates DB inserts, config resolution, and job submission
- **Job submission**: `viva_api/simulation/simulation_service.py` -- `SimulationServiceHpc.submit_ecoli_simulation_job()` generates an sbatch script and submits via SSH

### Where workflow status is tracked

- **Database**: `viva_api/simulation/tables_orm.py` -- `ORMHpcRun` table with `status`, `slurmjobid`, `start_time`, `end_time`, `error_message`
- **Polling loop**: `viva_api/simulation/job_scheduler.py` -- `JobScheduler.update_running_jobs()` polls SLURM every 5s, updates DB
- **Status query**: `viva_api/common/hpc/slurm_service.py` -- `SlurmService` wraps `squeue` and `scontrol` over SSH
- **Event stream**: `viva_api/common/hpc/nextflow_weblog.py` -- embedded HTTP server captures Nextflow events as NDJSON during execution

### Existing interfaces for job execution

- **`SimulationService` ABC** (`simulation/simulation_service.py`): backend-agnostic abstract methods -- `submit_build_image_job() -> JobId`, `submit_parca_job() -> JobId`, `submit_ecoli_simulation_job() -> JobId`, `cancel_job(JobId)`, `get_job_status(JobId) -> JobStatusInfo`, `read_config_template()`, `close()`. No SSH parameters -- each implementation manages its own connections.
- **`SimulationServiceHpc`** (`simulation/simulation_service.py`): SLURM implementation. Manages SSH sessions internally via `get_ssh_session_service()`.
- **`SimulationServiceK8s`** (`simulation/simulation_service_k8s.py`): K8s + AWS Batch implementation. Two-phase: SSH to EC2 for ARM64 Docker builds, K8s Jobs for Nextflow workflow execution. Config templates read via GitHub API.
- **`K8sJobService`** (`common/hpc/k8s_job_service.py`): K8s Job CRUD operations (create, status, cancel, logs) with Job condition to `JobStatus` mapping.
- **`JobId`** frozen dataclass (`common/models.py`): backend-tagged job identifier with factory methods `JobId.slurm(int)` and `JobId.k8s(str)`. Used throughout the domain layer; ORM converts at the persistence boundary.
- **`JobStatusInfo`** / **`JobStatusUpdate`** dataclasses (`common/hpc/job_service.py`): backend-agnostic status reporting and update objects, both using `JobId`.
- **`JobStatus` enum** (`common/models.py`): unified status with `from_slurm_state()`, includes `CANCELLED`.
- **`JobBackend` enum** (`common/models.py`): `SLURM` and `K8S` values.
- **`HpcRun` model** (`simulation/models.py`): tracks a job via `job_id: JobId` (excluded from serialization). Computed fields `slurmjobid`, `k8s_job_name`, `job_backend` provide API serialization compatibility. The ORM stores these as separate columns and reconstructs `JobId` at the boundary.

### Existing workflow pipeline (SLURM path)

The API manages a multi-step pipeline where each step is a separate job:

1. **Build Image** (`submit_build_image_job`) -- clone vEcoli at a specific commit, build Singularity/Apptainer container image
2. **Run Parca** (`submit_parca_job`) -- parameter calculator generates simulation dataset
3. **Run Simulation** (`submit_ecoli_simulation_job`) -- Nextflow workflow executing the simulation
4. **Run Analysis** -- post-simulation analysis

Each step is tracked as a separate `HpcRun` record with its own job type (`BUILD_IMAGE`, `PARCA`, `SIMULATION`). The build step is already managed independently from the simulation step -- the database tracks simulator versions and their build status separately.

### Nextflow integration

- **Nextflow is executed inside a SLURM job** -- the sbatch script in `workflow_slurm_script()` runs a 3-step process: (1) generate Nextflow files via container (`workflow.py --build-only`), (2) fix includes, (3) run `nextflow` on the host
- **Nextflow profile selection**: `ccam` or `aws_cdk` based on config filename
- **Nextflow models**: `common/hpc/models.py` has comprehensive Pydantic models for `NextflowWorkflow`, `NextflowStats`, `NextflowTrace`, `NextflowWave`, `NextflowFusion`
- **vEcoli `workflow.py`** supports `build_image: false` in the config, which skips container build and uses a pre-built image from a registry

### Kubernetes integration

- **K8s Job creation implemented** via `kubernetes` Python client in `K8sJobService` and `SimulationServiceK8s`
- **K8s is used for**: API Deployment (`kustomize/base/api.yaml`) and Nextflow head Jobs (created programmatically)
- **Multiple overlays**: `sms-api-rke`, `sms-api-rke-dev`, `sms-api-stanford`, `sms-api-stanford-test`, `sms-api-eks`, `sms-api-local`
- **Backend selection**: `get_job_backend()` in `config.py` returns `"k8s"` for Stanford namespaces, `"slurm"` otherwise

### Testing structure

- **Testcontainers**: PostgreSQL (`postgres_fixtures.py`), Redis (`redis_fixtures.py`), MongoDB (`mongodb_fixtures.py`)
- **Async**: `pytest-asyncio` throughout
- **Mocks**: `simulation_service_mocks.py` has `ConcreteSimulationService`, `MockSSHSession`, `MockSSHSessionService`
- **Integration tests**: `tests/integration/test_hpc_workflow.py` (requires SSH), `test_run_workflow_simple.py`
- **ASGI client**: `httpx.AsyncClient` with `ASGITransport` for in-process API testing

### Configuration model

- **`viva_api/config.py`**: Pydantic `Settings` with SLURM, Postgres, Redis, S3/GCS/Qumulo, GitHub creds
- **Deployment namespace**: `deployment_namespace` field maps to kustomize overlays
- **Backend selection**: `get_job_backend()` returns `"k8s"` for Stanford namespaces, `"slurm"` otherwise
- **K8s/Batch settings**: `k8s_job_namespace`, `nextflow_container_image`, `batch_job_queue`, `batch_region`, `s3_work_bucket`, `s3_work_prefix`, `s3_output_prefix`, `ecr_repository`, `submit_node_host`/`user`/`key_path`/`ssm_instance_id`

### Not yet implemented

- RBAC (ServiceAccount, Role, RoleBinding) for K8s Job management
- S3-based output retrieval (currently SSH/SCP for SLURM path)
- K8s pod log retrieval (currently SLURM log files via SSH)
- Nextflow submit container image (`Dockerfile-nextflow`)
- Real AWS integration tests

---

## 2. Current Architecture Summary

```
+-------------------------------------+
|         EKS (Kubernetes, AMD64)     |
|  +-------------------------------+  |
|  |   sms-api (FastAPI Deployment)|  |
|  |   +-- POST /simulations       |  |
|  |   +-- DELETE /simulations/cancel |
|  |   +-- JobScheduler (poll 5s)  |  |
|  |   +-- Redis subscriber        |  |
|  +----------+--------------------+  |
|             | SSH                    |
|  +----------v--------------------+  |
|  |   PostgreSQL  |  Redis        |  |
|  +---------------+---------------+  |
+-------------+------|----------------+
              | SSH (asyncssh)
              v
+-------------------------------------+
|     SLURM HPC Cluster               |
|  +-------------------------------+  |
|  | Login Node                    |  |
|  |  +-- sbatch (submit)          |  |
|  |  +-- squeue/scontrol (poll)   |  |
|  |  +-- scancel (cancel)        |  |
|  +----------+--------------------+  |
|             | SLURM scheduler        |
|  +----------v--------------------+  |
|  | Compute Node (sbatch job)     |  |
|  |  +-- Singularity: workflow.py |  |
|  |  |   +-- generates NF files   |  |
|  |  +-- Nextflow head process    |  |
|  |  |   +-- submits tasks->SLURM |  |
|  |  |   +-- weblog -> NDJSON     |  |
|  |  +-- Output -> shared FS      |  |
|  +-------------------------------+  |
+-------------------------------------+
```

**Key insight**: Nextflow currently runs as a subprocess inside a SLURM batch job, and Nextflow submits its tasks back to SLURM. The entire system is SSH-mediated.

---

## 3. Existing CDK Infrastructure

The `sms-cdk` repository (`lib/batch-stack.ts`) deploys AWS Batch infrastructure for running vEcoli workflows. This is the target compute backend.

### What already exists

| Component | Details |
|---|---|
| **Batch Compute (Spot)** | ARM64/Graviton instances (m8g.2xlarge, c8g.2xlarge, m7g.2xlarge), SPOT_CAPACITY_OPTIMIZED, priority 1 |
| **Batch Compute (On-Demand)** | Same instance types, BEST_FIT_PROGRESSIVE, priority 2 (fallback) |
| **Job Queue** | Routes to Spot first, falls back to On-Demand |
| **EC2 Submit Node** | t4g.medium (ARM64), AL2023, Docker/Java/Nextflow pre-installed, SSM access, 100 GiB GP3 |
| **S3 Bucket** | Shared bucket for `nextflow/work/` (staging) and `vecoli-output/` (results) |
| **ECR** | Stores vEcoli task images built on the submit node |
| **Networking** | Private subnets, NAT gateway, no public IPs |
| **IAM Roles** | `BatchSubmitNodeRole` (Batch job mgmt, ECR push/pull, S3 rw, PassRole, CW Logs), `BatchComputeRole` (S3 rw, ECR pull, ECS) |

### Architecture: CPU architecture

- **Batch compute**: ARM64 (Graviton) recommended for vEcoli's CPU-bound workloads. Configurable via CDK `cpuArchitecture`.
- **EC2 submit node**: Matches Batch architecture (ARM64). Builds Docker images natively -- no cross-compilation needed.
- **EKS nodes**: Currently AMD64. The Nextflow head process (orchestration only) runs here.

The vEcoli Dockerfile (`runscripts/container/Dockerfile`) and build script (`runscripts/container/build-and-push-ecr.sh`) are **architecture-agnostic**. The image architecture is determined by where `docker build` runs, not by any flag. Both Dockerfiles (task image and Nextflow submit image) should follow this pattern -- multi-arch base images, detect arch at build time for tool installs.

The Nextflow head does not execute simulation code. It submits Batch jobs referencing task images by ECR URI. **The head and task architectures do not need to match.**

### vEcoli workflow config structure

The `aws` section of the workflow config controls Batch execution:

```json
{
  "emitter_arg": {
    "out_uri": "s3://<shared-bucket>/vecoli-output/<experiment-id>"
  },
  "aws": {
    "build_image": false,
    "container_image": "<account>.dkr.ecr.<region>.amazonaws.com/vecoli:<tag>",
    "region": "us-gov-west-1",
    "batch_queue": "<job-queue-name>"
  },
  "progress_bar": false
}
```

- `build_image: false` -- use a pre-built image from ECR (the API builds it in a separate step)
- `container_image` -- ECR URI of the ARM64 vEcoli task image
- `NXF_WORK` environment variable -- `s3://<shared-bucket>/nextflow/work`

### S3 data flow

S3 is the sole shared storage. No shared filesystem between the head and compute nodes.

```
s3://<shared-bucket>/
  +-- nextflow/work/          Nextflow task staging (inputs, outputs, scripts)
  +-- vecoli-output/          Workflow results (parquet, analysis)
```

vEcoli writes simulation outputs to S3 directly via `fsspec` with S3 URIs. Nextflow manages task-level data staging to/from S3 within containers.

---

## 4. Option A: Kubernetes Job per Workflow Run (Recommended)

### Two-phase execution model

The vEcoli workflow has a hard constraint: the task container image must be ARM64, and building it requires an ARM64 host with Docker. The API already manages image builds as a separate pipeline step (`submit_build_image_job`). This maps naturally to a two-phase model:

| Phase | Where | Architecture | Duration | What |
|---|---|---|---|---|
| **1. Build image** | EC2 submit node via SSH/SSM | ARM64 | Minutes | Clone vEcoli at commit, `docker build`, push to ECR |
| **2. Run workflow** | K8s Job on EKS | AMD64 | Hours | Nextflow head orchestrates Batch tasks via `workflow.py` |

Phase 1 reuses the existing EC2 submit node and SSH/SSM access pattern. Phase 2 replaces the long-running SLURM job (or EC2 tmux session) with an ephemeral K8s Job.

### Target architecture

```
+------------------------------------------+
|         EKS Cluster (AMD64)              |
|                                          |
|  sms-api Deployment                      |
|  +------------------------------------+  |
|  | POST /simulations                  |  |
|  |   1. DB insert (simulator, sim)    |  |
|  |   2. SSH to EC2: build + push ECR  |  |
|  |   3. Create K8s Job (NF head)      |  |
|  |                                    |  |
|  | JobScheduler                       |  |
|  |   polls K8s Job status             |  |
|  |   updates HpcRun in DB            |  |
|  |                                    |  |
|  | DELETE /simulations/{id}/cancel    |  |
|  |   delete K8s Job (Foreground)      |  |
|  +------------------------------------+  |
|                                          |
|  K8s Job: nf-sim-{experiment-id}         |
|  +------------------------------------+  |
|  | Nextflow head (AMD64 container)    |  |
|  | - workflow.py --config ...         |  |
|  | - build_image: false               |  |
|  | - NXF_WORK=s3://bucket/nf/work     |  |
|  | - Submits tasks to Batch queue     |  |
|  +---------------+--------------------+  |
|                  |                        |
+------------------------------------------+
                   | Batch API
                   v
+------------------------------------------+
|  AWS Batch (ARM64/Graviton)              |
|  +------------------------------------+  |
|  | Spot CE (priority 1)              |  |
|  | On-Demand CE (priority 2)         |  |
|  |                                    |  |
|  | Task containers:                   |  |
|  |   - Pull ARM64 image from ECR     |  |
|  |   - Read inputs from S3           |  |
|  |   - Execute simulation step       |  |
|  |   - Write outputs to S3           |  |
|  +------------------------------------+  |
+------------------------------------------+
                   |
                   v
+------------------------------------------+
|  S3 Bucket                               |
|  +-- nextflow/work/{experiment-id}/      |
|  +-- vecoli-output/{experiment-id}/      |
+------------------------------------------+

+------------------------------------------+
|  EC2 Submit Node (ARM64, t4g.medium)     |
|  +------------------------------------+  |
|  | Used by Phase 1 only:              |  |
|  |   - Clone vEcoli repo at commit    |  |
|  |   - docker build (ARM64 native)    |  |
|  |   - docker push to ECR             |  |
|  | Access: SSH or SSM from API pod    |  |
|  +------------------------------------+  |
+------------------------------------------+
```

### How current code maps to Option A

| Current Component | Option A Phase 1 (Build) | Option A Phase 2 (Workflow) |
|---|---|---|
| `SimulationServiceHpc.submit_build_image_job()` | Reuse pattern: SSH to EC2 submit node, build Docker image, push ECR | N/A |
| `SimulationServiceHpc.submit_ecoli_simulation_job()` | N/A | New: `SimulationServiceK8s` creates K8s Job |
| `SlurmService.submit_job()` | SSH command to EC2 | `BatchV1Api.create_namespaced_job()` |
| `SlurmService.get_job_status_squeue()` | SSH poll or SSM | `BatchV1Api.read_namespaced_job_status()` |
| sbatch script | Shell commands over SSH | K8s Job spec (Python object) |
| `workflow_slurm_script()` | `docker build && docker push` script | Container `command` + `args` in Job spec |
| `JobScheduler` polling SLURM | Poll build status via SSH | Poll K8s Job status (in-cluster API, no SSH) |
| SSH session management | Retained for build phase | Not needed -- ServiceAccount + RBAC |

### New components needed

1. **`SimulationServiceK8s`** -- implements `SimulationService` ABC
   - `submit_build_image_job()`: SSH/SSM to EC2 submit node, run Docker build + push
   - `submit_ecoli_simulation_job()`: create K8s Job with Nextflow container
   - `cancel_job()`: `delete_namespaced_job(propagation_policy="Foreground")`
   - `get_job_status()`: `read_namespaced_job_status()`
2. **`K8sJobStatusService`** -- implements `JobStatusService`, maps K8s Job conditions to `JobStatus`
3. **Nextflow submit container image** (AMD64) -- Dockerfile with Java, Nextflow, vEcoli repo, `workflow.py` entrypoint
4. **IRSA role** -- same permissions as `BatchSubmitNodeRole` (Batch job mgmt, ECR pull, S3 rw, PassRole, CW Logs)
5. **K8s RBAC** -- ServiceAccount for API pod with `batch_v1` Job create/get/delete/list
6. **Config additions** -- `k8s_job_namespace`, `nextflow_container_image`, `batch_job_queue`, `batch_region`, `s3_work_bucket`, `s3_output_prefix`, `ecr_repository`, `submit_node_instance_id` (for SSM) or SSH connection details

### Reusable components (unchanged)

- `SimulationService` ABC (already has `cancel_job`)
- `JobStatusUpdate` dataclass
- `JobScheduler` (swap `JobStatusService` implementation)
- `DatabaseService` / `HpcRun` (already has `k8s_job_name`, `job_backend`, `external_job_id`)
- `gateway.py` router (unchanged)
- `handlers/simulations.py` (calls `SimulationService` interface)
- All Nextflow models in `common/hpc/models.py`

### Pros

- **Eliminates SSH for the long-running phase** -- the hours-long Nextflow orchestration runs as a K8s Job; SSH is only needed for the short build step (minutes)
- **Native scaling** -- K8s handles pod scheduling; 5-10 concurrent workflow Jobs are trivial
- **Isolation** -- each workflow run gets its own pod; no shared state, no process management
- **Cancel is simple** -- `delete_namespaced_job(propagation_policy="Foreground")` kills the Nextflow head; Nextflow's shutdown hook cancels Batch tasks
- **Observability** -- `kubectl logs`, pod events, K8s-native monitoring
- **Cost** -- Nextflow head is lightweight (~2 CPU, 4Gi RAM); runs on existing EKS nodes at near-zero marginal cost
- **Existing EKS** -- already deployed; no new infrastructure except IRSA role
- **Credentials** -- IRSA scoped to the specific pod, not an entire node
- **Resume-ready** -- S3 work directory (`s3://bucket/nextflow/work/{experiment_id}`) persists beyond pod lifecycle; adding `-resume` later is one flag

### Cons

- **SSH retained for builds** -- EC2 submit node is still needed for ARM64 Docker image builds; not a fully SSH-free architecture
- **New dependency** -- `kubernetes` Python client library
- **RBAC complexity** -- must grant API pod permission to create/manage Jobs
- **Nextflow container image** -- must build and maintain an AMD64 image with Nextflow + Java + vEcoli
- **Log retrieval** -- pod logs are ephemeral after Job cleanup; configure `ttlSecondsAfterFinished` or write logs to S3
- **Two execution substrates** -- build phase on EC2, workflow phase on K8s; slightly more complex than a single-substrate approach

### Design: Cancel flow

```
API DELETE /simulations/{id}/cancel
  -> lookup HpcRun -> get k8s_job_name
  -> BatchV1Api.delete_namespaced_job(name, namespace, propagation_policy="Foreground")
  -> update HpcRun status = CANCELLED
  -> Nextflow receives SIGTERM, cancels in-flight Batch tasks
```

### Design: Status flow

```
JobScheduler polling loop (every 30s):
  -> list_active_hpcruns() from DB (filter job_backend="k8s")
  -> for each: BatchV1Api.read_namespaced_job_status(k8s_job_name, namespace)
  -> map K8s Job .status.conditions -> JobStatus
  -> update_hpcrun_status() via JobStatusUpdate
```

### Design: Nextflow submit container

```dockerfile
FROM amazoncorretto:21-al2023
RUN dnf install -y git jq python3-pip && \
    pip3 install s3fs boto3 && \
    curl -fsSL https://github.com/nextflow-io/nextflow/releases/download/v25.10.2/nextflow \
      -o /usr/local/bin/nextflow && chmod +x /usr/local/bin/nextflow
COPY . /vEcoli
WORKDIR /vEcoli
ENTRYPOINT ["python", "runscripts/workflow.py"]
```

**Architecture**: AMD64 (runs on EKS nodes, not on Batch compute).

### Design: K8s Job spec (generated by `SimulationServiceK8s`)

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: nf-sim-{experiment_id}
  namespace: {k8s_job_namespace}
  labels:
    app: sms-api
    job-type: simulation
    experiment-id: {experiment_id}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 86400  # 24h, for log access
  template:
    spec:
      serviceAccountName: batch-submit
      containers:
      - name: nextflow
        image: {ecr_account}.dkr.ecr.{region}.amazonaws.com/vecoli-submit:latest
        args: ["--config", "/config/workflow.json"]
        env:
        - name: NXF_WORK
          value: "s3://{s3_bucket}/nextflow/work/{experiment_id}"
        - name: AWS_DEFAULT_REGION
          value: "{batch_region}"
        volumeMounts:
        - name: config
          mountPath: /config
        resources:
          requests:
            cpu: "2"
            memory: "4Gi"
          limits:
            cpu: "2"
            memory: "4Gi"
      volumes:
      - name: config
        configMap:
          name: nf-sim-{experiment_id}-config
      restartPolicy: Never
```

### Design: Workflow config (injected via ConfigMap)

```json
{
  "experiment_id": "{experiment_id}",
  "emitter_arg": {
    "out_uri": "s3://{s3_bucket}/vecoli-output/{experiment_id}"
  },
  "aws": {
    "build_image": false,
    "container_image": "{ecr_account}.dkr.ecr.{region}.amazonaws.com/vecoli:{git_commit_hash}",
    "region": "{batch_region}",
    "batch_queue": "{batch_job_queue}"
  },
  "progress_bar": false,
  "generations": 1,
  "parca_options": { ... }
}
```

---

## 5. Option B: Pet EC2 Launcher Service

### How current code maps to Option B

| Current Component | Option B Equivalent |
|---|---|
| `SimulationServiceHpc` (SSH+SLURM) | New `SimulationServiceLauncher` that calls launcher HTTP API |
| `SlurmService.submit_job()` | HTTP POST to launcher `/runs` endpoint |
| `SlurmService.get_job_status_squeue()` | HTTP GET from launcher `/runs/{id}/status` |
| sbatch script | Launcher creates `nextflow run` subprocess |
| SSH session management | HTTP client (httpx) to launcher |
| `JobScheduler` polling SLURM | `JobScheduler` polling launcher API |

### New components needed

1. **Launcher service** (separate application on EC2) -- significant new codebase:
   - HTTP API (FastAPI or similar)
   - Process manager (supervise Nextflow subprocesses)
   - State tracking (in-memory + optional persistence)
   - Log capture and streaming
   - Cancel endpoint (send SIGTERM to Nextflow process)
   - Health check / heartbeat
2. **`SimulationServiceLauncher`** -- implements `SimulationService`, calls launcher HTTP API
3. **`LauncherJobStatusService`** -- implements `JobStatusService`, polls launcher
4. **EC2 infrastructure** -- the submit node already exists, but the launcher service is new software
5. **Networking** -- EKS to EC2 communication requires stable addressing, security group rules

### Pros

- **Single execution substrate** -- both build and workflow run on the same ARM64 EC2 instance
- **Full filesystem** -- Nextflow has local scratch disk; no Fusion/S3 overhead for work directory
- **Nextflow resume** -- local work directory enables `-resume` trivially
- **Proven path** -- prototype already works this way

### Cons

- **Significant new service to build** -- the launcher is a real application (~500-1000 LOC) with process management, state tracking, error handling, graceful shutdown
- **Single point of failure** -- if the EC2 instance goes down, all running workflows are lost
- **Cost** -- EC2 instance runs 24/7 (~$25/mo for t4g.medium, more for a larger instance to handle 5-10 concurrent heads)
- **Operational burden** -- AMI updates, patching, monitoring, lifecycle management
- **Cancel complexity** -- must reliably SIGTERM the correct process, handle orphaned Batch tasks
- **Two deployment targets** -- must deploy and version both EKS API and EC2 launcher
- **Testing** -- harder to simulate locally; can't use kind/k3d

---

## 6. Testability Comparison

### Unit testing

**Option A wins.** The K8s Python client has well-established mock patterns. `create_namespaced_job()` and `read_namespaced_job_status()` return typed objects that are straightforward to mock. The build phase (SSH to EC2) is tested the same way as the existing SLURM SSH path.

Option B requires mocking an HTTP client to the launcher, and the launcher itself needs its own unit tests -- doubling the test surface.

### Local integration testing

| Aspect | Option A (K8s Job) | Option B (EC2 Launcher) |
|---|---|---|
| Testcontainers | Postgres, Redis (same as now) | Same + need to simulate launcher |
| LocalStack | Useful for S3 work-dir validation | Same |
| Local K8s (kind/k3d) | Natural fit -- create real K8s Jobs with stub containers | Not applicable |
| Build phase | Mock SSH (same as existing SLURM mocks) | Same |
| End-to-end local | `kind` + LocalStack + Testcontainers | Requires running a real launcher process |

### Real AWS integration testing

Both options are roughly equivalent:

- **What needs real AWS**: Batch job submission, S3 read/write, IAM permissions, ECR image builds
- **What stays local**: Database operations, API routing, status polling logic, config resolution

**Recommended test layers:**

| Layer | Scope | Infrastructure |
|---|---|---|
| Unit tests | All services, DB ops, config parsing, status mapping | Mocks only |
| Local integration | Testcontainers (Postgres, Redis) + mock job backend | kind for K8s Jobs, mock SSH for builds |
| Real AWS integration | Submit real Nextflow workflow to Batch, verify outputs, test cancel | Real AWS account with Batch + S3 + ECR |

### Testability verdict

**Option A is materially easier to test.** The K8s API is well-mocked, kind provides a real local cluster, and the testing surface is smaller.

---

## 7. Recommendation

### Option A: Kubernetes Job per workflow run

**Why Option A is better for this codebase:**

1. **Eliminates SSH for the long-running phase.** The hours-long Nextflow orchestration runs as a K8s Job with in-cluster API access. SSH is only needed for the short build step (minutes), which the existing codebase already handles.

2. **The existing `SimulationService` ABC is a perfect fit.** `SimulationServiceK8s` slots in as a new implementation. The handler code, `JobScheduler`, `DatabaseService` -- all unchanged.

3. **Cancel is trivial.** `delete_namespaced_job()` with foreground propagation kills the pod, sends SIGTERM to Nextflow, which cancels Batch tasks.

4. **Cost is lower.** Nextflow heads run on existing EKS nodes. No idle EC2 cost for the orchestration phase. The EC2 submit node is still needed for builds but can be stopped between builds.

5. **Operational simplicity.** K8s handles scheduling, restarts, and resource limits for the long-running phase. One primary deployment target.

6. **The CDK repo already sketches this approach.** The "Future Direction" section in `batch-architecture.md` describes K8s Job submission with IRSA, matching this design.

### When Option B would be reasonable

- If Nextflow resume with local work directories was a hard requirement
- If running on bare metal or a non-Kubernetes environment
- If image builds needed to be tightly coupled with workflow execution (single-step)

---

## 8. Detailed Implementation Plan

### Stage 1: Core abstractions and cancel support [DONE]

Completed changes:
- `cancel_job(JobId)` added to `SimulationService` ABC; implemented as `scancel` in `SimulationServiceHpc`
- `DELETE /simulations/{id}/cancel` endpoint with no-op for terminal jobs
- `JobStatus.CANCELLED` added (SLURM `CANCELLED` state maps to it instead of `FAILED`)
- `JobId` frozen dataclass with `JobId.slurm(int)` / `JobId.k8s(str)` factories and `as_slurm_int` property
- `HpcRun.job_id: JobId` replaces separate `slurmjobid`/`k8s_job_name` fields in domain model; computed fields provide API serialization
- `JobBackend` enum replaces string literals
- `JobStatusInfo` / `JobStatusUpdate` dataclasses using `JobId`
- `DatabaseService.insert_hpcrun(job_id: JobId, ...)` -- ORM decomposes `JobId` at persistence boundary
- Alembic migration: `k8s_job_name`, `job_backend` columns, nullable `slurmjobid`
- `SimulationService` ABC refactored: no SSH params, `JobId` return types, `get_job_status(JobId)`, `read_config_template()`
- `SimulationServiceHpc` manages SSH sessions internally
- Handlers no longer manage SSH context for service calls
- `get_simulation_status` delegates to `SimulationService.get_job_status()` instead of calling `SlurmService` directly

### Stage 2: K8s Job service implementation [DONE]

Completed changes:
- `viva_api/simulation/simulation_service_k8s.py` -- `SimulationServiceK8s(SimulationService)`:
  - `submit_build_image_job()`: SSH to ARM64 EC2 submit node, Docker build + ECR push
  - `submit_ecoli_simulation_job()`: creates K8s Job + ConfigMap with workflow config (aws section, `build_image: false`, S3 paths)
  - `submit_parca_job()`: placeholder (parca runs within Nextflow workflow)
  - `cancel_job()`: `delete_namespaced_job(propagation_policy="Foreground")` + ConfigMap cleanup
  - `get_job_status()`: delegates to `K8sJobService`
  - `read_config_template()`: GitHub Contents API via httpx
  - `get_latest_commit_hash()`: GitHub API via httpx
- `viva_api/common/hpc/k8s_job_service.py` -- `K8sJobService`: K8s Job CRUD, ConfigMap management, pod log retrieval, Job condition to `JobStatus` mapping
- `viva_api/config.py` -- K8s/Batch settings: `k8s_job_namespace`, `nextflow_container_image`, `batch_job_queue`, `batch_region`, `s3_work_bucket`, `s3_work_prefix`, `s3_output_prefix`, `ecr_repository`, `submit_node_*`. `get_job_backend()` function.
- `viva_api/dependencies.py` -- `init_standalone()` branches on `get_job_backend()`: creates `SimulationServiceK8s` for K8s, `SimulationServiceHpc` for SLURM. SSH targets EC2 submit node (K8s) or SLURM login node. Extracted `_init_simulation_service()` and `_init_ssh_service()` helpers.
- `viva_api/simulation/job_scheduler.py` -- `slurm_service` now optional; SLURM polling skipped for K8s backend
- `pyproject.toml` -- added `kubernetes>=31.0.0`, `httpx>=0.28.0`
- `tests/simulation/test_k8s_backend.py` -- 17 unit tests: K8s status mapping, backend selection, `JobId` type safety, `K8sJobService` with mocked K8s client

### Stage 3: Nextflow submit container and multi-arch builds [DONE]

The K8s Job uses an **init container pattern** — two containers in one pod sharing an `emptyDir` volume:

1. **Init container** (vEcoli task image from ECR, multi-arch): runs `workflow.py --build-only` to generate Nextflow files, copies `sim.nf`/`analysis.nf` to the shared volume, then persists all generated files to S3 for debugging/audit/resume.
2. **Main container** (`Dockerfile-nextflow`, minimal: Java 21 + Nextflow 25.10.2): reads generated files from the shared volume, fixes include paths, optionally starts weblog receiver, runs `nextflow`.

The Nextflow submit container does NOT contain vEcoli — it's a lightweight image (~400MB) with just the tools needed to orchestrate Nextflow.

**Multi-arch vEcoli task image builds**: `SimulationServiceK8s._run_build()` uses `docker buildx` on the ARM64 EC2 submit node to build both ARM64 (native) and AMD64 (via QEMU) images under a single ECR tag. ARM64 is used by Batch compute, AMD64 is used by the K8s init container on EKS nodes. Uses `build-and-push-ecr.sh -u` for ECR URI lookup.

**Async build tracking**: Build phase spawns as a background `asyncio.Task` via `LocalTaskService`, returning a `JobId.local(uuid)` immediately. `JobBackend.LOCAL` added for in-process async operations.

**DB simplification**: `slurmjobid` (int) and `k8s_job_name` (str) columns replaced by single `job_id_ext` (str) column. Migration converts existing SLURM integer IDs to strings. `HpcRun` model uses `JobId` internally with `computed_field` properties for API serialization.

Completed files:
- `Dockerfile-nextflow` -- `amazoncorretto:21-al2023` base, Nextflow 25.10.2, Python 3.9 (for weblog)
- `scripts/entrypoint-nextflow.sh` -- verifies init container output, fixes include paths, optional weblog receiver, runs nextflow
- `scripts/nextflow-weblog-receiver.py` -- standalone weblog receiver extracted from `nextflow_weblog.py`
- `viva_api/common/hpc/local_task_service.py` -- in-process async task tracker for build phase
- `viva_api/simulation/simulation_service_k8s.py` -- init container in Job spec, multi-arch `docker buildx` build
- `viva_api/common/models.py` -- `JobBackend.LOCAL`, `JobId.local()` factory
- `alembic/versions/0f991fad32ba_...` -- migration: `slurmjobid` (int) -> `job_id_ext` (str)
- OpenAPI spec and client regenerated

### Stage 4: Wiring, RBAC, and integration

**Goal**: End-to-end flow with K8s Jobs + AWS Batch tasks.

Files to modify:
- `viva_api/common/handlers/simulations.py` -- ensure `get_simulation_outputs()` works with S3 (not SSH/SCP)
- `viva_api/common/handlers/simulations.py` -- ensure `get_simulation_log()` works with K8s pod logs or S3
- `kustomize/base/` -- add RBAC (ServiceAccount, Role, RoleBinding) for Job management
- `kustomize/overlays/sms-api-stanford/` -- add K8s backend config

New files:
- `kustomize/base/rbac-jobs.yaml` -- ServiceAccount + Role + RoleBinding for K8s Job CRUD
- `tests/integration/test_k8s_workflow.py` -- integration test with kind
- `tests/integration/test_k8s_workflow_mock.py` -- mock integration

CDK-side changes (in `sms-cdk` repo):
- New IRSA role with `BatchSubmitNodeRole` permissions
- ServiceAccount annotation: `eks.amazonaws.com/role-arn: arn:...:role/batch-submit-irsa`

### Stage 5: Real AWS integration tests

**Goal**: Validate against real AWS Batch + S3 + ECR.

New files:
- `tests/integration/test_aws_batch_e2e.py` -- real Batch submission, S3 output verification, cancel test

Test markers:
```python
@pytest.mark.skipif(not os.getenv("AWS_BATCH_INTEGRATION"), reason="Requires AWS")
```

### Recommended rollout order

1. **Stage 1** -- done
2. **Stage 2** -- done
3. **Stage 3** -- done
4. **Stage 4** -- wiring and integration
5. **Stage 5** -- real AWS validation (requires 4)

### Migration from current state

- Keep `SimulationServiceHpc` (SLURM) for RKE deployments
- Deploy `SimulationServiceK8s` for Stanford/EKS deployments, selectable via `deployment_namespace`
- Keep EC2 submit node for ARM64 image builds (can be stopped between builds to save cost)
- Test on `sms-api-stanford-test` before production rollout
- Retire EC2 submit node for workflow orchestration once K8s path is validated; retain only for image builds (or migrate builds to CI/CD with ARM runners later)

---

## 9. Open Questions / Assumptions

### Resolved

1. **Nextflow executor config**: The vEcoli workflow config has an `aws` section with `batch_queue`, `container_image`, `region`. `workflow.py` supports `build_image: false` for pre-built images.

2. **Fusion**: Not required. vEcoli uses `fsspec` with S3 URIs directly for simulation outputs. Nextflow manages task-level data staging natively with S3 work directories.

3. **Batch job definitions**: Nextflow registers its own Batch job definitions dynamically. The API does not need to pre-create them.

4. **S3 output structure**: `s3://<bucket>/vecoli-output/{experiment_id}` via `emitter_arg.out_uri` in the workflow config.

### Open

5. **Worker events (Redis)**: The current worker event stream comes from the simulation container via Redis. In the Batch model, can Batch task containers reach the Redis endpoint? If not, switch to S3-based or CloudWatch-based event capture, or drop real-time worker events for the Batch path.

6. **EKS node capacity**: Are the existing EKS nodes sized to handle 5-10 additional pods (~2 CPU, 4Gi each)? If using Karpenter/Cluster Autoscaler, this is automatic.

7. **IAM/IRSA**: Does the EKS cluster already have IRSA configured? This is needed for the Nextflow pod to call Batch and S3.

8. **ECR image lifecycle**: How should old vEcoli task images be cleaned up? ECR lifecycle policies can auto-expire untagged images.

9. **Submit node access method**: Using SSH (consistent with existing SLURM path). SSM was considered but deferred — SSH is simpler and already implemented via `asyncssh`.

---

## 10. Current Status and Next Steps

Stages 1, 2, and 3 are complete. The application code for the K8s/Batch backend is fully implemented:
- Backend-agnostic `SimulationService` ABC with typed `JobId`
- `SimulationServiceK8s` with init container pattern and multi-arch builds
- `Dockerfile-nextflow` container image (builds successfully)
- Cancel endpoint, `LocalTaskService` for async builds
- OpenAPI spec and client regenerated

**Next steps (in order):**

1. **Stage 4: Wiring and integration**
   - S3-based output retrieval — `get_simulation_outputs()` currently uses SSH/SCP; needs S3 alternative for K8s path
   - K8s pod log retrieval — `get_simulation_log()` currently reads SLURM log files; needs K8s pod logs or S3
   - `kustomize/base/rbac-jobs.yaml` — ServiceAccount + Role + RoleBinding for K8s Job CRUD
   - Kustomize overlay config for Stanford deployments
   - Build and push `Dockerfile-nextflow` image to ECR
   - Ensure QEMU/buildx available on EC2 submit node (CDK user data)

2. **Stage 5: Real AWS integration tests**
   - End-to-end: submit workflow via API, verify init container runs, Nextflow submits to Batch, outputs land in S3
   - Cancel test: verify `delete_namespaced_job` propagates to Nextflow and Batch tasks
   - Use `AWS_PROFILE=stanford-sso`, `KUBECONFIG=kubeconfig_stanford_test.yaml`

3. **CDK-side (sms-cdk repo)**
   - IRSA role with `BatchSubmitNodeRole` permissions
   - ServiceAccount annotation: `eks.amazonaws.com/role-arn`
   - Ensure `docker buildx` and QEMU user-static installed on submit/build node

---

## Prior Art

### `aws-batch` branch (not merged)

A prior branch explored adding AWS Batch as an alternative backend. Patterns reused in this implementation:
- Strategy pattern with `SimulationService` ABC (adopted and extended with `JobId` type safety)
- Backend selection via `deployment_namespace` (adopted as `get_job_backend()`)
- GitHub API for `read_config_template()` (adopted in `SimulationServiceK8s`)

Issues from that branch that were addressed:
- Placeholder Alembic migration IDs -- auto-generated proper ID
- Duplicate dataclasses (`JobStatusInfo` / `JobStatusUpdate`) -- both retained but now use `JobId` consistently
- String-based job IDs -- replaced with typed `JobId` frozen dataclass

### CDK `batch-architecture.md`

The CDK repo's architecture doc includes a "Future Direction" section proposing K8s Job submission. Key resources from that sketch:
- Dockerfile for Nextflow submit container (`amazoncorretto:21-al2023`)
- K8s Job manifest with IRSA ServiceAccount
- ConfigMap pattern for workflow config injection
- Migration path: keep EC2 for interactive use, add K8s for automated runs, retire EC2 later

### CDK `manual_batch.md`

Summary of the manually-configured Batch architecture from `vEcoli-private/doc/aws.rst`. Confirms:
- S3-only data flow (no shared filesystem)
- Graviton/ARM64 for CPU-bound vEcoli workloads
- Spot instances for cost savings
- `NXF_WORK` pointing to S3
