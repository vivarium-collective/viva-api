# Multi-Arch Build Pipeline — AWS Batch with Docker-outside-of-Docker (DooD)

## Status: IMPLEMENTED

This document describes the multi-architecture Docker image build pipeline using AWS Batch with Docker-outside-of-Docker (DooD).

## Problem

- AWS Batch simulation tasks need **ARM64** containers (Graviton compute — cheaper, better perf)
- EKS worker nodes are **AMD64** only (GovCloud `us-gov-west-1` limitation)
- A single-arch build node cannot build both architectures
- CodeBuild ARM64 is not available in `us-gov-west-1`

## Solution

Two parallel AWS Batch jobs — one on ARM64 compute, one on AMD64 compute — each builds Docker images natively for its target architecture using **Docker-outside-of-Docker (DooD)**.

DooD mounts the host EC2 instance's Docker socket (`/var/run/docker.sock`) into the `docker:cli` container. No Docker daemon runs inside the container — it uses the host's daemon directly. This avoids all DinD cgroup/storage driver issues.

## Architecture

```
viva-api (submit_build_image_job)
│
├── AWS Batch Job (ARM64 Graviton compute)
│   └── docker:cli container (DooD via mounted socket)
│       └── Clones vEcoli, builds + pushes vecoli:{commit}
│           (ARM64 task image for Batch simulation tasks)
│
└── AWS Batch Job (AMD64 x86 compute)
    └── docker:cli container (DooD via mounted socket)
        └── Clones vEcoli, builds + pushes vecoli:{commit}-submit
            (AMD64 submit image: vEcoli + Java + Nextflow for EKS K8s Job)
```

## What Gets Built

| Image | Architecture | Used By | Built By |
|-------|-------------|---------|----------|
| `vecoli:{commit}` | ARM64 | AWS Batch tasks (Graviton) | Batch ARM64 job |
| `vecoli:{commit}-submit` | AMD64 | K8s Nextflow head Job (EKS) | Batch AMD64 job |

## Infrastructure (CDK stack: `smsvpctest-build-batch`)

| Resource | Details |
|----------|---------|
| ARM64 compute env | Graviton4 (m8g.xlarge, c8g.xlarge), on-demand, 16 max vCPUs |
| AMD64 compute env | x86 (m7i.xlarge, c7i.xlarge), on-demand, 16 max vCPUs |
| ARM64 job queue | `vecoli-build-arm64` |
| AMD64 job queue | `vecoli-build-amd64` |
| Job definition | `vecoli-dind-build` — `docker:cli`, unprivileged, host Docker socket mounted |
| GitHub PAT | `vecoli-github-pat` in Secrets Manager |
| ECR repository | `vecoli` |

## Build Script Flow

Each Batch job runs this sequence inside the `docker:cli` container:

1. `apk add aws-cli git bash` — install tools (Alpine base)
2. `docker info` — verify host Docker socket is accessible
3. `aws secretsmanager get-secret-value` — fetch GitHub PAT
4. `git clone` — clone vEcoli private repo via HTTPS + PAT
5. `git checkout {commit}` — checkout specific commit
6. `aws ecr get-login-password | docker login` — authenticate to ECR
7. `bash build-and-push-ecr.sh` — build Docker image and push to ECR

The AMD64 job additionally builds the submit image (vEcoli + Java + Nextflow) on top of the base image.

## Key Learnings

- **DinD doesn't work in AWS Batch** — Docker daemon inside containers fails due to cgroup/storage driver issues even with privileged mode on EC2-backed compute
- **DooD works perfectly** — mounting host Docker socket is simple and reliable
- **`docker:cli`** (not `docker:dind`) — only need the CLI, not the daemon
- **`build-and-push-ecr.sh` requires `USER` env var** — set `export USER=${USER:-viva-api}` before calling it
- **GitHub private repos don't support `git fetch --depth 1 origin {hash}`** — use full single-branch clone instead
- **GitHub PAT auth** — `https://x-access-token:${PAT}@github.com/...` is the standard convention

## viva-api Code

- `simulation_service_k8s.py`: `_build_command()` generates the shell script, `_submit_batch_build()` submits to Batch, `_poll_batch_jobs()` polls for completion, `_run_build()` orchestrates both jobs
- `config.py`: `build_arm64_queue`, `build_amd64_queue`, `build_job_definition`, `build_git_secret_arn`
- Build status polling via `batch:DescribeJobs` — both must succeed for overall build to complete
