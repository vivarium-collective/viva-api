import base64
import json
import os
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

from viva_api.common.storage.file_paths import HPCFilePath


def _parse_docker_config_json(path: str) -> tuple[str, str]:
    """Parse a Docker config.json file to extract GitHub credentials.

    Args:
        path: Path to the .dockerconfigjson file

    Returns:
        Tuple of (username, token) extracted from ghcr.io auth
    """
    if not path or not os.path.exists(path):
        return "", ""

    try:
        with open(path) as f:
            config = json.load(f)

        # Look for ghcr.io or github.com auth
        auths = config.get("auths", {})
        for registry in ["ghcr.io", "https://ghcr.io", "github.com", "https://github.com"]:
            if registry in auths:
                auth_b64 = auths[registry].get("auth", "")
                if auth_b64:
                    # auth is base64(username:token)
                    decoded = base64.b64decode(auth_b64).decode("utf-8")
                    if ":" in decoded:
                        username, token = decoded.split(":", 1)
                        return username, token
    except Exception:
        return "", ""

    return "", ""


KV_DRIVER = Literal["file", "s3", "gcs"]
TS_DRIVER = Literal["zarr", "n5", "zarr3"]
STORAGE_BACKEND = Literal["gcs", "s3", "qumulo"]

# -- load dev env -- #
REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
DEV_ENV_PATH = os.path.join(REPO_ROOT, "assets", "dev", "config", ".dev_env")
load_dotenv(DEV_ENV_PATH)  # NOTE: create an env config at this filepath if dev

ENV_CONFIG_ENV_FILE = "CONFIG_ENV_FILE"
ENV_SECRET_ENV_FILE = "SECRET_ENV_FILE"  # noqa: S105 Possible hardcoded password assigned to: "ENV_SECRET_ENV_FILE"

if os.getenv(ENV_CONFIG_ENV_FILE) is not None and os.path.exists(str(os.getenv(ENV_CONFIG_ENV_FILE))):
    load_dotenv(os.getenv(ENV_CONFIG_ENV_FILE))

if os.getenv(ENV_SECRET_ENV_FILE) is not None and os.path.exists(str(os.getenv(ENV_SECRET_ENV_FILE))):
    load_dotenv(os.getenv(ENV_SECRET_ENV_FILE))


class Namespace(StrEnum):
    DEVELOPMENT = "dev"
    PRODUCTION = "prod"
    TEST = "test"


class APIFilePath(Path):
    pass


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    storage_backend: STORAGE_BACKEND = "s3"

    # GCS configuration
    storage_gcs_bucket: str = "files.biosimulations.dev"
    storage_gcs_endpoint_url: str = "https://storage.googleapis.com"
    storage_gcs_region: str = "us-east4"
    storage_gcs_credentials_file: str = ""

    # Local storage configuration
    storage_local_cache_dir: str = "./local_cache"

    # AWS S3 configuration
    storage_s3_bucket: str = ""
    storage_s3_region: str = "us-east-1"
    storage_s3_access_key_id: str = ""
    storage_s3_secret_access_key: str = ""
    storage_s3_session_token: str = ""

    # Qumulo S3-compatible storage configuration
    storage_qumulo_endpoint_url: str = ""
    storage_qumulo_bucket: str = ""
    storage_qumulo_access_key_id: str = ""
    storage_qumulo_secret_access_key: str = ""
    storage_qumulo_verify_ssl: bool = True

    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_database: str = "biosimulations"
    mongodb_collection_omex: str = "BiosimOmex"
    mongodb_collection_sims: str = "BiosimSims"
    mongodb_collection_compare: str = "BiosimCompare"

    postgres_user: str = "<USER>"
    postgres_password: str = ""
    postgres_database: str = "sms"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_pool_size: int = 10  # number of connections in the pool
    postgres_max_overflow: int = 5  # maximum number of connections that can be created beyond the pool size
    postgres_pool_timeout: int = 30  # timeout for acquiring a connection from the pool in seconds
    postgres_pool_recycle: int = 1800  # recycle connections every seconds

    slurm_submit_host: str = ""
    slurm_submit_user: str = ""  # "svc_vivarium"
    slurm_submit_key_path: str = ""  # "/Users/jimschaff/.ssh/id_rsa"
    slurm_submit_known_hosts: str | None = None
    slurm_partition: str = ""
    slurm_node_list: str = ""  # comma-separated list of nodes, e.g., "node1,node2"
    slurm_qos: str = ""
    slurm_log_base_path: HPCFilePath = HPCFilePath(remote_path=Path(""))
    slurm_base_path: HPCFilePath = HPCFilePath(remote_path=Path(""))

    # Apptainer/Singularity temp directory for container builds
    # Use local SSD/NVMe (/tmp) for builds with many small files (faster metadata ops)
    # FSx Lustre has high latency for small file operations and can cause build timeouts
    apptainer_tmpdir: str = "/tmp/apptainer"  # noqa: S108 Intentional use of /tmp for fast metadata ops

    hpc_image_base_path: HPCFilePath = HPCFilePath(remote_path=Path(""))
    hpc_parca_base_path: HPCFilePath = HPCFilePath(remote_path=Path(""))
    hpc_repo_base_path: HPCFilePath = HPCFilePath(remote_path=Path(""))
    hpc_sim_base_path: HPCFilePath = HPCFilePath(remote_path=Path(""))
    hpc_sim_config_file: str = "default_with_publish.json"

    redis_internal_host: str = ""
    redis_internal_port: int = -1
    redis_external_host: str = ""
    redis_external_port: int = -1
    redis_channel: str = "worker.events"
    redis_emitter_magic_word: str = "emitter-magic-word"

    app_dir: str = f"{REPO_ROOT}/app"
    assets_dir: str = f"{REPO_ROOT}/assets"
    marimo_api_server: str = ""

    # data (outputs) retrieval
    hpc_user: str = ""
    hpc_group: str = ""
    deployment: str = "prod"
    namespace: Namespace = Namespace.TEST

    # external services
    biocyc_email: str = ""
    biocyc_password: str = ""

    # GitHub credentials for cloning private repos (PAT with repo scope)
    # Can be set directly or loaded from dockerconfigjson file (e.g., from K8s ghcr-secret)
    github_dockerconfig_path: str = ""  # Path to .dockerconfigjson file
    github_username: str = ""
    github_token: str = ""

    def model_post_init(self, __context: object) -> None:
        """Load GitHub credentials from dockerconfigjson if not set directly."""
        if (not self.github_username or not self.github_token) and self.github_dockerconfig_path:
            username, token = _parse_docker_config_json(self.github_dockerconfig_path)
            if username and token:
                object.__setattr__(self, "github_username", username)
                object.__setattr__(self, "github_token", token)

    simulation_outdir: HPCFilePath = HPCFilePath(remote_path=Path(""))
    analysis_outdir: HPCFilePath = HPCFilePath(remote_path=Path(""))
    vecoli_config_dir: HPCFilePath = HPCFilePath(remote_path=Path(""))
    cache_dir: str = f"{REPO_ROOT}/.results_cache"

    # Path prefix mapping for local vs remote (HPC) filesystem access
    # Example: path_local_prefix=/Volumes/SMS, path_remote_prefix=/projects/SMS
    path_local_prefix: str = ""
    path_remote_prefix: str = ""

    # valid namespaces correspond 1:1 with namespaces in kustomize/ config
    deployment_namespace: str = ""

    # Compute backend: "slurm" (SLURM via SSH) or "batch" (AWS Batch via Nextflow).
    # Must be set explicitly — no default.
    compute_backend: str = ""

    # Public mode exposes the CCAM fork repo and public simulation configs.
    # Private mode uses the Stanford private repo and private configs.
    # Must be set explicitly — no default.
    public_mode: str = ""

    # slurm constraint for arch mismatches
    slurm_constraint: str = ""

    # --- AWS Batch backend settings ---
    # Used when job_backend is "batch" (Stanford deployments)

    # K8s Job settings
    k8s_job_namespace: str = ""  # Namespace for Nextflow head Jobs (e.g. "sms-api-stanford")

    # AWS Batch settings (Nextflow submits tasks here)
    batch_task_arch: str = "amd64"  # Architecture for Batch task images: "amd64" or "arm64"
    batch_amd64_queue: str = ""  # AMD64 simulation task queue
    batch_arm64_queue: str = ""  # ARM64 simulation task queue (Graviton)
    batch_region: str = "us-gov-west-1"  # AWS region for Batch

    # S3 settings for workflow data
    s3_work_bucket: str = ""  # S3 bucket for Nextflow work dir and outputs
    s3_work_prefix: str = "nextflow/work"  # Prefix for Nextflow work directory
    s3_output_prefix: str = "vecoli-output"  # Prefix for workflow output data

    # ECR settings
    ecr_account_id: str = ""  # AWS account ID for ECR registry (e.g. "476270107793")
    ecr_repository: str = "vecoli"  # ECR repository name for vEcoli images

    # Docker image build settings (DooD via AWS Batch)
    build_arm64_queue: str = ""  # Batch queue for ARM64 builds (Graviton)
    build_amd64_queue: str = ""  # Batch queue for AMD64 builds
    build_job_definition: str = ""  # Batch job definition for DooD builds
    build_git_secret_arn: str = ""  # Secrets Manager ARN for GitHub PAT (private repo clone)

    # Optional: bake an ecoli-sources data repo into the simulator image at build time.
    # Empty URL = skip; workflow relies on runtime ECOLI_SOURCES env var (e.g. s3:// URI).
    ecoli_sources_repo_url: str = ""
    ecoli_sources_ref: str = "main"

    # --- Ray-on-Batch backend settings ---
    # Used when compute_backend is "ray": a single AWS Batch multi-node-parallel (MNP)
    # job forms a transient Ray cluster and runs the v2ecoli ensemble on the head.
    # Provisioned by sms-cdk lib/ray-batch-stack.ts (the <prefix>-ray-mnp queue/job-def).
    ray_mnp_queue: str = ""  # Batch MNP job queue (e.g. "smscdk-ray-mnp")
    ray_mnp_job_definition: str = ""  # Batch MNP job definition (e.g. "smscdk-ray-mnp")
    # Backlog item 65: a SEPARATE queue for genuinely standalone (numNodes=1) MNP
    # submissions -- chain-dispatch's per-seed-per-generation jobs and ParCa, which
    # have no inter-node traffic to protect and were paying the full concurrency
    # cost of ray_mnp_queue's cluster-placement-group compute environment for
    # nothing (confirmed live: stuck at 1 concurrent job with ~1000 more ready).
    # Provisioned by sms-cdk's RayStandaloneCE/ray-standalone queue, reusing the
    # SAME ray_mnp_job_definition (job definitions and queues are independently
    # addressable AWS Batch resources). Empty (default) = no behavior change,
    # _submit_mnp falls back to ray_mnp_queue for every submission, numNodes
    # included -- safe to deploy before the new queue exists; set once it's live.
    ray_mnp_standalone_queue: str = ""  # Batch MNP standalone queue (e.g. "smscdk-ray-standalone")
    # MNP node count for BOTH the vEcoli ensemble sim and generic compose runs (1 head
    # + N-1 workers; single node = head is also worker, per the ray-batch-entrypoint
    # --num-cpus conditional). One setting, not two: compose and the ensemble sim
    # submit through the same shared _submit_mnp() on the same MNP queue/job-def (see
    # ComposeSimulationServiceRay.__init__ / _submit_mnp's docstring), so a single
    # node-count knob is the only shape that can't silently drift between them again
    # (was ray_num_nodes + compose_ray_num_nodes, two independent settings feeding the
    # same call -- the CDK's 24-node capacity scale-up only ever updated the compose
    # one, leaving the actually-used ensemble sim path stuck at 4; see backlog item 26).
    # Must be <= CDK rayBatch.numNodes.
    ray_num_nodes: int = 3
    # -- env worker (vivarium-workbench#942 / REFACTOR-PLAN §2A.8) --
    # The workbench image an env-worker Job copies its worker module out of. The
    # module is DELIVERED, not installed: protocol §4 requires the workspace venv
    # to carry no vivarium-workbench dependency, and the simulator image is built
    # from the science repo with `--no-install-package vivarium-workbench`.
    # No default — pointing this at the wrong tag runs a worker whose protocol
    # version disagrees with the workbench's, so a deployment must say it.
    #
    # HARD PREREQUISITE: the image must contain the dial-back transport
    # (vivarium-workbench#945). Verified 2026-08-26 that 0.3.57 does NOT — its
    # `env_worker.py --help` offers only `--socket-fd`, so a worker built from it
    # exits 2 on `--connect-to`. That surfaces as a failed Job whose logs say
    # "unrecognized arguments", which `GET /env-worker/v1/workers/{name}?include_logs=true`
    # will show — but it is worth knowing before pointing this at a stale tag.
    env_worker_module_image: str = ""
    # Workspace root INSIDE the simulator image. This is the image's own checkout
    # — under §2A.8 that copy IS the execution environment, so the worker reads it
    # rather than mounting the PVC (which is ReadWriteOnce and single-node anyway).
    env_worker_workspace_path: str = "/app/v2ecoli"
    # --- caller identity (viva_api/api/auth.py) ---
    # The request header this deployment takes caller identity from, e.g.
    # X-Auth-Request-Email (oauth2-proxy), X-Amzn-Oidc-Identity (ALB OIDC), or
    # whatever an institutional SSO proxy sets. EMPTY BY DEFAULT and legitimately
    # so: most deployments have nothing in front of them that sets one, and
    # anonymous is the correct answer there.
    #
    # This is NOT authentication -- a header is as trustworthy as the proxy that
    # sets it, and where nothing sets one anybody may claim anything. It exists
    # for attribution and to stop one accident (cancelling a run you did not
    # start). See the module docstring in viva_api/api/auth.py.
    identity_header: str = ""

    ray_ecr_repository: str = "v2ecoli"  # ECR repo for the workload-owned Ray image (built by submit_build_image_job)
    ray_parca_mode: str = "full"  # v2ecoli-parca --mode (fast for debug, full for production)
    ray_parca_cpus: int = 8  # v2ecoli-parca --cpus
    ray_n_steps: int = 600  # default sim steps per seed (run_phase0_xarray_ensemble --n-steps)
    ray_chunk: int = 60  # default xarray emitter flush interval (--chunk)
    ray_log_s3_prefix: str = ""  # s3:// prefix for Ray session logs + report.json (RayLogS3Prefix stack output)

    # --- Ray-on-Batch ARRAY dispatch settings ---
    # Used for the canonical/batch_baseline multiseed x multigeneration sweep: one
    # AWS Batch ARRAY job, each child an independent single-seed task (no Ray
    # cluster) -- see sms-cdk lib/ray-batch-stack.ts's RayArrayJobDef. Kept as
    # dedicated settings (not reused from ray_mnp_*) even though ray_array_queue's
    # deployed value is BatchStack's vecoli-task-amd64 queue -- same convention as
    # every other explicit-not-implicit setting in this file.
    ray_array_queue: str = ""  # Batch Array job queue (BatchStack's vecoli-task-amd64, reused deliberately)
    ray_array_job_definition: str = ""  # Batch Array job definition (e.g. "smscdk-ray-array")

    # --- Ray-on-Batch plain CONTAINER dispatch settings (backlog item 71) ---
    # A third, non-MNP, non-array job shape: one Batch container-type job, no node
    # overrides, no array indexing. Used for ParCa, the analysis DAG node, and
    # (a later phase) chain-dispatch's per-seed-per-generation jobs -- none of
    # which have real inter-node traffic to protect, so they gain nothing from
    # ray_mnp_queue's cluster-placement-group compute environment (the same
    # reasoning that motivated ray_mnp_standalone_queue, item 65). Provisioned by
    # sms-cdk's RayContainerJobDef, reusing the existing RayStandaloneQueue (no
    # new queue). Dedicated settings, not reused from ray_mnp_*/ray_array_* --
    # same convention as every other explicit-not-implicit setting in this file.
    # Empty defaults: _ensure_container_job_def/_submit_container raise a clear
    # RuntimeError naming the unset setting rather than submit a doomed job with a
    # blank queue/job-def (matches compose_ray_image_tag's own precedent below).
    ray_container_queue: str = ""  # Batch container job queue (e.g. "smscdk-ray-standalone")
    ray_container_job_definition: str = ""  # Batch container job definition (e.g. "smscdk-ray-container")

    # EC2 build machine (legacy, replaced by Batch DooD builds)
    build_node_host: str = ""
    build_node_user: str = ""
    build_node_key_path: str = ""

    # --- Compose (process-bigraph) subsystem settings ---
    compose_image_base_path: str = ""  # HPC path for compose singularity images
    compose_sim_base_path: str = ""  # HPC path for compose simulation outputs
    compose_cache_base_path: str = ""  # HPC path for compose ParCa cache (bind-mounted into containers)
    compose_containers_output_dir: str = "/output"  # Container-internal output dir
    # Ray/Batch compose runner image (prebuilt, carries process-bigraph + pbg-emitters).
    # `<ray_ecr_repository>:<compose_ray_image_tag>` — the deploy points this at the served
    # workspace's image. Generic run_pbg.py runs inside it.
    #
    # Deliberately NOT defaulted to "latest": that repo is populated per-commit by
    # submit_build_image_job and has no "latest" tag, so the old default could only
    # ever resolve to an image that does not exist — a pull failure at job start
    # rather than a clear error at submit. Empty means "unset"; the compose Ray
    # service raises with the setting name instead of submitting a doomed job.
    # The tag is a workspace COMMIT, and it is also what keys the ParCa cache below.
    compose_ray_image_tag: str = ""
    # Container dir the commit-keyed ParCa cache is staged into before the run, e.g.
    # "/app/v2ecoli/out/cache" (v2ecoli's baseline resolves a relative "out/cache"
    # against the image's WORKDIR). Empty disables staging — correct for any
    # workspace whose composites don't need a prebuilt cache. Staging reuses the
    # ensemble path's existing RAY_STAGE_S3/RAY_STAGE_DIR entrypoint mechanism.
    compose_parca_cache_dir: str = ""
    # "module:callable" naming the WORKSPACE's own core builder, e.g.
    # "v2ecoli.core:build_core". run_pbg.py's generic core registers only
    # process-bigraph's base types plus the pbg-emitters links; a workspace that
    # registers its own types (v2ecoli's ECOLI_TYPES) needs its own builder or its
    # documents won't resolve. Empty = use the generic core.
    compose_pbg_core_builder: str = ""
    compose_nats_url: str = ""  # NATS server URL (optional)
    compose_nats_worker_event_subject: str = "compose.worker.events"
    compose_has_messaging: bool = False  # Enable NATS messaging


class ComputeBackend(StrEnum):
    """Compute backend for simulation workloads."""

    SLURM = "slurm"  # SLURM via SSH to a login node (UCONN CCAM)
    BATCH = "batch"  # AWS Batch via Nextflow (Stanford)
    RAY = "ray"  # AWS Batch multi-node-parallel transient Ray cluster (Stanford, v2ecoli)


def get_job_backend() -> ComputeBackend:
    """Return the DEFAULT compute backend for the current deployment.

    This is the fallback when a simulator's repo doesn't map to a specific backend
    (see ``compute_backend_for_repo``). Raises ValueError if COMPUTE_BACKEND is unset/invalid.
    """
    value = get_settings().compute_backend
    if not value:
        raise ValueError("COMPUTE_BACKEND must be set explicitly to 'slurm', 'batch', or 'ray'")
    return ComputeBackend(value)


def compute_backend_for_repo(repo_url: str) -> ComputeBackend | None:
    """Map a simulator repo to its compute backend, so one deployment can serve both.

    The KNOWN repos (the ``RepoUrl`` enum) are mapped explicitly — this is the
    authoritative dispatch, and it's the only way repos whose name matches neither
    substring (e.g. the production Ray repo ``CovertLabEcoli/sms-ecoli``) are routed
    correctly. A substring check is kept as a fallback for forks/variants at other
    URLs. Returns None for a true unknown, in which case callers fall back to
    ``get_job_backend()`` (the deployment default).

    Ray: v2ecoli + sms-ecoli. Batch/Nextflow: the vEcoli repos. Note ``v2ecoli`` /
    ``sms-ecoli`` do not contain the ``vecoli`` substring, so the checks are independent.
    """
    # Lazy import: simulator_defaults imports this module, so importing RepoUrl at
    # module load would be circular. RepoUrl is the single source of the repo URLs.
    from viva_api.common.simulator_defaults import RepoUrl

    known: dict[str, ComputeBackend] = {
        RepoUrl.V2ECOLI_REPO_URL: ComputeBackend.RAY,
        RepoUrl.SMS_ECOLI_REPO_URL: ComputeBackend.RAY,
        RepoUrl.VECOLI_FORK_REPO_URL: ComputeBackend.BATCH,
        RepoUrl.VECOLI_PUBLIC_REPO_URL: ComputeBackend.BATCH,
        RepoUrl.VECOLI_PRIVATE_REPO_URL: ComputeBackend.BATCH,
    }
    if repo_url in known:
        return known[repo_url]

    # Fallback for forks/variants at other URLs.
    url = repo_url.lower()
    if "v2ecoli" in url or "sms-ecoli" in url:
        return ComputeBackend.RAY
    if "vecoli" in url:
        return ComputeBackend.BATCH
    return None


def get_public_mode() -> bool:
    """Return whether the deployment runs in public mode.

    Defaults to ``False`` when PUBLIC_MODE is not set so that the CLI and
    other local tooling can import ``simulator_defaults`` without requiring
    every server-side env var to be present.
    """
    value = get_settings().public_mode
    if not value:
        return False
    return value.lower() == "true"


@lru_cache
def get_settings(env_file: Path | None = None) -> Settings:
    if env_file is not None:
        DEV_ENV_PATH = str(env_file)
        load_dotenv(DEV_ENV_PATH)
    return Settings()


def get_local_cache_dir() -> Path:
    settings = get_settings()
    local_cache_dir = Path(settings.storage_local_cache_dir)
    local_cache_dir.mkdir(parents=True, exist_ok=True)
    return local_cache_dir
