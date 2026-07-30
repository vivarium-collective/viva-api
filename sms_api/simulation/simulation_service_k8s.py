"""Kubernetes + AWS Batch implementation of SimulationService.

Two-phase execution model:
  Phase 1 (build): DooD Batch jobs build Docker images (ARM64 task + AMD64 submit)
  Phase 2 (workflow): K8s Job running Nextflow head, which submits tasks to AWS Batch
"""

import json
import logging
from typing import override

from kubernetes import client as k8s_client

from sms_api.common.hpc.job_service import JobStatusInfo
from sms_api.common.hpc.k8s_job_service import K8sJobService
from sms_api.common.hpc.local_task_service import LocalTaskService
from sms_api.common.models import JobBackend, JobId
from sms_api.common.simulator_defaults import DEFAULT_BRANCH, DEFAULT_REPO
from sms_api.config import get_settings
from sms_api.simulation import batch_build
from sms_api.simulation.database_service import DatabaseService
from sms_api.simulation.github_repo import (
    fetch_config_template,
    fetch_latest_commit_hash,
    fetch_repo_discovery,
)
from sms_api.simulation.models import ParcaDataset, RepoDiscovery, Simulation, Simulator, SimulatorVersion
from sms_api.simulation.simulation_service import SimulationService

logger = logging.getLogger(__name__)


class SimulationServiceK8s(SimulationService):
    """K8s Job + AWS Batch implementation of SimulationService.

    - Build phase: SSH to ARM64 EC2 submit node for Docker build + ECR push
    - Workflow phase: Creates K8s Job running Nextflow, which submits tasks to Batch
    - Config reads: GitHub API (no SSH needed)
    - Status/cancel: K8s Job API
    """

    def __init__(self, k8s_job_service: K8sJobService, local_task_service: LocalTaskService | None = None) -> None:
        self._k8s = k8s_job_service
        self._local = local_task_service or LocalTaskService()

    @override
    async def get_latest_commit_hash(
        self,
        git_repo_url: str = DEFAULT_REPO,
        git_branch: str = DEFAULT_BRANCH,
    ) -> str:
        """Get the latest commit hash from GitHub API (no SSH needed)."""
        return await fetch_latest_commit_hash(git_repo_url, git_branch, get_settings().github_token)

    @override
    async def submit_build_image_job(self, simulator_version: SimulatorVersion) -> JobId:
        """Build Docker images via DooD Batch jobs (ARM64 task + AMD64 submit).

        Submits two parallel Batch jobs:
        - ARM64 queue: builds vecoli:{commit} (task image for Batch Graviton)
        - AMD64 queue: builds vecoli:{commit}-submit (submit image for EKS K8s Job)

        Returns immediately with a LOCAL JobId. The _run_build method polls
        both Batch jobs for completion.
        """
        commit = simulator_version.git_commit_hash
        return self._local.submit(
            self._run_build(simulator_version),
            name=f"build-{commit}",
        )

    def _build_command(self, simulator_version: Simulator, image_tag: str, submit_image: bool = False) -> list[str]:
        """Generate the DooD build command for a Batch job.

        Args:
            simulator_version: Simulator with commit, branch, repo URL
            image_tag: ECR image tag to build and push
            submit_image: If True, also build the submit image (base + Java + Nextflow)
        """
        settings = get_settings()
        commit = simulator_version.git_commit_hash
        branch = simulator_version.git_branch

        repo_url = simulator_version.git_repo_url

        base_script = f"""\
set -ex
export USER=${{USER:-sms-api}}

# Install dependencies (docker:cli is Alpine-based, missing aws-cli and git)
apk add --no-cache aws-cli git bash

# Docker daemon runs on the host — verify socket is mounted
docker info >/dev/null 2>&1 || {{ echo "ERROR: Docker socket not available"; exit 1; }}

# Get GitHub PAT from Secrets Manager for private repo access.
# Disable xtrace around the secret so the PAT (and the clone URL embedding it) never lands
# in the build logs (CloudWatch). Re-enable tracing once the clone is done.
set +x
GH_PAT=$(aws secretsmanager get-secret-value \
    --secret-id {settings.build_git_secret_arn} \
    --query SecretString --output text)
# Inject PAT into clone URL for HTTPS auth (x-access-token is GitHub's convention)
CLONE_URL=$(echo "{repo_url}" | sed "s|https://github.com/|https://x-access-token:${{GH_PAT}}@github.com/|")
export GIT_TERMINAL_PROMPT=0
git clone --branch {branch} --single-branch "$CLONE_URL" /build/vEcoli
unset GH_PAT CLONE_URL
set -x
cd /build/vEcoli
git checkout {commit}

# ECR login
aws ecr get-login-password --region $AWS_DEFAULT_REGION | \
    docker login --username AWS --password-stdin $ECR_REGISTRY

# Build and push
bash runscripts/container/build-and-push-ecr.sh \
    -i {image_tag} -r {settings.ecr_repository} -R {settings.batch_region}
"""

        if submit_image:
            base_script += f"""
# Build submit image (base + Java + Nextflow) on top
BASE_URI=$ECR_REGISTRY/{settings.ecr_repository}:{image_tag}

cat > /tmp/Dockerfile-submit <<'DOCKERFILE'
ARG BASE_IMAGE
FROM ${{BASE_IMAGE}}
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \\
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ARG NEXTFLOW_VERSION=25.10.2
RUN curl -fsSL "https://github.com/nextflow-io/nextflow/releases/download/v${{NEXTFLOW_VERSION}}/nextflow" \\
    -o /usr/local/bin/nextflow && chmod +x /usr/local/bin/nextflow
WORKDIR /vEcoli
DOCKERFILE

docker build -t "$ECR_REGISTRY/{settings.ecr_repository}:{image_tag}-submit" \
    --build-arg BASE_IMAGE="$BASE_URI" \
    -f /tmp/Dockerfile-submit /tmp
docker push "$ECR_REGISTRY/{settings.ecr_repository}:{image_tag}-submit"
echo "Submit image pushed: $ECR_REGISTRY/{settings.ecr_repository}:{image_tag}-submit"
"""

        return ["sh", "-c", base_script]

    async def _submit_batch_build(self, job_name: str, queue: str, command: list[str], commit: str) -> str:
        """Submit a DooD build job to AWS Batch. Returns the Batch job ID."""
        settings = get_settings()
        env = [{"name": "IMAGE_TAG", "value": commit}]
        if settings.ecoli_sources_repo_url:
            env.append({"name": "ECOLI_SOURCES_REPO_URL", "value": settings.ecoli_sources_repo_url})
        if settings.ecoli_sources_ref:
            env.append({"name": "ECOLI_SOURCES_REF", "value": settings.ecoli_sources_ref})
        return await batch_build.submit_batch_build(job_name, queue, command, environment=env)

    async def _poll_batch_jobs(self, job_ids: list[str]) -> None:
        """Poll Batch jobs until all complete. Raises on failure."""
        await batch_build.poll_batch_jobs(job_ids)

    async def _run_build(self, simulator_version: SimulatorVersion) -> None:
        """Build Docker images via parallel DooD Batch jobs.

        Submits two Batch jobs in parallel:
        - ARM64: builds vecoli:{commit}-arm64 (task image for Graviton Batch)
        - AMD64: builds vecoli:{commit}-amd64 (task image for x86 Batch)
                 + vecoli:{commit}-amd64-submit (submit image for EKS K8s Job)

        The Batch task architecture is configurable via batch_task_arch setting.
        The K8s submit image is always AMD64 (EKS GovCloud constraint).
        """
        settings = get_settings()
        commit = simulator_version.git_commit_hash

        # ARM64: task image
        arm64_cmd = self._build_command(simulator_version, image_tag=f"{commit}-arm64")
        arm64_job_id = await self._submit_batch_build(
            job_name=f"build-arm64-{commit}",
            queue=settings.build_arm64_queue,
            command=arm64_cmd,
            commit=commit,
        )

        # AMD64: task image + submit image
        amd64_cmd = self._build_command(simulator_version, image_tag=f"{commit}-amd64", submit_image=True)
        amd64_job_id = await self._submit_batch_build(
            job_name=f"build-amd64-{commit}",
            queue=settings.build_amd64_queue,
            command=amd64_cmd,
            commit=commit,
        )

        # Poll both until done
        await self._poll_batch_jobs([arm64_job_id, amd64_job_id])
        logger.info(f"Multi-arch build complete for {settings.ecr_repository}:{commit}")

    @override
    async def submit_parca_job(self, parca_dataset: ParcaDataset) -> JobId:
        """Submit parca as a K8s Job.

        Parca runs inside the Nextflow workflow (not as a separate step) when
        using the K8s/Batch backend. This creates a placeholder entry.
        For the K8s path, parca execution is handled within the Nextflow workflow
        based on the sim_data_path config (null = run parca, path = use cached).
        """
        # Parca runs as part of the Nextflow workflow, not separately
        # Return a placeholder ID
        parca_id = parca_dataset.database_id
        return JobId.k8s(f"parca-placeholder-{parca_id}")

    @override
    async def submit_ecoli_simulation_job(
        self, ecoli_simulation: Simulation, database_service: DatabaseService, correlation_id: str
    ) -> JobId:
        """Create a K8s Job that runs Nextflow with the awsbatch executor."""
        settings = get_settings()

        experiment_id = ecoli_simulation.config.experiment_id
        # K8s names must be RFC 1123: lowercase alphanumeric, '-' or '.'
        safe_id = experiment_id.replace("_", "-").lower()
        job_name = f"nf-{safe_id}"[:63]

        # Build the workflow config from the simulation record
        # (AWS overrides already applied by handler before DB insert)
        config_data = ecoli_simulation.config.model_dump()
        config_json = json.dumps(config_data)

        # Build full ECR URI for the submit image (vecoli + Java + Nextflow)
        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {ecoli_simulation.simulator_id} not found")
        submit_image = (
            f"{settings.ecr_account_id}.dkr.ecr.{settings.batch_region}.amazonaws.com"
            f"/{settings.ecr_repository}:{simulator.git_commit_hash}-amd64-submit"
        )

        # Create ConfigMap with workflow config
        configmap_name = f"{job_name}-config"
        configmap = k8s_client.V1ConfigMap(
            metadata=k8s_client.V1ObjectMeta(
                name=configmap_name,
                labels={"app": "sms-api", "experiment-id": experiment_id},
            ),
            data={"workflow.json": config_json},
        )
        self._k8s.create_configmap(configmap)

        # Build the container command:
        # 1. Inject GovCloud S3 endpoint into config.template (before workflow.py reads it)
        # 2. Run workflow.py in full mode (generates Nextflow files + runs Nextflow)
        # 3. Upload .nextflow.log to S3 on completion (success or failure)
        s3_endpoint = f"https://s3.{settings.batch_region}.amazonaws.com"
        s3_log_dir = f"s3://{settings.s3_work_bucket}/{settings.s3_work_prefix}/{experiment_id}/logs"
        # If sim_data_path is /tmp/simData.cPickle, download cached simData from S3 first
        # (vEcoli only accepts local paths for sim_data_path)
        download_step = ""
        sim_data_path = config_data.get("sim_data_path")
        _cached_simdata = "/tmp/simData.cPickle"  # noqa: S108
        if sim_data_path == _cached_simdata:
            s3_sim_data = f"s3://{settings.s3_work_bucket}/sim_data/default/kb/simData.cPickle"
            download_step = f"aws s3 cp {s3_sim_data} {sim_data_path} && "
        command = (
            f"{download_step}"
            f'sed -i "/region = params.aws_region/a\\            client {{ endpoint = \\"{s3_endpoint}\\" }}"'
            " runscripts/nextflow/config.template"
            " && python runscripts/workflow.py --config /config/workflow.json"
            f" ; NF_EXIT=$? ; aws s3 cp .nextflow.log {s3_log_dir}/.nextflow.log 2>/dev/null || true"
            " ; exit $NF_EXIT"
        )

        # Build the container env. ECOLI_SOURCES / ECOLI_SOURCES_OVERLAYS are
        # surfaced when the user passed --sources via the CLI, which syncs local
        # dirs to S3 and forwards the resulting URIs through the API.
        container_env = [
            k8s_client.V1EnvVar(name="AWS_DEFAULT_REGION", value=settings.batch_region),
            k8s_client.V1EnvVar(name="AWS_REGION", value=settings.batch_region),
            k8s_client.V1EnvVar(name="AWS_STS_REGIONAL_ENDPOINTS", value="regional"),
            k8s_client.V1EnvVar(name="USER", value="sms-api"),
        ]
        ecoli_sources_uri = config_data.get("ecoli_sources_uri")
        ecoli_sources_overlays = config_data.get("ecoli_sources_overlays")
        if ecoli_sources_uri:
            container_env.append(k8s_client.V1EnvVar(name="ECOLI_SOURCES", value=ecoli_sources_uri))
        if ecoli_sources_overlays:
            container_env.append(k8s_client.V1EnvVar(name="ECOLI_SOURCES_OVERLAYS", value=ecoli_sources_overlays))

        # Single-container K8s Job using vecoli-submit image (vEcoli + Java + Nextflow)
        job = k8s_client.V1Job(
            metadata=k8s_client.V1ObjectMeta(
                name=job_name,
                labels={
                    "app": "sms-api",
                    "job-type": "simulation",
                    "experiment-id": experiment_id,
                },
            ),
            spec=k8s_client.V1JobSpec(
                backoff_limit=0,
                ttl_seconds_after_finished=86400,  # 24h for log access
                template=k8s_client.V1PodTemplateSpec(
                    spec=k8s_client.V1PodSpec(
                        service_account_name="batch-submit",
                        restart_policy="Never",
                        containers=[
                            k8s_client.V1Container(
                                name="workflow",
                                image=submit_image,
                                command=["/bin/bash", "-c", command],
                                env=container_env,
                                volume_mounts=[
                                    k8s_client.V1VolumeMount(name="config", mount_path="/config"),
                                ],
                                resources=k8s_client.V1ResourceRequirements(
                                    requests={"cpu": "500m", "memory": "1Gi"},
                                    limits={"cpu": "1", "memory": "2Gi"},
                                ),
                            ),
                        ],
                        volumes=[
                            k8s_client.V1Volume(
                                name="config",
                                config_map=k8s_client.V1ConfigMapVolumeSource(name=configmap_name),
                            ),
                        ],
                    ),
                ),
            ),
        )
        self._k8s.create_job(job)
        logger.info(f"Created K8s Job {job_name} for experiment {experiment_id}")
        return JobId.k8s(job_name)

    async def submit_standalone_analysis(
        self,
        experiment_id: str,
        analysis_config: dict,  # type: ignore[type-arg]
        database_service: "DatabaseService",
        simulator_id: int,
    ) -> JobId:
        """Create a K8s Job that runs vEcoli standalone analysis on existing simulation output."""
        settings = get_settings()
        safe_id = experiment_id.replace("_", "-").lower()
        job_name = f"ana-{safe_id}"[:63]
        config_json = json.dumps(analysis_config)

        simulator = await database_service.get_simulator(simulator_id=simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {simulator_id} not found")
        submit_image = (
            f"{settings.ecr_account_id}.dkr.ecr.{settings.batch_region}.amazonaws.com"
            f"/{settings.ecr_repository}:{simulator.git_commit_hash}-amd64-submit"
        )

        configmap_name = f"{job_name}-config"
        configmap = k8s_client.V1ConfigMap(
            metadata=k8s_client.V1ObjectMeta(
                name=configmap_name,
                labels={"app": "sms-api", "job-type": "analysis", "experiment-id": experiment_id},
            ),
            data={"analysis.json": config_json},
        )
        self._k8s.create_configmap(configmap)

        s3_endpoint = f"https://s3.{settings.batch_region}.amazonaws.com"
        command = (
            f'sed -i "/region = params.aws_region/a\\            client {{ endpoint = \\"{s3_endpoint}\\" }}"'
            " runscripts/nextflow/config.template"
            " && python runscripts/analysis.py --config /config/analysis.json"
        )

        job = k8s_client.V1Job(
            metadata=k8s_client.V1ObjectMeta(
                name=job_name,
                labels={"app": "sms-api", "job-type": "analysis", "experiment-id": experiment_id},
            ),
            spec=k8s_client.V1JobSpec(
                backoff_limit=0,
                ttl_seconds_after_finished=86400,
                template=k8s_client.V1PodTemplateSpec(
                    spec=k8s_client.V1PodSpec(
                        service_account_name="batch-submit",
                        restart_policy="Never",
                        containers=[
                            k8s_client.V1Container(
                                name="analysis",
                                image=submit_image,
                                command=["/bin/bash", "-c", command],
                                env=[
                                    k8s_client.V1EnvVar(name="AWS_DEFAULT_REGION", value=settings.batch_region),
                                    k8s_client.V1EnvVar(name="AWS_REGION", value=settings.batch_region),
                                    k8s_client.V1EnvVar(name="AWS_STS_REGIONAL_ENDPOINTS", value="regional"),
                                    k8s_client.V1EnvVar(name="USER", value="sms-api"),
                                ],
                                volume_mounts=[
                                    k8s_client.V1VolumeMount(name="config", mount_path="/config"),
                                ],
                                resources=k8s_client.V1ResourceRequirements(
                                    requests={"cpu": "500m", "memory": "1Gi"},
                                    limits={"cpu": "1", "memory": "2Gi"},
                                ),
                            ),
                        ],
                        volumes=[
                            k8s_client.V1Volume(
                                name="config",
                                config_map=k8s_client.V1ConfigMapVolumeSource(name=configmap_name),
                            ),
                        ],
                    ),
                ),
            ),
        )
        self._k8s.create_job(job)
        logger.info(f"Created K8s analysis Job {job_name} for experiment {experiment_id}")
        return JobId.k8s(job_name)

    async def submit_ray_native_analysis(
        self,
        experiment_id: str,
        params: dict,  # type: ignore[type-arg]
        commit: str,
    ) -> JobId:
        """Create a K8s Job running v2ecoli's standalone analysis entrypoint.

        For simulations dispatched via the Ray/xarray pipeline (v2ecoli/sms-ecoli):
        submit_standalone_analysis above targets the legacy vEcoli-private/Nextflow
        image (vecoli:<commit>-amd64-submit), which is never produced for this
        pipeline -- confirmed live via ImagePullBackOff. This targets the
        already-built v2ecoli:<commit> Ray image and its own
        scripts/run_standalone_analysis.py instead.

        job_name is derived from ``params["analysis_name"]`` (unique per trigger --
        see _run_standalone_analysis_ray_native), not bare experiment_id: the legacy
        submit_standalone_analysis's ``ana-{safe_id}`` naming collides across repeat
        triggers for the same simulation (a real, separate gap in that path), and
        this path must not repeat it.
        """
        settings = get_settings()
        analysis_name = params.get("analysis_name") or experiment_id
        safe_id = analysis_name.replace("_", "-").lower()
        job_name = f"ana-{safe_id}"[:63]
        registry = f"{settings.ecr_account_id}.dkr.ecr.{settings.batch_region}.amazonaws.com"
        submit_image = f"{registry}/{settings.ray_ecr_repository}:{commit}"

        configmap_name = f"{job_name}-config"
        configmap = k8s_client.V1ConfigMap(
            metadata=k8s_client.V1ObjectMeta(
                name=configmap_name,
                labels={"app": "sms-api", "job-type": "analysis", "experiment-id": experiment_id},
            ),
            data={"params.json": json.dumps(params)},
        )
        self._k8s.create_configmap(configmap)

        command = "cd /app/v2ecoli && python scripts/run_standalone_analysis.py --config-file /config/params.json"

        job = k8s_client.V1Job(
            metadata=k8s_client.V1ObjectMeta(
                name=job_name,
                labels={"app": "sms-api", "job-type": "analysis", "experiment-id": experiment_id},
            ),
            spec=k8s_client.V1JobSpec(
                backoff_limit=0,
                ttl_seconds_after_finished=86400,
                template=k8s_client.V1PodTemplateSpec(
                    spec=k8s_client.V1PodSpec(
                        service_account_name="batch-submit",
                        restart_policy="Never",
                        containers=[
                            k8s_client.V1Container(
                                name="analysis",
                                image=submit_image,
                                command=["/bin/bash", "-c", command],
                                env=[
                                    k8s_client.V1EnvVar(name="AWS_DEFAULT_REGION", value=settings.batch_region),
                                    k8s_client.V1EnvVar(name="AWS_REGION", value=settings.batch_region),
                                    k8s_client.V1EnvVar(name="AWS_STS_REGIONAL_ENDPOINTS", value="regional"),
                                    k8s_client.V1EnvVar(name="USER", value="sms-api"),
                                ],
                                volume_mounts=[
                                    k8s_client.V1VolumeMount(name="config", mount_path="/config"),
                                ],
                                resources=k8s_client.V1ResourceRequirements(
                                    # v2ecoli's full import surface (pandas/scipy/cvxpy/numba)
                                    # is heavier than the legacy analysis script's.
                                    requests={"cpu": "500m", "memory": "2Gi"},
                                    limits={"cpu": "1", "memory": "4Gi"},
                                ),
                            ),
                        ],
                        volumes=[
                            k8s_client.V1Volume(
                                name="config",
                                config_map=k8s_client.V1ConfigMapVolumeSource(name=configmap_name),
                            ),
                        ],
                    ),
                ),
            ),
        )
        self._k8s.create_job(job)
        logger.info(f"Created K8s Ray-native analysis Job {job_name} for experiment {experiment_id}")
        return JobId.k8s(job_name)

    @override
    async def read_config_template(
        self,
        simulator_version: SimulatorVersion,
        config_filename: str,
        allow_default_fallback: bool = False,
    ) -> str:
        """Read a vEcoli config template from the GitHub repo via the Contents API.

        Raises HTTPException(404) if the config file does not exist in the
        repo at the requested commit. Set ``allow_default_fallback=True`` to
        silently substitute the embedded ``_DEFAULT_CONFIG_TEMPLATE`` instead.
        """
        return await fetch_config_template(
            simulator_version, config_filename, get_settings().github_token, allow_default_fallback
        )

    @override
    async def discover_repo_contents(self, simulator_version: SimulatorVersion) -> RepoDiscovery:
        """Discover available configs and analysis modules via GitHub Contents API."""
        return await fetch_repo_discovery(simulator_version, get_settings().github_token)

    @override
    async def get_job_status(self, job_id: JobId) -> JobStatusInfo | None:
        """Get job status — dispatches to K8s or local task tracker."""
        if job_id.backend == JobBackend.LOCAL:
            return self._local.get_status(job_id.value)
        return self._k8s.get_job_status(job_id.value)

    @override
    async def cancel_job(self, job_id: JobId) -> None:
        """Cancel a job — dispatches to K8s or local task tracker."""
        if job_id.backend == JobBackend.LOCAL:
            self._local.cancel(job_id.value)
            logger.info(f"Cancelled local task {job_id.value}")
            return
        # K8s Job: delete with foreground propagation, sends SIGTERM to Nextflow head
        self._k8s.delete_job(job_id.value)
        configmap_name = f"{job_id.value}-config"
        self._k8s.delete_configmap(configmap_name)
        logger.info(f"Cancelled K8s Job {job_id.value}")

    @override
    async def close(self) -> None:
        pass
