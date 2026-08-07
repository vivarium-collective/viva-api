import json
import logging
import random
import re
import string
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from textwrap import dedent
from typing import override

from fastapi import HTTPException

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.hpc.models import SlurmJob
from viva_api.common.hpc.nextflow_weblog import WEBLOG_RECEIVER_SCRIPT
from viva_api.common.hpc.slurm_service import SlurmService
from viva_api.common.models import JobId, JobStatus, SSHTarget
from viva_api.common.simulator_defaults import DEFAULT_BRANCH, DEFAULT_REPO
from viva_api.common.storage.file_paths import HPCFilePath
from viva_api.common.utils import capture_slurm_script
from viva_api.config import get_settings
from viva_api.dependencies import get_ssh_session_service
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.hpc_utils import (
    get_apptainer_image_file,
    get_parca_dataset_dir,
    get_slurm_log_file,
    get_slurm_submit_file,
    get_vEcoli_repo_dir,
)
from viva_api.simulation.models import (
    ParcaDataset,
    RepoDiscovery,
    Simulation,
    SimulationConfig,
    SimulatorVersion,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MAX_SIMULATION_CPUS = 5
# N_NODES_UPSCALED = 15  # 20
# N_TASKS_PER_NODE_UPSCALED = 32


def _get_authenticated_git_url(repo_url: str, username: str | None, token: str | None) -> str:
    """Convert a GitHub HTTPS URL to include authentication credentials.

    Args:
        repo_url: GitHub repository URL (https://github.com/org/repo or https://github.com/org/repo.git)
        username: GitHub username
        token: GitHub personal access token

    Returns:
        URL with credentials embedded: https://{username}:{token}@github.com/org/repo.git
        Returns original URL if username or token is missing.
    """
    if not username or not token:
        return repo_url
    # Match https://github.com/... URLs
    match = re.match(r"https://github\.com/(.+)", repo_url)
    if match:
        return f"https://{username}:{token}@github.com/{match.group(1)}"
    return repo_url


class SimulationService(ABC):
    @abstractmethod
    async def get_latest_commit_hash(
        self,
        git_repo_url: str = DEFAULT_REPO,
        git_branch: str = DEFAULT_BRANCH,
    ) -> str:
        pass

    @abstractmethod
    async def submit_build_image_job(self, simulator_version: SimulatorVersion) -> JobId:
        """Submit a container image build job. Returns backend-tagged job ID."""
        pass

    @abstractmethod
    async def submit_parca_job(self, parca_dataset: ParcaDataset) -> JobId:
        """Submit a parca parameter calculator job. Returns backend-tagged job ID."""
        pass

    @abstractmethod
    async def submit_ecoli_simulation_job(
        self, ecoli_simulation: Simulation, database_service: DatabaseService, correlation_id: str
    ) -> JobId:
        """Submit a simulation workflow job. Returns backend-tagged job ID."""
        pass

    @abstractmethod
    async def read_config_template(
        self,
        simulator_version: SimulatorVersion,
        config_filename: str,
        allow_default_fallback: bool = False,
    ) -> str:
        """Read a vEcoli config template file for the given simulator version.

        Args:
            simulator_version: The simulator whose repo contains the config.
            config_filename: Filename under vEcoli/configs/ (e.g. "api_simulation_default.json").
            allow_default_fallback: If True, silently return the embedded default
                template when the requested file is missing or lacks API placeholders.
                When False (default), raise an HTTPException instead.

        Returns:
            The raw JSON string contents of the config template.
        """
        pass

    async def discover_repo_contents(self, simulator_version: SimulatorVersion) -> RepoDiscovery:
        """Discover available config files and analysis modules in the simulator's repo.

        Default implementation returns empty discovery. Backends override with
        actual introspection (GitHub API for K8s, SSH for SLURM).
        """
        return RepoDiscovery(
            simulator_id=simulator_version.database_id,
            git_repo_url=simulator_version.git_repo_url,
            git_commit_hash=simulator_version.git_commit_hash,
        )

    @abstractmethod
    async def get_job_status(self, job_id: JobId) -> JobStatusInfo | None:
        """Get the current status of a job by its backend-tagged ID."""
        pass

    @abstractmethod
    async def cancel_job(self, job_id: JobId) -> None:
        """Cancel a running job."""
        pass

    @abstractmethod
    async def close(self) -> None:
        pass


class SimulationServiceHpc(SimulationService):
    _latest_commit_hash: str | None = None

    @override
    async def get_latest_commit_hash(
        self,
        git_repo_url: str = DEFAULT_REPO,
        git_branch: str = DEFAULT_BRANCH,
    ) -> str:
        """
        :rtype: `str`
        :return: The last 7 characters of the latest commit hash.
        """
        settings = get_settings()
        auth_url = _get_authenticated_git_url(git_repo_url, settings.github_username, settings.github_token)
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            return_code, stdout, stderr = await ssh.run_command(f"git ls-remote -h {auth_url} {git_branch}")
        if return_code != 0:
            raise RuntimeError(f"Failed to list git commits for repository: {stderr.strip()}")
        latest_commit_hash = stdout.strip("\n")[:7]
        self._latest_commit_hash = latest_commit_hash
        return latest_commit_hash

    @override
    async def submit_build_image_job(self, simulator_version: SimulatorVersion) -> JobId:
        settings = get_settings()
        slurm_service = SlurmService()

        random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
        slurm_job_name = f"build-image-{simulator_version.git_commit_hash}-{random_suffix}"

        slurm_log_file = get_slurm_log_file(slurm_job_name=slurm_job_name)
        slurm_submit_file = get_slurm_submit_file(slurm_job_name=slurm_job_name)
        remote_vEcoli_path = get_vEcoli_repo_dir(simulator_version=simulator_version)
        apptainer_image_path = get_apptainer_image_file(simulator_version=simulator_version)
        auth_repo_url = _get_authenticated_git_url(
            simulator_version.git_repo_url, settings.github_username, settings.github_token
        )

        # build the submit script
        with tempfile.TemporaryDirectory() as tmpdir:
            local_submit_file = Path(tmpdir) / f"{slurm_job_name}.sbatch"
            with open(local_submit_file, "w") as f:
                qos_clause = f"#SBATCH --qos={get_settings().slurm_qos}" if get_settings().slurm_qos else ""
                nodelist_clause = (
                    f"#SBATCH --nodelist={get_settings().slurm_node_list}" if get_settings().slurm_node_list else ""
                )
                script_content = dedent(f"""\
                    #!/bin/bash
                    #SBATCH --job-name={slurm_job_name}
                    #SBATCH --time=1:00:00
                    #SBATCH --cpus-per-task 3
                    #SBATCH --mem=8GB
                    #SBATCH --partition={settings.slurm_partition}
                    {qos_clause}
                    #SBATCH --mail-type=ALL
                    {nodelist_clause}
                    #SBATCH -o {slurm_log_file!s}
                    #SBATCH -e {slurm_log_file!s}

                    set -eu
                    env

                    # Determine which container runtime to use (prefer apptainer over singularity)
                    if command -v apptainer &> /dev/null; then
                        CONTAINER_CMD="apptainer"
                        echo "Using apptainer for container build"
                    elif command -v singularity &> /dev/null; then
                        CONTAINER_CMD="singularity"
                        echo "Using singularity for container build"
                    else
                        echo "ERROR: Neither apptainer nor singularity found in PATH"
                        exit 1
                    fi

                    # Set Apptainer/Singularity directories
                    # TMPDIR uses local disk (not network FS) for fast metadata operations during builds
                    # CACHEDIR can use shared storage for layer caching across nodes
                    export APPTAINER_CACHEDIR=${{APPTAINER_CACHEDIR:-$HOME/.apptainer/cache}}
                    export APPTAINER_TMPDIR="{settings.apptainer_tmpdir}"
                    mkdir -p $APPTAINER_CACHEDIR $APPTAINER_TMPDIR

                    # Step 1: Clone repository if needed
                    echo "=== Step 1: Cloning repository ==="
                    FINAL_REPO_PATH="{remote_vEcoli_path!s}"

                    if [ -d "$FINAL_REPO_PATH" ]; then
                        echo "Repository already exists at $FINAL_REPO_PATH"
                        # If repo already exists and image exists, skip everything
                        if [ -f {apptainer_image_path!s} ]; then
                            echo "Image {apptainer_image_path!s} already exists. Skipping build."
                            exit 0
                        fi
                        # Use existing repo for build
                        TMP_REPO_PATH="$FINAL_REPO_PATH"
                        MOVE_REPO=false
                    else
                        echo "Repository not found. Cloning to /tmp..."
                        # Clone to /tmp to avoid NFS issues during build
                        TMP_REPO_PATH="/tmp/slurm_job_${{SLURM_JOB_ID}}_vEcoli"
                        cd /tmp
                        git clone --branch {simulator_version.git_branch} \\
                                  --single-branch {auth_repo_url} "$TMP_REPO_PATH"
                        cd "$TMP_REPO_PATH"
                        git checkout {simulator_version.git_commit_hash}
                        echo "Repository cloned successfully to $TMP_REPO_PATH"
                        MOVE_REPO=true
                        # Cleanup temp repo on exit (if we fail before moving)
                        trap "rm -rf '$TMP_REPO_PATH'" EXIT
                    fi

                    # Step 2: Build Apptainer image
                    echo "=== Step 2: Building Apptainer image ==="
                    mkdir -p {apptainer_image_path.parent!s}

                    echo "Building vEcoli image for commit {simulator_version.git_commit_hash} on $(hostname)..."
                    echo "Building from $TMP_REPO_PATH (local filesystem)"

                    cd "$TMP_REPO_PATH"

                    # Get git info
                    GIT_HASH=$(git rev-parse HEAD)
                    GIT_BRANCH=$(git symbolic-ref --short HEAD 2>/dev/null || echo "detached")
                    TIMESTAMP=$(date '+%Y%m%d.%H%M%S')

                    # Create git diff
                    mkdir -p source-info
                    git diff HEAD > source-info/git_diff.txt

                    # Create repo.tar (respecting .dockerignore)
                    echo "Creating repo tarball..."
                    EXCLUDE_PATTERNS=$(mktemp)
                    if [ -f .dockerignore ]; then
                        grep -v "^#" .dockerignore | grep -v "^$" | grep -v "^!" | while read -r pattern; do
                            if [[ "$pattern" == /* ]]; then
                                echo ".${{pattern}}/*" >> "$EXCLUDE_PATTERNS"
                            elif [[ "$pattern" == */ ]]; then
                                echo "./${{pattern}}*" >> "$EXCLUDE_PATTERNS"
                            else
                                echo "./${{pattern}}" >> "$EXCLUDE_PATTERNS"
                                echo "./${{pattern}}/*" >> "$EXCLUDE_PATTERNS"
                            fi
                        done
                    fi

                    FIND_CMD="find . -type f"
                    while read -r pattern; do
                        FIND_CMD="$FIND_CMD ! -path \\"$pattern\\""
                    done < "$EXCLUDE_PATTERNS"

                    TEMP_FILE_LIST=$(mktemp)
                    eval "$FIND_CMD -print0" > "$TEMP_FILE_LIST"
                    tar -cf repo.tar --null -T "$TEMP_FILE_LIST"
                    rm -f "$EXCLUDE_PATTERNS" "$TEMP_FILE_LIST"
                    echo "Created repo.tar ($(du -sh repo.tar | awk '{{print $1}}'))"

                    # Process .env file for environment variables
                    DOT_ENV_VARS="    "
                    if [ -f .env ]; then
                        echo "Processing .env for Singularity environment..."
                        while IFS= read -r line || [ -n "$line" ]; do
                            if [[ -n "$line" && ! "$line" =~ ^[[:space:]]*# ]]; then
                                line=${{line#export }}
                                DOT_ENV_VARS+="export $line; "
                            fi
                        done < .env
                        echo "Found $(echo "$DOT_ENV_VARS" | grep -c 'export ') environment variables"
                    fi

                    # Build container image
                    echo "=== Building Container Image: {apptainer_image_path!s} ==="
                    echo "=== git hash $GIT_HASH, git branch $GIT_BRANCH ==="

                    # Use --disable-cache to ensure correct architecture is pulled for multi-arch images
                    # (avoids using potentially wrong-architecture cached base image layers)
                    if ! $CONTAINER_CMD build --fakeroot --force --disable-cache \\
                        --build-arg git_hash="$GIT_HASH" \\
                        --build-arg git_branch="$GIT_BRANCH" \\
                        --build-arg timestamp="$TIMESTAMP" \\
                        --build-arg dot_env_vars="$DOT_ENV_VARS" \\
                        {apptainer_image_path!s} runscripts/container/Singularity; then
                        echo "ERROR: Container build failed."
                        exit 1
                    fi
                    echo "Container build successful!"

                    # Cleanup temp files (keep git_diff.txt as it's needed for simulation runs)
                    rm -f repo.tar

                    echo "Build completed. Image saved to {apptainer_image_path!s}."

                    # Step 3: Move repository to final location if needed
                    if [ "$MOVE_REPO" = true ]; then
                        echo "=== Step 3: Moving repository to final location ==="
                        mkdir -p "{remote_vEcoli_path.parent!s}"
                        mv "$TMP_REPO_PATH" "$FINAL_REPO_PATH"
                        echo "Repository moved to $FINAL_REPO_PATH"
                    fi
                    """)
                capture_slurm_script(script_content, "build_image.sbatch")
                f.write(script_content)

            # submit the build script to slurm
            async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
                slurm_jobid = await slurm_service.submit_job(
                    ssh, local_sbatch_file=local_submit_file, remote_sbatch_file=slurm_submit_file
                )
            return JobId.slurm(slurm_jobid)

    @override
    async def submit_parca_job(self, parca_dataset: ParcaDataset) -> JobId:
        settings = get_settings()
        slurm_service = SlurmService()
        simulator_version = parca_dataset.parca_dataset_request.simulator_version

        random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
        slurm_job_name = f"parca-{simulator_version.git_commit_hash}-{parca_dataset.database_id}-{random_suffix}"

        slurm_log_file = get_slurm_log_file(slurm_job_name=slurm_job_name)
        slurm_submit_file = get_slurm_submit_file(slurm_job_name=slurm_job_name)
        parca_remote_path = get_parca_dataset_dir(parca_dataset=parca_dataset)
        remote_vEcoli_repo_path = get_vEcoli_repo_dir(simulator_version=simulator_version)
        apptainer_image_path = get_apptainer_image_file(simulator_version=simulator_version)

        # build the submit script
        with tempfile.TemporaryDirectory() as tmpdir:
            local_submit_file = Path(tmpdir) / f"{slurm_job_name}.sbatch"
            with open(local_submit_file, "w") as f:
                qos_clause = f"#SBATCH --qos={get_settings().slurm_qos}" if get_settings().slurm_qos else ""
                nodelist_clause = (
                    f"#SBATCH --nodelist={get_settings().slurm_node_list}" if get_settings().slurm_node_list else ""
                )
                script_content = dedent(f"""\
                    #!/bin/bash
                    #SBATCH --job-name={slurm_job_name}
                    #SBATCH --time=30:00
                    #SBATCH --cpus-per-task 3
                    #SBATCH --mem=8GB
                    #SBATCH --partition={settings.slurm_partition}
                    {qos_clause}
                    #SBATCH --mail-type=ALL
                    {nodelist_clause}
                    #SBATCH -o {slurm_log_file!s}
                    #SBATCH -e {slurm_log_file!s}

                    set -e
                    # env
                    mkdir -p {parca_remote_path!s}

                    # check to see if the parca output directory is empty, if not, exit
                    if [ "$(ls -A {parca_remote_path!s})" ]; then
                        echo "Parca output directory {parca_remote_path!s} is not empty. Skipping job."
                        exit 0
                    fi

                    commit_hash="{simulator_version.git_commit_hash}"
                    parca_id="{parca_dataset.database_id}"
                    echo "running parca: commit=$commit_hash, parca id=$parca_id on $(hostname) ..."

                    binds="-B {remote_vEcoli_repo_path!s}:/vEcoli -B {parca_remote_path!s}:/parca_out"
                    image="{apptainer_image_path!s}"
                    cd {remote_vEcoli_repo_path!s}
                    singularity run $binds $image uv run \\
                         --env-file /vEcoli/.env /vEcoli/runscripts/parca.py \\
                         --config /vEcoli/configs/run_parca.json -c 3 -o /parca_out

                    # if the parca directory is empty after the run, fail the job
                    if [ ! "$(ls -A {parca_remote_path!s})" ]; then
                        echo "Parca output directory {parca_remote_path!s} is empty. Job must have failed."
                        exit 1
                    fi

                    echo "Parca run completed. data saved to {parca_remote_path!s}."
                    """)
                capture_slurm_script(script_content, "parca.sbatch")
                f.write(script_content)

            # submit the parca script to slurm
            async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
                slurm_jobid = await slurm_service.submit_job(
                    ssh, local_sbatch_file=local_submit_file, remote_sbatch_file=slurm_submit_file
                )
            return JobId.slurm(slurm_jobid)

    @override
    async def submit_ecoli_simulation_job(
        self, ecoli_simulation: Simulation, database_service: DatabaseService, correlation_id: str
    ) -> JobId:
        # settings = get_settings()
        if database_service is None:
            raise RuntimeError("DatabaseService is not available. Cannot submit Simulation job.")

        parca_dataset = await database_service.get_parca_dataset(parca_dataset_id=ecoli_simulation.parca_dataset_id)
        if parca_dataset is None:
            raise ValueError(f"ParcaDataset with ID {ecoli_simulation.parca_dataset_id} not found.")

        slurm_service = SlurmService()
        simulator_version = parca_dataset.parca_dataset_request.simulator_version

        random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
        slurm_job_name = f"sim-{simulator_version.git_commit_hash}-{ecoli_simulation.database_id}-{random_suffix}"
        slurm_log_file = get_slurm_log_file(slurm_job_name=slurm_job_name)
        slurm_submit_file = get_slurm_submit_file(slurm_job_name=slurm_job_name)

        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)

        # build the submit script
        with tempfile.TemporaryDirectory() as tmpdir:
            local_submit_file = Path(tmpdir) / f"{slurm_job_name}.sbatch"
            with open(local_submit_file, "w") as f:
                script_content = workflow_slurm_script(
                    slurm_log_file=slurm_log_file,
                    slurm_job_name=slurm_job_name,
                    simulator_hash=simulator.git_commit_hash,  # type: ignore[union-attr]
                    config=ecoli_simulation.config,
                    config_filename=ecoli_simulation.simulation_config_filename,
                )
                capture_slurm_script(script_content, "simulation_workflow.sbatch")
                f.write(script_content)

            # submit the simulation script to slurm
            async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
                slurm_jobid = await slurm_service.submit_job(
                    ssh, local_sbatch_file=local_submit_file, remote_sbatch_file=slurm_submit_file
                )
            return JobId.slurm(slurm_jobid)

    @override
    async def read_config_template(
        self,
        simulator_version: SimulatorVersion,
        config_filename: str,
        allow_default_fallback: bool = False,
    ) -> str:
        from viva_api.simulation.github_repo import _DEFAULT_CONFIG_TEMPLATE

        if config_filename.startswith("configs/"):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"config_filename {config_filename!r} starts with 'configs/' — "
                    "drop the prefix. The path is resolved relative to the repo's "
                    "configs/ directory."
                ),
            )

        settings = get_settings()
        remote_config_path = (
            settings.hpc_repo_base_path.remote_path
            / simulator_version.git_commit_hash
            / "vEcoli"
            / "configs"
            / config_filename
        )
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            returncode, stdout, stderr = await ssh.run_command(f"cat {remote_config_path}")
            if returncode != 0:
                if allow_default_fallback:
                    logger.warning(
                        "Config %s not found at %s — using embedded default template (allow_default_fallback=True)",
                        config_filename,
                        remote_config_path,
                    )
                    return json.dumps(_DEFAULT_CONFIG_TEMPLATE)
                logger.error(
                    "Config %s not found at %s (rc=%d, stderr=%s)",
                    config_filename,
                    remote_config_path,
                    returncode,
                    stderr.strip()[:200],
                )
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"Config file {config_filename!r} not found at "
                        f"{remote_config_path}. Verify the path is relative "
                        f"to the repo's configs/ directory and the simulator's "
                        f"checkout includes it."
                    ),
                )

        if "EXPERIMENT_ID_PLACEHOLDER" not in stdout:
            if allow_default_fallback:
                logger.warning(
                    "Config %s is a vanilla vEcoli config (no API placeholders) — "
                    "using embedded default template (allow_default_fallback=True)",
                    config_filename,
                )
                return json.dumps(_DEFAULT_CONFIG_TEMPLATE)
            logger.error(
                "Config %s lacks EXPERIMENT_ID_PLACEHOLDER — not silently switching to default template.",
                config_filename,
            )
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Config {config_filename!r} is a vanilla vEcoli config "
                    f"(no EXPERIMENT_ID_PLACEHOLDER) and cannot be used directly "
                    f"by the API pipeline. Use a config with API placeholders, "
                    f"or use the default config."
                ),
            )

        return stdout

    @override
    async def discover_repo_contents(self, simulator_version: SimulatorVersion) -> RepoDiscovery:
        settings = get_settings()
        repo_root = settings.hpc_repo_base_path.remote_path / simulator_version.git_commit_hash / "vEcoli"
        configs_dir = repo_root / "configs"
        analysis_base = repo_root / "ecoli" / "analysis"
        categories = ["single", "multiseed", "multigeneration", "multidaughter", "multivariant"]

        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            # List config files
            rc, stdout, _ = await ssh.run_command(f"ls {configs_dir}/*.json 2>/dev/null", check=False)
            config_filenames = [Path(p).name for p in stdout.strip().splitlines()] if rc == 0 and stdout.strip() else []

            # List analysis modules per category
            analysis_modules: dict[str, list[str]] = {}
            for category in categories:
                rc, stdout, _ = await ssh.run_command(f"ls {analysis_base / category}/*.py 2>/dev/null", check=False)
                if rc == 0 and stdout.strip():
                    modules = [Path(p).stem for p in stdout.strip().splitlines() if not Path(p).name.startswith("__")]
                    if modules:
                        analysis_modules[category] = sorted(modules)

        return RepoDiscovery(
            simulator_id=simulator_version.database_id,
            git_repo_url=simulator_version.git_repo_url,
            git_commit_hash=simulator_version.git_commit_hash,
            config_filenames=sorted(config_filenames),
            analysis_modules=analysis_modules,
        )

    @override
    async def get_job_status(self, job_id: JobId) -> JobStatusInfo | None:
        slurm_service = SlurmService()
        slurmjobid = job_id.as_slurm_int
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            slurm_jobs: list[SlurmJob] = await slurm_service.get_job_status_squeue(ssh, job_ids=[slurmjobid])
            if len(slurm_jobs) == 0:
                slurm_jobs = await slurm_service.get_job_status_scontrol(ssh, job_ids=[slurmjobid])
                if len(slurm_jobs) == 0:
                    logger.warning(f"No job found with ID {slurmjobid} in both squeue and scontrol.")
                    return None
        if len(slurm_jobs) == 1:
            slurm_job = slurm_jobs[0]
            status = JobStatus.from_slurm_state(slurm_job.job_state)
            return JobStatusInfo(
                job_id=job_id,
                status=status,
                start_time=slurm_job.start_time,
                end_time=slurm_job.end_time,
                exit_code=slurm_job.exit_code,
            )
        else:
            raise RuntimeError(f"Multiple jobs found with ID {slurmjobid}: {slurm_jobs}")

    @override
    async def cancel_job(self, job_id: JobId) -> None:
        """Cancel a SLURM job via scancel."""
        slurm_id = job_id.as_slurm_int
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            return_code, _stdout, stderr = await ssh.run_command(f"scancel {slurm_id}")
            if return_code != 0:
                raise RuntimeError(f"Failed to cancel SLURM job {slurm_id}: {stderr.strip()}")
            logger.info(f"Cancelled SLURM job {slurm_id}")

    @override
    async def close(self) -> None:
        pass


def workflow_slurm_script(
    slurm_log_file: HPCFilePath,
    slurm_job_name: str,
    simulator_hash: str,
    config: SimulationConfig,
    config_filename: str,
) -> str:
    """Generate SLURM script for workflow orchestration.

    This script uses a three-step container-based approach:
    1. Run workflow.py --build-only inside container to generate Nextflow files
    2. Copy Nextflow module files from container and fix include paths
    3. Run Nextflow directly on the host (only requires Java/Nextflow)

    This avoids GLIBC compatibility issues and creates a self-contained output directory.

    See: vEcoli/docs/aws-slurm-deployment.md "Container-Based Workflow Execution" section.
    """
    env = get_settings()

    qos_clause = f"#SBATCH --qos={env.slurm_qos}" if env.slurm_qos else ""
    nodelist_clause = f"#SBATCH --nodelist={env.slurm_node_list}" if env.slurm_node_list else ""
    constraint_clause = f"#SBATCH --constraint={env.slurm_constraint}" if env.slurm_constraint else ""

    simulation_outdir_base = env.simulation_outdir
    slurm_log_base_path = env.slurm_log_base_path
    apptainer_image_path = env.hpc_image_base_path / f"vecoli-{simulator_hash}.sif"
    experiment_id = config.experiment_id

    # Build the config dict with required fields for container-based execution
    # sim_data_path: null forces ParCa to run (no cached kb lookup)
    # aws_cdk.container_image: path to Singularity image for Nextflow tasks
    config_dict = config.model_dump()
    # Only force ParCa to run if sim_data_path is not already set (allows using cached simData)
    if not config_dict.get("sim_data_path"):
        config_dict["sim_data_path"] = None  # Force ParCa to run
    if "parca_options" not in config_dict:
        config_dict["parca_options"] = {}
    config_dict["parca_options"]["load_intermediate"] = None  # Don't load cached intermediates

    nf_profile_key = "ccam" if "ccam" in config_filename else "aws_cdk"
    config_dict[nf_profile_key] = {
        "container_image": str(apptainer_image_path),
        "build_image": False,
    }
    config_json = json.dumps(config_dict).replace("'", "'\\''")

    script = dedent(f"""\
        #!/bin/bash
        #SBATCH --job-name={slurm_job_name}
        #SBATCH --time=7-00:00:00
        #SBATCH --cpus-per-task 1
        #SBATCH --mem=8GB
        #SBATCH --partition={env.slurm_partition}
        {qos_clause}
        #SBATCH --mail-type=ALL
        {nodelist_clause}
        #SBATCH -o {slurm_log_file!s}
        #SBATCH -e {slurm_log_file!s}
        {constraint_clause}

        set -e

        ### Configuration
        CONTAINER_IMAGE="{apptainer_image_path!s}"
        OUTPUT_DIR="{simulation_outdir_base!s}"
        IMAGE_DIR="{env.hpc_image_base_path!s}"
        EXPERIMENT_ID="{experiment_id}"
        SLURM_LOG_PATH="{slurm_log_base_path!s}"

        ### Create directories
        mkdir -p "$OUTPUT_DIR"
        mkdir -p "$SLURM_LOG_PATH"

        ### Write workflow config to temp file
        tmp_config=$(mktemp)
        echo '{config_json}' > "$tmp_config"
        chmod 644 "$tmp_config"

        ### Step 1: Generate Nextflow files using workflow.py --build-only
        echo "Step 1: Generating Nextflow files..."
        export SLURM_PARTITION={env.slurm_partition}
        export SLURM_LOG_BASE_PATH="$SLURM_LOG_PATH"

        mkdir -p "$OUTPUT_DIR/nextflow_temp_$EXPERIMENT_ID"
        singularity exec \\
            --fakeroot \\
            --writable-tmpfs \\
            --pwd /vEcoli \\
            --env UV_CACHE_DIR=/tmp/uv_cache \\
            -B "$OUTPUT_DIR":"$OUTPUT_DIR" \\
            -B "$IMAGE_DIR":"$IMAGE_DIR" \\
            -B "$SLURM_LOG_PATH":"$SLURM_LOG_PATH" \\
            -B "$OUTPUT_DIR/nextflow_temp_$EXPERIMENT_ID":"/vEcoli/nextflow_temp" \\
            "$CONTAINER_IMAGE" \\
            /vEcoli/.venv/bin/python /vEcoli/runscripts/workflow.py \\
                --config "$tmp_config" \\
                --build-only

        rm -f "$tmp_config"

        ### Step 2: Copy module files and fix include paths
        echo "Step 2: Copying Nextflow modules and fixing include paths..."
        NEXTFLOW_DIR="$OUTPUT_DIR/$EXPERIMENT_ID/nextflow"

        # Copy Nextflow module files from container
        singularity exec -B "$OUTPUT_DIR":"$OUTPUT_DIR" "$CONTAINER_IMAGE" \\
            cp /vEcoli/runscripts/nextflow/sim.nf "$NEXTFLOW_DIR/"
        singularity exec -B "$OUTPUT_DIR":"$OUTPUT_DIR" "$CONTAINER_IMAGE" \\
            cp /vEcoli/runscripts/nextflow/analysis.nf "$NEXTFLOW_DIR/"

        # Update includes to use relative paths (same directory as main.nf)
        sed -i "s|from '/vEcoli/runscripts/nextflow/sim'|from './sim'|g" "$NEXTFLOW_DIR/main.nf"
        sed -i "s|from '/vEcoli/runscripts/nextflow/analysis'|from './analysis'|g" "$NEXTFLOW_DIR/main.nf"

        # Fix ccam profile clusterOptions to include required QoS
        sed -i "s|clusterOptions = '--constraint=cpu_amd'|clusterOptions = '--constraint=cpu_amd --qos={env.slurm_qos}'|g" "$NEXTFLOW_DIR/nextflow.config"

        ### Step 3: Run Nextflow directly on host
        echo "Step 3: Running Nextflow..."
        export JAVA_HOME=$HOME/.local/bin/java-22
        export PATH=$JAVA_HOME/bin:$HOME/.local/bin:$PATH

        cd "$NEXTFLOW_DIR"

        ### Start weblog receiver for Nextflow event capture
        export EVENTS_FILE="$NEXTFLOW_DIR/${{EXPERIMENT_ID}}_events.ndjson"

        python3 << 'WEBLOG_SCRIPT' &
        __WEBLOG_SCRIPT_PLACEHOLDER__
        WEBLOG_SCRIPT

        WEBLOG_PID=$!
        sleep 1

        # Read the port from temp file
        WEBLOG_PORT=$(cat /tmp/weblog_port_$$ 2>/dev/null || echo "9999")
        rm -f /tmp/weblog_port_$$
        echo "Weblog receiver running on port $WEBLOG_PORT (PID: $WEBLOG_PID)"

        nextflow -C "$NEXTFLOW_DIR/nextflow.config" run "$NEXTFLOW_DIR/main.nf" \\
            -profile {nf_profile_key} \\
            -with-report "$NEXTFLOW_DIR/${{EXPERIMENT_ID}}_report.html" \\
            -with-weblog http://localhost:$WEBLOG_PORT \\
            -work-dir "$NEXTFLOW_DIR/nextflow_workdirs"

        ### Cleanup weblog receiver
        NF_EXIT_CODE=$?
        kill $WEBLOG_PID 2>/dev/null || true
        wait $WEBLOG_PID 2>/dev/null || true

        echo "Workflow completed with exit code: $NF_EXIT_CODE"
        exit $NF_EXIT_CODE
    """)

    script = script.replace("__WEBLOG_SCRIPT_PLACEHOLDER__", WEBLOG_RECEIVER_SCRIPT.strip())
    return script
