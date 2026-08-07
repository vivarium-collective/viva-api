"""Tests for workflow.py integration.

These tests run workflow.py from the vEcoli repo outside of a container,
allowing proper SLURM integration for Nextflow subtasks.

Run with: uv run pytest tests/common/test_workflow.py -v
"""

import asyncio
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

from tests.fixtures.api_fixtures import SimulatorRepoInfo
from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.hpc.models import SlurmJob
from viva_api.common.hpc.slurm_service import SlurmService
from viva_api.common.models import JobStatus, SSHTarget
from viva_api.common.ssh.ssh_service import SSHSessionService
from viva_api.common.storage.file_paths import HPCFilePath
from viva_api.config import get_settings
from viva_api.dependencies import get_ssh_session_service
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.hpc_utils import get_apptainer_image_file
from viva_api.simulation.models import SimulatorVersion
from viva_api.simulation.simulation_service import SimulationServiceHpc


@dataclass
class WorkflowTestResult:
    """Result from running a workflow.py test."""

    job_id: int
    final_job: SlurmJob
    remote_output_dir: HPCFilePath
    remote_output_file: HPCFilePath
    remote_error_file: HPCFilePath
    experiment_id: str
    vecoli_repo_path: HPCFilePath


async def _check_repo_exists(commit_hash: str) -> bool:
    """Check if the vEcoli repo exists at the expected location."""
    settings = get_settings()
    repo_path = settings.hpc_repo_base_path / commit_hash / "vEcoli"

    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        check_cmd = f"test -d {repo_path} && echo 'EXISTS' || echo 'MISSING'"
        _, result, _ = await ssh.run_command(check_cmd)
        return "EXISTS" in result


async def _check_image_exists(simulator: SimulatorVersion) -> bool:
    """Check if the singularity image already exists on HPC."""
    image_path = get_apptainer_image_file(simulator)
    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        check_cmd = f"test -f {image_path.remote_path} && echo 'EXISTS' || echo 'MISSING'"
        _, result, _ = await ssh.run_command(check_cmd)
        return "EXISTS" in result


async def _get_or_create_simulator(
    database_service: DatabaseServiceSQL, repo_info: SimulatorRepoInfo
) -> SimulatorVersion:
    """Get or create simulator entry in database."""
    for _simulator in await database_service.list_simulators():
        if (
            _simulator.git_commit_hash == repo_info.commit_hash
            and _simulator.git_repo_url == repo_info.url
            and _simulator.git_branch == repo_info.branch
        ):
            return _simulator

    return await database_service.insert_simulator(
        git_commit_hash=repo_info.commit_hash, git_repo_url=repo_info.url, git_branch=repo_info.branch
    )


async def _ensure_repo_and_image_exist(
    simulation_service: SimulationServiceHpc,
    database_service: DatabaseServiceSQL,
    repo_info: SimulatorRepoInfo,
) -> tuple[SimulatorVersion, HPCFilePath]:
    """
    Ensure the vEcoli repo is cloned and image is built.

    If the repo doesn't exist, clone it and build the image.
    Returns the simulator version and repo path.
    """
    settings = get_settings()
    repo_path = settings.hpc_repo_base_path / repo_info.commit_hash / "vEcoli"

    # Get or create simulator in database
    simulator = await _get_or_create_simulator(database_service, repo_info)

    # Check if repo already exists
    repo_exists = await _check_repo_exists(repo_info.commit_hash)
    image_exists = await _check_image_exists(simulator)

    if repo_exists and image_exists:
        print(f"\nRepo and image already exist for {repo_info.commit_hash}")
        return simulator, repo_path

    # Need to build - this clones the repo and builds the image
    print(f"\nCloning repo and building image for {repo_info.commit_hash}...")

    job_id = await simulation_service.submit_build_image_job(simulator_version=simulator)
    assert job_id is not None, "Failed to submit build job"

    print(f"  Submitted build job {job_id}")

    # Poll for completion (30 minute timeout for build)
    start_time = time.time()
    job_info: JobStatusInfo | None = None
    while start_time + 1800 > time.time():
        job_info = await simulation_service.get_job_status(job_id=job_id)
        if job_info is not None and job_info.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            break
        elapsed = int(time.time() - start_time)
        if elapsed % 60 == 0:
            print(f"  Build job {job_id} running... ({elapsed}s elapsed)")
        await asyncio.sleep(10)

    assert job_info is not None, "Build job did not complete in time"
    assert job_info.status == JobStatus.COMPLETED, (
        f"Build job failed with status: {job_info.status}, exit code: {job_info.exit_code}"
    )

    print(f"  Build job {job_id} completed successfully")

    return simulator, repo_path


async def _run_workflow_test(
    slurm_service: SlurmService,
    sbatch_template: str,
    workflow_config_content: str,
    simulator: SimulatorVersion,
    vecoli_repo_path: HPCFilePath,
    *,
    file_prefix: str,
    experiment_id: str,
    expected_job_name: str,
    max_wait_seconds: int = 3600,
    poll_interval_seconds: int = 30,
) -> WorkflowTestResult:
    """
    Helper to run workflow.py via SLURM and poll for completion.

    This uploads the workflow_config.json to the SLURM cluster and runs
    workflow.py from the vEcoli repo root (outside container).
    """
    settings = get_settings()
    remote_base_path = settings.slurm_log_base_path

    # Get container image path for the simulator
    container_image_path = get_apptainer_image_file(simulator)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_dir = Path(tmpdir)

        # Create output directory path
        remote_output_dir = remote_base_path / f"{file_prefix}_output"

        # Prepare workflow config with placeholder replacements
        config_content = workflow_config_content.replace("EXPERIMENT_ID_PLACEHOLDER", experiment_id).replace(
            "CONTAINER_IMAGE_PLACEHOLDER", str(container_image_path.remote_path)
        )

        # Write workflow_config.json to local temp file
        local_workflow_config = tmp_dir / "workflow_config.json"
        with open(local_workflow_config, "w") as f:
            f.write(config_content)

        # Calculate remote paths
        remote_workflow_config = remote_output_dir / "workflow_config.json"
        remote_output_file = remote_base_path / f"{file_prefix}.out"
        remote_error_file = remote_base_path / f"{file_prefix}.err"

        # Build sbatch content with placeholder replacements
        sbatch_content = (
            sbatch_template.replace("VECOLI_REPO_PATH_PLACEHOLDER", str(vecoli_repo_path))
            .replace("WORKFLOW_CONFIG_PATH_PLACEHOLDER", str(remote_workflow_config))
            .replace("OUTPUT_DIR_PLACEHOLDER", str(remote_output_dir))
            .replace("EXPERIMENT_ID_PLACEHOLDER", experiment_id)
            .replace("REMOTE_LOG_OUTPUT_FILE", str(remote_output_file))
            .replace("REMOTE_LOG_ERROR_FILE", str(remote_error_file))
        )

        # Write sbatch script to local temp file
        local_sbatch_file = tmp_dir / f"{file_prefix}.sbatch"
        with open(local_sbatch_file, "w") as f:
            f.write(sbatch_content)

        remote_sbatch_file = remote_base_path / local_sbatch_file.name

        # Use single SSH session for all operations
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            # Create remote output directory
            await ssh.run_command(f"mkdir -p {remote_output_dir}")

            # Upload workflow config
            await ssh.scp_upload(local_file=local_workflow_config, remote_path=remote_workflow_config)

            # Submit the Slurm job
            job_id: int = await slurm_service.submit_job(
                ssh, local_sbatch_file=local_sbatch_file, remote_sbatch_file=remote_sbatch_file
            )
            assert job_id > 0, "Failed to get valid job ID"

            print(f"\nSubmitted workflow job {job_id}")
            print(f"  vEcoli repo: {vecoli_repo_path}")
            print(f"  Config: {remote_workflow_config}")
            print(f"  Output: {remote_output_dir}")
            print(f"  Logs: {remote_output_file}")

            # Poll for job completion
            elapsed_seconds = 0
            final_job: SlurmJob | None = None

            while elapsed_seconds < max_wait_seconds:
                # Check squeue first (for running/pending jobs)
                jobs: list[SlurmJob] = await slurm_service.get_job_status_squeue(ssh, job_ids=[job_id])
                if len(jobs) > 0 and jobs[0].job_state.upper() in ["PENDING", "RUNNING", "CONFIGURING"]:
                    if elapsed_seconds % 60 == 0:
                        print(f"  Job {job_id} status: {jobs[0].job_state} ({elapsed_seconds}s elapsed)")
                    await asyncio.sleep(poll_interval_seconds)
                    elapsed_seconds += poll_interval_seconds
                    continue

                # Check sacct for completed jobs
                jobs = await slurm_service.get_job_status_scontrol(ssh, job_ids=[job_id])
                if len(jobs) > 0:
                    final_job = jobs[0]
                    if final_job.is_done():
                        break

                await asyncio.sleep(poll_interval_seconds)
                elapsed_seconds += poll_interval_seconds

        # Assertions
        assert final_job is not None, (
            f"Workflow job {job_id} not found in squeue or sacct after {max_wait_seconds} seconds"
        )
        assert final_job.name == expected_job_name, f"Unexpected job name: {final_job.name}"

        return WorkflowTestResult(
            job_id=job_id,
            final_job=final_job,
            remote_output_dir=remote_output_dir,
            remote_output_file=remote_output_file,
            remote_error_file=remote_error_file,
            experiment_id=experiment_id,
            vecoli_repo_path=vecoli_repo_path,
        )


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_workflow_py_execution(
    slurm_service: SlurmService,
    ssh_session_service: SSHSessionService,
    simulation_service_slurm: SimulationServiceHpc,
    database_service: DatabaseServiceSQL,
    slurm_template_workflow: str,
    workflow_test_config_content: str,
    simulator_repo_info: SimulatorRepoInfo,
) -> None:
    """
    Test workflow.py execution from the vEcoli repo.

    This test:
    1. Ensures vEcoli repo is cloned and image is built (clones if needed)
    2. Uploads workflow_config.json to the cluster
    3. Runs workflow.py from the vEcoli repo root (outside container)
    4. workflow.py internally calls Nextflow with SLURM executor
    5. Verifies the workflow completes successfully

    Running outside the container allows proper SLURM integration for
    Nextflow subtasks, avoiding the complexity of calling SLURM from
    inside a Singularity container.

    Expected runtime: Several minutes to hours depending on cluster load
    and workflow scope. Build step adds ~10-20 minutes if image doesn't exist.
    """
    # Ensure repo and image exist (clone and build if needed)
    simulator, vecoli_repo_path = await _ensure_repo_and_image_exist(
        simulation_service=simulation_service_slurm,
        database_service=database_service,
        repo_info=simulator_repo_info,
    )

    job_uuid = uuid.uuid4().hex[:8]
    experiment_id = f"workflow_test_{job_uuid}"

    result = await _run_workflow_test(
        slurm_service=slurm_service,
        sbatch_template=slurm_template_workflow,
        workflow_config_content=workflow_test_config_content,
        simulator=simulator,
        vecoli_repo_path=vecoli_repo_path,
        file_prefix=f"workflow_{job_uuid}",
        experiment_id=experiment_id,
        expected_job_name="workflow_test",
        max_wait_seconds=7200,  # 2 hour timeout
        poll_interval_seconds=30,
    )

    # Verify job completed successfully
    assert result.job_id > 0, "Job should have been submitted"
    assert result.final_job is not None, "Should have a final job status"

    # Log result for debugging
    print(f"\nWorkflow job {result.job_id} finished")
    print(f"  State: {result.final_job.job_state}")
    print(f"  Exit code: {result.final_job.exit_code}")
    print(f"  Output dir: {result.remote_output_dir}")

    # Check job output for errors
    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        # Read last 50 lines of output file
        tail_cmd = f"tail -50 {result.remote_output_file} 2>/dev/null || echo 'NO_OUTPUT'"
        _, output, _ = await ssh.run_command(tail_cmd)
        print(f"\n=== Last 50 lines of output ===\n{output}")

        # Read last 50 lines of error file
        tail_err_cmd = f"tail -50 {result.remote_error_file} 2>/dev/null || echo 'NO_ERRORS'"
        _, errors, _ = await ssh.run_command(tail_err_cmd)
        if errors.strip() and errors.strip() != "NO_ERRORS":
            print(f"\n=== Last 50 lines of errors ===\n{errors}")

        # List output directory
        ls_cmd = f"ls -la {result.remote_output_dir}/ 2>/dev/null || echo 'DIR_NOT_FOUND'"
        _, ls_output, _ = await ssh.run_command(ls_cmd)
        print(f"\n=== Output directory contents ===\n{ls_output}")

    assert result.final_job.job_state.upper() == "COMPLETED", (
        f"Workflow should complete successfully. State: {result.final_job.job_state}, "
        f"Exit code: {result.final_job.exit_code}"
    )
