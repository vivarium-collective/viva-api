"""Integration test for run_workflow_simple().

This test exercises the simplified workflow execution path that:
1. Reads configuration from a template file on the HPC
2. Replaces placeholders with runtime values
3. Submits the workflow job to SLURM
4. Polls for completion

Run with: uv run pytest tests/integration/test_run_workflow_simple.py -v

Prerequisites:
- SSH access to HPC (SLURM_SUBMIT_KEY_PATH configured)
- Simulator already exists (repo cloned, image built)
- Config template exists at {HPC_REPO_BASE_PATH}/{hash}/vEcoli/configs/api_simulation_default_with_profile.json
"""

import asyncio
import time
import uuid
from pathlib import Path

import pytest

from tests.fixtures.api_fixtures import SimulatorRepoInfo
from viva_api.common.handlers import simulations
from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.models import JobId, JobStatus, SSHTarget
from viva_api.config import get_settings
from viva_api.dependencies import get_ssh_session_service
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.hpc_utils import get_apptainer_image_file
from viva_api.simulation.models import SimulatorVersion
from viva_api.simulation.simulation_service import SimulationServiceHpc

# Config file name expected in vEcoli/configs/
CONFIG_FILENAME = "api_simulation_default_with_profile.json"

# Skip all tests if SSH not configured
pytestmark = pytest.mark.skipif(
    not Path(get_settings().slurm_submit_key_path).exists(),
    reason="slurm ssh key file not supplied",
)


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


async def _check_repo_exists(commit_hash: str) -> bool:
    """Check if the vEcoli repo exists at the expected location."""
    settings = get_settings()
    repo_path = settings.hpc_repo_base_path.remote_path / commit_hash / "vEcoli"

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


async def _check_config_exists(commit_hash: str, config_filename: str) -> bool:
    """Check if the config template file exists on the HPC."""
    settings = get_settings()
    config_path = settings.hpc_repo_base_path.remote_path / commit_hash / "vEcoli" / "configs" / config_filename

    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        check_cmd = f"test -f {config_path} && echo 'EXISTS' || echo 'MISSING'"
        _, result, _ = await ssh.run_command(check_cmd)
        return "EXISTS" in result


async def _ensure_prerequisites(
    simulation_service: SimulationServiceHpc,
    database_service: DatabaseServiceSQL,
    repo_info: SimulatorRepoInfo,
) -> SimulatorVersion:
    """
    Ensure all prerequisites are met for the test.

    Checks that:
    1. Simulator exists in database
    2. Repo is cloned on HPC
    3. Singularity image is built
    4. Config template exists

    If repo/image don't exist, submits build job and waits for completion.
    """
    simulator = await _get_or_create_simulator(database_service, repo_info)

    repo_exists = await _check_repo_exists(repo_info.commit_hash)
    image_exists = await _check_image_exists(simulator)

    if not repo_exists or not image_exists:
        print(f"\nBuilding repo and image for {repo_info.commit_hash}...")

        job_id = await simulation_service.submit_build_image_job(simulator_version=simulator)
        assert job_id is not None, "Failed to submit build job"

        print(f"  Submitted build job {job_id}")

        # Poll for completion (30 minute timeout)
        start_time = time.time()
        job_info: JobStatusInfo | None = None
        while start_time + 1800 > time.time():
            job_info = await simulation_service.get_job_status(job_id=job_id)
            if job_info is not None and job_info.status in (
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.CANCELLED,
            ):
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

    # Check config exists
    config_exists = await _check_config_exists(repo_info.commit_hash, CONFIG_FILENAME)
    if not config_exists:
        settings = get_settings()
        config_path = (
            settings.hpc_repo_base_path.remote_path / repo_info.commit_hash / "vEcoli" / "configs" / CONFIG_FILENAME
        )
        pytest.skip(f"Config template not found: {config_path}")

    return simulator


@pytest.mark.asyncio
async def test_run_workflow_simple(
    simulation_service_slurm: SimulationServiceHpc,
    database_service: DatabaseServiceSQL,
    simulator_repo_info: SimulatorRepoInfo,
) -> None:
    """
    Integration test for run_workflow_simple().

    This test:
    1. Ensures prerequisites are met (repo cloned, image built, config exists)
    2. Calls run_workflow_simple() with test parameters
    3. Polls for workflow completion
    4. Verifies the workflow completed successfully

    Expected runtime: 30-60 minutes depending on cluster load.
    """
    # Ensure prerequisites
    simulator = await _ensure_prerequisites(
        simulation_service=simulation_service_slurm,
        database_service=database_service,
        repo_info=simulator_repo_info,
    )

    # Generate unique experiment ID for this test run
    job_uuid = uuid.uuid4().hex[:8]
    experiment_id = f"test_simple_{job_uuid}"

    print(f"\nRunning run_workflow_simple() with experiment_id={experiment_id}")
    print(f"  Simulator ID: {simulator.database_id}")
    print(f"  Config file: {CONFIG_FILENAME}")

    # Call run_workflow_simple
    simulation = await simulations.run_simulation_workflow(
        database_service=database_service,
        simulation_service=simulation_service_slurm,
        simulator_id=simulator.database_id,
        experiment_id=experiment_id,
        simulation_config_filename=CONFIG_FILENAME,
        num_generations=2,  # Override for faster test
        num_seeds=2,  # Override for faster test
        description="Integration test for run_workflow_simple",
    )

    assert simulation is not None, "Simulation should be created"
    assert simulation.database_id is not None, "Simulation should have database ID"
    assert simulation.job_id is not None, "Simulation should have SLURM job ID"

    print(f"  Simulation DB ID: {simulation.database_id}")
    print(f"  SLURM Job ID: {simulation.job_id}")

    # Poll for workflow completion
    start_time = time.time()
    max_wait_seconds = 7200  # 2 hour timeout
    poll_interval = 30

    job_info: JobStatusInfo | None = None
    while time.time() - start_time < max_wait_seconds:
        job_info = await simulation_service_slurm.get_job_status(job_id=JobId.slurm(int(simulation.job_id)))

        if job_info is not None:
            elapsed = int(time.time() - start_time)
            if job_info.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
                print(f"\n  Job {simulation.job_id} completed after {elapsed}s")
                print(f"  Status: {job_info.status}")
                print(f"  Exit code: {job_info.exit_code}")
                break
            elif elapsed % 60 == 0:
                print(f"  Job {simulation.job_id} status: {job_info.status} ({elapsed}s elapsed)")

        await asyncio.sleep(poll_interval)
    else:
        pytest.fail(f"Workflow job {simulation.job_id} did not complete within {max_wait_seconds}s")

    # Verify job completed successfully
    assert job_info is not None, "Should have final job status"
    assert job_info.status == JobStatus.COMPLETED, (
        f"Workflow should complete successfully. Status: {job_info.status}, Exit code: {job_info.exit_code}"
    )

    # Verify simulation output exists
    settings = get_settings()
    output_path = settings.simulation_outdir.remote_path / experiment_id

    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        check_cmd = f"test -d {output_path} && echo 'EXISTS' || echo 'MISSING'"
        _, result, _ = await ssh.run_command(check_cmd)

        if "EXISTS" in result:
            # List output directory contents
            ls_cmd = f"ls -la {output_path}/"
            _, ls_output, _ = await ssh.run_command(ls_cmd)
            print(f"\n=== Output directory contents ===\n{ls_output}")
        else:
            print(f"\nWarning: Output directory not found at {output_path}")

    print("\nTest completed successfully!")
    print(f"  Experiment ID: {experiment_id}")
    print(f"  Simulation DB ID: {simulation.database_id}")
