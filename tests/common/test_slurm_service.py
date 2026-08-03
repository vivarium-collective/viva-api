import asyncio
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

from viva_api.common.hpc.models import SlurmJob
from viva_api.common.hpc.slurm_service import SlurmService
from viva_api.common.models import SSHTarget
from viva_api.common.ssh.ssh_service import SSHSessionService
from viva_api.common.storage.file_paths import HPCFilePath
from viva_api.config import get_settings
from viva_api.dependencies import get_ssh_session_service

# =============================================================================
# Unit tests for SlurmJob parsing (no SSH required)
# =============================================================================


def test_slurm_job_from_scontrol_output() -> None:
    """Test parsing scontrol show job output."""
    scontrol_output = """JobId=1285550 JobName=sim-bc0add1-1-qmu1tf
   UserId=svc_vivarium(12345) GroupId=pi-agmon(67890)
   Priority=1000 Nice=0 Account=pi-agmon QOS=normal
   JobState=COMPLETED Reason=None Dependency=(null)
   Requeue=0 Restarts=0 BatchFlag=1 Reboot=0 ExitCode=0:0
   RunTime=00:31:00 TimeLimit=01:00:00 TimeMin=N/A
   SubmitTime=2026-01-14T00:18:50 EligibleTime=2026-01-14T00:18:50
   AccrueTime=2026-01-14T00:18:50
   StartTime=2026-01-14T00:19:08 EndTime=2026-01-14T00:50:08 Deadline=N/A
   SuspendTime=None SecsPreSuspend=0 LastSchedEval=2026-01-14T00:19:08"""

    job = SlurmJob.from_scontrol_output(scontrol_output)

    assert job.job_id == 1285550
    assert job.name == "sim-bc0add1-1-qmu1tf"
    assert job.account == "pi-agmon"
    assert job.user_name == "svc_vivarium"
    assert job.job_state == "COMPLETED"
    assert job.start_time == "2026-01-14T00:19:08"
    assert job.end_time == "2026-01-14T00:50:08"
    assert job.elapsed == "00:31:00"
    assert job.exit_code == "0:0"
    assert job.reason is None  # "None" should become None


def test_slurm_job_from_scontrol_output_running() -> None:
    """Test parsing scontrol output for a running job."""
    scontrol_output = """JobId=1286339 JobName=sim-4c58f7e-1-17mw8v
   UserId=svc_vivarium(12345) GroupId=pi-agmon(67890)
   Priority=1000 Nice=0 Account=pi-agmon QOS=normal
   JobState=RUNNING Reason=None Dependency=(null)
   RunTime=00:10:00 TimeLimit=02:00:00 TimeMin=N/A
   StartTime=2026-01-14T11:01:04 EndTime=Unknown"""

    job = SlurmJob.from_scontrol_output(scontrol_output)

    assert job.job_id == 1286339
    assert job.name == "sim-4c58f7e-1-17mw8v"
    assert job.job_state == "RUNNING"
    assert job.start_time == "2026-01-14T11:01:04"
    assert job.end_time is None  # "Unknown" should become None
    assert job.elapsed == "00:10:00"
    assert job.reason is None


def test_slurm_job_from_scontrol_output_failed_with_reason() -> None:
    """Test parsing scontrol output for a failed job with reason."""
    scontrol_output = """JobId=1286500 JobName=sim-failed-job
   UserId=svc_vivarium(12345) GroupId=pi-agmon(67890)
   Priority=1000 Nice=0 Account=pi-agmon QOS=normal
   JobState=FAILED Reason=NonZeroExitCode Dependency=(null)
   Requeue=0 Restarts=0 BatchFlag=1 Reboot=0 ExitCode=1:0
   RunTime=00:05:00 TimeLimit=01:00:00 TimeMin=N/A
   StartTime=2026-01-14T12:00:00 EndTime=2026-01-14T12:05:00"""

    job = SlurmJob.from_scontrol_output(scontrol_output)

    assert job.job_id == 1286500
    assert job.name == "sim-failed-job"
    assert job.job_state == "FAILED"
    assert job.reason == "NonZeroExitCode"
    assert job.exit_code == "1:0"


def test_slurm_job_from_scontrol_output_cancelled_with_reason() -> None:
    """Test parsing scontrol output for a cancelled job."""
    scontrol_output = """JobId=1286501 JobName=sim-cancelled-job
   UserId=svc_vivarium(12345) GroupId=pi-agmon(67890)
   Priority=1000 Nice=0 Account=pi-agmon QOS=normal
   JobState=CANCELLED Reason=TimeLimit Dependency=(null)
   ExitCode=0:15
   StartTime=2026-01-14T12:00:00 EndTime=2026-01-14T13:00:00"""

    job = SlurmJob.from_scontrol_output(scontrol_output)

    assert job.job_id == 1286501
    assert job.job_state == "CANCELLED"
    assert job.reason == "TimeLimit"
    assert job.exit_code == "0:15"


# =============================================================================
# Integration tests (SSH required)
# =============================================================================


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_slurm_job_query_squeue(slurm_service: SlurmService) -> None:
    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        all_jobs: list[SlurmJob] = await slurm_service.get_job_status_squeue(ssh)
        assert all_jobs is not None
        if len(all_jobs) > 0:
            assert isinstance(all_jobs[0], SlurmJob)
            one_job: list[SlurmJob] = await slurm_service.get_job_status_squeue(ssh, job_ids=[all_jobs[0].job_id])
            assert one_job is not None
            assert len(one_job) == 1
            assert one_job[0] == all_jobs[0]


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_slurm_job_query_sacct(slurm_service: SlurmService) -> None:
    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        all_jobs: list[SlurmJob] = await slurm_service._get_job_status_sacct(ssh)
        assert all_jobs is not None
        if len(all_jobs) > 0:
            assert isinstance(all_jobs[0], SlurmJob)
            one_job: list[SlurmJob] = await slurm_service._get_job_status_sacct(ssh, job_ids=[all_jobs[0].job_id])
            assert one_job is not None
            assert len(one_job) == 1
            assert one_job[0] == all_jobs[0]


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_slurm_job_query_scontrol(slurm_service: SlurmService) -> None:
    """Test scontrol-based job query (alternative to sacct when accounting is disabled)."""
    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        # First get running jobs from squeue
        running_jobs: list[SlurmJob] = await slurm_service.get_job_status_squeue(ssh)
        if len(running_jobs) == 0:
            pytest.skip("No running jobs to test scontrol query")

        # Query the same job via scontrol
        job_id = running_jobs[0].job_id
        scontrol_jobs: list[SlurmJob] = await slurm_service.get_job_status_scontrol(ssh, job_ids=[job_id])

        assert len(scontrol_jobs) == 1
        assert scontrol_jobs[0].job_id == job_id
        assert scontrol_jobs[0].name == running_jobs[0].name
        # State should match (both should report RUNNING or PENDING)
        assert scontrol_jobs[0].job_state.upper() == running_jobs[0].job_state.upper()


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_slurm_job_submit(slurm_service: SlurmService, slurm_template_hello_1s: str) -> None:
    settings = get_settings()
    remote_path = settings.slurm_log_base_path
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_dir = Path(tmpdir)
        # write slurm_template_hello_1s to a temp file
        local_sbatch_file = tmp_dir / f"job_{uuid.uuid4().hex}.sbatch"
        with open(local_sbatch_file, "w") as f:
            f.write(slurm_template_hello_1s)

        remote_sbatch_file = remote_path / local_sbatch_file.name
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            job_id: int = await slurm_service.submit_job(
                ssh, local_sbatch_file=local_sbatch_file, remote_sbatch_file=remote_sbatch_file
            )

            submitted_job: list[SlurmJob] = await slurm_service.get_job_status_squeue(ssh, job_ids=[job_id])
            assert submitted_job is not None and len(submitted_job) == 1
            assert submitted_job[0].job_id == job_id
            assert submitted_job[0].name == "my_test_job"


# =============================================================================
# Nextflow Workflow Tests
# =============================================================================


@dataclass
class NextflowTestResult:
    """Result from running a Nextflow workflow test."""

    job_id: int
    final_job: SlurmJob
    remote_output_file: HPCFilePath
    remote_error_file: HPCFilePath
    remote_events_file: HPCFilePath
    remote_report_file: HPCFilePath
    remote_trace_file: HPCFilePath


async def _run_nextflow_workflow_test(
    slurm_service: SlurmService,
    nextflow_script: str,
    nextflow_config: str,
    sbatch_template: str,
    *,
    file_prefix: str,
    expected_job_name: str,
    max_wait_seconds: int = 300,
    poll_interval_seconds: int = 5,
) -> NextflowTestResult:
    """
    Shared helper to run a Nextflow workflow via Slurm and poll for completion.

    Args:
        slurm_service: The Slurm service for job submission and status checks
        nextflow_script: The Nextflow workflow script content
        nextflow_config: Nextflow config file content (sets executor and workDir)
        sbatch_template: The sbatch template with placeholders
        file_prefix: Prefix for all generated files (e.g., "nextflow_test_<uuid>")
        expected_job_name: Expected Slurm job name for assertion
        max_wait_seconds: Maximum time to wait for job completion
        poll_interval_seconds: Interval between status checks

    Returns:
        NextflowTestResult with job details and file paths

    Raises:
        AssertionError: If job fails or times out
    """
    settings = get_settings()
    remote_base_path = settings.slurm_log_base_path

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_dir = Path(tmpdir)

        # Write Nextflow script to local temp file
        local_nf_script = tmp_dir / f"{file_prefix}.nf"
        with open(local_nf_script, "w") as f:
            f.write(nextflow_script)

        # Calculate remote paths
        remote_nf_script = remote_base_path / local_nf_script.name
        remote_output_file = remote_base_path / f"{file_prefix}.out"
        remote_error_file = remote_base_path / f"{file_prefix}.err"
        remote_report_file = remote_base_path / f"{file_prefix}.report.html"
        remote_trace_file = remote_base_path / f"{file_prefix}.trace.txt"
        remote_events_file = remote_base_path / f"{file_prefix}.events.ndjson"

        # Start with common placeholder replacements
        sbatch_content = (
            sbatch_template.replace("NEXTFLOW_SCRIPT_PATH", str(remote_nf_script))
            .replace("REMOTE_LOG_OUTPUT_FILE", str(remote_output_file))
            .replace("REMOTE_LOG_ERROR_FILE", str(remote_error_file))
            .replace("REMOTE_REPORT_FILE", str(remote_report_file))
            .replace("REMOTE_TRACE_FILE", str(remote_trace_file))
            .replace("REMOTE_EVENTS_FILE", str(remote_events_file))
        )

        # Write Nextflow config to local temp file
        remote_work_dir = remote_base_path / f"{file_prefix}_work"
        config_content = nextflow_config.replace("WORK_DIR_PLACEHOLDER", str(remote_work_dir))

        local_nf_config = tmp_dir / f"{file_prefix}.config"
        with open(local_nf_config, "w") as f:
            f.write(config_content)

        remote_nf_config = remote_base_path / local_nf_config.name
        sbatch_content = sbatch_content.replace("NEXTFLOW_CONFIG_PATH", str(remote_nf_config))

        # Write sbatch script to local temp file
        local_sbatch_file = tmp_dir / f"{file_prefix}.sbatch"
        with open(local_sbatch_file, "w") as f:
            f.write(sbatch_content)

        remote_sbatch_file = remote_base_path / local_sbatch_file.name

        # Use single SSH session for file upload, job submission, and polling
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            # Upload files to remote
            await ssh.scp_upload(local_file=local_nf_script, remote_path=remote_nf_script)
            await ssh.scp_upload(local_file=local_nf_config, remote_path=remote_nf_config)

            # Submit the Slurm job
            job_id: int = await slurm_service.submit_job(
                ssh, local_sbatch_file=local_sbatch_file, remote_sbatch_file=remote_sbatch_file
            )
            assert job_id > 0, "Failed to get valid job ID"

            # Poll for job completion
            elapsed_seconds = 0
            final_job: SlurmJob | None = None

            while elapsed_seconds < max_wait_seconds:
                # Check squeue first (for running/pending jobs)
                jobs: list[SlurmJob] = await slurm_service.get_job_status_squeue(ssh, job_ids=[job_id])
                if len(jobs) > 0 and jobs[0].job_state.upper() in ["PENDING", "RUNNING", "CONFIGURING"]:
                    await asyncio.sleep(poll_interval_seconds)
                    elapsed_seconds += poll_interval_seconds
                    continue

                # Check sacct for completed jobs (may have delay before appearing)
                jobs = await slurm_service.get_job_status_scontrol(ssh, job_ids=[job_id])
                if len(jobs) > 0:
                    final_job = jobs[0]
                    if final_job.is_done():
                        break

                await asyncio.sleep(poll_interval_seconds)
                elapsed_seconds += poll_interval_seconds

        # Assertions
        assert final_job is not None, (
            f"Nextflow job {job_id} not found in squeue or sacct after {max_wait_seconds} seconds"
        )
        assert final_job.name == expected_job_name, f"Unexpected job name: {final_job.name}"
        assert final_job.job_state.upper() == "COMPLETED", (
            f"Nextflow job failed with state: {final_job.job_state}, exit code: {final_job.exit_code}"
        )

        return NextflowTestResult(
            job_id=job_id,
            final_job=final_job,
            remote_output_file=remote_output_file,
            remote_error_file=remote_error_file,
            remote_events_file=remote_events_file,
            remote_report_file=remote_report_file,
            remote_trace_file=remote_trace_file,
        )


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_nextflow_workflow_local_executor(
    slurm_service: SlurmService,
    ssh_session_service: SSHSessionService,
    nextflow_script_hello: str,
    nextflow_config_local_executor: str,
    slurm_template_nextflow: str,
) -> None:
    """
    Test Nextflow workflow using the LOCAL executor.

    In this mode:
    - Nextflow runs as a Slurm job
    - Each Nextflow process runs on the SAME node as the parent job
    - No additional Slurm jobs are submitted for processes
    - Faster execution, simpler setup
    - Uses unique work directory per run
    """
    job_uuid = uuid.uuid4().hex

    result = await _run_nextflow_workflow_test(
        slurm_service=slurm_service,
        nextflow_script=nextflow_script_hello,
        nextflow_config=nextflow_config_local_executor,
        sbatch_template=slurm_template_nextflow,
        file_prefix=f"nextflow_test_{job_uuid}",
        expected_job_name="nextflow_test",
        max_wait_seconds=300,
        poll_interval_seconds=5,
    )

    assert result.final_job.job_state.upper() == "COMPLETED"


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_nextflow_workflow_slurm_executor(
    slurm_service: SlurmService,
    ssh_session_service: SSHSessionService,
    nextflow_script_hello_slurm: str,
    nextflow_config_slurm_executor: str,
    slurm_template_nextflow_slurm_executor: str,
) -> None:
    """
    Test Nextflow workflow using the SLURM executor.

    In this mode:
    - Nextflow runs as a parent Slurm job
    - Each Nextflow process is submitted as a SEPARATE Slurm job
    - Child jobs can run on different nodes in the cluster
    - Better for distributed workloads, but has scheduling overhead
    """
    job_uuid = uuid.uuid4().hex

    result = await _run_nextflow_workflow_test(
        slurm_service=slurm_service,
        nextflow_script=nextflow_script_hello_slurm,
        nextflow_config=nextflow_config_slurm_executor,
        sbatch_template=slurm_template_nextflow_slurm_executor,
        file_prefix=f"nextflow_slurm_{job_uuid}",
        expected_job_name="nextflow_slurm_test",
        max_wait_seconds=600,
        poll_interval_seconds=10,
    )

    assert result.final_job.job_state.upper() == "COMPLETED"
