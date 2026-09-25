"""The SSH layer and core's ``SlurmService`` against a real scheduler -- the SLURM cluster in Docker
in CI, the real submit host with ``--slurm-backend cluster`` (``docs/plan-core.md`` §4b, U1).

Until now every SLURM test in this repository self-skipped without a key to Mantis, so the SLURM
code was covered only by recorded-fixture parsers. These run on every pull request. The
conformance tests are the drift alarm between the two backends: they assert the SHAPE of the
scheduler's output, because that is where a container and the production cluster can silently
diverge -- a different SLURM version changes a field count or a state spelling, the parsers keep
running, and the meaning quietly changes. Running with both backends compares them.
"""

from __future__ import annotations

import tempfile
import uuid
from pathlib import Path
from textwrap import dedent

import pytest

from tests.fixtures.slurm_fixtures_backend import SlurmBackend
from viva_core.backends.models import SlurmJob
from viva_core.backends.slurm_service import SlurmService
from viva_core.models import JobStatus
from viva_core.storage.file_paths import HPCFilePath

SQUEUE_FIELDS = 5  # %i|%j|%a|%u|%T -- what SlurmJob.from_squeue_formatted_output indexes
POLL_SECONDS = 2.0
JOB_TIMEOUT_SECONDS = 180.0


def _hello_sbatch(backend: SlurmBackend, *, sleep_seconds: int, job_name: str) -> str:
    """The canary the production templates are modelled on: partition, an optional QoS (sbatch
    rejects an empty ``--qos=``), one core, a minute."""
    qos = f"#SBATCH --qos={backend.qos}" if backend.qos else ""
    nodelist = f"#SBATCH --nodelist={backend.node_list}" if backend.node_list else ""
    return dedent(f"""\
        #!/bin/bash
        #SBATCH --job-name={job_name}
        #SBATCH --output={backend.remote_base}/htclogs/{job_name}.out
        #SBATCH --error={backend.remote_base}/htclogs/{job_name}.err
        #SBATCH --partition={backend.partition}
        {qos}
        {nodelist}
        #SBATCH --nodes=1
        #SBATCH --ntasks-per-node=1
        #SBATCH --cpus-per-task=1
        #SBATCH --time=0-00:02:00
        echo "hello from $SLURM_JOB_ID on $(hostname)"
        sleep {sleep_seconds}
        """)


async def _submit(backend: SlurmBackend, service: SlurmService, *, sleep_seconds: int) -> tuple[int, str]:
    job_name = f"viva_canary_{uuid.uuid4().hex[:8]}"
    with tempfile.TemporaryDirectory() as tmpdir:
        local = Path(tmpdir) / f"{job_name}.sbatch"
        local.write_text(_hello_sbatch(backend, sleep_seconds=sleep_seconds, job_name=job_name))
        remote = HPCFilePath(remote_path=backend.remote_base / "htclogs" / local.name)
        async with backend.ssh.session() as ssh:
            job_id = await service.submit_job(ssh, local_sbatch_file=local, remote_sbatch_file=remote)
    return job_id, job_name


async def _wait_terminal(backend: SlurmBackend, service: SlurmService, job_id: int) -> SlurmJob:
    """``scontrol`` still answers for a finished job; ``squeue`` drops it, which is why the
    scheduler's own poll (``ComposeJobMonitor._update_slurm_jobs``) asks both."""
    import asyncio

    deadline = asyncio.get_running_loop().time() + JOB_TIMEOUT_SECONDS
    async with backend.ssh.session() as ssh:
        while True:
            jobs = await service.get_job_status_scontrol(ssh, job_ids=[job_id])
            if jobs and jobs[0].get_job_status().is_terminal:
                return jobs[0]
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"job {job_id} not terminal after {JOB_TIMEOUT_SECONDS:.0f}s: {jobs}")
            await asyncio.sleep(POLL_SECONDS)


# --------------------------------------------------------------------------- SSH


@pytest.mark.slurm
@pytest.mark.asyncio
async def test_ssh_runs_a_command_and_says_who_it_is(slurm_backend: SlurmBackend) -> None:
    async with slurm_backend.ssh.session() as ssh:
        retcode, stdout, _ = await ssh.run_command("whoami")
    assert retcode == 0
    assert stdout.strip() == slurm_backend.ssh.username


@pytest.mark.slurm
@pytest.mark.asyncio
async def test_ssh_uploads_and_downloads_a_file(slurm_backend: SlurmBackend) -> None:
    payload = f"round trip {uuid.uuid4().hex}"
    remote = HPCFilePath(remote_path=slurm_backend.remote_base / "htclogs" / f"scp_{uuid.uuid4().hex}.txt")
    with tempfile.TemporaryDirectory() as tmpdir:
        up, down = Path(tmpdir) / "up.txt", Path(tmpdir) / "down.txt"
        up.write_text(payload)
        async with slurm_backend.ssh.session() as ssh:
            await ssh.scp_upload(local_file=up, remote_path=remote)
            await ssh.scp_download(remote_path=remote, local_file=down)
            await ssh.run_command(f"rm -f {remote}")
        assert down.read_text() == payload


# --------------------------------------------------------------------------- conformance


@pytest.mark.slurm
@pytest.mark.asyncio
async def test_sbatch_parsable_returns_a_bare_integer(slurm_backend: SlurmBackend) -> None:
    """``SlurmService.submit_job`` does ``int(stdout)`` with no parsing, so this must hold."""
    job_id, _ = await _submit(slurm_backend, SlurmService(), sleep_seconds=1)
    assert isinstance(job_id, int) and job_id > 0


@pytest.mark.slurm
@pytest.mark.asyncio
async def test_squeue_emits_the_fields_the_parser_indexes_and_a_state_it_knows(slurm_backend: SlurmBackend) -> None:
    service = SlurmService()
    job_id, job_name = await _submit(slurm_backend, service, sleep_seconds=20)
    command = f'squeue -u $USER --noheader --format="{SlurmJob.get_squeue_format_string()}" -j {job_id}'
    async with slurm_backend.ssh.session() as ssh:
        retcode, stdout, _ = await ssh.run_command(command)
    assert retcode == 0
    lines = [line for line in stdout.splitlines() if line.strip()]
    assert lines, f"squeue reported nothing for job {job_id}"
    for line in lines:
        fields = line.strip().split("|")
        assert len(fields) == SQUEUE_FIELDS, f"squeue emitted {len(fields)} fields; the parser indexes {SQUEUE_FIELDS}"
        assert fields[0].isdigit()
        parsed = SlurmJob.from_squeue_formatted_output(line)
        assert parsed.job_id == job_id and parsed.name == job_name
        assert parsed.get_job_status() is not JobStatus.UNKNOWN, f"squeue state {fields[4]!r} maps to UNKNOWN"
    # the service's own batch query agrees
    async with slurm_backend.ssh.session() as ssh:
        seen = await service.get_job_status_squeue(ssh, job_ids=[job_id])
    assert [job.job_id for job in seen] == [job_id]


@pytest.mark.slurm
@pytest.mark.asyncio
async def test_scontrol_reports_a_finished_job_as_completed_with_a_zero_exit_code(slurm_backend: SlurmBackend) -> None:
    """The poll the monitor relies on once a job has left ``squeue``."""
    service = SlurmService()
    job_id, job_name = await _submit(slurm_backend, service, sleep_seconds=1)
    final = await _wait_terminal(slurm_backend, service, job_id)
    assert final.job_id == job_id and final.name == job_name
    assert final.get_job_status() is JobStatus.COMPLETED, final
    assert (final.exit_code or "").startswith("0"), final
    async with slurm_backend.ssh.session() as ssh:
        _, out, _ = await ssh.run_command(f"cat {slurm_backend.remote_base}/htclogs/{job_name}.out")
    assert f"hello from {job_id}" in out


@pytest.mark.slurm
@pytest.mark.asyncio
async def test_an_unknown_job_id_is_not_an_error_for_the_batch_query(slurm_backend: SlurmBackend) -> None:
    """A job that has completed and left ``squeue`` makes the batch query fail; the service falls
    back to per-job queries and returns what is still there rather than raising."""
    service = SlurmService()
    live_id, _ = await _submit(slurm_backend, service, sleep_seconds=20)
    async with slurm_backend.ssh.session() as ssh:
        seen = await service.get_job_status_squeue(ssh, job_ids=[live_id, 99999999])
    assert [job.job_id for job in seen] == [live_id]
