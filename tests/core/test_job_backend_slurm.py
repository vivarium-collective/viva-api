"""Core's ``JobBackend`` Protocol, SLURM implementation, against a real scheduler (U2g): the Docker
cluster in CI, Mantis with ``--slurm-backend cluster``. The same four operations the Batch
implementation answers in ``test_job_backend_batch.py``; here they are proven end to end -- a job
in a container, a bare one, one cancelled, one the scheduler never knew.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

import pytest

from tests.fixtures.slurm_fixtures_backend import SlurmBackend
from viva_core.backends.base import BackendStatus, JobHandle, JobSpec, Resources
from viva_core.backends.slurm_backend import SlurmJobBackend
from viva_core.models import JobStatus

POLL_SECONDS = 2.0
JOB_TIMEOUT_SECONDS = 240.0
PROBE_IMAGE = "docker://busybox:latest"  # pulled by the compute node at run time; small


def _backend(cluster: SlurmBackend) -> SlurmJobBackend:
    return SlurmJobBackend(
        lambda: cluster.ssh,
        partition=cluster.partition,
        work_dir=cluster.remote_base / "jobs",
        qos=cluster.qos,
        node_list=cluster.node_list,
    )


async def _wait_terminal(backend: SlurmJobBackend, handle: JobHandle) -> BackendStatus:
    deadline = asyncio.get_running_loop().time() + JOB_TIMEOUT_SECONDS
    last: BackendStatus | None = None
    while asyncio.get_running_loop().time() < deadline:
        last = (await backend.status([handle])).get(handle.id)
        if last is not None and last.status.is_terminal:
            return last
        await asyncio.sleep(POLL_SECONDS)
    raise AssertionError(f"{handle} not terminal after {JOB_TIMEOUT_SECONDS:.0f}s; last {last}")


# --------------------------------------------------------------------------- pure


def test_the_script_carries_the_spec() -> None:
    backend = SlurmJobBackend(
        lambda: None,  # type: ignore[arg-type,return-value]  # never called: render is pure
        partition="cpu",
        work_dir=Path("/data/jobs"),
        qos="normal",
        node_list="c1",
    )
    earlier = JobHandle(backend="slurm", id="41", name="earlier")
    script = backend.render(
        JobSpec(
            name="probe",
            command="echo $GREETING; exit 0",
            image="/images/probe.sif",
            env={"GREETING": "hi there"},
            resources=Resources(cpus=2, memory_mb=2048, time_minutes=5),
            depends_on=(earlier,),
        )
    )
    for line in (
        "#SBATCH --job-name=probe",
        "#SBATCH --output=/data/jobs/logs/%x-%j.out",
        "#SBATCH --partition=cpu",
        "#SBATCH --qos=normal",
        "#SBATCH --nodelist=c1",
        "#SBATCH --dependency=afterok:41",
        "#SBATCH --cpus-per-task=2",
        "#SBATCH --mem=2048M",
        "#SBATCH --time=5",
        "export GREETING='hi there'",
        "singularity exec /images/probe.sif sh -c 'echo $GREETING; exit 0'",
    ):
        assert line in script, f"{line!r} missing from:\n{script}"
    assert "--dependency" not in backend.render(JobSpec(name="lone", command="true"))
    with pytest.raises(ValueError, match="only wait on SLURM jobs"):
        backend.render(JobSpec(name="x", command="true", depends_on=(JobHandle(backend="batch-container", id="1"),)))


# --------------------------------------------------------------------------- live


@pytest.mark.slurm
@pytest.mark.asyncio
async def test_a_job_runs_in_its_image_and_its_output_is_readable(slurm_backend: SlurmBackend) -> None:
    backend = _backend(slurm_backend)
    mark = uuid.uuid4().hex[:8]
    handle = await backend.submit(
        JobSpec(
            name=f"core_probe_{mark}",
            command="echo hello-$MARK from $(cat /etc/hostname 2>/dev/null || hostname)",
            image=PROBE_IMAGE,
            env={"MARK": mark},
        )
    )
    assert handle.backend == "slurm" and handle.id.isdigit() and handle.name == f"core_probe_{mark}"
    final = await _wait_terminal(backend, handle)
    lines = await backend.logs(handle)
    assert final.status is JobStatus.COMPLETED, (final, lines)
    assert final.exit_code == 0 and final.start_time and final.end_time, final
    assert any(f"hello-{mark}" in line for line in lines), lines
    assert await backend.logs(handle, tail=1) == lines[-1:]


@pytest.mark.slurm
@pytest.mark.asyncio
async def test_a_bare_command_runs_on_the_node_and_its_exit_code_is_reported(slurm_backend: SlurmBackend) -> None:
    backend = _backend(slurm_backend)
    handle = await backend.submit(JobSpec(name=f"core_bare_{uuid.uuid4().hex[:8]}", command="echo bare; exit 3"))
    final = await _wait_terminal(backend, handle)
    assert final.status is JobStatus.FAILED and final.exit_code == 3, final
    assert (await backend.logs(handle)) == ["bare"]


@pytest.mark.slurm
@pytest.mark.asyncio
async def test_cancel_stops_a_job_and_the_scheduler_says_cancelled(slurm_backend: SlurmBackend) -> None:
    backend = _backend(slurm_backend)
    handle = await backend.submit(JobSpec(name=f"core_sleep_{uuid.uuid4().hex[:8]}", command="sleep 300"))
    # let the scheduler see it before cancelling; scancel on a not-yet-visible id is also fine
    for _ in range(30):
        if handle.id in await backend.status([handle]):
            break
        await asyncio.sleep(1)
    await backend.cancel(handle)
    final = await _wait_terminal(backend, handle)
    assert final.status is JobStatus.CANCELLED, final


@pytest.mark.slurm
@pytest.mark.asyncio
async def test_a_handle_the_scheduler_never_knew_is_absent_not_an_error(slurm_backend: SlurmBackend) -> None:
    backend = _backend(slurm_backend)
    unknown = JobHandle(backend="slurm", id="99999999", name="ghost")
    assert await backend.status([unknown]) == {}
    assert await backend.status([]) == {}
    assert await backend.logs(unknown) == []
