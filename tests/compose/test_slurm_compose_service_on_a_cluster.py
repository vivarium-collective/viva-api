"""Core's SLURM compose service against a real scheduler: build a container, run a composite in it,
fetch the results (``docs/plan-core.md`` §4b, U2b-2).

Until now the SLURM compose path was exercised only by mocked sessions and, on a laptop with a key,
by hand. This is the lifecycle the service exists for, on the cluster in Docker in CI and on the
real submit host with ``--slurm-backend cluster``: the ``--fakeroot`` build sbatch, the run sbatch
with its bind clause and the application's ``ContainerRun`` hook, the zip the job leaves behind,
and its download over SSH into the results cache.

A busybox definition stands in for a science image: the same templates, the same job monitoring,
in a minute rather than an hour. What that does not prove -- that *the* simulator builds -- stays
``cluster_only``.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import string
import uuid
import zipfile
from pathlib import Path
from textwrap import dedent
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.fixtures.slurm_fixtures_backend import SlurmBackend
from viva_core.backends.models import SlurmJob
from viva_core.backends.slurm_service import SlurmService
from viva_core.compose.container_def import ContainerizationFileRepr
from viva_core.compose.hpc_paths import (
    get_compose_experiment_dir,
    get_compose_singularity_container_file,
    get_compose_singularity_def_file,
    get_compose_slurm_log_file,
)
from viva_core.compose.models import (
    ComposeHpcRun,
    ComposeJobType,
    ComposeSimulation,
    ComposeSimulationRequest,
    ComposeSimulatorVersion,
    SimulationFileType,
)
from viva_core.compose.simulation_service_hpc import ComposeSimulationServiceHpc, RunPlan
from viva_core.models import JobBackend, JobStatus

POLL_SECONDS = 2.0
BUILD_TIMEOUT_SECONDS = 600.0  # a cold busybox pull through a slow mirror
RUN_TIMEOUT_SECONDS = 180.0

# Small enough that a build is seconds, real enough that %post runs, so a container that builds
# but cannot execute still fails.
PROBE_DEF = """\
Bootstrap: docker
From: busybox:latest

%post
    echo "viva-core build probe" > /built.txt

%runscript
    cat /built.txt
"""
PROBE_MODE = "probe"  # the override command the test hook answers to
PROBE_SCRIPT = "probe.sh"


def _probe_definition() -> ComposeSimulatorVersion:
    """A unique definition per run, so two runs never race on the same remote ``.sif`` name."""
    representation = PROBE_DEF + f"\n# {uuid.uuid4().hex}\n"
    return ComposeSimulatorVersion(
        singularity_def=ContainerizationFileRepr(representation=representation),
        singularity_def_hash=hashlib.sha256(representation.encode()).hexdigest()[:16],
        packages=None,
        database_id=1,
    )


def _probe_run(
    override_command: str | None, bind_clause: str, container: Path, job_name: str, simulation: ComposeSimulation
) -> RunPlan | None:
    """An application's ``ContainerRun``: a script placed in the experiment directory and run in the
    container -- the shape ``v2ecoli_run_command`` has -- writing what the runscript prints."""
    if override_command != PROBE_MODE:
        return None
    indent = " " * 16
    command = (
        f"singularity exec \\\n"
        f"{indent}    --compat \\\n"
        f"{indent}    {bind_clause} \\\n"
        f"{indent}    {container} \\\n"
        f"{indent}    sh /experiment/{PROBE_SCRIPT}"
    )
    script = dedent(f"""\
        cat /built.txt > /experiment/output/probe.txt
        echo "run {job_name} ended at $(date -u +%FT%TZ)" >> /experiment/output/probe.txt
        """)
    return RunPlan(command=command, files={PROBE_SCRIPT: script})


def _recording_db() -> MagicMock:
    """``build_container`` records its job through the compose database; here that record is
    whatever the service asked to insert, so the test can read the job id back."""

    async def insert_hpcrun(
        slurmjobid: int,
        job_type: ComposeJobType,
        ref_id: int,
        correlation_id: str,
        backend: JobBackend | None = None,
        job_id_ext: str | None = None,
    ) -> ComposeHpcRun:
        return ComposeHpcRun(
            database_id=1,
            slurmjobid=slurmjobid,
            job_id_ext=job_id_ext,
            correlation_id=correlation_id,
            job_type=job_type,
            sim_id=None,
            simulator_id=ref_id,
            job_backend=backend.value if backend is not None else "ray",
        )

    db = MagicMock()
    db.get_hpc_db.return_value.insert_hpcrun = AsyncMock(side_effect=insert_hpcrun)
    return db


async def _wait_terminal(backend: SlurmBackend, job_id: int, timeout_seconds: float) -> SlurmJob:
    """``scontrol`` still answers for a finished job; ``squeue`` drops it -- the monitor's own poll."""
    service = SlurmService()
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    async with backend.ssh.session() as ssh:
        while True:
            jobs = await service.get_job_status_scontrol(ssh, job_ids=[job_id])
            if jobs and jobs[0].get_job_status().is_terminal:
                return jobs[0]
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"job {job_id} not terminal after {timeout_seconds:.0f}s: {jobs}")
            await asyncio.sleep(POLL_SECONDS)


async def _log_tail(backend: SlurmBackend, job_name_glob: str) -> str:
    log = get_compose_slurm_log_file(slurm_job_name=job_name_glob)
    async with backend.ssh.session() as ssh:
        _, out, _ = await ssh.run_command(f"tail -40 {log} 2>/dev/null")
    return out


@pytest.mark.slurm
@pytest.mark.asyncio
async def test_build_run_and_fetch_results_on_a_real_scheduler(slurm_backend: SlurmBackend, tmp_path: Path) -> None:
    if not slurm_backend.can_build_singularity:
        pytest.skip(f"the {slurm_backend.kind} backend cannot build singularity images")

    service = ComposeSimulationServiceHpc(
        slurm_ssh=lambda: slurm_backend.ssh, run_command=_probe_run, results_cache_dir=tmp_path / "cache"
    )
    simulator = _probe_definition()
    sif = get_compose_singularity_container_file(singularity_hash=simulator.singularity_def_hash)
    experiment_id = f"probe_{uuid.uuid4().hex[:8]}"
    experiment_dir = get_compose_experiment_dir(experiment_id=experiment_id)
    try:
        # 1. build: the --fakeroot sbatch, which also proves the backend has a subordinate id range
        random_str = "".join(random.choices(string.hexdigits, k=7))
        hpc_run = await service.build_container(simulator, random_str=random_str, db_service=_recording_db())
        assert hpc_run.job_type is ComposeJobType.BUILD_CONTAINER and hpc_run.simulator_id == simulator.database_id
        # tagged SLURM at insert, so the monitor polls it over SSH (UConn UB: an untagged row stayed RUNNING forever)
        assert hpc_run.job_backend == JobBackend.SLURM.value
        build = await _wait_terminal(slurm_backend, hpc_run.slurmjobid, BUILD_TIMEOUT_SECONDS)
        if build.get_job_status() is not JobStatus.COMPLETED:
            tail = await _log_tail(slurm_backend, f"singularity_build_{simulator.singularity_def_hash[:5]}_*")
            raise AssertionError(f"build job {hpc_run.slurmjobid} ended {build.job_state}; log tail:\n{tail}")
        async with slurm_backend.ssh.session() as ssh:
            retcode, out, _ = await ssh.run_command(f"singularity run {sif}")
        assert retcode == 0 and "viva-core build probe" in out, out

        # 2. run: the run sbatch with the hook's plan, its file uploaded beside the input
        request_file = tmp_path / "input.pbg"
        request_file.write_text("{}")
        simulation = ComposeSimulation(
            database_id=1,
            sim_request=ComposeSimulationRequest(
                request_file_path=request_file, simulation_file_type=SimulationFileType.PBG, is_batch=True
            ),
            simulator_version=simulator,
        )
        job_id = int(await service.submit_simulation_job(simulation, experiment_id, override_command=PROBE_MODE))
        run = await _wait_terminal(slurm_backend, job_id, RUN_TIMEOUT_SECONDS)
        if run.get_job_status() is not JobStatus.COMPLETED:
            tail = await _log_tail(slurm_backend, experiment_id)
            raise AssertionError(f"run job {job_id} ended {run.job_state}; log tail:\n{tail}")
        log = await _log_tail(slurm_backend, experiment_id)
        assert f"Simulation {experiment_id} running." in log and "Simulation run completed" in log, log

        # 3. results: the zip the job built, downloaded once into the cache
        archive = await service.results_archive(experiment_id)
        assert archive is not None and archive.parent == tmp_path / "cache"
        with zipfile.ZipFile(archive) as zf:
            assert zf.namelist() == ["probe.txt"]
            probe = zf.read("probe.txt").decode()
        assert probe.startswith("viva-core build probe\n") and f"run {experiment_id} ended" in probe, probe
        assert await service.results_archive(experiment_id) == archive  # cached: no second download
    finally:
        definition = get_compose_singularity_def_file(singularity_hash=simulator.singularity_def_hash)
        async with slurm_backend.ssh.session() as ssh:
            await ssh.run_command(f"rm -rf {sif} {definition} {experiment_dir}")
