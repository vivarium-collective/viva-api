"""A compose run's ``job_backend`` decides which poller owns it -- found at UConn checkpoint UB
(2026-09-26): the SLURM container build inserted its row untagged, the column's default is
``ray``, and the monitor never polled it over SSH, so the build (which had already FAILED on the
cluster) stayed RUNNING forever and the run waiting on it with it.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from viva_core.compose.container_def import ContainerizationFileRepr
from viva_core.compose.database_service import ComposeDatabaseService
from viva_core.compose.job_monitor import ComposeJobMonitor
from viva_core.compose.models import ComposeHpcRun, ComposeJobStatus, ComposeJobType
from viva_core.compose.tables_orm import create_compose_db
from viva_core.models import JobBackend


@pytest_asyncio.fixture
async def compose_db(postgres_url: str) -> AsyncGenerator[ComposeDatabaseService]:
    engine = create_async_engine(postgres_url)
    await create_compose_db(engine)
    try:
        yield ComposeDatabaseService(async_sessionmaker(engine, expire_on_commit=False))
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_a_build_inserted_as_slurm_is_tagged_slurm_and_a_placeholder_keeps_the_default(
    compose_db: ComposeDatabaseService,
) -> None:
    hpc_db = compose_db.get_hpc_db()
    simulator = await compose_db.get_simulator_db().insert_simulator(
        ContainerizationFileRepr(representation="Bootstrap: docker\nFrom: busybox\n")
    )
    build = await hpc_db.insert_hpcrun(
        slurmjobid=3238993,
        job_type=ComposeJobType.BUILD_CONTAINER,
        ref_id=simulator.database_id,
        correlation_id="b",
        backend=JobBackend.SLURM,
    )
    untagged = await hpc_db.insert_hpcrun(
        slurmjobid=-1, job_type=ComposeJobType.BUILD_CONTAINER, ref_id=simulator.database_id, correlation_id="u"
    )
    assert build.job_backend == "slurm"
    assert untagged.job_backend == "ray"  # the column's default -- what every SLURM build row carried before this
    running = {run.database_id: run for run in await hpc_db.list_running_hpcruns()}
    assert running[build.database_id].job_backend == "slurm"


def _run(database_id: int, slurmjobid: int, backend: str) -> ComposeHpcRun:
    return ComposeHpcRun(
        database_id=database_id,
        slurmjobid=slurmjobid,
        job_backend=backend,
        correlation_id=f"c{database_id}",
        job_type=ComposeJobType.BUILD_CONTAINER if slurmjobid > 0 else ComposeJobType.SIMULATION,
        sim_id=None,
        simulator_id=None,
    )


@pytest.mark.asyncio
async def test_the_monitor_polls_slurm_tagged_rows_over_ssh_and_leaves_placeholders_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = MagicMock()
    tagged, placeholder = _run(1, 3238993, "slurm"), _run(2, -1, "ray")
    db.get_hpc_db.return_value.list_running_hpcruns = AsyncMock(return_value=[tagged, placeholder])
    monitor = ComposeJobMonitor(nats_client=None, database_service=db, sim_registry={}, slurm_ssh=None)
    seen: dict[str, list[int]] = {}

    async def slurm(runs: list[ComposeHpcRun]) -> None:
        seen["slurm"] = [r.database_id for r in runs]

    async def backend(runs: list[ComposeHpcRun]) -> None:
        seen["backend"] = [r.database_id for r in runs]

    monkeypatch.setattr(monitor, "_update_slurm_jobs", slurm)
    monkeypatch.setattr(monitor, "_update_backend_jobs", backend)
    await monitor.update_running_jobs()
    assert seen == {"slurm": [1], "backend": [2]}


@pytest.mark.asyncio
async def test_a_placeholder_tagged_slurm_is_not_asked_of_squeue() -> None:
    """A -1 job id is "not dispatched yet", never a job to query."""
    db = MagicMock()
    ssh = MagicMock()
    monitor = ComposeJobMonitor(nats_client=None, database_service=db, sim_registry={}, slurm_ssh=lambda: ssh)
    await monitor._update_slurm_jobs([_run(3, -1, "slurm")])
    ssh.session.assert_not_called()


@pytest.mark.asyncio
async def test_a_failed_build_does_not_suppress_the_next_one(compose_db: ComposeDatabaseService) -> None:
    """#717, seen again at UConn: the dispatch asks "is there a build for this simulator?" and a FAILED
    one used to answer yes, so no run ever built again and every run died for lack of an image."""
    hpc_db = compose_db.get_hpc_db()
    simulator = await compose_db.get_simulator_db().insert_simulator(
        ContainerizationFileRepr(representation="Bootstrap: docker\nFrom: busybox\n# retry\n")
    )
    failed = await hpc_db.insert_hpcrun(
        slurmjobid=1, job_type=ComposeJobType.BUILD_CONTAINER, ref_id=simulator.database_id, correlation_id="f"
    )
    assert await hpc_db.get_hpcrun_id_by_simulator_id(simulator.database_id) == failed.database_id
    await hpc_db.mark_hpcrun_failed(failed.database_id, "no mapping entry found in /etc/subuid")
    assert await hpc_db.get_hpcrun_id_by_simulator_id(simulator.database_id) is None  # build again
    again = await hpc_db.insert_hpcrun(
        slurmjobid=2, job_type=ComposeJobType.BUILD_CONTAINER, ref_id=simulator.database_id, correlation_id="a"
    )
    assert await hpc_db.get_hpcrun_id_by_simulator_id(simulator.database_id) == again.database_id


@pytest.mark.asyncio
async def test_a_kubernetes_jobs_tz_aware_times_are_stored_as_naive_utc(compose_db: ComposeDatabaseService) -> None:
    """The first completed build Job at UConn could not be recorded: its times carry +00:00 and the
    column is TIMESTAMP WITHOUT TIME ZONE, which asyncpg refuses a tz-aware value for."""
    hpc_db = compose_db.get_hpc_db()
    simulator = await compose_db.get_simulator_db().insert_simulator(
        ContainerizationFileRepr(representation="Bootstrap: docker\nFrom: busybox\n# tz\n")
    )
    build = await hpc_db.insert_hpcrun(
        slurmjobid=-1,
        job_type=ComposeJobType.BUILD_CONTAINER,
        ref_id=simulator.database_id,
        correlation_id="k",
        backend=JobBackend.K8S,
        job_id_ext="singularity-build-abc12-00000",
    )
    await hpc_db.update_hpcrun_result(
        build.database_id,
        ComposeJobStatus.COMPLETED,
        start_time="2026-09-26T14:23:28+00:00",
        end_time="2026-09-26T16:24:26+02:00",  # any offset lands as UTC
    )
    stored = await hpc_db.get_hpcrun(build.database_id)
    assert stored is not None and stored.status is ComposeJobStatus.COMPLETED
    assert stored.start_time == "2026-09-26 14:23:28" and stored.end_time == "2026-09-26 14:24:26"
