"""DatabaseService tests for the generalized `analysis` table (analysis-result flow).

Run against the testcontainer Postgres (the enum + new columns are created by
create_all from the ORM).
"""

from typing import TYPE_CHECKING, Any

import pytest

from viva_api.common.models import JobStatus
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.tables_orm import AnalysisStatusDB

if TYPE_CHECKING:
    from viva_api.simulation.models import SimulationRequest


def _config(experiment_id: str, n_tp: int) -> dict[str, Any]:
    return {
        "analysis_options": {
            "experiment_id": [experiment_id],
            "multiseed": {"ptools_rna": {"n_tp": n_tp}},
        }
    }


@pytest.mark.asyncio
async def test_record_and_get_by_experiment_ntp(database_service: DatabaseServiceSQL) -> None:
    rec = await database_service.record_analysis(
        experiment_id="exp-a",
        n_tp=10,
        status=AnalysisStatusDB.COMPUTING,
        config=_config("exp-a", 10),
        name="analysis-exp-a-ntp10-abcd",
        job_name="ana-exp-a-ntp10",
        job_id_ext="ana-exp-a-ntp10",
        result_uri="s3://bucket/vecoli-output/exp-a/exp-a/analyses/analysis-exp-a-ntp10-abcd",
    )
    assert rec.n_tp == 10
    assert rec.experiment_id == "exp-a"
    assert rec.status == JobStatus.RUNNING  # COMPUTING -> RUNNING in the DTO

    fetched = await database_service.get_analysis_by_experiment_ntp("exp-a", 10)
    assert fetched is not None
    assert fetched.database_id == rec.database_id
    assert fetched.result_uri and fetched.result_uri.endswith("analysis-exp-a-ntp10-abcd")


@pytest.mark.asyncio
async def test_record_twice_updates_in_place(database_service: DatabaseServiceSQL) -> None:
    r1 = await database_service.record_analysis(
        experiment_id="exp-b", n_tp=50, status=AnalysisStatusDB.COMPUTING, config=_config("exp-b", 50), name="n1"
    )
    r2 = await database_service.record_analysis(
        experiment_id="exp-b",
        n_tp=50,
        status=AnalysisStatusDB.READY,
        config=_config("exp-b", 50),
        name="n1",
        result_uri="s3://bucket/ready",
    )
    assert r2.database_id == r1.database_id  # same row, updated
    assert r2.status == JobStatus.COMPLETED
    rows = await database_service.list_analyses(experiment_id="exp-b")
    assert len([r for r in rows if r.n_tp == 50]) == 1


@pytest.mark.asyncio
async def test_list_analyses_filters(database_service: DatabaseServiceSQL) -> None:
    await database_service.record_analysis(
        experiment_id="exp-c",
        n_tp=10,
        status=AnalysisStatusDB.READY,
        config=_config("exp-c", 10),
        name="c10",
        simulation_id=None,
    )
    await database_service.record_analysis(
        experiment_id="exp-c", n_tp=100, status=AnalysisStatusDB.READY, config=_config("exp-c", 100), name="c100"
    )
    await database_service.record_analysis(
        experiment_id="exp-d", n_tp=10, status=AnalysisStatusDB.READY, config=_config("exp-d", 10), name="d10"
    )
    by_exp = await database_service.list_analyses(experiment_id="exp-c")
    assert {r.n_tp for r in by_exp} == {10, 100}
    assert all(r.experiment_id == "exp-c" for r in by_exp)


@pytest.mark.asyncio
async def test_update_analysis_status(database_service: DatabaseServiceSQL) -> None:
    rec = await database_service.record_analysis(
        experiment_id="exp-e", n_tp=10, status=AnalysisStatusDB.COMPUTING, config=_config("exp-e", 10), name="e10"
    )
    updated = await database_service.update_analysis_status(
        rec.database_id, AnalysisStatusDB.READY, result_uri="s3://bucket/e10"
    )
    assert updated.status == JobStatus.COMPLETED
    assert updated.result_uri == "s3://bucket/e10"


@pytest.mark.asyncio
async def test_get_missing_returns_none(database_service: DatabaseServiceSQL) -> None:
    assert await database_service.get_analysis_by_experiment_ntp("nope", 10) is None


@pytest.mark.asyncio
async def test_list_active_analyses_only_returns_computing_ray_rows_with_a_simulation(
    experiment_request: "SimulationRequest", database_service: DatabaseServiceSQL
) -> None:
    """The OOM-retry-escalation poller (backlog item 38 track B,
    JobScheduler.update_analysis_retries) must only ever see rows it can
    actually poll a live AWS Batch job for and resubmit (job_id_ext +
    simulation_id both set) and that are still in flight (COMPUTING) --
    never a READY/FAILED row, and never a legacy SLURM row (no job_id_ext)."""
    simulation = await database_service.insert_simulation(sim_request=experiment_request)

    computing = await database_service.record_analysis(
        experiment_id="exp-active",
        n_tp=None,
        status=AnalysisStatusDB.COMPUTING,
        config={"analysis_options": {"experiment_id": ["exp-active"]}, "n_seeds": 2, "analysis_name": "a1"},
        name="a1",
        simulation_id=simulation.database_id,
        backend="ray",
        job_id_ext="batch-job-active",
    )
    await database_service.record_analysis(
        experiment_id="exp-ready",
        n_tp=None,
        status=AnalysisStatusDB.READY,
        config=_config("exp-ready", 1),
        name="a2",
        simulation_id=simulation.database_id,
        backend="ray",
        job_id_ext="batch-job-ready",
    )
    await database_service.record_analysis(
        experiment_id="exp-legacy-slurm",
        n_tp=None,
        status=AnalysisStatusDB.COMPUTING,
        config=_config("exp-legacy-slurm", 1),
        name="a3",
        simulation_id=simulation.database_id,
        backend="slurm",
        job_id_ext=None,
    )

    active = await database_service.list_active_analyses()
    assert [a.database_id for a in active] == [computing.database_id]
    assert active[0].job_id_ext == "batch-job-active"
    assert active[0].simulation_id == simulation.database_id
    assert active[0].attempt == 1
    assert active[0].config["n_seeds"] == 2


@pytest.mark.asyncio
async def test_update_analysis_job_id_bumps_attempt_and_swaps_the_physical_job(
    experiment_request: "SimulationRequest", database_service: DatabaseServiceSQL
) -> None:
    """Same logical row, new physical job id per retry attempt -- mirrors
    vEcoli-private's own Nextflow trace (one logical task, incrementing
    attempt, new native job id each retry)."""
    simulation = await database_service.insert_simulation(sim_request=experiment_request)
    rec = await database_service.record_analysis(
        experiment_id="exp-retry",
        n_tp=None,
        status=AnalysisStatusDB.COMPUTING,
        config=_config("exp-retry", 1),
        name="retry-me",
        simulation_id=simulation.database_id,
        backend="ray",
        job_id_ext="batch-job-attempt1",
    )

    await database_service.update_analysis_job_id(rec.database_id, job_id_ext="batch-job-attempt2", attempt=2)

    active = await database_service.list_active_analyses()
    (refetched,) = [a for a in active if a.database_id == rec.database_id]
    assert refetched.job_id_ext == "batch-job-attempt2"
    assert refetched.attempt == 2


@pytest.mark.asyncio
async def test_update_analysis_job_id_raises_for_missing_row(database_service: DatabaseServiceSQL) -> None:
    with pytest.raises(RuntimeError, match="Analysis 999999 not found"):
        await database_service.update_analysis_job_id(999999, job_id_ext="x", attempt=2)
