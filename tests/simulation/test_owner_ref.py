"""The owner-ref (plan P4a): one ``(owner_kind, owner_id)`` pair, dual-written beside the foreign keys."""

import pytest

from viva_api.simulation.models import JobType
from viva_api.simulation.owner_ref import OWNER_KIND_BY_JOB_TYPE, dataset_owner, run_owner


def test_every_job_type_has_an_owning_table() -> None:
    assert set(OWNER_KIND_BY_JOB_TYPE) == set(JobType)


def test_a_runs_owner_is_the_owning_table_and_the_id_as_text() -> None:
    assert run_owner(JobType.SIMULATION, 1002) == ("simulation", "1002")
    assert run_owner(JobType.BUILD_IMAGE, 216) == ("simulator", "216")


def test_a_datasets_owner_is_the_first_producer_named() -> None:
    assert dataset_owner(simulation_id=5, parca_dataset_id=9, analysis_id=None) == ("simulation", "5")
    assert dataset_owner(simulation_id=None, parca_dataset_id=9, analysis_id=3) == ("parca_dataset", "9")
    assert dataset_owner(simulation_id=None, parca_dataset_id=None, analysis_id=None) is None


@pytest.mark.asyncio
async def test_a_new_run_and_a_new_dataset_carry_the_owner_ref(database_service) -> None:  # type: ignore[no-untyped-def]
    """The dual-write: a row written by this build has both the foreign key and the owner-ref."""
    from sqlalchemy import select

    from viva_api.common.models import JobId
    from viva_api.simulation.tables_orm import AnalysisStatusDB, ORMDataset, ORMHpcRun

    simulator = await database_service.insert_simulator(
        git_commit_hash="abc1234", git_repo_url="https://x/y", git_branch="main"
    )
    run = await database_service.insert_hpcrun(
        JobId.ray("j-1"), JobType.BUILD_IMAGE, simulator.database_id, correlation_id="c-1"
    )
    async with database_service.async_sessionmaker() as session:
        row = (await session.execute(select(ORMHpcRun).where(ORMHpcRun.id == run.database_id))).scalar_one()
    expected = (simulator.database_id, "simulator", str(simulator.database_id))
    assert (row.jobref_simulator_id, row.owner_kind, row.owner_id) == expected

    analysis = await database_service.record_analysis(
        experiment_id="exp-1",
        n_tp=None,
        status=AnalysisStatusDB.READY,
        config={"analysis_options": {"experiment_id": ["exp-1"]}},
        name="a-1",
    )
    await database_service.upsert_dataset(
        uri="s3://b/one", kind="analysis", origin="walk", analysis_id=analysis.database_id
    )
    async with database_service.async_sessionmaker() as session:
        ds = (await session.execute(select(ORMDataset).where(ORMDataset.uri == "s3://b/one"))).scalar_one()
    assert (ds.analysis_id, ds.owner_kind, ds.owner_id) == (analysis.database_id, "analysis", str(analysis.database_id))
