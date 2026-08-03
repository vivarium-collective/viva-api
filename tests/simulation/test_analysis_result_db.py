"""DatabaseService tests for the generalized `analysis` table (analysis-result flow).

Run against the testcontainer Postgres (the enum + new columns are created by
create_all from the ORM).
"""

from typing import Any

import pytest

from viva_api.common.models import JobStatus
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.tables_orm import AnalysisStatusDB


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
