"""The datasets family through the application: ``/viva/v1/datasets`` served from the ``dataset``
table with core's record model on the P4a-1 columns, beside the dated ``/api/v1/datasets`` facade
(plan P4a-2, slice 3).

Through the ASGI app against the testcontainer Postgres (``database_service``). Nothing here
reaches S3 or a hosted database.
"""

import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from tests.simulation.test_event_ingest import _insert_run
from viva_api.common.models import JobId
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import JobType
from viva_api.simulation.tables_orm import AnalysisStatusDB


async def _client() -> AsyncClient:
    from viva_api.api.main import app

    return AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver")


def _uri(name: str) -> str:
    return f"s3://work/vecoli-output/exp/analyses/a-{uuid.uuid4().hex[:6]}/ptools/{name}.tsv"


@pytest.mark.asyncio
async def test_the_family_is_served_from_the_table_with_core_record_and_the_facade_still_answers(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation, run = await _insert_run(database_service, correlation_id=f"sim-{uuid.uuid4().hex[:8]}")
    record = await database_service.record_analysis(
        experiment_id=simulation.experiment_id,
        n_tp=None,
        status=AnalysisStatusDB.READY,
        config={"analysis_options": {"experiment_id": [simulation.experiment_id]}},
        name=f"analysis-{uuid.uuid4().hex[:8]}",
        simulation_id=simulation.database_id,
        backend="walk",
    )
    analysis_run = await database_service.insert_hpcrun(
        job_id=JobId.k8s(f"ana-{record.database_id}"),
        job_type=JobType.ANALYSIS,
        ref_id=record.database_id,
        correlation_id=f"analysis-{record.database_id}",
    )
    walked = _uri("walked")
    traced = _uri("traced")
    await database_service.upsert_dataset(
        uri=walked, kind="ptools-analysis", origin="walk", analysis_id=record.database_id, tags=["cd2"]
    )
    await database_service.upsert_dataset(
        uri=traced,
        kind="ptools-analysis",
        origin="event",
        simulation_id=simulation.database_id,
        producer_job_id=run.database_id,
        trace_id=run.trace_id,
        attributes={"seed": 3},
    )

    async with await _client() as client:
        # Core's family: the owner-ref off the P4a-1 columns, the producing job and trace, core's ``id``.
        answer = await client.get("/viva/v1/datasets", params={"owner": f"analysis:{record.database_id}"})
        assert answer.status_code == 200, answer.text
        by_analysis = answer.json()
        assert by_analysis["total"] == 1
        (row,) = by_analysis["datasets"]
        assert (row["owner_kind"], row["owner_id"]) == ("analysis", str(record.database_id))
        assert row["uri"] == walked and row["tags"] == ["cd2"] and "simulation_id" not in row
        assert row["producer_job_id"] is None and row["trace_id"] is None  # a walk knows no job

        by_simulation = (
            await client.get(
                "/viva/v1/datasets", params={"owner": f"simulation:{simulation.database_id}", "attr": "seed=3"}
            )
        ).json()
        assert by_simulation["total"] == 1
        (row,) = by_simulation["datasets"]
        assert (row["owner_kind"], row["owner_id"]) == ("simulation", str(simulation.database_id))
        assert (row["producer_job_id"], row["trace_id"]) == (run.database_id, run.trace_id)
        one = (await client.get(f"/viva/v1/datasets/{row['id']}")).json()
        assert one["id"] == row["id"] and one["uri"] == traced

        # The facade: SMS shapes and producer ids unchanged, and now the four columns beside them.
        facade = (await client.get("/api/v1/datasets", params={"analysis_id": record.database_id})).json()
        assert facade["total"] == 1
        (dto,) = facade["datasets"]
        assert dto["database_id"] == by_analysis["datasets"][0]["id"] and dto["analysis_id"] == record.database_id
        assert (dto["owner_kind"], dto["owner_id"]) == ("analysis", str(record.database_id))
        provenance = (await client.get(f"/api/v1/datasets/{dto['database_id']}/provenance")).json()
        assert (
            provenance["producer"]["kind"] == "analysis"
            and provenance["producer"]["hpcrun_id"] == analysis_run.database_id
        )
        assert (await client.get(f"/viva/v1/datasets/{dto['database_id']}/provenance")).status_code == 404  # stays SMS

        # Advertised the same way on both capability routes, once the database exists.
        assert "viva-v1-datasets" in (await client.get("/viva/v1/capabilities")).json()["capabilities"]
        assert "viva-v1-datasets" in (await client.get("/core/v1/capabilities")).json()["capabilities"]
        assert (await client.get("/viva/v1/health")).json()["services"]["datasets"] is True
