"""DatabaseService-level tests for the simulation tags column (tags-as-data).

Run against the testcontainer Postgres (the tags filter uses JSONB containment,
which SQLite cannot emulate).
"""

import pytest

from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import SimulationRequest


async def _insert(db: DatabaseServiceSQL, req: SimulationRequest, experiment_id: str, tags: list[str]) -> int:
    req.experiment_id = experiment_id
    req.config.experiment_id = experiment_id
    req.tags = tags
    sim = await db.insert_simulation(req)
    return sim.database_id


@pytest.mark.asyncio
async def test_tags_persist_through_insert_and_get(
    database_service: DatabaseServiceSQL, experiment_request: SimulationRequest
) -> None:
    sim_id = await _insert(database_service, experiment_request, "tagdb-persist", ["cd1", "smoke"])
    fetched = await database_service.get_simulation(sim_id)
    assert fetched is not None
    assert sorted(fetched.tags) == ["cd1", "smoke"]


@pytest.mark.asyncio
async def test_add_tags_union_merges_without_duplicates(
    database_service: DatabaseServiceSQL, experiment_request: SimulationRequest
) -> None:
    sim_id = await _insert(database_service, experiment_request, "tagdb-add", ["a"])
    updated = await database_service.add_tags(sim_id, ["a", "b"])  # 'a' already present
    assert updated.tags == ["a", "b"]  # order preserved, no duplicate


@pytest.mark.asyncio
async def test_list_distinct_tags_maps_tag_to_experiment_ids(
    database_service: DatabaseServiceSQL, experiment_request: SimulationRequest
) -> None:
    await _insert(database_service, experiment_request, "tagdb-d1", ["dbench"])
    await _insert(database_service, experiment_request, "tagdb-d2", ["dbench"])
    tag_map = await database_service.list_distinct_tags()
    assert set(tag_map["dbench"]) == {"tagdb-d1", "tagdb-d2"}


@pytest.mark.asyncio
async def test_filter_union_of_experiment_ids_and_tags(
    database_service: DatabaseServiceSQL, experiment_request: SimulationRequest
) -> None:
    await _insert(database_service, experiment_request, "tagdb-u1", ["ubundle"])
    await _insert(database_service, experiment_request, "tagdb-u2", ["ubundle"])
    await _insert(database_service, experiment_request, "tagdb-u3", ["untagged-here"])

    # experiment_id OR tag => union (u3 by id, u1/u2 by tag)
    result = await database_service.list_simulations_filtered(experiment_ids=["tagdb-u3"], tags=["ubundle"])
    assert {s.experiment_id for s in result} == {"tagdb-u1", "tagdb-u2", "tagdb-u3"}


@pytest.mark.asyncio
async def test_filter_empty_args_returns_empty(
    database_service: DatabaseServiceSQL, experiment_request: SimulationRequest
) -> None:
    assert await database_service.list_simulations_filtered(experiment_ids=[], tags=[]) == []
    assert await database_service.list_simulations_filtered() == []
