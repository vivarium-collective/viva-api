"""scripts/walk_datasets.py: the walk run by hand, and its dry run reporting what --apply would do.

The dry run is the point of the script: it wraps the real database service and intercepts the
two writes the walk makes, so what it reports is the walk's own behaviour rather than a
re-implementation of it. These run the real `reconcile_simulation` against the testcontainer
Postgres and an in-memory S3, never a hosted database or a real bucket.
"""

from __future__ import annotations

import importlib.util
import sys
import uuid
from pathlib import Path
from typing import Any, cast

import pytest

from tests.simulation.test_dataset_walk import REAL_HEADER, REAL_ROW
from tests.simulation.test_event_ingest import _S3
from viva_api.simulation.database_service import DatabaseService, DatabaseServiceSQL
from viva_api.simulation.dataset_walk import reconcile_simulation
from viva_api.simulation.models import (
    ParcaDatasetRequest,
    ParcaOptions,
    Simulation,
    SimulationConfig,
    SimulationRequest,
)

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "walk_datasets.py"
_spec = importlib.util.spec_from_file_location("walk_datasets", _SCRIPT)
assert _spec is not None and _spec.loader is not None
walker: Any = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = walker
_spec.loader.exec_module(walker)

BUCKET = "walk-bucket"


async def _simulation(db: DatabaseServiceSQL, experiment_id: str) -> Simulation:
    simulator = await db.insert_simulator(
        git_commit_hash=uuid.uuid4().hex, git_repo_url="https://github.com/CovertLabEcoli/sms-ecoli", git_branch="main"
    )
    parca = await db.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    config = SimulationConfig(  # type: ignore[call-arg]
        experiment_id=experiment_id, emitter_arg={"out_uri": f"s3://{BUCKET}/vecoli-output/{experiment_id}"}
    )
    return await db.insert_simulation(
        sim_request=SimulationRequest(
            simulation_config_filename="walk.json",
            experiment_id=experiment_id,
            parca_dataset_id=parca.database_id,
            simulator_id=simulator.database_id,
            config=config,
        )
    )


def _objects(experiment_id: str) -> dict[str, bytes]:
    """One bundle: two ptools TSVs, a figure, and a log the walk must ignore."""
    root = f"vecoli-output/{experiment_id}/analyses/analysis-multiseed"
    tsv = (REAL_HEADER + REAL_ROW).encode()
    return {
        f"{root}/ptools/ptools_rna_multiseed__variant=0.tsv": tsv,
        f"{root}/ptools/ptools_rxns_multiseed__variant=0.tsv": tsv,
        f"{root}/viz/mass_fraction__variant=0.html": b"<html></html>",
        f"{root}/driver.log": b"log",
    }


def _dry_run(db: DatabaseServiceSQL) -> tuple[Any, DatabaseService]:
    proxy = walker._DryRunDatabase(db)
    return proxy, cast("DatabaseService", proxy)


def test_arguments_default_to_analyze_and_the_modes_exclude_each_other() -> None:
    args = walker.parse_args([])
    assert not args.apply and args.limit == 25 and args.after == 0 and args.simulation == []
    args = walker.parse_args(["--simulation", "7", "--simulation", "9", "--apply", "-v"])
    assert args.apply and args.simulation == [7, 9] and args.verbose
    with pytest.raises(SystemExit):
        walker.parse_args(["--analyze", "--apply"])


@pytest.mark.asyncio
async def test_the_dry_run_reports_what_would_be_registered_and_writes_nothing(
    database_service: DatabaseServiceSQL,
) -> None:
    experiment_id = f"walk-{uuid.uuid4().hex[:8]}"
    simulation = await _simulation(database_service, experiment_id)
    s3 = _S3(_objects(experiment_id))
    proxy, db = _dry_run(database_service)

    result = await reconcile_simulation(simulation, db=db, file_service=s3, storage_bucket=BUCKET)

    # The bundle nobody claims is attributed to the simulation, and the log is not a dataset.
    assert (result.bundles, result.unclaimed, result.registered) == (1, 1, 3)
    assert len(proxy.would_register) == 3 and not proxy.would_update and not proxy.would_flip
    assert all(uri.startswith(f"s3://{BUCKET}/vecoli-output/{experiment_id}/analyses/") for uri in proxy.would_register)
    assert not any(uri.endswith("driver.log") for uri in proxy.would_register)
    assert await database_service.list_datasets(uri_prefix=f"s3://{BUCKET}/", available=None) == []


@pytest.mark.asyncio
async def test_after_a_real_walk_the_dry_run_reports_nothing_left_to_do(
    database_service: DatabaseServiceSQL,
) -> None:
    experiment_id = f"walk-{uuid.uuid4().hex[:8]}"
    simulation = await _simulation(database_service, experiment_id)
    objects = _objects(experiment_id)
    s3 = _S3(objects)

    applied = await reconcile_simulation(simulation, db=database_service, file_service=s3, storage_bucket=BUCKET)
    assert applied.registered == 3

    proxy, db = _dry_run(database_service)
    again = await reconcile_simulation(simulation, db=db, file_service=s3, storage_bucket=BUCKET)
    assert (again.registered, again.unchanged) == (0, 3)
    assert not proxy.would_register and not proxy.would_update and not proxy.would_flip


@pytest.mark.asyncio
async def test_a_vanished_object_is_reported_as_a_flip_but_not_written(
    database_service: DatabaseServiceSQL,
) -> None:
    experiment_id = f"walk-{uuid.uuid4().hex[:8]}"
    simulation = await _simulation(database_service, experiment_id)
    objects = _objects(experiment_id)
    await reconcile_simulation(simulation, db=database_service, file_service=_S3(objects), storage_bucket=BUCKET)

    gone = f"vecoli-output/{experiment_id}/analyses/analysis-multiseed/viz/mass_fraction__variant=0.html"
    remaining = {key: value for key, value in objects.items() if key != gone}
    proxy, db = _dry_run(database_service)
    result = await reconcile_simulation(simulation, db=db, file_service=_S3(remaining), storage_bucket=BUCKET)

    assert result.unavailable == 1
    assert len(proxy.would_flip) == 1 and "available=false" in proxy.would_flip[0]
    rows = await database_service.list_datasets(uri_prefix=f"s3://{BUCKET}/", available=None)
    assert {row.available for row in rows} == {True}, "the dry run must not flip the row"


@pytest.mark.asyncio
async def test_simulations_are_selected_by_id_or_by_the_ticks_cursor(database_service: DatabaseServiceSQL) -> None:
    first = await _simulation(database_service, f"walk-{uuid.uuid4().hex[:8]}")
    second = await _simulation(database_service, f"walk-{uuid.uuid4().hex[:8]}")

    named = await walker._simulations(database_service, walker.parse_args(["--simulation", str(second.database_id)]))
    assert [sim.database_id for sim in named] == [second.database_id]

    after = await walker._simulations(
        database_service, walker.parse_args(["--after", str(first.database_id - 1), "--limit", "2"])
    )
    assert first.database_id in [sim.database_id for sim in after]

    missing = await walker._simulations(database_service, walker.parse_args(["--simulation", "999999999"]))
    assert missing == []
