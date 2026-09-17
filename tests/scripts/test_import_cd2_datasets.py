"""scripts/import_cd2_datasets.py: CD2 fill bundles are registered and tagged, never given run rows (plan §8).

The importer's functions run against the testcontainer Postgres and an in-memory S3, fed a
small manifest slice shaped like the real ``cd2_ptools_manifest.json``
(tests/fixtures/datasets/cd2_manifest_slice.json; synthetic bucket and store names).
"""

from __future__ import annotations

import importlib.util
import json
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

from tests.simulation.test_dataset_walk import REAL_HEADER, REAL_ROW
from tests.simulation.test_event_ingest import _S3
from viva_api.analysis.models import ExperimentAnalysisDTO
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.dataset_walk import reconcile_simulation
from viva_api.simulation.models import (
    ParcaDatasetRequest,
    ParcaOptions,
    Simulation,
    SimulationConfig,
    SimulationRequest,
)
from viva_api.simulation.tables_orm import AnalysisStatusDB

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "import_cd2_datasets.py"
_spec = importlib.util.spec_from_file_location("import_cd2_datasets", _SCRIPT)
assert _spec is not None and _spec.loader is not None
importer: Any = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = importer  # @dataclass resolves the module through sys.modules
_spec.loader.exec_module(importer)

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "datasets" / "cd2_manifest_slice.json"
BUCKET = "cd2-bucket"


def _manifest(prefix: str) -> dict[str, Any]:
    """The fixture slice, its store names made unique to one test."""
    manifest: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8").replace("@", f"{prefix}-"))
    return manifest


def _objects(manifest: dict[str, Any]) -> dict[str, bytes]:
    """What S3 holds for the slice: a TSV per view, one figure and a driver.log per bundle."""
    tsv = (REAL_HEADER + REAL_ROW).encode()
    objects: dict[str, bytes] = {}
    for store in manifest["stores"]:
        for info in store["fill_bundles"].values():
            key = info["s3_uri"].removeprefix(f"s3://{BUCKET}/").rstrip("/")
            for view in info["view_types"]:
                objects[f"{key}/ptools/{view}__variant=0.tsv"] = tsv
            objects[f"{key}/viz/{info['view_types'][0]}__variant=0.html"] = b"<html></html>"
            objects[f"{key}/driver.log"] = b"log"
    return objects


async def _simulation(db: DatabaseServiceSQL, experiment_id: str, tags: list[str] | None = None) -> Simulation:
    simulator = await db.insert_simulator(
        git_commit_hash=uuid.uuid4().hex, git_repo_url="https://github.com/CovertLabEcoli/sms-ecoli", git_branch="main"
    )
    parca = await db.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    config = SimulationConfig(  # type: ignore[call-arg]
        experiment_id=experiment_id, emitter_arg={"out_uri": f"s3://{BUCKET}/vecoli-output/{experiment_id}"}
    )
    simulation = await db.insert_simulation(
        sim_request=SimulationRequest(
            simulation_config_filename="cd2.json",
            experiment_id=experiment_id,
            parca_dataset_id=parca.database_id,
            simulator_id=simulator.database_id,
            config=config,
        )
    )
    return await db.add_tags(simulation.database_id, tags) if tags else simulation


async def _gather_run(db: DatabaseServiceSQL, simulation: Simulation, result_uri: str) -> ExperimentAnalysisDTO:
    """The run row a sim-time gather records for its own bundle, as the dispatch path does."""
    return await db.record_analysis(
        experiment_id=simulation.experiment_id,
        n_tp=None,
        status=AnalysisStatusDB.READY,
        config={"analysis_options": {"experiment_id": [simulation.experiment_id]}},
        name=result_uri.rsplit("/", 1)[-1],
        simulation_id=simulation.database_id,
        backend="ray",
        result_uri=result_uri,
    )


def test_bundles_load_with_their_mode_family_tags_and_experiment_id() -> None:
    manifest = _manifest("t")
    bundles, empty = importer.load_bundles(manifest)
    assert [b.name for b in bundles] == [
        "analysis-multiseed-run2j3",
        "analysis-mnp-sim201-cd2-run4-nati-mhh2hv",
        "analysis-percellfill",
    ]
    assert empty == ["t-run1-store"]
    run2, run4 = bundles[0], bundles[1]
    assert run2.experiment_id == "t-run2-store"  # no experiment_id field: the store name is it
    assert run2.tags == ["cd2", "cd2-run2", "cd2-fill"]
    assert run4.store == "t-run4-store" and run4.experiment_id == "t-run4-exp"
    assert run4.mode == "append-metabolites" and run4.tags == ["cd2", "cd2-run4min-native"]
    assert not run4.uri.endswith("/") and run4.fill_jobs == ("cd2fill-run4-percell",)

    run2_only, _ = importer.load_bundles(manifest, families={"run2"})
    assert [b.family for b in run2_only] == ["run2"]
    one, none_empty = importer.load_bundles(manifest, stores={"t-run4-store"})
    assert [b.store for b in one] == ["t-run4-store"] and none_empty == []


def test_arguments_default_to_analyze_and_the_modes_exclude_each_other() -> None:
    args = importer.parse_args(["--manifest", "m.json"])
    assert not args.apply and args.family == [] and args.store == []
    args = importer.parse_args(["--manifest", "m.json", "--apply", "--family", "run3", "--family", "run2"])
    assert args.apply and args.family == ["run3", "run2"]
    with pytest.raises(SystemExit):
        importer.parse_args(["--manifest", "m.json", "--apply", "--analyze"])


@pytest.mark.asyncio
async def test_analyze_reports_each_bundles_producer_and_writes_nothing(database_service: DatabaseServiceSQL) -> None:
    prefix = uuid.uuid4().hex[:8]
    manifest = _manifest(prefix)
    run2 = await _simulation(database_service, f"{prefix}-run2-store")
    run4 = await _simulation(database_service, f"{prefix}-run4-exp")
    bundles, _ = importer.load_bundles(manifest)
    gather = await _gather_run(database_service, run4, bundles[1].uri)

    report = await importer.import_bundles(
        bundles, db=database_service, file_service=None, storage_bucket=BUCKET, apply=False
    )
    assert (report.bundles, report.claimed, report.unclaimed, report.registered) == (2, 1, 1, 0)
    assert len(report.skipped) == 1 and "no simulation" in report.skipped[0]
    assert any(f"simulation {run2.database_id} (no analysis run claims it)" in line for line in report.lines)
    assert any(f"analysis run {gather.database_id} (ray)" in line for line in report.lines)
    for bundle in bundles:
        assert await database_service.list_datasets(uri_prefix=f"{bundle.uri}/", available=None) == []
    assert await database_service.list_analyses(simulation_id=run2.database_id) == []
    assert "nothing written" in report.summary(applied=False)

    with pytest.raises(ValueError, match="file service"):
        await importer.import_bundles(
            bundles, db=database_service, file_service=None, storage_bucket=BUCKET, apply=True
        )


@pytest.mark.asyncio
async def test_apply_registers_and_tags_without_creating_run_rows_and_a_later_walk_agrees(
    database_service: DatabaseServiceSQL,
) -> None:
    prefix = uuid.uuid4().hex[:8]
    manifest = _manifest(prefix)
    run2 = await _simulation(database_service, f"{prefix}-run2-store", tags=["cd2-sim"])
    run4 = await _simulation(database_service, f"{prefix}-run4-exp")
    bundles, _ = importer.load_bundles(manifest)
    gather = await _gather_run(database_service, run4, bundles[1].uri)
    s3 = _S3(_objects(manifest))

    report = await importer.import_bundles(
        bundles, db=database_service, file_service=s3, storage_bucket=BUCKET, apply=True
    )
    assert (report.bundles, report.claimed, report.unclaimed, report.registered) == (2, 1, 1, 5)
    assert len(report.skipped) == 1  # the store with no simulation row

    # The dedicated fill: nothing claims it, so its simulation is the producer, and no run row is made.
    assert await database_service.list_analyses(simulation_id=run2.database_id) == []
    fill = await database_service.list_datasets(uri_prefix=f"{bundles[0].uri}/")
    assert len(fill) == 3 and {(d.simulation_id, d.analysis_id) for d in fill} == {(run2.database_id, None)}
    assert sorted(d.view or "" for d in fill if d.kind == "ptools-analysis") == ["ptools_rna", "ptools_rxns"]
    assert all({"cd2", "cd2-run2", "cd2-fill", "cd2-sim"} <= set(d.tags) for d in fill)
    # No `n_tp`: the walk registers from the LISTING and never opens an object (viva-api#675).
    # The producer reports it in the `artifact.written` payload, and the ptools page counts its
    # own columns from the TSV it already downloads to POST as `datatext`.
    assert {d.attributes.get("n_tp") for d in fill if d.kind == "ptools-analysis"} == {None}
    assert {d.origin for d in fill} == {"walk"}

    # The sim-time bundle keeps the gather run that claims it, and is not tagged as a fill.
    appended = await database_service.list_datasets(uri_prefix=f"{bundles[1].uri}/")
    assert len(appended) == 2 and {d.analysis_id for d in appended} == {gather.database_id}
    assert all("cd2-run4min-native" in d.tags and "cd2-fill" not in d.tags for d in appended)

    again = await importer.import_bundles(
        bundles, db=database_service, file_service=s3, storage_bucket=BUCKET, apply=True
    )
    assert (again.registered, again.unchanged) == (0, 5)

    walked = await reconcile_simulation(run2, db=database_service, file_service=s3, storage_bucket=BUCKET)
    assert (walked.unclaimed, walked.registered, walked.unchanged) == (1, 0, 3)


@pytest.mark.asyncio
async def test_bundles_outside_the_storage_bucket_are_skipped(database_service: DatabaseServiceSQL) -> None:
    bundles, _ = importer.load_bundles(_manifest(uuid.uuid4().hex[:8]))
    report = await importer.import_bundles(
        bundles, db=database_service, file_service=_S3({}), storage_bucket="another-bucket", apply=True
    )
    assert report.bundles == 0 and len(report.skipped) == 3
    assert all("not in the storage bucket" in reason for reason in report.skipped)
