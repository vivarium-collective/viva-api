"""scripts/import_cd2_datasets.py: CD2 fill bundles become fill run rows and datasets (plan §8).

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
from viva_api.common.models import JobStatus
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.dataset_walk import reconcile_simulation
from viva_api.simulation.models import (
    ParcaDatasetRequest,
    ParcaOptions,
    Simulation,
    SimulationConfig,
    SimulationRequest,
)

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


def test_bundles_load_with_their_mode_family_and_experiment_id() -> None:
    manifest = _manifest("t")
    bundles, empty = importer.load_bundles(manifest)
    assert [b.name for b in bundles] == [
        "analysis-multiseed-run2j3",
        "analysis-mnp-sim201-cd2-run4-nati-mhh2hv",
        "analysis-percellfill",
    ]
    assert empty == ["t-run1-store"]
    run4 = bundles[1]
    assert run4.store == "t-run4-store" and run4.experiment_id == "t-run4-exp"
    assert run4.mode == "append-metabolites"
    assert run4.tags == ["cd2", "cd2-run4min-native", "append-metabolites"]
    assert not run4.uri.endswith("/") and run4.fill_jobs == ("cd2fill-run4-percell",)
    assert bundles[0].experiment_id == "t-run2-store"  # no experiment_id field: the store name is it

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
async def test_analyze_reports_what_it_would_do_and_writes_nothing(database_service: DatabaseServiceSQL) -> None:
    prefix = uuid.uuid4().hex[:8]
    manifest = _manifest(prefix)
    await _simulation(database_service, f"{prefix}-run2-store")
    await _simulation(database_service, f"{prefix}-run4-exp")
    bundles, _ = importer.load_bundles(manifest)

    report = await importer.import_bundles(
        bundles, db=database_service, file_service=None, storage_bucket=BUCKET, apply=False
    )
    assert (report.bundles, report.runs_created, report.registered) == (2, 0, 0)
    assert len(report.skipped) == 1 and "no simulation" in report.skipped[0]
    assert sum("would create a fill run row" in line for line in report.lines) == 2
    for bundle in bundles:
        assert await database_service.get_analysis_by_result_uri(bundle.uri) is None
    assert "nothing written" in report.summary(applied=False)

    with pytest.raises(ValueError, match="file service"):
        await importer.import_bundles(
            bundles, db=database_service, file_service=None, storage_bucket=BUCKET, apply=True
        )


@pytest.mark.asyncio
async def test_apply_creates_fill_runs_and_tagged_datasets_that_a_later_walk_agrees_with(
    database_service: DatabaseServiceSQL,
) -> None:
    prefix = uuid.uuid4().hex[:8]
    manifest = _manifest(prefix)
    run2 = await _simulation(database_service, f"{prefix}-run2-store", tags=["cd2-sim"])
    run4 = await _simulation(database_service, f"{prefix}-run4-exp")
    bundles, _ = importer.load_bundles(manifest)
    s3 = _S3(_objects(manifest))

    report = await importer.import_bundles(
        bundles, db=database_service, file_service=s3, storage_bucket=BUCKET, apply=True, generated_at="2026-09-14"
    )
    assert (report.bundles, report.runs_created, report.runs_reused, report.registered) == (2, 2, 0, 5)
    assert len(report.skipped) == 1  # the store with no simulation row

    fill = await database_service.get_analysis_by_result_uri(bundles[0].uri)
    assert fill is not None
    assert fill.backend == "fill" and fill.status == JobStatus.COMPLETED and fill.simulation_id == run2.database_id
    assert set(fill.tags) == {"cd2", "cd2-run2", "dedicated-fill"}
    assert fill.source == {
        "kind": "simulation",
        "ref": str(run2.database_id),
        "resolved_id": run2.database_id,
        "uri": f"s3://{BUCKET}/vecoli-output/{prefix}-run2-store",
    }
    datasets = await database_service.list_datasets(analysis_id=fill.database_id)
    assert sorted(d.view or "" for d in datasets if d.kind == "ptools-analysis") == ["ptools_rna", "ptools_rxns"]
    assert all({"cd2", "cd2-run2", "dedicated-fill", "cd2-sim"} <= set(d.tags) for d in datasets)
    assert {d.attributes.get("n_tp") for d in datasets if d.kind == "ptools-analysis"} == {8}
    assert {d.origin for d in datasets} == {"walk"}

    # The sim-time bundle the append-metabolites fill wrote into is a fill run of its own too.
    mnp = await database_service.get_analysis_by_result_uri(bundles[1].uri)
    assert mnp is not None and mnp.simulation_id == run4.database_id and "append-metabolites" in mnp.tags
    assert await database_service.count_datasets(analysis_id=mnp.database_id) == 2

    again = await importer.import_bundles(
        bundles, db=database_service, file_service=s3, storage_bucket=BUCKET, apply=True
    )
    assert (again.runs_created, again.runs_reused, again.registered, again.unchanged) == (0, 2, 0, 5)

    # The reconciliation walk finds the same bundle, reuses the fill run and changes nothing.
    walked = await reconcile_simulation(run2, db=database_service, file_service=s3, storage_bucket=BUCKET)
    assert (walked.analyses_created, walked.registered, walked.unchanged) == (0, 0, 3)


@pytest.mark.asyncio
async def test_a_run_row_the_walk_already_made_is_reused_and_tagged(database_service: DatabaseServiceSQL) -> None:
    prefix = uuid.uuid4().hex[:8]
    manifest = _manifest(prefix)
    run2 = await _simulation(database_service, f"{prefix}-run2-store")
    s3 = _S3(_objects(manifest))
    assert (
        await reconcile_simulation(run2, db=database_service, file_service=s3, storage_bucket=BUCKET)
    ).analyses_created == 1

    bundles, _ = importer.load_bundles(manifest, stores={f"{prefix}-run2-store"})
    walk_row = await database_service.get_analysis_by_result_uri(bundles[0].uri)
    assert walk_row is not None and walk_row.backend == "walk"

    report = await importer.import_bundles(
        bundles, db=database_service, file_service=s3, storage_bucket=BUCKET, apply=True
    )
    assert (report.runs_created, report.runs_reused, report.registered) == (0, 1, 3)  # the rows gain the tags
    row = await database_service.get_analysis(walk_row.database_id)
    assert {"cd2", "cd2-run2", "dedicated-fill"} <= set(row.tags)


@pytest.mark.asyncio
async def test_bundles_outside_the_storage_bucket_are_skipped(database_service: DatabaseServiceSQL) -> None:
    bundles, _ = importer.load_bundles(_manifest(uuid.uuid4().hex[:8]))
    report = await importer.import_bundles(
        bundles, db=database_service, file_service=_S3({}), storage_bucket="another-bucket", apply=True
    )
    assert report.bundles == 0 and len(report.skipped) == 3
    assert all("not in the storage bucket" in reason for reason in report.skipped)
