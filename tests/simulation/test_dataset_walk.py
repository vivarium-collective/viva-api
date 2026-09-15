"""The walk feeder: registering a simulation's analysis bundles by listing S3 (plan §5).

Names and the TSV header below are REAL conventions verified on dev (2026-09-15): v2ecoli's
``<name>__<group>`` file names, a 240 MB ``analysis.json`` and a ``driver.log`` next to the
``ptools/`` and ``viz/`` directories, and a ptools header of 8 timepoints each followed by an
``_sd`` column. The S3 double and the testcontainer Postgres keep everything local.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.simulation.test_event_ingest import _S3
from viva_api.simulation import dataset_walk
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.dataset_walk import (
    ArtifactName,
    WalkResult,
    classify,
    parse_artifact_name,
    reconcile_simulation,
    timepoint_columns,
)
from viva_api.simulation.job_scheduler import JobScheduler
from viva_api.simulation.models import (
    ParcaDatasetRequest,
    ParcaOptions,
    Simulation,
    SimulationConfig,
    SimulationRequest,
)
from viva_api.simulation.tables_orm import AnalysisStatusDB

#: Verbatim header line of a real dev file:
#: sim200-cd2-run3-sweep-combo00-25fc/analyses/analysis-ptools-multiseed/ptools/ptools_rna_multiseed__variant=0.tsv
REAL_HEADER = (
    "$\t0m\t124m\t249m\t373m\t497m\t622m\t746m\t870m\t"
    "0m_sd\t124m_sd\t249m_sd\t373m_sd\t497m_sd\t622m_sd\t746m_sd\t870m_sd\n"
)
REAL_ROW = (
    "EG10001\t0.2542\t0.2778\t0.2650\t0.2754\t0.2033\t0.2606\t0.3076\t0.2569\t"
    "0.05\t0.11\t0.07\t0.01\t0.08\t0.05\t0.06\t0.08\n"
)


# ---------------------------------------------------------------------------
# pure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        (
            "ptools_metabolites__variant=0_seed=0_gen=10_agent=00000000000.tsv",
            ArtifactName(
                "ptools_metabolites", "single", {"variant": 0, "seed": 0, "generation": 10, "agent": "00000000000"}
            ),
        ),
        ("ptools_rna_multiseed__variant=0.tsv", ArtifactName("ptools_rna", "multiseed", {"variant": 0})),
        (
            "mass_fraction__variant=1_seed=2.html",
            ArtifactName("mass_fraction", "multigeneration", {"variant": 1, "seed": 2}),
        ),
        (
            "division__variant=0_seed=0_gen=1_parent=0.tsv",
            ArtifactName("division", "multidaughter", {"variant": 0, "seed": 0, "generation": 1, "parent": "0"}),
        ),
        ("summary_multivariant__all.html", ArtifactName("summary", "multivariant", {})),
        ("analysis.json", None),
        ("driver.log", None),
        ("ptools_rna__variant.tsv", None),
        ("ptools_rna__variant=zero.tsv", None),
    ],
)
def test_names_parse_by_v2ecoli_conventions(filename: str, expected: ArtifactName | None) -> None:
    assert parse_artifact_name(filename) == expected


def test_timepoints_skip_the_sd_columns_and_accept_the_legacy_t_header() -> None:
    assert timepoint_columns(REAL_HEADER + REAL_ROW) == 8
    assert timepoint_columns("$\tt0\tt1\tt2\ngeneA\t1\t2\t3\n") == 3
    assert timepoint_columns("$\n") == 0


def test_only_consumable_files_are_datasets() -> None:
    assert classify("analysis.json") == "report"
    assert classify("ptools/ptools_rna__variant=0.tsv") == "ptools-analysis"
    assert classify("viz/ptools_rna__variant=0.html") == "figure"
    assert classify("driver.log") is None
    assert classify("ptools/nested/deeper.tsv") is None
    assert classify("viz/notes.txt") is None


# ---------------------------------------------------------------------------
# against the database
# ---------------------------------------------------------------------------


async def _simulation(db: DatabaseServiceSQL, *, bucket: str = "work", tags: list[str] | None = None) -> Simulation:
    simulator = await db.insert_simulator(
        git_commit_hash=uuid.uuid4().hex, git_repo_url="https://github.com/CovertLabEcoli/sms-ecoli", git_branch="main"
    )
    parca = await db.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    experiment_id = f"sim200-walk-{uuid.uuid4().hex[:8]}"
    config = SimulationConfig(  # type: ignore[call-arg]
        experiment_id=experiment_id, emitter_arg={"out_uri": f"s3://{bucket}/vecoli-output/{experiment_id}"}
    )
    simulation = await db.insert_simulation(
        sim_request=SimulationRequest(
            simulation_config_filename="walk.json",
            experiment_id=experiment_id,
            parca_dataset_id=parca.database_id,
            simulator_id=simulator.database_id,
            config=config,
        )
    )
    return await db.add_tags(simulation.database_id, tags) if tags else simulation


def _bundle_objects(simulation: Simulation) -> dict[str, bytes]:
    root = f"vecoli-output/{simulation.experiment_id}/analyses"
    return {
        f"{root}/analysis-ptools-multiseed/ptools/ptools_rna_multiseed__variant=0.tsv": (
            REAL_HEADER + REAL_ROW
        ).encode(),
        f"{root}/analysis-ptools-multiseed/viz/ptools_rna_multiseed__variant=0.html": b"<html></html>",
        f"{root}/analysis-ptools-multiseed/analysis.json": b'{"status": "OK"}',
        f"{root}/analysis-ptools-multiseed/driver.log": b"log",
        f"{root}/analysis-percell-run3/ptools/ptools_rna__variant=0_seed=3_gen=12_agent=000.tsv": (
            REAL_HEADER + REAL_ROW
        ).encode(),
        f"{root}/analysis-empty/driver.log": b"nothing consumable here",
    }


def _uri(simulation: Simulation, relative: str) -> str:
    return f"s3://work/vecoli-output/{simulation.experiment_id}/analyses/{relative}"


async def _walk(db: DatabaseServiceSQL, simulation: Simulation, s3: _S3) -> WalkResult:
    return await reconcile_simulation(simulation, db=db, file_service=s3, storage_bucket="work")


@pytest.mark.asyncio
async def test_a_walk_registers_bundles_and_attributes_unclaimed_ones_to_the_simulation(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation = await _simulation(database_service, tags=["cd2"])
    existing = await database_service.record_analysis(
        experiment_id=simulation.experiment_id,
        n_tp=None,
        status=AnalysisStatusDB.COMPUTING,
        config={"analysis_options": {"experiment_id": [simulation.experiment_id]}},
        name="analysis-percell-run3",
        simulation_id=simulation.database_id,
        backend="ray",
        result_uri=_uri(simulation, "analysis-percell-run3"),
    )
    s3 = _S3(_bundle_objects(simulation))

    result = await _walk(database_service, simulation, s3)

    assert (result.bundles, result.unclaimed, result.registered, result.skipped) == (2, 1, 4, 0)
    # No run row is invented for the bundle no run claims.
    analyses = await database_service.list_analyses(simulation_id=simulation.database_id)
    assert [a.database_id for a in analyses] == [existing.database_id]

    tsv = await database_service.get_dataset_by_uri(
        _uri(simulation, "analysis-ptools-multiseed/ptools/ptools_rna_multiseed__variant=0.tsv")
    )
    assert tsv is not None and tsv.kind == "ptools-analysis"
    assert tsv.simulation_id == simulation.database_id and tsv.analysis_id is None
    assert tsv.view == "ptools_rna" and tsv.size_bytes == len(REAL_HEADER + REAL_ROW)
    assert {k: tsv.attributes[k] for k in ("protocol", "variant", "n_tp", "origin", "analysis_dir")} == {
        "protocol": "multiseed",
        "variant": 0,
        "n_tp": 8,
        "origin": "walk",
        "analysis_dir": "analysis-ptools-multiseed",
    }
    assert tsv.tags == ["cd2"]
    assert tsv.display_name == f"{simulation.experiment_id} · ptools_rna · multiseed"

    cell = await database_service.get_dataset_by_uri(
        _uri(simulation, "analysis-percell-run3/ptools/ptools_rna__variant=0_seed=3_gen=12_agent=000.tsv")
    )
    assert cell is not None and cell.analysis_id == existing.database_id  # the existing run row is reused
    assert {k: cell.attributes[k] for k in ("protocol", "seed", "generation", "agent")} == {
        "protocol": "single",
        "seed": 3,
        "generation": 12,
        "agent": "000",
    }
    assert cell.display_name is not None and cell.display_name.endswith("single · s3 g12")

    figure = await database_service.get_dataset_by_uri(
        _uri(simulation, "analysis-ptools-multiseed/viz/ptools_rna_multiseed__variant=0.html")
    )
    report = await database_service.get_dataset_by_uri(_uri(simulation, "analysis-ptools-multiseed/analysis.json"))
    assert figure is not None and figure.kind == "figure"
    assert report is not None and report.kind == "report"
    assert await database_service.get_dataset_by_uri(_uri(simulation, "analysis-ptools-multiseed/driver.log")) is None


@pytest.mark.asyncio
async def test_a_second_walk_changes_nothing_and_rereads_no_headers(database_service: DatabaseServiceSQL) -> None:
    simulation = await _simulation(database_service)
    s3 = _S3(_bundle_objects(simulation))
    await _walk(database_service, simulation, s3)
    s3.reads.clear()

    again = await _walk(database_service, simulation, s3)

    assert (again.unclaimed, again.registered, again.unchanged, again.unavailable) == (2, 0, 4, 0)
    assert s3.reads == []  # sizes unchanged, so n_tp is reused without a header read


@pytest.mark.asyncio
async def test_a_vanished_object_is_marked_unavailable_and_recovers_when_it_returns(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation = await _simulation(database_service)
    objects = _bundle_objects(simulation)
    s3 = _S3(objects)
    await _walk(database_service, simulation, s3)
    key = (
        f"vecoli-output/{simulation.experiment_id}/analyses/"
        "analysis-ptools-multiseed/ptools/ptools_rna_multiseed__variant=0.tsv"
    )
    uri = f"s3://work/{key}"

    saved = s3.objects.pop(key)
    gone = await _walk(database_service, simulation, s3)
    row = await database_service.get_dataset_by_uri(uri)
    assert gone.unavailable == 1 and row is not None and row.available is False

    s3.objects[key] = saved
    back = await _walk(database_service, simulation, s3)
    row = await database_service.get_dataset_by_uri(uri)
    assert back.registered == 1 and row is not None and row.available is True


@pytest.mark.asyncio
async def test_a_vanished_bundle_marks_every_row_under_it_unavailable(database_service: DatabaseServiceSQL) -> None:
    simulation = await _simulation(database_service)
    s3 = _S3(_bundle_objects(simulation))
    await _walk(database_service, simulation, s3)
    prefix = f"vecoli-output/{simulation.experiment_id}/analyses/analysis-ptools-multiseed/"
    for key in [k for k in s3.objects if k.startswith(prefix)]:
        del s3.objects[key]

    result = await _walk(database_service, simulation, s3)

    assert result.unavailable == 3  # the TSV, the figure and the report
    rows = await database_service.list_datasets(
        uri_prefix=_uri(simulation, "analysis-ptools-multiseed/"), available=None
    )
    assert len(rows) == 3 and all(not row.available for row in rows)


@pytest.mark.asyncio
async def test_a_run_recorded_later_takes_its_bundle_over_from_the_simulation(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation = await _simulation(database_service)
    s3 = _S3(_bundle_objects(simulation))
    await _walk(database_service, simulation, s3)
    uri = _uri(simulation, "analysis-ptools-multiseed/ptools/ptools_rna_multiseed__variant=0.tsv")
    before = await database_service.get_dataset_by_uri(uri)
    assert before is not None and before.simulation_id == simulation.database_id

    run = await database_service.record_analysis(
        experiment_id=simulation.experiment_id,
        n_tp=None,
        status=AnalysisStatusDB.READY,
        config={"analysis_options": {"experiment_id": [simulation.experiment_id]}},
        name="analysis-ptools-multiseed",
        simulation_id=simulation.database_id,
        backend="ray",
        result_uri=_uri(simulation, "analysis-ptools-multiseed"),
    )
    moved = await _walk(database_service, simulation, s3)

    after = await database_service.get_dataset_by_uri(uri)
    assert (moved.unclaimed, moved.registered) == (1, 3)  # analysis-percell-run3 is still unclaimed
    assert after is not None and after.analysis_id == run.database_id and after.simulation_id is None


@pytest.mark.asyncio
async def test_the_walk_never_overwrites_an_event_sourced_row(database_service: DatabaseServiceSQL) -> None:
    simulation = await _simulation(database_service)
    s3 = _S3(_bundle_objects(simulation))
    await _walk(database_service, simulation, s3)
    uri = _uri(simulation, "analysis-ptools-multiseed/ptools/ptools_rna_multiseed__variant=0.tsv")
    row = await database_service.get_dataset_by_uri(uri)
    assert row is not None
    await database_service.upsert_dataset(
        uri=uri, kind="ptools-analysis", origin="event", analysis_id=row.analysis_id, attributes={"n_tp": 9}
    )

    await _walk(database_service, simulation, s3)

    after = await database_service.get_dataset_by_uri(uri)
    assert after is not None and after.origin == "event" and after.attributes["n_tp"] == 9

    # Availability follows the object, even on a row the walk may not rewrite.
    key = uri.removeprefix("s3://work/")
    saved = s3.objects.pop(key)
    await _walk(database_service, simulation, s3)
    gone = await database_service.get_dataset_by_uri(uri)
    assert gone is not None and gone.available is False
    s3.objects[key] = saved
    await _walk(database_service, simulation, s3)
    back = await database_service.get_dataset_by_uri(uri)
    assert back is not None and back.available is True and back.origin == "event"


@pytest.mark.asyncio
async def test_a_root_outside_the_file_service_bucket_is_skipped_without_listing(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation = await _simulation(database_service, bucket="another-bucket")
    s3 = _S3({})

    result = await _walk(database_service, simulation, s3)

    assert result.skipped == 1 and "another-bucket" in result.reasons[0]
    assert s3.listings == []


# ---------------------------------------------------------------------------
# the scheduler tick
# ---------------------------------------------------------------------------


def _reconcile_settings(**overrides: Any) -> MagicMock:
    settings = MagicMock()
    settings.datasets_reconcile_enabled = True
    settings.datasets_reconcile_batch_interval_seconds = 0
    settings.datasets_reconcile_batch_size = 2
    settings.storage_s3_bucket = "work"
    for key, value in overrides.items():
        setattr(settings, key, value)
    return settings


@pytest.mark.asyncio
async def test_the_tick_walks_simulations_round_robin_and_wraps(database_service: DatabaseServiceSQL) -> None:
    ours = [(await _simulation(database_service)).database_id for _ in range(3)]
    walked: list[int] = []

    async def _record(simulation: Simulation, **_kwargs: Any) -> WalkResult:
        walked.append(simulation.database_id)
        return WalkResult()

    scheduler = JobScheduler(messaging_service=MagicMock(), database_service=database_service)
    with (
        patch("viva_api.simulation.job_scheduler.get_settings", return_value=_reconcile_settings()),
        patch("viva_api.dependencies.get_file_service", return_value=_S3({})),
        patch.object(dataset_walk, "reconcile_simulation", AsyncMock(side_effect=_record)),
    ):
        for _ in range(10_000):
            await scheduler.reconcile_datasets()
            if scheduler._dataset_reconcile_cursor == 0 and walked:
                break

    positions = [walked.index(sim_id) for sim_id in ours]
    assert positions == sorted(positions) and all(walked.count(sim_id) == 1 for sim_id in ours)
    assert walked == sorted(walked)  # ascending ids within one full cycle


@pytest.mark.asyncio
async def test_the_tick_waits_for_its_interval_and_honours_the_switch(database_service: DatabaseServiceSQL) -> None:
    await _simulation(database_service)
    scheduler = JobScheduler(messaging_service=MagicMock(), database_service=database_service)
    walk = AsyncMock(return_value=WalkResult())

    with (
        patch("viva_api.dependencies.get_file_service", return_value=_S3({})),
        patch.object(dataset_walk, "reconcile_simulation", walk),
    ):
        with patch(
            "viva_api.simulation.job_scheduler.get_settings",
            return_value=_reconcile_settings(datasets_reconcile_enabled=False),
        ):
            await scheduler.reconcile_datasets()
        assert walk.await_count == 0

        hourly = _reconcile_settings(datasets_reconcile_batch_interval_seconds=3600)
        with patch("viva_api.simulation.job_scheduler.get_settings", return_value=hourly):
            await scheduler.reconcile_datasets()
            first = walk.await_count
            await scheduler.reconcile_datasets()
        assert first > 0 and walk.await_count == first


def test_the_base_file_service_header_read_slices_a_full_read() -> None:
    import asyncio
    from pathlib import Path

    from viva_api.common.storage.file_paths import S3FilePath

    s3 = _S3({"a/b.tsv": REAL_HEADER.encode()})
    head = asyncio.run(s3.get_file_head(S3FilePath(s3_path=Path("a/b.tsv")), 4))
    missing = asyncio.run(s3.get_file_head(S3FilePath(s3_path=Path("a/none.tsv")), 4))
    assert head == b"$\t0m" and missing is None


def test_the_walk_result_merge_keeps_exact_counts() -> None:
    total = WalkResult()
    part = WalkResult(bundles=1, registered=2, unchanged=1, unavailable=1)
    for i in range(7):
        part.skip(f"reason {i}")
    total.merge(part)
    total.merge(part)
    assert (total.bundles, total.registered, total.unchanged, total.unavailable, total.skipped) == (2, 4, 2, 2, 14)
    assert len(total.reasons) == 5
