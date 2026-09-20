"""viva-api#709: a run owns more Batch jobs than the one it tracks, and cancel stops all of them.

Found live by the ``sim-cancel`` smoke check (dev 0.9.147): cancelling a default-path
simulation terminated the simulation's job and left the ParCa job it depended on RUNNING --
757 s of it, under a row that said CANCELLED -- because the ParCa id was kept only as the
simulation job's ``dependsOn`` and never recorded.
"""

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

from tests.simulation.test_ray_backend import _fake_batch, _ray_settings
from viva_api.common.handlers.simulations import cancel_simulation
from viva_api.common.models import JobId, JobStatus
from viva_api.simulation.models import HpcRun, JobType
from viva_api.simulation.simulation_service_ray import SimulationServiceRay

if TYPE_CHECKING:
    from viva_api.simulation.database_service import DatabaseServiceSQL
    from viva_api.simulation.models import SimulationRequest


def _patched(mock_batch: Any) -> Any:
    from contextlib import ExitStack

    stack = ExitStack()
    stack.enter_context(patch("viva_api.simulation.ray._seams.get_settings", _ray_settings))
    stack.enter_context(patch("viva_api.common.storage.data_layout.get_settings", _ray_settings))
    stack.enter_context(patch("viva_api.simulation.ray._seams.boto3.client", return_value=mock_batch))
    return stack


@pytest.mark.asyncio
async def test_the_default_path_records_its_parca_job_on_the_runs_own_row(
    experiment_request: "SimulationRequest", database_service: "DatabaseServiceSQL"
) -> None:
    simulation = await database_service.insert_simulation(sim_request=experiment_request)
    service = SimulationServiceRay()
    with _patched(_fake_batch(["parca-123", "sim-456"])):
        job_id = await service.submit_ecoli_simulation_job(
            ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-709"
        )

    assert job_id == JobId.ray("sim-456")
    run = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
    assert run is not None
    assert (run.job_id, run.correlation_id, run.external_job_ids) == (JobId.ray("sim-456"), "corr-709", ["parca-123"])
    # ...and it is the ONLY row: the generic caller's insert-if-absent guard keys on this.
    assert await database_service.get_hpcrun_id_by_correlation_id(correlation_id="corr-709") == run.database_id


@pytest.mark.asyncio
async def test_cancel_stops_the_parca_job_first_then_the_simulation_job(
    experiment_request: "SimulationRequest", database_service: "DatabaseServiceSQL"
) -> None:
    """Through the real handler and the real row. Dependency first: Batch keeps a terminated
    job PENDING until what it depends on ends."""
    simulation = await database_service.insert_simulation(sim_request=experiment_request)
    service = SimulationServiceRay()
    mock_batch = _fake_batch(["parca-123", "sim-456"])
    with (
        _patched(mock_batch),
        patch("viva_api.common.handlers.simulations.get_simulation_service_for_job", return_value=service),
    ):
        await service.submit_ecoli_simulation_job(
            ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-709"
        )
        answered = await cancel_simulation(
            db_service=database_service, simulation_service=service, simulation_id=simulation.database_id
        )

    assert answered.status == JobStatus.CANCELLED
    assert [call.kwargs["jobId"] for call in mock_batch.terminate_job.call_args_list] == ["parca-123", "sim-456"]


def _run(**overrides: Any) -> HpcRun:
    fields: dict[str, Any] = {
        "database_id": 1,
        "job_id": JobId.ray("sim-456"),
        "correlation_id": "corr",
        "job_type": JobType.SIMULATION,
        "ref_id": 1,
        "status": JobStatus.RUNNING,
    }
    return HpcRun(**{**fields, **overrides})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("run", "expected"),
    [
        (_run(external_job_ids=["parca-123"]), ["parca-123"]),
        (_run(external_job_ids=["parca-123", "sim-456", ""]), ["parca-123"]),  # never the tracked job twice
        (_run(external_job_ids=None), []),  # every row written before this fix
        (_run(job_id=JobId.local("t-1"), external_job_ids=["build-arm", "build-amd"]), []),  # a build's jobs
    ],
    ids=["one companion", "tracked id and blanks ignored", "no companions", "a LOCAL row is not ours"],
)
async def test_cancel_companion_jobs(run: HpcRun, expected: list[str]) -> None:
    mock_batch = MagicMock()
    with _patched(mock_batch):
        stopped = await SimulationServiceRay().cancel_companion_jobs(run)
    assert [call.kwargs["jobId"] for call in mock_batch.terminate_job.call_args_list] == expected
    assert stopped == len(expected)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("parca_done", "expected"), [(False, ["parca-1", "s1g0"]), (None, ["parca-1", "s1g0"]), (True, ["s1g0"])]
)
async def test_a_chain_campaign_cancelled_during_parca_stops_parca(
    parca_done: bool | None, expected: list[str]
) -> None:
    """The campaign's own job id IS its ParCa job, and until ParCa succeeds no seed has a
    current job -- so a cancel in that phase used to terminate nothing at all."""
    campaign = _run(
        job_id=JobId.ray("parca-1"),
        chain_n_generations=3,
        chain_final_job_ids=[],
        chain_current_job_ids=[None, "s1g0"],
        chain_current_generation=[None, 0],
        chain_parca_done=parca_done,
    )
    mock_batch = MagicMock()
    with _patched(mock_batch):
        await SimulationServiceRay().cancel_chain_campaign(campaign)
    assert [call.kwargs["jobId"] for call in mock_batch.terminate_job.call_args_list] == expected
