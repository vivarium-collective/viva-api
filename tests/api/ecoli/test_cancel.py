"""Tests for simulation cancel flow.

Run with: uv run pytest tests/api/ecoli/test_cancel.py -v
"""

from unittest.mock import AsyncMock, patch

import pytest

from viva_api.common.hpc.job_service import JobStatusUpdate
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import (
    JobType,
    SimulationRequest,
)


@pytest.mark.asyncio
async def test_cancel_running_simulation(
    experiment_request: SimulationRequest,
    database_service: DatabaseServiceSQL,
) -> None:
    """Test cancelling a running simulation updates status to CANCELLED."""
    simulation = await database_service.insert_simulation(sim_request=experiment_request)
    hpcrun = await database_service.insert_hpcrun(
        job_id=JobId.slurm(12345),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id="test-correlation",
    )
    assert hpcrun.status == JobStatus.RUNNING

    # Cancel it
    update = JobStatusUpdate(job_id=JobId.slurm(12345), status=JobStatus.CANCELLED)
    await database_service.update_hpcrun_status(hpcrun_id=hpcrun.database_id, update=update)

    # Verify
    updated = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
    assert updated is not None
    assert updated.status == JobStatus.CANCELLED


@pytest.mark.asyncio
async def test_cancel_with_error_message(
    experiment_request: SimulationRequest,
    database_service: DatabaseServiceSQL,
) -> None:
    """Test that cancel can record an error message."""
    simulation = await database_service.insert_simulation(sim_request=experiment_request)
    hpcrun = await database_service.insert_hpcrun(
        job_id=JobId.slurm(12345),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id="test-correlation",
    )

    update = JobStatusUpdate(
        job_id=JobId.slurm(12345),
        status=JobStatus.CANCELLED,
        error_message="Cancelled by user",
    )
    await database_service.update_hpcrun_status(hpcrun_id=hpcrun.database_id, update=update)

    updated = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
    assert updated is not None
    assert updated.status == JobStatus.CANCELLED
    assert updated.error_message == "Cancelled by user"


@pytest.mark.asyncio
async def test_cancel_already_completed_is_noop(
    experiment_request: SimulationRequest,
    database_service: DatabaseServiceSQL,
) -> None:
    """Test that the cancel handler returns existing status for terminal jobs."""
    from tests.fixtures.simulation_service_mocks import ConcreteSimulationService
    from viva_api.common.handlers.simulations import cancel_simulation

    simulation = await database_service.insert_simulation(sim_request=experiment_request)
    await database_service.insert_hpcrun(
        job_id=JobId.slurm(12345),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id="test-correlation",
    )

    # Mark as completed
    update = JobStatusUpdate(job_id=JobId.slurm(12345), status=JobStatus.COMPLETED)
    hpcrun = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
    assert hpcrun is not None
    await database_service.update_hpcrun_status(hpcrun_id=hpcrun.database_id, update=update)

    # Cancel should return COMPLETED without calling cancel_job
    mock_service = ConcreteSimulationService()
    result = await cancel_simulation(
        db_service=database_service,
        simulation_service=mock_service,
        simulation_id=simulation.database_id,
    )
    assert result.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_cancel_chain_dispatch_campaign_terminates_every_seeds_current_job(
    experiment_request: SimulationRequest,
    database_service: DatabaseServiceSQL,
) -> None:
    """Backlog item 71 Phase 4, folding in backlog item 53: cancelling a
    chain-dispatch campaign (a row with chain_final_job_ids set, unlike the
    plain single-job rows the other tests in this file cover) must route
    through cancel_chain_campaign -- every seed's CURRENT in-flight job --
    not the plain single-job cancel_job path."""
    from viva_api.common.handlers.simulations import cancel_simulation
    from viva_api.simulation.simulation_service_ray import SimulationServiceRay

    simulation = await database_service.insert_simulation(sim_request=experiment_request)
    hpcrun = await database_service.insert_hpcrun(
        job_id=JobId.ray("parca-1"),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id="chain-campaign-test",
        chain_n_generations=3,
        chain_final_job_ids=[],
        chain_current_job_ids=["s0g1", "s1g0"],
        chain_current_generation=[1, 0],
        chain_parca_done=True,
    )
    assert hpcrun.status == JobStatus.RUNNING

    mock_ray_service = AsyncMock(spec=SimulationServiceRay)
    with patch(
        "viva_api.common.handlers.simulations.get_simulation_service_for_job",
        return_value=mock_ray_service,
    ):
        result = await cancel_simulation(
            db_service=database_service,
            simulation_service=mock_ray_service,
            simulation_id=simulation.database_id,
        )

    assert result.status == JobStatus.CANCELLED
    mock_ray_service.cancel_chain_campaign.assert_awaited_once()
    call_args = mock_ray_service.cancel_chain_campaign.call_args.args
    assert call_args[0].database_id == hpcrun.database_id
    mock_ray_service.cancel_job.assert_not_awaited()  # never the plain single-job path

    updated = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
    assert updated is not None
    assert updated.status == JobStatus.CANCELLED


@pytest.mark.asyncio
async def test_hpcrun_job_id_slurm(
    experiment_request: SimulationRequest,
    database_service: DatabaseServiceSQL,
) -> None:
    """Test that HpcRun.job_id round-trips correctly for SLURM backend."""
    simulation = await database_service.insert_simulation(sim_request=experiment_request)
    hpcrun = await database_service.insert_hpcrun(
        job_id=JobId.slurm(99999),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id="test-correlation",
    )
    assert hpcrun.job_id == JobId.slurm(99999)
    assert hpcrun.job_id.backend == JobBackend.SLURM
    assert hpcrun.job_id.as_slurm_int == 99999
    assert str(hpcrun.job_id) == "99999"


@pytest.mark.asyncio
async def test_hpcrun_job_id_k8s(
    experiment_request: SimulationRequest,
    database_service: DatabaseServiceSQL,
) -> None:
    """Test that HpcRun.job_id round-trips correctly for K8s backend."""
    simulation = await database_service.insert_simulation(sim_request=experiment_request)
    hpcrun = await database_service.insert_hpcrun(
        job_id=JobId.k8s("nf-sim-abc1234"),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id="test-correlation",
    )
    assert hpcrun.job_id == JobId.k8s("nf-sim-abc1234")
    assert hpcrun.job_id.backend == JobBackend.K8S
    assert str(hpcrun.job_id) == "nf-sim-abc1234"
