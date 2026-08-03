"""Tests for simulation endpoints.

This module contains:
1. Database-level tests for simulation CRUD operations
2. Integration tests for the /simulations endpoint via HTTP API

Run with: uv run pytest tests/api/ecoli/test_simulations.py -v

Prerequisites for API tests:
- SSH access to HPC (SLURM_SUBMIT_KEY_PATH configured)
- Config template exists at {HPC_REPO_BASE_PATH}/{hash}/vEcoli/configs/api_simulation_default_with_profile.json
"""

import asyncio
import os
import time
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from tests.fixtures.api_fixtures import SimulatorRepoInfo
from viva_api.api.main import app
from viva_api.common import handlers
from viva_api.common.models import JobStatus
from viva_api.common.ssh.ssh_service import SSHSessionService
from viva_api.config import get_settings
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import (
    SimulationRequest,
    SimulatorVersion,
)
from viva_api.simulation.simulation_service import SimulationServiceHpc

if TYPE_CHECKING:
    from viva_api.simulation.job_scheduler import JobScheduler

# Config file name expected in vEcoli/configs/
CONFIG_FILENAME = "api_simulation_default_ccam.json"

# Core router prefix (for simulator endpoints)
CORE_ROUTER = "/core/v1"


# =============================================================================
# Database-level tests (no SSH required)
# =============================================================================


@pytest.mark.asyncio
async def test_list_simulations(
    experiment_request: SimulationRequest,
    database_service: DatabaseServiceSQL,
) -> None:
    """Test listing simulations from the database."""
    n = 3
    inserted_sims = []
    for i in range(n):
        exp_request = experiment_request
        exp_request.experiment_id = f"{i}"
        exp_request.config.experiment_id = f"{i}"
        sim_i = await database_service.insert_simulation(sim_request=experiment_request)
        inserted_sims.append(sim_i.model_dump())
    all_sims = await database_service.list_simulations()
    assert len(inserted_sims) == n
    assert len(inserted_sims) == len(all_sims)


@pytest.mark.asyncio
async def test_get_simulation(database_service: DatabaseServiceSQL, experiment_request: SimulationRequest) -> None:
    """Test getting a single simulation from the database."""
    sim_i = await database_service.insert_simulation(experiment_request)

    fetched_i = await database_service.get_simulation(simulation_id=sim_i.database_id)
    assert fetched_i.model_dump() == sim_i.model_dump()  # type: ignore[union-attr]


# =============================================================================
# API integration tests (require SSH access)
# =============================================================================


async def _ensure_simulator_ready(
    client: AsyncClient,
    repo_info: SimulatorRepoInfo,
) -> SimulatorVersion:
    """
    Ensure simulator is registered and built using REST endpoints.

    1. GET /simulator/versions to find existing simulator
    2. POST /simulator/upload if not found (triggers build job)
    3. Poll GET /simulator/status until build completes

    Returns the simulator database ID.
    """
    # Check if simulator already exists
    versions_response = await client.get(f"{CORE_ROUTER}/simulator/versions")
    versions_response.raise_for_status()
    versions_data = versions_response.json()

    simulator_id: int | None = None
    simulator_data: dict[str, str] | None = None
    for sim in versions_data.get("versions", []):
        if (
            sim.get("git_commit_hash") == repo_info.commit_hash
            and sim.get("git_repo_url") == repo_info.url
            and sim.get("git_branch") == repo_info.branch
        ):
            simulator_id = int(sim["database_id"])
            print(f"  Found existing simulator: ID={simulator_id}")
            break

    # If not found, upload/create it
    if simulator_id is None:
        print(f"  Creating new simulator for {repo_info.commit_hash}...")
        upload_response = await client.post(
            f"{CORE_ROUTER}/simulator/upload",
            json={
                "git_commit_hash": repo_info.commit_hash,
                "git_repo_url": repo_info.url,
                "git_branch": repo_info.branch,
            },
        )
        assert upload_response.status_code == 200, f"Upload failed: {upload_response.text}"
        simulator_data = upload_response.json()
        simulator_id = int(simulator_data["database_id"])
        print(f"  Created simulator: ID={simulator_id}")

    # Poll build status until complete (build includes repo clone + image build)
    start_time = time.time()
    max_wait_seconds = 1800  # 30 minute timeout for build

    while time.time() - start_time < max_wait_seconds:
        try:
            status_response = await client.get(
                f"{CORE_ROUTER}/simulator/status",
                params={"simulator_id": simulator_id},
            )
            if status_response.status_code == 404:
                # No build job found - simulator was already built previously
                print("  Simulator already built (no pending build job)")
                break

            status_response.raise_for_status()
            status_data = status_response.json()
            build_status = status_data.get("status")

            elapsed = int(time.time() - start_time)
            if build_status in ["COMPLETED", "completed"]:
                print(f"  Build completed after {elapsed}s")
                break
            elif build_status in ["FAILED", "failed"]:
                pytest.fail(f"Simulator build failed: {status_data}")
            elif elapsed % 60 == 0 and elapsed > 0:
                print(f"  Build status: {build_status} ({elapsed}s elapsed)")

            await asyncio.sleep(10)
        except Exception as e:
            # If status check fails, assume simulator is ready (no active build)
            print(f"  Build status check: {e}, assuming ready")
            break
    else:
        pytest.fail(f"Simulator build did not complete within {max_wait_seconds}s")

    assert simulator_data is not None, "Simulator data not found or set."
    assert simulator_id is not None, "Simulator ID should be set"
    # return simulator_id
    return SimulatorVersion(**simulator_data)  # type: ignore[arg-type]


async def prepare_simulator(
    simulator_repo_info: SimulatorRepoInfo,
    simulation_service_slurm: SimulationServiceHpc,
    database_service: DatabaseServiceSQL,
) -> SimulatorVersion:
    # Insert a simulator directly into the database (no HPC access needed)
    return await handlers.simulators.upload_simulator(
        commit_hash=simulator_repo_info.commit_hash,
        git_repo_url=simulator_repo_info.url,
        git_branch=simulator_repo_info.branch,
        simulation_service_slurm=simulation_service_slurm,
        database_service=database_service,
    )


@asynccontextmanager
async def api_client() -> AsyncGenerator[httpx.AsyncClient]:
    transport = ASGITransport(app=app)
    client = AsyncClient(transport=transport, base_url="http://testserver")
    try:
        yield client
    finally:
        pass


@pytest.mark.integration
@pytest.mark.skipif(
    not Path(get_settings().slurm_submit_key_path).exists(),
    reason="slurm ssh key file not supplied",
)
@pytest.mark.skipif(
    not Path("/Volumes/SMS").exists(),
    reason="NFS Mount not connected — HPC repo/config data required",
)
@pytest.mark.asyncio
async def test_run_simulation_workflow_e2e(
    base_router: str,
    simulation_service_slurm: SimulationServiceHpc,
    database_service: DatabaseServiceSQL,
    simulator_repo_info: SimulatorRepoInfo,
    ssh_session_service: SSHSessionService,
    job_scheduler: "JobScheduler",
) -> None:
    """
    E2E integration test for POST /api/v1/simulations endpoint.

    This test uses only REST endpoints:
    1. GET/POST /simulator/* to ensure simulator is ready
    2. POST /simulations with query parameters
    3. Poll GET /simulations/{id}/status until workflow completes

    The job_scheduler fixture polls SLURM and updates the database with job statuses.

    Expected runtime: 30-60 minutes depending on cluster load.
    """
    artifacts_dir = Path("artifacts").absolute()
    artifact_files = ["simulation_workflow.sbatch", "workflow_config__sms-api-rke-dev.json"]
    for artifact in artifact_files:
        path = artifacts_dir / artifact
        if path.exists():
            os.remove(path)

    # Step 1: Ensure simulator is ready via REST endpoints
    async with api_client() as client:
        print("\nStep 1: Ensuring simulator is ready...")
        simulator: SimulatorVersion = await _ensure_simulator_ready(client=client, repo_info=simulator_repo_info)

    simulator_id = simulator.database_id
    print(f"  Using simulator ID: {simulator_id}")

    job_uuid = uuid.uuid4().hex[:8]
    experiment_id = f"sim{simulator_id}-e2e_test_{job_uuid}"

    print("\n=== Test: run_simulation_e2e ===")
    print(f"  Experiment ID: {experiment_id}")

    async with api_client() as client:
        # Step 2: POST to /simulations
        print("\nStep 2: POST /simulations")
        print(f"  Config file: {CONFIG_FILENAME}")

        response = await client.post(
            f"{base_router}/simulations",
            params={
                "simulator_id": simulator_id,
                "experiment_id": experiment_id,
                "simulation_config_filename": CONFIG_FILENAME,
                "num_generations": 1,  # Override for faster test
                "num_seeds": 1,  # Override for faster test
                "description": "Integration test via /simulations endpoint",
                "run_parca": False,  # Use cached simdata for faster test
            },
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"

        sim_response = response.json()
        assert "database_id" in sim_response, "Response should contain database_id"
        assert "job_id" in sim_response, "Response should contain job_id"
        assert sim_response["job_id"] is not None, "job_id should not be None"

        db_id = sim_response["database_id"]
        job_id = sim_response["job_id"]

        print(f"  Simulation DB ID: {db_id}")
        print(f"  SLURM Job ID: {job_id}")

        # Step 3: Poll for workflow completion
        print("\nStep 3: Polling for completion...")
        start_time = time.time()
        max_wait_seconds = 7200  # 2 hour timeout
        poll_interval = 30
        final_status = None
        while time.time() - start_time < max_wait_seconds:
            status_response = await client.get(f"{base_router}/simulations/{db_id}/status")
            assert status_response.status_code == 200, f"Status check failed: {status_response.text}"

            status_data = status_response.json()
            status = status_data.get("status")
            elapsed = int(time.time() - start_time)

            if status in [JobStatus.COMPLETED, JobStatus.FAILED]:
                print(f"\n  Job {job_id} finished after {elapsed}s")
                print(f"  Status: {status}")
                final_status = status
                break
            elif elapsed % 60 == 0:
                print(f"  Job {job_id} status: {status} ({elapsed}s elapsed)")

            await asyncio.sleep(poll_interval)
        else:
            pytest.fail(f"Workflow job {job_id} did not complete within {max_wait_seconds}s")

    # Verify job completed successfully
    assert final_status == JobStatus.COMPLETED, f"Workflow should complete successfully. Status: {final_status}"

    print("\n=== Test completed successfully! ===")
    print(f"  Experiment ID: {experiment_id}")
    print(f"  Simulation DB ID: {db_id}")


# =============================================================================
# Filter tests (no SSH required)
# =============================================================================


@pytest.mark.asyncio
async def test_list_simulations_filtered_by_experiment_id(
    database_service: DatabaseServiceSQL,
    experiment_request: SimulationRequest,
) -> None:
    """Test filtering simulations by comma-separated experiment IDs."""
    experiment_ids = ["filter-test-a", "filter-test-b", "filter-test-c", "filter-test-d", "filter-test-e"]
    for eid in experiment_ids:
        sim_request = experiment_request
        sim_request.experiment_id = eid
        sim_request.config.experiment_id = eid
        await database_service.insert_simulation(sim_request)

    filtered = await database_service.list_simulations_filtered(experiment_ids=["filter-test-a", "filter-test-c"])
    assert len(filtered) == 2
    filtered_eids = {s.experiment_id for s in filtered}
    assert filtered_eids == {"filter-test-a", "filter-test-c"}


@pytest.mark.asyncio
async def test_list_simulations_filtered_no_results(
    database_service: DatabaseServiceSQL,
) -> None:
    """Test filtering by an experiment ID that doesn't exist returns empty list."""
    filtered = await database_service.list_simulations_filtered(experiment_ids=["nonexistent-experiment-id"])
    assert filtered == []


@pytest.mark.asyncio
async def test_list_simulations_unfiltered_backwards_compat(
    database_service: DatabaseServiceSQL,
    experiment_request: SimulationRequest,
) -> None:
    """Test that calling list_simulations without filters returns all simulations."""
    for i in range(3):
        eid = f"compat-test-{i}"
        sim_request = experiment_request
        sim_request.experiment_id = eid
        sim_request.config.experiment_id = eid
        await database_service.insert_simulation(sim_request)

    all_sims = await database_service.list_simulations()
    assert len(all_sims) >= 3


async def _insert_tagged(
    database_service: DatabaseServiceSQL,
    experiment_request: SimulationRequest,
    experiment_id: str,
    tags: list[str],
) -> None:
    experiment_request.experiment_id = experiment_id
    experiment_request.config.experiment_id = experiment_id
    experiment_request.tags = tags
    await database_service.insert_simulation(experiment_request)


@pytest.mark.asyncio
async def test_list_simulation_tags_endpoint(
    base_router: str,
    database_service: DatabaseServiceSQL,
    experiment_request: SimulationRequest,
) -> None:
    """The discovery endpoint reflects tags actually present in the database."""
    from httpx import ASGITransport, AsyncClient

    from viva_api.api.main import app

    await _insert_tagged(database_service, experiment_request, "tagdisc-a", ["disc-bundle"])
    await _insert_tagged(database_service, experiment_request, "tagdisc-b", ["disc-bundle"])

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get(f"{base_router}/simulations/tags")
        assert response.status_code == 200
        tags = response.json()
        assert isinstance(tags, dict)
        assert set(tags["disc-bundle"]) == {"tagdisc-a", "tagdisc-b"}


@pytest.mark.asyncio
async def test_list_simulations_filtered_by_tag(
    base_router: str,
    database_service: DatabaseServiceSQL,
    experiment_request: SimulationRequest,
) -> None:
    """Filtering by tag returns exactly the simulations carrying that tag."""
    from httpx import ASGITransport, AsyncClient

    from viva_api.api.main import app

    await _insert_tagged(database_service, experiment_request, "tagfilt-x", ["filt-bundle"])
    await _insert_tagged(database_service, experiment_request, "tagfilt-y", ["filt-bundle"])
    await _insert_tagged(database_service, experiment_request, "tagfilt-z", ["other"])

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get(f"{base_router}/simulations", params={"tag": "filt-bundle"})
        assert response.status_code == 200
        simulations = response.json()
        returned = {s["experiment_id"] for s in simulations}
        assert returned == {"tagfilt-x", "tagfilt-y"}
        assert all("filt-bundle" in s["tags"] for s in simulations)


@pytest.mark.asyncio
async def test_list_simulations_filtered_unknown_tag(
    base_router: str,
    database_service: DatabaseServiceSQL,
) -> None:
    """An unknown tag matches nothing and returns an empty 200 (tags are free-form data)."""
    from httpx import ASGITransport, AsyncClient

    from viva_api.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get(f"{base_router}/simulations", params={"tag": "no-such-tag-xyz"})
        assert response.status_code == 200
        assert response.json() == []


@pytest.mark.asyncio
async def test_add_simulation_tags_endpoint(
    base_router: str,
    database_service: DatabaseServiceSQL,
    experiment_request: SimulationRequest,
) -> None:
    """POST /simulations/{id}/tags union-merges tags onto an existing simulation."""
    from httpx import ASGITransport, AsyncClient

    from viva_api.api.main import app

    experiment_request.experiment_id = "tagadd-1"
    experiment_request.config.experiment_id = "tagadd-1"
    experiment_request.tags = ["initial"]
    sim = await database_service.insert_simulation(experiment_request)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            f"{base_router}/simulations/{sim.database_id}/tags",
            json={"tags": ["cd1", "initial"]},  # 'initial' is a duplicate, must not double
        )
        assert response.status_code == 200
        assert sorted(response.json()["tags"]) == ["cd1", "initial"]

        missing = await client.post(f"{base_router}/simulations/99999999/tags", json={"tags": ["x"]})
        assert missing.status_code == 404
