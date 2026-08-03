"""E2E tests for POST /api/v1/simulations/{id}/data — S3-backed output download.

Uses httpx AsyncClient + ASGITransport to test the full HTTP path through
the FastAPI app, downloading real simulation outputs from the stanford-test
S3 bucket referenced by TEST_BUCKET_EXPERIMENT_OUTDIR in .dev_env.

Run with:
    uv run pytest tests/api/ecoli/test_outputs.py -v -s

Prerequisites:
- Valid AWS credentials (AWS_ACCESS_KEY_ID, etc. in .dev_env or environment)
- Docker running (for Postgres testcontainer)
- TEST_BUCKET_EXPERIMENT_OUTDIR set in .dev_env
"""

import gzip
import io
import os
import tarfile
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from viva_api.api.main import app
from viva_api.common.models import JobId
from viva_api.common.simulator_defaults import DEFAULT_SIMULATOR
from viva_api.common.storage.file_service_s3 import FileServiceS3
from viva_api.config import get_settings
from viva_api.dependencies import get_file_service, set_file_service
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import (
    JobType,
    ParcaDatasetRequest,
    ParcaOptions,
    SimulationConfig,
    SimulationRequest,
)

# ---------------------------------------------------------------------------
# Derive experiment_id from TEST_BUCKET_EXPERIMENT_OUTDIR
# (last path segment of the S3 URI, e.g. "baseline_20260331-210125")
# ---------------------------------------------------------------------------
_TEST_OUTDIR = os.environ.get("TEST_BUCKET_EXPERIMENT_OUTDIR", "")
_TEST_EXPERIMENT_ID = urlparse(_TEST_OUTDIR).path.strip("/").rsplit("/", 1)[-1] if _TEST_OUTDIR else ""

pytestmark = pytest.mark.skipif(
    not _TEST_OUTDIR,
    reason="TEST_BUCKET_EXPERIMENT_OUTDIR not set — skipping S3 output e2e tests",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture()
async def s3_file_service() -> AsyncGenerator[FileServiceS3, Any]:
    """Wire a real S3 FileService into the global dependency so the handler uses it."""
    fs = FileServiceS3()
    saved = get_file_service()
    set_file_service(fs)
    yield fs
    await fs.close()
    set_file_service(saved)


@pytest_asyncio.fixture()
async def simulation_id(database_service: DatabaseServiceSQL) -> int:
    """Insert a simulation record whose config.experiment_id points at the test bucket data."""
    # Check if already inserted (module-scoped postgres container persists data)
    existing = await database_service.get_simulation_by_experiment_id(_TEST_EXPERIMENT_ID)
    if existing is not None:
        # Ensure an HpcRun with K8S backend exists for this simulation
        hpc_run = await database_service.get_hpcrun_by_ref(ref_id=existing.database_id, job_type=JobType.SIMULATION)
        if hpc_run is not None:
            return existing.database_id

    # Simulator
    unique_hash = f"test_{uuid.uuid4().hex[:7]}"
    simulator = await database_service.insert_simulator(
        git_commit_hash=unique_hash,
        git_repo_url=DEFAULT_SIMULATOR.git_repo_url,
        git_branch=DEFAULT_SIMULATOR.git_branch,
    )

    # Parca dataset
    parca_ds = await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )

    # Simulation — config.experiment_id drives the S3 prefix lookup
    config = SimulationConfig(experiment_id=_TEST_EXPERIMENT_ID, generations=1, n_init_sims=1)  # type: ignore[call-arg]
    sim_request = SimulationRequest(
        config=config,
        simulator_id=simulator.database_id,
        parca_dataset_id=parca_ds.database_id,
        simulation_config_filename="api_simulation_default.json",
        experiment_id=_TEST_EXPERIMENT_ID,
    )
    simulation = await database_service.insert_simulation(sim_request=sim_request)

    # HpcRun with K8S backend → routes to S3 download path
    await database_service.insert_hpcrun(
        job_id=JobId.k8s(f"sim-{_TEST_EXPERIMENT_ID}"),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id="e2e-test",
    )

    return simulation.database_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.skipif(
    len(get_settings().storage_s3_bucket) == 0 or len(get_settings().storage_s3_region) == 0,
    reason="aws s3 settings not provided",
)
@pytest.mark.skipif(
    os.environ.get("RUN_S3_TESTS", "") != "1",
    reason="RUN_S3_TESTS=1 not set — requires active AWS credentials",
)
@pytest.mark.parametrize("response_type", ["file", "streaming"])
async def test_get_simulation_data_from_s3(
    database_service: DatabaseServiceSQL,
    s3_file_service: FileServiceS3,
    simulation_id: int,
    base_router: str,
    response_type: str,
) -> None:
    """E2E: POST /simulations/{id}/data returns a valid tar.gz with analyses + workflow_config.json."""
    transport = ASGITransport(app=app)
    url = f"{base_router}/simulations/{simulation_id}/data?response_type={response_type}"

    async with (
        AsyncClient(transport=transport, base_url="http://testserver") as client,
        client.stream("POST", url) as response,
    ):
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.aread()!r}"
        assert response.headers["content-type"] == "application/gzip"
        assert "attachment" in response.headers.get("content-disposition", "")

        chunks: list[bytes] = []
        async for chunk in response.aiter_bytes():
            chunks.append(chunk)

    content = b"".join(chunks)
    assert len(content) > 0, "Response body is empty"

    # Decompress and inspect tar archive
    decompressed = gzip.decompress(content)
    tar_buffer = io.BytesIO(decompressed)

    with tarfile.open(fileobj=tar_buffer, mode="r") as tar:
        members = tar.getnames()

        # --- analyses/ must be present with .tsv and .json files ---
        analyses_members = [m for m in members if "analyses/" in m]
        assert len(analyses_members) > 0, f"No analyses/ entries in archive. Members: {members}"

        tsv_files = [m for m in members if m.endswith(".tsv")]
        json_files = [m for m in members if m.endswith(".json")]
        assert len(tsv_files) > 0, f"No .tsv files in archive. Members: {members}"
        assert len(json_files) > 0, f"No .json files in archive. Members: {members}"

        # --- workflow_config.json must be present ---
        wf_configs = [m for m in members if "workflow_config.json" in m]
        assert len(wf_configs) > 0, f"workflow_config.json not in archive. Members: {members}"

        # --- large data dirs must NOT be present ---
        for excluded in ("history/", "daughter_states/", "configuration/"):
            bad = [m for m in members if excluded in m]
            assert len(bad) == 0, f"Excluded dir '{excluded}' found in archive: {bad}"

        # --- every file member should have non-zero extractable content ---
        for member in tar.getmembers():
            if member.isfile():
                extracted = tar.extractfile(member)
                if extracted is not None:
                    data = extracted.read()
                    assert len(data) == member.size, f"Size mismatch for {member.name}"

    # Save artifact for inspection
    if response_type == "file":
        artifacts_dir = Path(__file__).parent.parent.parent.parent / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifacts_dir / f"{_TEST_EXPERIMENT_ID}.tar.gz"
        artifact_path.write_bytes(content)
        print(f"\n  Archive saved to: {artifact_path}")
