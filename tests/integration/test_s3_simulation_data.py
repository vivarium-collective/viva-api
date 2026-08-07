"""Integration tests for POST /api/v1/simulations/{id}/data — S3 output download.

Tests the full S3 download path using real data in the stanford-test bucket.
Requires AWS credentials with access to the test bucket (set in .dev_env).

Run with:
    uv run pytest tests/integration/test_s3_simulation_data.py -v -s

Prerequisites:
- Valid AWS credentials (AWS_ACCESS_KEY_ID, etc. in .dev_env or environment)
- Docker running (for Postgres testcontainer)
"""

import os
import tarfile
import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pytest
import pytest_asyncio
from starlette.responses import FileResponse

from viva_api.common.handlers.simulations import (
    SimulationAnalysisDataResponseType,
    _download_outputs_from_s3,
    get_simulation_outputs,
)
from viva_api.common.models import JobId
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
# Parse TEST_BUCKET_EXPERIMENT_OUTDIR into bucket, prefix, experiment_id
# Loaded from .dev_env via config.py's module-level load_dotenv()
# ---------------------------------------------------------------------------
_TEST_OUTDIR = os.environ.get("TEST_BUCKET_EXPERIMENT_OUTDIR", "")

_parsed = urlparse(_TEST_OUTDIR)
_TEST_BUCKET = _parsed.netloc  # e.g. smsvpctest-shared-...
_TEST_PREFIX = _parsed.path.strip("/")  # e.g. vecoli-output/baseline_20260331-210125
_TEST_EXPERIMENT_ID = _TEST_PREFIX.rsplit("/", 1)[-1] if "/" in _TEST_PREFIX else _TEST_PREFIX

pytestmark = pytest.mark.skipif(
    not _TEST_OUTDIR,
    reason="TEST_BUCKET_EXPERIMENT_OUTDIR not set — skipping S3 simulation data tests",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture()
async def s3_file_service() -> AsyncGenerator[FileServiceS3, Any]:
    """Create a real S3 FileService and set it as global."""
    fs = FileServiceS3()
    saved = get_file_service()
    set_file_service(fs)
    yield fs
    await fs.close()
    set_file_service(saved)


@pytest.fixture()
def local_cache(tmp_path: Path) -> Path:
    """Provide a clean temp directory for downloaded files."""
    cache = tmp_path / _TEST_EXPERIMENT_ID
    cache.mkdir()
    return cache


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDownloadOutputsFromS3:
    """Test the _download_outputs_from_s3 helper directly against real S3."""

    @pytest.mark.asyncio
    async def test_downloads_analyses_and_workflow_config(
        self, s3_file_service: FileServiceS3, local_cache: Path
    ) -> None:
        """Verify that _download_outputs_from_s3 downloads analyses/ and workflow_config.json."""
        await _download_outputs_from_s3(_TEST_EXPERIMENT_ID, local_cache)

        # --- analyses/ should be populated ---
        analyses_dir = local_cache / "analyses"
        assert analyses_dir.exists(), "analyses/ directory was not created"

        analysis_files = list(analyses_dir.rglob("*"))
        analysis_files = [f for f in analysis_files if f.is_file()]
        assert len(analysis_files) > 0, "No analysis files downloaded"

        tsv_files = [f for f in analysis_files if f.suffix == ".tsv"]
        json_files = [f for f in analysis_files if f.suffix == ".json"]
        assert len(tsv_files) > 0, "No .tsv files in analyses/"
        assert len(json_files) > 0, "No .json (metadata) files in analyses/"

        # Every file should be either .tsv or .json
        for f in analysis_files:
            assert f.suffix in (".tsv", ".json"), f"Unexpected file type in analyses/: {f}"

        # --- nextflow/workflow_config.json ---
        wf_config = local_cache / "nextflow" / "workflow_config.json"
        assert wf_config.exists(), "nextflow/workflow_config.json was not downloaded"
        assert wf_config.stat().st_size > 0, "workflow_config.json is empty"

    @pytest.mark.asyncio
    async def test_does_not_download_history_or_daughter_states(
        self, s3_file_service: FileServiceS3, local_cache: Path
    ) -> None:
        """Verify large data directories (history/, daughter_states/, configuration/) are excluded."""
        await _download_outputs_from_s3(_TEST_EXPERIMENT_ID, local_cache)

        for excluded_dir in ("history", "daughter_states", "configuration"):
            excluded = local_cache / excluded_dir
            assert not excluded.exists(), f"{excluded_dir}/ should not be downloaded"

    @pytest.mark.asyncio
    async def test_preserves_directory_structure(self, s3_file_service: FileServiceS3, local_cache: Path) -> None:
        """Verify the analyses directory structure is preserved locally."""
        await _download_outputs_from_s3(_TEST_EXPERIMENT_ID, local_cache)

        # Should have variant=0/plots/analysis=*/ structure
        variant_dirs = list((local_cache / "analyses").glob("variant=*"))
        assert len(variant_dirs) > 0, "No variant=* directories found in analyses/"

        plots_dirs = list((local_cache / "analyses").rglob("plots"))
        assert len(plots_dirs) > 0, "No plots/ directories found in analyses/"

        analysis_dirs = list((local_cache / "analyses").rglob("analysis=*"))
        assert len(analysis_dirs) > 0, "No analysis=* directories found"

    @pytest.mark.asyncio
    async def test_skips_already_cached_files(self, s3_file_service: FileServiceS3, local_cache: Path) -> None:
        """Second download should be a no-op for already-cached files."""
        await _download_outputs_from_s3(_TEST_EXPERIMENT_ID, local_cache)

        # Record mtimes
        analyses_dir = local_cache / "analyses"
        first_pass_files = {f: f.stat().st_mtime for f in analyses_dir.rglob("*") if f.is_file()}
        assert len(first_pass_files) > 0

        # Download again
        await _download_outputs_from_s3(_TEST_EXPERIMENT_ID, local_cache)

        # mtimes should be unchanged (files were not re-downloaded)
        for f, mtime in first_pass_files.items():
            assert f.stat().st_mtime == mtime, f"File {f} was re-downloaded unexpectedly"


class TestGetSimulationOutputsS3:
    """Test the full get_simulation_outputs handler with a real DB record and real S3."""

    @pytest_asyncio.fixture()
    async def simulation_id(self, database_service: DatabaseServiceSQL, request: pytest.FixtureRequest) -> int:
        """Insert a minimal simulation + HpcRun pointing at the test experiment_id."""
        # Use test node id to create a unique experiment_id per test (DB has unique constraint)
        test_suffix = request.node.name.replace("test_", "")[:8]
        experiment_id = f"{_TEST_EXPERIMENT_ID}__{test_suffix}"

        # Simulator (get-or-create to handle shared DB across tests)
        try:
            simulator = await database_service.insert_simulator(
                git_commit_hash="abc123test", git_repo_url="https://github.com/test/vecoli", git_branch="main"
            )
        except RuntimeError:
            simulators = await database_service.list_simulators()
            simulator = next(s for s in simulators if s.git_commit_hash == "abc123test")

        # Parca dataset
        parca_ds = await database_service.insert_parca_dataset(
            parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
        )

        # Simulation — use unique experiment_id but the handler reads it from config,
        # so set config.experiment_id to the REAL test experiment_id for S3 path resolution
        config = SimulationConfig(experiment_id=_TEST_EXPERIMENT_ID, generations=1, n_init_sims=1)  # type: ignore[call-arg]
        sim_request = SimulationRequest(
            config=config,
            simulator_id=simulator.database_id,
            parca_dataset_id=parca_ds.database_id,
            simulation_config_filename="api_simulation_default.json",
            experiment_id=experiment_id,
        )
        simulation = await database_service.insert_simulation(sim_request=sim_request)

        # HpcRun with K8S backend so it routes to S3 download path
        await database_service.insert_hpcrun(
            job_id=JobId.k8s(f"sim-{experiment_id}"),
            job_type=JobType.SIMULATION,
            ref_id=simulation.database_id,
            correlation_id="test-correlation",
        )

        return simulation.database_id

    @pytest.mark.asyncio
    async def test_streaming_response_returns_valid_tar_gz(
        self,
        database_service: DatabaseServiceSQL,
        s3_file_service: FileServiceS3,
        simulation_id: int,
    ) -> None:
        """Full handler test: streaming response produces a valid tar.gz with expected contents."""
        settings = get_settings()

        response = await get_simulation_outputs(
            db_service=database_service,
            simulation_id=simulation_id,
            hpc_sim_base_path=settings.hpc_sim_base_path,
            data_response_type=SimulationAnalysisDataResponseType.STREAMING,
        )

        assert response.media_type == "application/gzip"

        # Collect the streamed bytes
        chunks: list[bytes | memoryview[int]] = []
        async for chunk in response.body_iterator:  # type: ignore[union-attr]
            if isinstance(chunk, str):
                chunk = chunk.encode()
            chunks.append(chunk)
        archive_bytes = b"".join(chunks)
        assert len(archive_bytes) > 0, "Streamed archive is empty"

        # Write to temp file and extract
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
            tmp.write(archive_bytes)
            tmp_path = tmp.name

        try:
            with tarfile.open(tmp_path, "r:gz") as tar:
                members = tar.getnames()

            # Should contain analyses files
            analyses_members = [m for m in members if "/analyses/" in m or m.startswith("analyses/")]
            assert len(analyses_members) > 0, f"No analyses/ entries in archive. Members: {members}"

            tsv_members = [m for m in members if m.endswith(".tsv")]
            json_members = [m for m in members if m.endswith(".json")]
            assert len(tsv_members) > 0, "No .tsv files in archive"
            assert len(json_members) > 0, "No .json files in archive"

            # Should contain workflow_config.json
            wf_config_members = [m for m in members if "workflow_config.json" in m]
            assert len(wf_config_members) > 0, f"workflow_config.json not in archive. Members: {members}"

            # Should NOT contain history, daughter_states, etc.
            for excluded in ("history", "daughter_states", "configuration"):
                excluded_members = [m for m in members if f"/{excluded}/" in m]
                assert len(excluded_members) == 0, f"{excluded}/ should not be in archive"
        finally:
            os.unlink(tmp_path)

    @pytest.mark.asyncio
    async def test_file_response_returns_valid_tar_gz(
        self,
        database_service: DatabaseServiceSQL,
        s3_file_service: FileServiceS3,
        simulation_id: int,
    ) -> None:
        """Full handler test: FILE response type produces a downloadable tar.gz."""
        from fastapi import BackgroundTasks

        settings = get_settings()
        bg_tasks = BackgroundTasks()

        response = await get_simulation_outputs(
            db_service=database_service,
            simulation_id=simulation_id,
            hpc_sim_base_path=settings.hpc_sim_base_path,
            data_response_type=SimulationAnalysisDataResponseType.FILE,
            bg_tasks=bg_tasks,
        )

        assert response.media_type == "application/gzip"
        assert isinstance(response, FileResponse)
        archive_path = Path(response.path)
        assert archive_path.exists(), "Archive file was not created"
        assert archive_path.stat().st_size > 0, "Archive file is empty"

        with tarfile.open(archive_path, "r:gz") as tar:
            members = tar.getnames()

        tsv_members = [m for m in members if m.endswith(".tsv")]
        json_members = [m for m in members if m.endswith(".json")]
        assert len(tsv_members) > 0, "No .tsv files in archive"
        assert len(json_members) > 0, "No .json files in archive"

        wf_config_members = [m for m in members if "workflow_config.json" in m]
        assert len(wf_config_members) > 0, "workflow_config.json not in archive"

        # Cleanup (normally bg_tasks handles this)
        if archive_path.exists():
            archive_path.unlink()
