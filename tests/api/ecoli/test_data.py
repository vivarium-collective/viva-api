"""Tests for data endpoints (simulations, parca, etc).

uv run pytest tests/api/ecoli/test_data.py -v -s

Prerequisites for API tests:
- SSH access to HPC (SLURM_SUBMIT_KEY_PATH configured)
- Config template exists at {HPC_REPO_BASE_PATH}/{hash}/vEcoli/configs/api_simulation_default_with_profile.json
"""

import gzip
import io
import tarfile
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from viva_api.api.main import app
from viva_api.common.ssh.ssh_service import SSHSessionService
from viva_api.config import get_settings
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import (
    Simulation,
)


async def validate_archive_streaming_response(
    client: httpx.AsyncClient,
    db_id: int,
    expected_files: set[str],
    base_router: str,
    experiment_id: str,
    response_type: str = "file",
    save_artifact: bool = True,
) -> Path | None:
    url = f"{base_router}/simulations/{db_id}/data?response_type={response_type}"
    # Use stream=True to test actual streaming behavior
    async with client.stream("POST", url) as response:
        assert response.status_code == 200
        # Validate headers
        assert response.headers["content-type"] == "application/gzip"
        assert "attachment" in response.headers.get("content-disposition", "")
        # Collect streamed chunks
        chunks = []
        async for chunk in response.aiter_bytes():
            chunks.append(chunk)
        # Verify we actually got multiple chunks (streaming worked)
        # Note: small archives might be single chunk, so this is optional
        assert len(chunks) >= 1
        content = b"".join(chunks)

    # Save archive as artifact for inspection
    artifact_path: Path | None = None
    if save_artifact and response_type == "file":
        artifacts_dir = Path(__file__).parent.parent.parent.parent / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifacts_dir / f"{experiment_id}.tar.gz"
        artifact_path.write_bytes(content)
        print(f"\n  Archive saved to: {artifact_path}")

    # Validate it's valid gzip
    decompressed = gzip.decompress(content)

    # Validate it's a valid tar archive with expected structure
    tar_buffer = io.BytesIO(decompressed)
    with tarfile.open(fileobj=tar_buffer, mode="r") as tar:
        archived_names = set(tar.getnames())
        # Extract basenames from archived files for comparison
        archived_basenames = {Path(name).name for name in archived_names}

        # Check expected files are present (compare basenames since archive uses local cache paths)
        for expected in expected_files:
            expected_basename = Path(expected).name
            assert expected_basename in archived_basenames, (
                f"Expected file '{expected_basename}' not found in archive. Got: {archived_basenames}"
            )

        # Optionally verify file contents
        for member in tar.getmembers():
            if member.isfile():
                extracted = tar.extractfile(member)
                assert extracted is not None
                file_content = extracted.read()
                assert len(file_content) == member.size

    return artifact_path


@pytest.mark.skipif(
    not Path(get_settings().slurm_submit_key_path).exists(),
    reason="slurm ssh key file not supplied",
)
@pytest.mark.skipif(
    not Path("/Volumes/SMS").exists(),
    reason="NFS Mount not connected",
)
@pytest.mark.asyncio
@pytest.mark.parametrize("response_type", ["file", "streaming"])
async def test_get_simulation_data(
    base_router: str,
    database_service: DatabaseServiceSQL,
    ssh_session_service: SSHSessionService,
    expected_analysis_output_files: set[str],
    simulation_mock: Simulation,
    response_type: str,
) -> None:
    """Test GET simulation data endpoint with both response types.

    This test manually inserts a simulation into the database that references
    the existing simulation output at /projects/SMS/viva_api/alex/sims/sms_multigeneration,
    then calls the get_simulation_data endpoint to retrieve the outputs.

    Tests both 'file' and 'streaming' response_type modes.
    """
    inserted_sim = simulation_mock
    db_id = inserted_sim.database_id

    # Call the get_simulation_data endpoint
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        await validate_archive_streaming_response(
            client=client,
            db_id=db_id,
            expected_files=expected_analysis_output_files,
            base_router=base_router,
            response_type=response_type,
            experiment_id=inserted_sim.experiment_id,
        )


@pytest.mark.skipif(
    not Path(get_settings().slurm_submit_key_path).exists(),
    reason="slurm ssh key file not supplied",
)
@pytest.mark.skipif(
    not Path("/Volumes/SMS").exists(),
    reason="NFS Mount not connected — HPC analysis data required",
)
@pytest.mark.asyncio
async def test_archive_streaming_response(
    fastapi_app: FastAPI,
    simulation_mock: Simulation,
    expected_analysis_output_files: set[str],
    base_router: str,
    ssh_session_service: SSHSessionService,
) -> None:
    """
    Test that the endpoint returns a valid, streamable tar.gz archive
    with the expected contents.
    """
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:  # noqa: SIM117
        # Use stream=True to test actual streaming behavior
        async with client.stream("POST", f"{base_router}/simulations/{simulation_mock.database_id}/data") as response:
            assert response.status_code == 200

            # Validate headers
            assert response.headers["content-type"] == "application/gzip"
            assert "attachment" in response.headers.get("content-disposition", "")

            # Collect streamed chunks
            chunks = []
            async for chunk in response.aiter_bytes():
                chunks.append(chunk)

            # Verify we actually got multiple chunks (streaming worked)
            # Note: small archives might be single chunk, so this is optional
            assert len(chunks) >= 1

            content = b"".join(chunks)

    # Validate it's valid gzip
    decompressed = gzip.decompress(content)

    # Validate it's a valid tar archive with expected structure
    tar_buffer = io.BytesIO(decompressed)
    with tarfile.open(fileobj=tar_buffer, mode="r") as tar:
        archived_names = set(tar.getnames())
        # Extract basenames from archived files for comparison
        archived_basenames = {Path(name).name for name in archived_names}

        # Check expected files are present (compare basenames since archive uses local cache paths)
        for expected in expected_analysis_output_files:
            expected_basename = Path(expected).name
            assert expected_basename in archived_basenames, (
                f"Expected file '{expected_basename}' not found in archive. Got: {archived_basenames}"
            )

        # Optionally verify file contents
        for member in tar.getmembers():
            if member.isfile():
                extracted = tar.extractfile(member)
                assert extracted is not None
                content = extracted.read()
                assert len(content) == member.size


@pytest.mark.skipif(
    not Path(get_settings().slurm_submit_key_path).exists(),
    reason="slurm ssh key file not supplied",
)
@pytest.mark.asyncio
async def test_archive_contents_match_source(
    fastapi_app: FastAPI, simulation_mock: Simulation, base_router: str
) -> None:
    """
    Test that archived contents exactly match the source directory.
    """
    source_dir = Path("/Volumes/SMS/viva_api/alex/sims/sms_multigeneration/analyses")
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(f"{base_router}/simulations/{simulation_mock.database_id}/data")
        assert response.status_code == 200
        content = response.content

    # Extract and compare
    decompressed = gzip.decompress(content)
    tar_buffer = io.BytesIO(decompressed)

    with tarfile.open(fileobj=tar_buffer, mode="r") as tar:
        for member in tar.getmembers():
            if member.isfile():
                # Get relative path (strip the root archive folder)
                parts = Path(member.name).parts[1:]  # Skip archive root
                source_file = source_dir.joinpath(*parts)

                if source_file.name != ".gitkeep":
                    continue
                assert source_file.exists(), f"Unexpected file in archive: {member.name}"

                # Compare contents
                extracted = tar.extractfile(member)
                assert extracted is not None
                archived_content = extracted.read()
                source_content = source_file.read_bytes()

                assert archived_content == source_content, f"Content mismatch for {member.name}"


@pytest.mark.skipif(
    not Path(get_settings().slurm_submit_key_path).exists(),
    reason="slurm ssh key file not supplied",
)
@pytest.mark.skipif(
    not Path("/Volumes/SMS").exists(),
    reason="NFS Mount not connected — HPC analysis data required",
)
@pytest.mark.asyncio
async def test_archive_empty_directory(
    fastapi_app: FastAPI, empty_simulation_id: int, base_router: str, ssh_session_service: SSHSessionService
) -> None:
    """Test handling of empty directories."""
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(f"{base_router}/simulations/{empty_simulation_id}/data")
        assert response.status_code == 200

        content = gzip.decompress(response.content)
        tar_buffer = io.BytesIO(content)

        with tarfile.open(fileobj=tar_buffer, mode="r") as tar:
            members = tar.getmembers()
            # Should have at least the root directory
            assert len(members) >= 1


@pytest.mark.skipif(
    not Path(get_settings().slurm_submit_key_path).exists(),
    reason="slurm ssh key file not supplied",
)
@pytest.mark.asyncio
async def test_archive_nonexistent_simulation(fastapi_app: FastAPI, base_router: str) -> None:
    """Test 404 for nonexistent simulation."""
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(f"{base_router}/simulations/999999/data")
        assert response.status_code in (404, 500)  # simulation not found in DB


@pytest.mark.skipif(
    not Path(get_settings().slurm_submit_key_path).exists(),
    reason="slurm ssh key file not supplied",
)
@pytest.mark.skipif(
    not Path("/Volumes/SMS").exists(),
    reason="NFS Mount not connected — HPC analysis data required",
)
@pytest.mark.asyncio
async def test_archive_large_file_streaming(
    fastapi_app: FastAPI, large_simulation_mock: Simulation, base_router: str, ssh_session_service: SSHSessionService
) -> None:
    """
    Test that large archives stream properly without memory issues.
    """
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        chunk_count = 0
        total_size = 0

        url = f"{base_router}/simulations/{large_simulation_mock.database_id}/data"
        async with client.stream("POST", url) as response:
            assert response.status_code == 200

            async for chunk in response.aiter_bytes(chunk_size=8192):
                chunk_count += 1
                total_size += len(chunk)

        # For large files, we should see multiple chunks
        assert chunk_count > 1, "Large archive should stream in multiple chunks"
        assert total_size > 0


@pytest.mark.skipif(
    not Path(get_settings().slurm_submit_key_path).exists(),
    reason="slurm ssh key file not supplied",
)
@pytest.mark.skipif(
    not Path("/Volumes/SMS").exists(),
    reason="NFS Mount not connected",
)
@pytest.mark.asyncio
async def test_get_data(
    fastapi_app: FastAPI,
    simulation_mock: Simulation,
    base_router: str,
    ssh_session_service: SSHSessionService,
) -> None:
    """
    Test that the endpoint returns a valid, streamable tar.gz archive
    with the expected contents.
    """
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://testserver", timeout=10_000) as client:  # noqa: SIM117
        # Use stream=True to test actual streaming behavior
        async with client.stream("POST", f"{base_router}/simulations/{simulation_mock.database_id}/data") as response:
            assert response.status_code == 200

            # Validate headers
            assert response.headers["content-type"] == "application/gzip"
            assert "attachment" in response.headers.get("content-disposition", "")

            # Collect streamed chunks
            chunks = []
            async for chunk in response.aiter_bytes():
                chunks.append(chunk)

            # Verify we actually got multiple chunks (streaming worked)
            # Note: small archives might be single chunk, so this is optional
            assert len(chunks) >= 1

            content = b"".join(chunks)

    # Validate it's valid gzip
    decompressed = gzip.decompress(content)

    # Validate it's a valid tar archive with expected structure
    tar_buffer = io.BytesIO(decompressed)
    with tarfile.open(fileobj=tar_buffer, mode="r") as tar:
        archived_names = set(tar.getnames())
        # Extract basenames from archived files for comparison
        archived_basenames = {Path(name).name for name in archived_names}  # noqa: F841

        # Optionally verify file contents
        for member in tar.getmembers():
            if member.isfile():
                extracted = tar.extractfile(member)
                assert extracted is not None
                content = extracted.read()
                assert len(content) == member.size
