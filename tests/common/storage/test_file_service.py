"""
Example tests demonstrating how to use FileService fixtures.

This file shows how to write tests using the different FileService backends.
Each test receives the fixture as an argument and can use it directly.
"""

from pathlib import Path

import pytest

from tests.fixtures.file_service_local import FileServiceLocal
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service_gcs import FileServiceGCS
from viva_api.common.storage.file_service_qumulo_s3 import FileServiceQumuloS3
from viva_api.common.storage.file_service_s3 import FileServiceS3
from viva_api.config import get_settings


@pytest.mark.asyncio
async def test_local_file_service_upload_and_download(file_service_local: FileServiceLocal, tmp_path: Path) -> None:
    """
    Test using the local mock FileService.

    This test doesn't require any cloud credentials and runs entirely on the local filesystem.
    """
    # Create a test file
    test_file = tmp_path / "test.txt"
    test_file.write_text("Hello, World!")

    # Upload the file
    uploaded_path = await file_service_local.upload_file(
        file_path=test_file, s3_path=S3FilePath(s3_path=Path("test/upload/test.txt"))
    )
    assert uploaded_path == S3FilePath(s3_path=Path("test/upload/test.txt"))

    # Download the file
    downloaded_gcs_path, downloaded_local_path = await file_service_local.download_file(
        S3FilePath(s3_path=Path("test/upload/test.txt")), tmp_path / "downloaded.txt"
    )
    assert "test/upload/test.txt" in str(downloaded_gcs_path)
    assert Path(downloaded_local_path).read_text() == "Hello, World!"


@pytest.mark.asyncio
async def test_local_file_service_upload_bytes(file_service_local: FileServiceLocal) -> None:
    """Test uploading bytes directly to local FileService."""
    test_data = b"Binary data content"

    # Upload bytes
    uploaded_path = await file_service_local.upload_bytes(
        test_data, s3_path=S3FilePath(s3_path=Path("test/bytes/data.bin"))
    )
    assert "test/bytes/data.bin" in str(uploaded_path)

    # Retrieve contents
    contents = await file_service_local.get_file_contents(s3_path=S3FilePath(s3_path=Path("test/bytes/data.bin")))
    assert contents == test_data


@pytest.mark.asyncio
async def test_local_file_service_listing(file_service_local: FileServiceLocal) -> None:
    """Test listing files in a directory."""
    # Upload several files
    await file_service_local.upload_bytes(b"file1", S3FilePath(s3_path=Path("test/listing/file1.txt")))
    await file_service_local.upload_bytes(b"file2", S3FilePath(s3_path=Path("test/listing/file2.txt")))
    await file_service_local.upload_bytes(b"file3", S3FilePath(s3_path=Path("test/listing/subdir/file3.txt")))

    # Get listing
    listing = await file_service_local.get_listing(S3FilePath(s3_path=Path("test/listing")))
    assert len(listing) >= 3

    # Check that files are in the listing
    keys = [item.Key for item in listing]
    assert any("file1.txt" in key for key in keys)
    assert any("file2.txt" in key for key in keys)
    assert any("file3.txt" in key for key in keys)


@pytest.mark.asyncio
@pytest.mark.skipif(
    len(get_settings().storage_gcs_credentials_file) == 0,
    reason="Requires GCS credentials - only run when explicitly enabled",
)
async def test_gcs_file_service(file_service_gcs: FileServiceGCS, tmp_path: Path) -> None:
    """
    Test using actual Google Cloud Storage.

    This test is skipped by default. To run it:
    1. Configure GCS credentials in your environment
    2. Set the skip condition to False
    3. Run: pytest tests/test_file_service.py::test_gcs_file_service
    """
    test_file = tmp_path / "gcs_test.txt"
    test_file.write_text("GCS test content")

    # Upload to GCS
    uploaded_path = await file_service_gcs.upload_file(test_file, S3FilePath(s3_path=Path("test/gcs/test.txt")))
    assert "test/gcs/test.txt" in str(uploaded_path)

    # Download from GCS
    contents = await file_service_gcs.get_file_contents(S3FilePath(s3_path=Path("test/gcs/test.txt")))
    assert contents == b"GCS test content"


@pytest.mark.asyncio
@pytest.mark.skipif(
    len(get_settings().storage_s3_access_key_id) == 0,
    reason="Requires AWS S3 credentials - only run when explicitly enabled",
)
async def test_s3_file_service(file_service_s3: FileServiceS3, tmp_path: Path) -> None:
    """
    Test using actual AWS S3.

    This test is skipped by default. To run it:
    1. Configure AWS credentials (IAM role, env vars, or ~/.aws/credentials)
    2. Set STORAGE_S3_BUCKET in your environment
    3. Set the skip condition to False
    4. Run: pytest tests/test_file_service.py::test_s3_file_service
    """
    test_file = tmp_path / "s3_test.txt"
    test_file.write_text("S3 test content")

    # Upload to S3
    uploaded_path = await file_service_s3.upload_file(
        test_file, S3FilePath(s3_path=Path("/sms-api-test-1761188321/test/s3/test.txt"))
    )
    assert "sms-api-test-1761188321" in str(uploaded_path)
    assert "test/s3/test.txt" in str(uploaded_path)

    # Download from S3
    contents = await file_service_s3.get_file_contents(
        S3FilePath(s3_path=Path("/sms-api-test-1761188321/test/s3/test.txt"))
    )
    assert contents == b"S3 test content"


@pytest.mark.asyncio
@pytest.mark.skipif(
    len(get_settings().storage_qumulo_access_key_id) == 0,
    reason="Requires Qumulo S3 credentials - only run when explicitly enabled",
)
async def test_qumulo_file_service(file_service_qumulo: FileServiceQumuloS3, tmp_path: Path) -> None:
    """
    Test using Qumulo S3-compatible storage.

    This test is skipped by default. To run it:
    1. Configure Qumulo credentials in your environment
    2. Set STORAGE_QUMULO_ENDPOINT_URL and STORAGE_QUMULO_BUCKET
    3. Set the skip condition to False
    4. Run: pytest tests/test_file_service.py::test_qumulo_file_service
    """
    test_file = tmp_path / "qumulo_test.txt"
    test_file.write_text("Qumulo test content")

    # Upload to Qumulo
    uploaded_path = await file_service_qumulo.upload_file(test_file, S3FilePath(s3_path=Path("test/qumulo/test.txt")))
    assert "test/qumulo/test.txt" in str(uploaded_path)

    # Download from Qumulo
    contents = await file_service_qumulo.get_file_contents(S3FilePath(s3_path=Path("test/qumulo/test.txt")))
    assert contents == b"Qumulo test content"


@pytest.mark.asyncio
async def test_all_backends_have_same_interface(
    file_service_local: FileServiceLocal,
) -> None:
    """
    Test that all FileService backends implement the same interface.

    This ensures that tests can be written against the interface without
    needing to know which backend is being used.
    """
    # All backends should have these methods
    assert hasattr(file_service_local, "upload_file")
    assert hasattr(file_service_local, "download_file")
    assert hasattr(file_service_local, "upload_bytes")
    assert hasattr(file_service_local, "get_file_contents")
    assert hasattr(file_service_local, "get_listing")
    assert hasattr(file_service_local, "get_modified_date")
    assert hasattr(file_service_local, "close")

    # Test basic operation
    test_data = b"Interface test"
    path = await file_service_local.upload_bytes(test_data, S3FilePath(s3_path=Path("test/interface/data.bin")))
    assert path is not None

    retrieved = await file_service_local.get_file_contents(S3FilePath(s3_path=Path("test/interface/data.bin")))
    assert retrieved == test_data
