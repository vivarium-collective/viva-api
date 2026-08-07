"""
Test Qumulo S3-compatible FileService integration.

This test demonstrates how to use the Qumulo FileService with path-based buckets.
"""

from pathlib import Path

import pytest

from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service_qumulo_s3 import FileServiceQumuloS3
from viva_api.config import get_settings


@pytest.mark.skipif(
    len(get_settings().storage_qumulo_access_key_id) == 0, reason="qumulo access key id is not supplied"
)
@pytest.mark.asyncio
async def test_qumulo_file_service(file_service_qumulo: FileServiceQumuloS3, tmp_path: Path) -> None:
    """
    Test using Qumulo S3-compatible storage.

    Configuration required in .dev_env:
    - STORAGE_QUMULO_ENDPOINT_URL=https://cfs15.cam.uchc.edu:9000
    - STORAGE_QUMULO_BUCKET=sms-vivarium
    - STORAGE_QUMULO_ACCESS_KEY_ID=<your-key>
    - STORAGE_QUMULO_SECRET_ACCESS_KEY=<your-secret>
    - STORAGE_QUMULO_VERIFY_SSL=false
    """
    test_file = tmp_path / "qumulo_test.txt"
    test_file.write_text("Qumulo test content from SMS API")

    # Upload to Qumulo (using path-based bucket)
    # Qumulo uses filesystem-style paths
    uploaded_path = await file_service_qumulo.upload_file(test_file, S3FilePath(s3_path=Path("test/qumulo/test.txt")))
    print(f"Uploaded to: {uploaded_path}")
    # assert "qumulo://" in uploaded_path
    assert "test/qumulo/test.txt" in str(uploaded_path)

    # Test upload_bytes
    test_data = b"Binary test data for Qumulo"
    bytes_path = await file_service_qumulo.upload_bytes(test_data, S3FilePath(s3_path=Path("test/qumulo/binary.bin")))
    print(f"Uploaded bytes to: {bytes_path}")

    # Download from Qumulo
    contents = await file_service_qumulo.get_file_contents(s3_path=S3FilePath(s3_path=Path("test/qumulo/test.txt")))
    assert contents == b"Qumulo test content from SMS API"
    print(f"Downloaded and verified: {len(contents)} bytes")

    # Test binary download
    binary_contents = await file_service_qumulo.get_file_contents(
        s3_path=S3FilePath(s3_path=Path("test/qumulo/binary.bin"))
    )
    assert binary_contents == test_data
    print(f"Binary download verified: {len(binary_contents)} bytes")

    # Test listing
    listing = await file_service_qumulo.get_listing(s3_path=S3FilePath(s3_path=Path("test/qumulo/")))
    print(f"Found {len(listing)} files in test/qumulo/")
    assert len(listing) >= 2  # At least our two test files

    file_keys = [item.Key for item in listing]
    assert any("test.txt" in key for key in file_keys)
    assert any("binary.bin" in key for key in file_keys)

    for item in listing:
        print(f"  - {item.Key} ({item.Size} bytes)")

    # Test get_modified_date
    modified = await file_service_qumulo.get_modified_date(s3_path=S3FilePath(s3_path=Path("test/qumulo/test.txt")))
    print(f"File modified: {modified}")
    assert modified is not None

    print("\n✅ All Qumulo S3 tests passed!")


@pytest.mark.skipif(
    len(get_settings().storage_qumulo_access_key_id) == 0, reason="qumulo access key id is not supplied"
)
@pytest.mark.asyncio
async def test_qumulo_download_to_file(file_service_qumulo: FileServiceQumuloS3, tmp_path: Path) -> None:
    """Test downloading a file from Qumulo to local filesystem."""
    # First upload a test file
    test_file = tmp_path / "upload_test.txt"
    test_file.write_text("Test download functionality")

    await file_service_qumulo.upload_file(test_file, s3_path=S3FilePath(s3_path=Path("test/qumulo/download_test.txt")))

    # Download to a specific location
    download_path = tmp_path / "downloaded.txt"
    returned_qumulo_path, returned_local_path = await file_service_qumulo.download_file(
        s3_path=S3FilePath(s3_path=Path("test/qumulo/download_test.txt")), file_path=download_path
    )

    print(f"Downloaded from: {returned_qumulo_path}")
    print(f"Downloaded to: {returned_local_path}")

    assert Path(returned_local_path).exists()
    assert Path(returned_local_path).read_text() == "Test download functionality"

    print("✅ Download test passed!")


@pytest.mark.skipif(
    len(get_settings().storage_qumulo_access_key_id) == 0, reason="qumulo access key id is not supplied"
)
@pytest.mark.asyncio
async def test_qumulo_path_formats(file_service_qumulo: FileServiceQumuloS3) -> None:
    """Test that Qumulo handles various path formats correctly."""
    test_data = b"Path format test"

    # Test different path formats
    paths_to_test = [
        S3FilePath(s3_path=Path("test/qumulo/path1.txt")),  # Relative path
        S3FilePath(s3_path=Path("/test/qumulo/path2.txt")),  # Absolute-style path
        S3FilePath(s3_path=Path("qumulo://test/qumulo/path3.txt")),  # Protocol prefix
    ]

    for path in paths_to_test:
        result = await file_service_qumulo.upload_bytes(test_data, s3_path=path)
        print(f"Path '{path}' -> '{result}'")

        # Verify we can read it back
        contents = await file_service_qumulo.get_file_contents(path)
        assert contents == test_data

    print("✅ Path format tests passed!")
