import os
import uuid
from pathlib import Path

import pytest

from tests.fixtures.file_service_local import FileServiceLocal
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service_gcs import FileServiceGCS
from viva_api.config import get_settings


@pytest.mark.asyncio
async def test_file_service_local(file_service_local: FileServiceLocal) -> None:
    expected_file_content = b"Hello, World!"
    file_service = file_service_local
    s3_path = S3FilePath(s3_path=Path("some/s3/path/fname.txt"))
    orig_file_path = Path("temp.txt")

    with open(orig_file_path, "wb") as f:
        f.write(expected_file_content)

    # upload the file
    returned_gcs_path = await file_service.upload_file(orig_file_path, s3_path)
    assert returned_gcs_path == s3_path

    # download the file
    new_file_path = Path("temp2.txt")
    await file_service.download_file(s3_path, new_file_path)
    assert new_file_path.exists()
    with open(new_file_path, "rb") as f:
        content = f.read()
        assert content == expected_file_content

    os.remove(orig_file_path)
    os.remove(new_file_path)


@pytest.mark.skipif(
    len(get_settings().storage_gcs_credentials_file) == 0, reason="gcs_credentials.json file not supplied"
)
@pytest.mark.asyncio
async def test_file_service_gcs(file_service_gcs: FileServiceGCS, file_service_gcs_test_base_path: Path) -> None:
    expected_file_content = b"Hello, World!"
    file_service = file_service_gcs

    s3_path = S3FilePath(
        s3_path=file_service_gcs_test_base_path / "test_file_service_gcs" / f"fname-{uuid.uuid4().hex}.txt"
    )
    orig_file_path = Path("temp.txt")

    with open(orig_file_path, "wb") as f:
        f.write(expected_file_content)

    # upload the file
    absolute_gcs_path = await file_service.upload_file(file_path=orig_file_path, s3_path=s3_path)
    assert absolute_gcs_path is not None

    # download the file
    new_file_path = Path("temp2.txt")
    await file_service.download_file(s3_path=s3_path, file_path=new_file_path)
    assert new_file_path.exists()
    with open(new_file_path, "rb") as f:
        content = f.read()
        assert content == expected_file_content

    os.remove(orig_file_path)
    os.remove(new_file_path)
