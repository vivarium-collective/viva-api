"""
Test fixtures for FileService implementations.

Provides fixtures for all FileService backends (GCS, S3, Qumulo, and Local mock).
Each fixture follows the pattern:
1. Save the current global file service
2. Instantiate and configure the test service
3. Set it as the global file service
4. Yield the service for use in tests
5. Clean up the service
6. Restore the original global file service
"""

from collections.abc import AsyncGenerator
from pathlib import Path

import pytest_asyncio
from gcloud.aio.auth import Token

from tests.fixtures.file_service_local import FileServiceLocal
from viva_api.common.storage.file_service_gcs import FileServiceGCS
from viva_api.common.storage.file_service_qumulo_s3 import FileServiceQumuloS3
from viva_api.common.storage.file_service_s3 import FileServiceS3
from viva_api.common.storage.gcs_aio import create_token
from viva_api.config import get_local_cache_dir
from viva_api.dependencies import get_file_service, set_file_service

temp_data_dir = get_local_cache_dir() / "test_temp_dir"
temp_data_dir.mkdir(exist_ok=True)


@pytest_asyncio.fixture(scope="function")
async def temp_test_data_dir() -> AsyncGenerator[Path]:
    """
    Provides a temporary directory for test data.

    Yields:
        Path to temporary test directory

    Cleanup:
        Removes all files and the directory itself
    """
    temp_data_dir.mkdir(exist_ok=True)

    yield temp_data_dir

    for file in temp_data_dir.iterdir():
        file.unlink()
    temp_data_dir.rmdir()


@pytest_asyncio.fixture(scope="function")
async def file_service_local() -> AsyncGenerator[FileServiceLocal]:
    """
    Local filesystem mock of FileService for testing.

    Creates a temporary directory structure that mimics cloud storage behavior
    without requiring actual cloud credentials or network access.

    Yields:
        FileServiceLocal instance

    Cleanup:
        - Removes all temporary files
        - Restores original global file service
    """
    file_service_local = FileServiceLocal()
    file_service_local.init()
    saved_file_service = get_file_service()
    set_file_service(file_service_local)

    yield file_service_local

    await file_service_local.close()
    set_file_service(saved_file_service)


@pytest_asyncio.fixture(scope="function")
async def file_service_gcs() -> AsyncGenerator[FileServiceGCS]:
    """
    Google Cloud Storage FileService for testing.

    Requires GCS credentials to be configured in the environment.
    Use this fixture when testing actual GCS integration.

    Yields:
        FileServiceGCS instance

    Cleanup:
        - Closes GCS client connections
        - Restores original global file service

    Environment:
        Requires STORAGE_GCS_CREDENTIALS_FILE or default GCS credentials
    """
    file_service_gcs: FileServiceGCS = FileServiceGCS()
    saved_file_service = get_file_service()
    set_file_service(file_service_gcs)

    yield file_service_gcs

    await file_service_gcs.close()
    set_file_service(saved_file_service)


@pytest_asyncio.fixture(scope="function")
async def file_service_s3() -> AsyncGenerator[FileServiceS3]:
    """
    AWS S3 FileService for testing.

    Requires AWS credentials to be configured in the environment or via IAM roles.
    Use this fixture when testing actual S3 integration.

    Yields:
        FileServiceS3 instance

    Cleanup:
        - Closes S3 client connections
        - Restores original global file service

    Environment:
        Requires one of:
        - STORAGE_S3_ACCESS_KEY_ID and STORAGE_S3_SECRET_ACCESS_KEY and STORAGE_S3_SESSION_TOKEN
        - AWS IAM role credentials
        - ~/.aws/credentials file
        - AWS environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)

        Also requires:
        - STORAGE_S3_BUCKET (bucket name)
        - STORAGE_S3_REGION (AWS region, default: us-east-1)
    """
    file_service_s3: FileServiceS3 = FileServiceS3()
    saved_file_service = get_file_service()
    set_file_service(file_service_s3)

    yield file_service_s3

    await file_service_s3.close()
    set_file_service(saved_file_service)


@pytest_asyncio.fixture(scope="function")
async def file_service_qumulo() -> AsyncGenerator[FileServiceQumuloS3]:
    """
    Qumulo S3-compatible FileService for testing.

    Requires Qumulo S3 endpoint and credentials to be configured.
    Use this fixture when testing Qumulo integration.

    Yields:
        FileServiceQumuloS3 instance

    Cleanup:
        - Closes Qumulo S3 client connections
        - Restores original global file service

    Environment:
        Requires:
        - STORAGE_QUMULO_ENDPOINT_URL (e.g., https://qumulo.example.com:8000)
        - STORAGE_QUMULO_ACCESS_KEY_ID
        - STORAGE_QUMULO_SECRET_ACCESS_KEY
        - STORAGE_QUMULO_BUCKET (filesystem/bucket name)
        - STORAGE_QUMULO_VERIFY_SSL (optional, default: true)
    """
    file_service_qumulo: FileServiceQumuloS3 = FileServiceQumuloS3()
    saved_file_service = get_file_service()
    set_file_service(file_service_qumulo)

    yield file_service_qumulo

    await file_service_qumulo.close()
    set_file_service(saved_file_service)


@pytest_asyncio.fixture(scope="function")
async def file_service_gcs_test_base_path() -> Path:
    """
    Base path for GCS test files.

    Returns:
        Path object pointing to 'verify_test' directory
    """
    return Path("verify_test")


@pytest_asyncio.fixture(scope="function")
async def file_service_s3_test_base_path() -> Path:
    """
    Base path for S3 test files.

    Returns:
        Path object pointing to 'verify_test' directory
    """
    return Path("verify_test")


@pytest_asyncio.fixture(scope="function")
async def file_service_qumulo_test_base_path() -> Path:
    """
    Base path for Qumulo test files.

    Returns:
        Path object pointing to 'verify_test' directory
    """
    return Path("verify_test")


@pytest_asyncio.fixture(scope="module")
async def gcs_token() -> AsyncGenerator[Token]:
    """
    GCS authentication token for module-level reuse.

    Creates a single GCS token that can be shared across multiple tests
    in the same module to avoid repeated authentication overhead.

    Yields:
        GCS Token object

    Cleanup:
        Closes the token connection
    """
    token: Token = create_token()

    yield token

    await token.close()
