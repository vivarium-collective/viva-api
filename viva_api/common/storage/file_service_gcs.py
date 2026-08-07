import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import override

from gcloud.aio.auth import Token

from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service import FileService, ListingItem
from viva_api.common.storage.gcs_aio import (
    close_token,
    create_token,
    download_gcs_file,
    get_gcs_file_contents,
    get_gcs_modified_date,
    get_listing_of_gcs_path,
    upload_bytes_to_gcs,
    upload_file_to_gcs,
)
from viva_api.config import get_local_cache_dir

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FileServiceGCS(FileService):
    token: Token

    def __init__(self) -> None:
        self.token = create_token()

    @override
    async def download_file(self, s3_path: S3FilePath, file_path: Path | None = None) -> tuple[S3FilePath, str]:
        logger.info(f"Downloading {s3_path} to {file_path}")
        if file_path is None:
            file_path = get_local_cache_dir() / ("temp_file_" + uuid.uuid4().hex)
        full_gcs_path = await download_gcs_file(s3_path=s3_path, file_path=file_path, token=self.token)
        return full_gcs_path, str(file_path)

    @override
    async def upload_file(self, file_path: Path, s3_path: S3FilePath) -> S3FilePath:
        logger.info(f"Uploading {file_path} to {s3_path}")
        return await upload_file_to_gcs(file_path=file_path, s3_path=s3_path, token=self.token)

    @override
    async def upload_bytes(self, file_contents: bytes, s3_path: S3FilePath) -> S3FilePath:
        logger.info(f"Uploading {len(file_contents)} bytes to {s3_path}")
        return await upload_bytes_to_gcs(file_contents=file_contents, s3_path=s3_path, token=self.token)

    @override
    async def get_modified_date(self, s3_path: S3FilePath) -> datetime:
        logger.info(f"Getting modified date of {s3_path}")
        return await get_gcs_modified_date(s3_path=s3_path, token=self.token)

    @override
    async def get_listing(self, s3_path: S3FilePath) -> list[ListingItem]:
        logger.info(f"Getting listing of {s3_path}")
        return await get_listing_of_gcs_path(s3_path, token=self.token)

    @override
    async def get_file_contents(self, s3_path: S3FilePath) -> bytes | None:
        logger.info(f"Getting contents of {s3_path}")
        return await get_gcs_file_contents(s3_path=s3_path, token=self.token)

    @override
    async def delete_file(self, s3_path: S3FilePath) -> None:
        logger.info(f"Deleting GCS object: {s3_path}")
        # TODO: Implement GCS delete functionality
        raise NotImplementedError("GCS delete not yet implemented")

    @override
    async def close(self) -> None:
        await close_token(self.token)
