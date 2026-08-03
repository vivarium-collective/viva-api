import logging
from datetime import datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

from gcloud.aio.auth import Token
from gcloud.aio.storage import Storage
from gcloud.aio.storage.constants import DEFAULT_TIMEOUT

from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service import ListingItem
from viva_api.config import get_settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class _StorageWithListPrefix(Storage):
    def __init__(self, token: Token):
        super().__init__(token=token)

    async def list_objects_with_prefix(self, bucket: str, prefix: str) -> dict[str, Any]:
        encoded_prefix = quote(string=prefix, safe="")
        url = f"{self._api_root_read}/{bucket}/o?prefix={encoded_prefix}/"
        headers: dict[str, Any] = {}
        headers.update(await self._headers())

        s = self.session
        resp = await s.get(url=url, headers=headers, params={}, timeout=DEFAULT_TIMEOUT)
        data: dict[str, Any] = await resp.json(content_type=None)
        return data


def create_token() -> Token:
    return Token(
        service_file=get_settings().storage_gcs_credentials_file,
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform.read-only",
            "https://www.googleapis.com/auth/devstorage.read_write",
        ],
    )


async def close_token(token: Token) -> None:
    if token.session:
        await token.close()


async def download_gcs_file(s3_path: S3FilePath, file_path: Path, token: Token) -> S3FilePath:
    logger.info(f"Downloading {file_path} to {s3_path}")
    async with Storage(token=token) as client:
        await client.download_to_filename(
            bucket=get_settings().storage_gcs_bucket, object_name=str(s3_path), filename=str(file_path)
        )
        return s3_path


async def upload_file_to_gcs(file_path: Path, s3_path: S3FilePath, token: Token) -> S3FilePath:
    logger.info(f"Uploading {file_path} to {s3_path}")
    async with Storage(token=token) as client:
        result: dict[str, Any] = await client.upload_from_filename(
            bucket=get_settings().storage_gcs_bucket, object_name=str(s3_path), filename=str(file_path)
        )
        logger.info(f"Upload result: {result}")
        return s3_path


async def upload_bytes_to_gcs(file_contents: bytes, s3_path: S3FilePath, token: Token) -> S3FilePath:
    logger.info(f"Uploading {len(file_contents)} bytes to {s3_path}")
    async with Storage(token=token) as client:
        await client.upload(bucket=get_settings().storage_gcs_bucket, file_data=file_contents, object_name=str(s3_path))
        return s3_path


async def get_gcs_modified_date(s3_path: S3FilePath, token: Token) -> datetime:
    logger.info(f"Getting modified date for {s3_path}")
    async with Storage(token=token) as client:
        metadata: dict[str, Any] = await client.download_metadata(
            bucket=get_settings().storage_gcs_bucket, object_name=str(s3_path)
        )
        return datetime.fromisoformat(metadata["updated"])


async def get_listing_of_gcs(token: Token) -> list[ListingItem]:
    logger.info("Retrieving file list from root of bucket")
    async with Storage(token=token) as client:
        metadata: dict[str, Any] = await client.list_objects(bucket=get_settings().storage_gcs_bucket)
        files: list[ListingItem] = [
            ListingItem(
                Key=item["id"],
                LastModified=datetime.fromisoformat(item["updated"]),
                Size=item["size"],
                ETag=item["etag"],
            )
            for item in metadata["items"]
        ]
        return files


async def get_listing_of_gcs_path(s3_path: S3FilePath, token: Token) -> list[ListingItem]:
    logger.info(f"Retrieving file list from {s3_path}")
    async with _StorageWithListPrefix(token=token) as _my_client:
        my_client = cast(_StorageWithListPrefix, _my_client)
        metadata: dict[str, Any] = await my_client.list_objects_with_prefix(
            bucket=get_settings().storage_gcs_bucket, prefix=str(s3_path)
        )
        files: list[ListingItem] = [
            ListingItem(
                Key=item["id"],
                LastModified=datetime.fromisoformat(item["updated"]),
                Size=item["size"],
                ETag=item["etag"],
            )
            for item in metadata["items"]
        ]
        return files


async def get_gcs_file_contents(s3_path: S3FilePath, token: Token) -> bytes | None:
    logger.info(f"Getting file contents for {s3_path}")
    try:
        async with Storage(token=token) as client:
            return await client.download(bucket=get_settings().storage_gcs_bucket, object_name=str(s3_path))
    except FileNotFoundError as e:
        logger.exception(f"File not found: {e.filename}")
        return None
