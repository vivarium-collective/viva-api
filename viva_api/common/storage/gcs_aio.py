import logging
from datetime import datetime
from pathlib import Path
from typing import cast
from urllib.parse import quote

from gcloud.aio.auth import Token
from gcloud.aio.storage import Storage
from gcloud.aio.storage.constants import DEFAULT_TIMEOUT
from pydantic import JsonValue

from viva_api.common.models import JsonDict
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service import ListingItem
from viva_api.config import get_settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class _StorageWithListPrefix(Storage):
    def __init__(self, token: Token):
        super().__init__(token=token)

    async def list_objects_with_prefix(self, bucket: str, prefix: str) -> JsonDict:
        encoded_prefix = quote(string=prefix, safe="")
        url = f"{self._api_root_read}/{bucket}/o?prefix={encoded_prefix}/"
        # `Storage._headers()` is annotated `-> dict[str, str]` by gcloud.aio itself.
        headers: dict[str, str] = {}
        headers.update(await self._headers())

        s = self.session
        resp = await s.get(url=url, headers=headers, params={}, timeout=DEFAULT_TIMEOUT)
        data: JsonDict = await resp.json(content_type=None)
        return data


def _text(entry: JsonDict, key: str, uri: str) -> str:
    """One string field of a GCS listing entry, or a clear error naming what was wrong."""
    value = entry.get(key)
    if not isinstance(value, str):
        raise TypeError(f"GCS listing entry for {uri!r}: {key} is {type(value).__name__}, expected a string")
    return value


def _listing_item(raw: JsonValue) -> ListingItem:
    """A GCS listing entry as a ``ListingItem``, checked rather than assumed.

    The response is arbitrary JSON as far as the type system is concerned, and the fields below
    are what this code has always relied on; the difference is that a malformed entry now fails
    here, with the offending field named, instead of somewhere downstream."""
    if not isinstance(raw, dict):
        raise TypeError(f"GCS listing entry is {type(raw).__name__}, expected an object")
    uri = raw.get("id") if isinstance(raw.get("id"), str) else "<no id>"
    size = raw.get("size")
    if isinstance(size, str) and size.isdigit():  # the JSON API returns size as a string
        size = int(size)
    if not isinstance(size, int) or isinstance(size, bool):
        raise TypeError(f"GCS listing entry for {uri!r}: size is {size!r}, expected an integer")
    return ListingItem(
        Key=_text(raw, "id", str(uri)),
        LastModified=datetime.fromisoformat(_text(raw, "updated", str(uri))),
        Size=size,
        ETag=_text(raw, "etag", str(uri)),
    )


def _listing_items(metadata: JsonDict) -> list[ListingItem]:
    items = metadata.get("items", [])
    if not isinstance(items, list):
        raise TypeError(f"GCS listing 'items' is {type(items).__name__}, expected a list")
    return [_listing_item(item) for item in items]


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
        result: JsonDict = await client.upload_from_filename(
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
        metadata: JsonDict = await client.download_metadata(
            bucket=get_settings().storage_gcs_bucket, object_name=str(s3_path)
        )
        return datetime.fromisoformat(_text(metadata, "updated", str(s3_path)))


async def get_listing_of_gcs(token: Token) -> list[ListingItem]:
    logger.info("Retrieving file list from root of bucket")
    async with Storage(token=token) as client:
        metadata: JsonDict = await client.list_objects(bucket=get_settings().storage_gcs_bucket)
        return _listing_items(metadata)


async def get_listing_of_gcs_path(s3_path: S3FilePath, token: Token) -> list[ListingItem]:
    logger.info(f"Retrieving file list from {s3_path}")
    async with _StorageWithListPrefix(token=token) as _my_client:
        my_client = cast(_StorageWithListPrefix, _my_client)
        metadata: JsonDict = await my_client.list_objects_with_prefix(
            bucket=get_settings().storage_gcs_bucket, prefix=str(s3_path)
        )
        return _listing_items(metadata)


async def get_gcs_file_contents(s3_path: S3FilePath, token: Token) -> bytes | None:
    logger.info(f"Getting file contents for {s3_path}")
    try:
        async with Storage(token=token) as client:
            return await client.download(bucket=get_settings().storage_gcs_bucket, object_name=str(s3_path))
    except FileNotFoundError as e:
        logger.exception(f"File not found: {e.filename}")
        return None
