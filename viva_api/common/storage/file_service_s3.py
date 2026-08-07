import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import override

import aioboto3
from botocore.exceptions import ClientError

from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service import FileService, ListingItem
from viva_api.config import get_local_cache_dir, get_settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FileServiceS3(FileService):
    """
    AWS S3 implementation of FileService using aioboto3 for async operations.

    This implementation uses standard AWS S3 with bucket/key addressing.
    Credentials are loaded from environment variables or AWS config files via boto3.

    Configuration (via Settings):
    - storage_s3_bucket: S3 bucket name
    - storage_s3_region: AWS region (e.g., 'us-east-1')
    - storage_s3_access_key_id: AWS access key (optional, can use IAM roles)
    - storage_s3_secret_access_key: AWS secret key (optional, can use IAM roles)
    - storage_s3_session_token: AWS session token (optional can use IAM rols)
    """

    session: aioboto3.Session

    def __init__(self) -> None:
        settings = get_settings()
        # Create session with explicit credentials if provided, otherwise use default credential chain
        if (
            settings.storage_s3_access_key_id
            and settings.storage_s3_secret_access_key
            and settings.storage_s3_session_token
        ):
            self.session = aioboto3.Session(
                aws_access_key_id=settings.storage_s3_access_key_id,
                aws_secret_access_key=settings.storage_s3_secret_access_key,
                aws_session_token=settings.storage_s3_session_token,
                region_name=settings.storage_s3_region,
            )
        else:
            # Use default credential chain (IAM roles, env vars, ~/.aws/credentials, etc.)
            self.session = aioboto3.Session(region_name=settings.storage_s3_region)

    @override
    async def download_file(self, s3_path: S3FilePath, file_path: Path | None = None) -> tuple[S3FilePath, str]:
        """
        Download a file from S3 to local filesystem.

        Note: Parameter name kept as 's3_path' for interface compatibility,
        but accepts S3 paths.
        """
        logger.info(f"Downloading S3 file: {s3_path} to {file_path}")
        if file_path is None:
            file_path = get_local_cache_dir() / ("temp_file_" + uuid.uuid4().hex)

        bucket, key = get_settings().storage_s3_bucket, str(s3_path.s3_path)

        async with self.session.client("s3") as s3_client:
            try:
                await s3_client.download_file(bucket, key, str(file_path))
                logger.info(f"Successfully downloaded {s3_path} to {file_path}")
                return s3_path, str(file_path)
            except ClientError:
                logger.exception(f"Failed to download {bucket}/{key}")
                raise

    @override
    async def upload_file(self, file_path: Path, s3_path: S3FilePath) -> S3FilePath:
        """Upload a file from local filesystem to S3."""
        logger.info(f"Uploading file: {file_path} to S3: {s3_path}")
        bucket, key = get_settings().storage_s3_bucket, str(s3_path.s3_path)

        async with self.session.client("s3") as s3_client:
            try:
                await s3_client.upload_file(str(file_path), bucket, key)
                logger.info(f"Successfully uploaded {file_path} to {s3_path}")
                return s3_path
            except ClientError:
                logger.exception(f"Failed to upload {file_path} to {bucket}/{key}")
                raise

    @override
    async def upload_bytes(self, file_contents: bytes, s3_path: S3FilePath) -> S3FilePath:
        """Upload bytes directly to S3."""
        logger.info(f"Uploading {len(file_contents)} bytes to S3: {s3_path}")
        bucket, key = get_settings().storage_s3_bucket, str(s3_path.s3_path)

        async with self.session.client("s3") as s3_client:
            try:
                # Explicitly use AES256 (SSE-S3) to avoid KMS permission issues
                await s3_client.put_object(Bucket=bucket, Key=key, Body=file_contents, ServerSideEncryption="AES256")
                logger.info(f"Successfully uploaded {len(file_contents)} bytes to {s3_path}")
                return s3_path
            except ClientError:
                logger.exception(f"Failed to upload bytes to {bucket}/{key}")
                raise

    @override
    async def get_modified_date(self, s3_path: S3FilePath) -> datetime:
        """Get the last modified timestamp of an S3 object."""
        logger.info(f"Getting modified date of S3 object: {s3_path}")
        bucket, key = get_settings().storage_s3_bucket, str(s3_path.s3_path)

        async with self.session.client("s3") as s3_client:
            try:
                response = await s3_client.head_object(Bucket=bucket, Key=key)
                last_modified = response["LastModified"]
                logger.info(f"Last modified date of {bucket}/{key}: {last_modified}")
                return last_modified
            except ClientError:
                logger.exception(f"Failed to get modified date for {bucket}/{key}")
                raise

    @override
    async def get_listing(self, s3_path: S3FilePath) -> list[ListingItem]:
        """
        List objects in an S3 prefix.

        Returns a list of ListingItem objects for all objects with the given prefix.
        """
        logger.info(f"Getting listing of S3 path: {s3_path}")
        bucket, prefix = get_settings().storage_s3_bucket, str(s3_path.s3_path)

        # Ensure prefix ends with / for directory-style listing
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"

        async with self.session.client("s3") as s3_client:
            try:
                listing: list[ListingItem] = []
                paginator = s3_client.get_paginator("list_objects_v2")

                async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    if "Contents" not in page:
                        continue

                    for obj in page["Contents"]:
                        listing.append(
                            ListingItem(
                                Key=obj["Key"],
                                LastModified=obj["LastModified"],
                                ETag=obj["ETag"],
                                Size=obj["Size"],
                            )
                        )

                logger.info(f"Found {len(listing)} objects in {bucket}/{prefix}")
                return listing
            except ClientError:
                logger.exception(f"Failed to list objects in {bucket}/{prefix}")
                raise

    @override
    async def get_file_contents(self, s3_path: S3FilePath) -> bytes | None:
        """Download and return the contents of an S3 object as bytes."""
        logger.info(f"Getting contents of S3 object: {s3_path}")
        bucket, key = get_settings().storage_s3_bucket, str(s3_path.s3_path)

        async with self.session.client("s3") as s3_client:
            try:
                response = await s3_client.get_object(Bucket=bucket, Key=key)
                async with response["Body"] as stream:
                    contents = await stream.read()
                logger.info(f"Successfully read {len(contents)} bytes from {bucket}/{key}")
                return contents
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    logger.warning(f"Object not found: {bucket}/{key}")
                    return None
                logger.exception(f"Failed to get contents of {bucket}/{key}")
                raise

    @override
    async def delete_file(self, s3_path: S3FilePath) -> None:
        """Delete a file from S3."""
        logger.info(f"Deleting S3 object: {s3_path}")
        bucket, key = get_settings().storage_s3_bucket, str(s3_path.s3_path)

        async with self.session.client("s3") as s3_client:
            try:
                await s3_client.delete_object(Bucket=bucket, Key=key)
                logger.info(f"Successfully deleted {bucket}/{key}")
            except ClientError:
                logger.exception(f"Failed to delete {bucket}/{key}")
                raise

    @override
    async def close(self) -> None:
        """Close the S3 session. aioboto3 sessions are closed automatically via context managers."""
        logger.info("Closing S3 session")
        # aioboto3.Session doesn't require explicit cleanup
        # Clients are closed via async context managers
        pass
