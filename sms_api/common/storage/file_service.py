from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel

from sms_api.common.storage.file_paths import S3FilePath


class ListingItem(BaseModel):
    Key: str
    LastModified: datetime
    ETag: str
    Size: int


class FileService(ABC):
    @abstractmethod
    async def download_file(self, s3_path: S3FilePath, file_path: Path | None = None) -> tuple[S3FilePath, str]:
        pass

    @abstractmethod
    async def upload_file(self, file_path: Path, s3_path: S3FilePath) -> S3FilePath:
        pass

    @abstractmethod
    async def upload_bytes(self, file_contents: bytes, s3_path: S3FilePath) -> S3FilePath:
        pass

    @abstractmethod
    async def get_modified_date(self, s3_path: S3FilePath) -> datetime:
        pass

    @abstractmethod
    async def get_listing(self, s3_path: S3FilePath) -> list[ListingItem]:
        pass

    async def list_prefixes(self, s3_path: S3FilePath) -> list[str]:
        """List only the immediate child "directory" prefixes under ``s3_path``.

        The cheap, bounded counterpart to ``get_listing`` (which paginates every
        object): this returns one level of ``CommonPrefixes`` (delimiter ``/``), so a
        caller can walk a large hive-partitioned tree without listing millions of
        chunk keys. Only the S3 backend (the compose Ray/Batch output store)
        implements it; other backends have no such consumer.
        """
        raise NotImplementedError(f"{type(self).__name__} does not implement list_prefixes")

    @abstractmethod
    async def get_file_contents(self, s3_path: S3FilePath) -> bytes | None:
        pass

    @abstractmethod
    async def delete_file(self, s3_path: S3FilePath) -> None:
        """Delete a file from storage. Raises exception if file doesn't exist or delete fails."""
        pass

    @abstractmethod
    async def close(self) -> None:
        pass
