from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel

from viva_core.storage.file_paths import S3FilePath


class ListingItem(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
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

    @abstractmethod
    async def get_file_contents(self, s3_path: S3FilePath) -> bytes | None:
        pass

    async def get_file_head(self, s3_path: S3FilePath, n_bytes: int) -> bytes | None:
        """The first ``n_bytes`` of an object, or ``None`` when it does not exist.

        The default reads the whole object and slices it; a backend that can range-read
        overrides this (``FileServiceS3``). Used for cheap header reads (a ptools TSV's
        ``n_tp``) without downloading the file."""
        contents = await self.get_file_contents(s3_path)
        return None if contents is None else contents[: max(0, n_bytes)]

    async def open_file_stream(self, s3_path: S3FilePath, chunk_size: int = 1024 * 1024) -> AsyncIterator[bytes] | None:
        """An object's bytes as an async iterator of chunks, or ``None`` when it does not exist.

        Existence is settled before the first chunk, so a caller can still answer 404. The
        default reads the whole object; ``FileServiceS3`` streams it."""
        contents = await self.get_file_contents(s3_path)
        if contents is None:
            return None
        step = max(1, chunk_size)

        async def chunks() -> AsyncIterator[bytes]:
            for start in range(0, len(contents), step):
                yield contents[start : start + step]

        return chunks()

    @abstractmethod
    async def delete_file(self, s3_path: S3FilePath) -> None:
        """Delete a file from storage. Raises exception if file doesn't exist or delete fails."""
        pass

    @abstractmethod
    async def close(self) -> None:
        pass
