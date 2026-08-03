import logging
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import override

import aiofiles

from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service import FileService, ListingItem
from viva_api.config import get_local_cache_dir

logger = logging.getLogger(__name__)


def generate_fake_etag(file_path: Path) -> str:
    return file_path.absolute().as_uri()


class FileServiceLocal(FileService):
    # temporary base directory for the mock S3 file store
    BASE_DIR_PARENT = get_local_cache_dir() / "local_data"
    BASE_DIR_PARENT.mkdir(exist_ok=True)
    BASE_DIR = BASE_DIR_PARENT / ("s3_" + uuid.uuid4().hex)

    s3_files_written: list[Path] = []

    def init(self) -> None:
        self.BASE_DIR.mkdir(parents=True, exist_ok=False)

    @override
    async def close(self) -> None:
        # remove all files in the mock s3 file store
        shutil.rmtree(self.BASE_DIR)

    @override
    async def download_file(self, s3_path: S3FilePath, file_path: Path | None = None) -> tuple[S3FilePath, str]:
        logger.info(f"Downloading {s3_path} to {file_path}")
        if file_path is None:
            file_path = get_local_cache_dir() / ("temp_file_" + uuid.uuid4().hex)
        # copy file from mock s3 to local file system
        s3_file_path = self.BASE_DIR / s3_path.s3_path
        local_file_path = Path(file_path)
        local_file_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(s3_file_path, mode="rb") as f:
            contents = await f.read()
            async with aiofiles.open(local_file_path, mode="wb") as f2:
                await f2.write(contents)
        return s3_path, str(local_file_path)

    @override
    async def upload_file(self, file_path: Path, s3_path: S3FilePath) -> S3FilePath:
        logger.info(f"Uploading {file_path} to {s3_path}")
        # copy file from local file_path to mock s3 using aoifiles
        local_file_path = Path(file_path)
        s3_file_path = self.BASE_DIR / s3_path.s3_path
        s3_file_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(local_file_path, mode="rb") as f:
            contents = await f.read()
            async with aiofiles.open(s3_file_path, mode="wb") as f2:
                await f2.write(contents)
        self.s3_files_written.append(s3_file_path)
        return s3_path

    @override
    async def upload_bytes(self, file_contents: bytes, s3_path: S3FilePath) -> S3FilePath:
        logger.info(f"Uploading bytes to {s3_path}")
        # write bytes to mock s3
        s3_file_path = self.BASE_DIR / s3_path.s3_path
        s3_file_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(s3_file_path, mode="wb") as f:
            await f.write(file_contents)
        self.s3_files_written.append(s3_file_path)
        return s3_path

    @override
    async def get_modified_date(self, s3_path: S3FilePath) -> datetime:
        # get the modified date of the file in mock s3
        s3_file_path = self.BASE_DIR / s3_path.s3_path
        return datetime.fromtimestamp(s3_file_path.stat().st_mtime)

    @override
    async def get_listing(self, s3_path: S3FilePath) -> list[ListingItem]:
        # get the listing of the directory in mock s3
        s3_dir_path = self.BASE_DIR / s3_path.s3_path
        return [
            ListingItem(
                Key=str(file.relative_to(self.BASE_DIR)),
                Size=file.stat().st_size,
                LastModified=datetime.fromtimestamp(file.stat().st_mtime),
                ETag=generate_fake_etag(file),
            )
            for file in s3_dir_path.rglob("*")
        ]

    @override
    async def get_file_contents(self, s3_path: S3FilePath) -> bytes | None:
        # get the file contents from mock s3
        s3_file_path = self.BASE_DIR / s3_path.s3_path
        if not s3_file_path.exists():
            return None
        return s3_file_path.read_bytes()

    @override
    async def delete_file(self, s3_path: S3FilePath) -> None:
        # delete the file from mock s3
        s3_file_path = self.BASE_DIR / s3_path.s3_path
        if s3_file_path.exists():
            s3_file_path.unlink()
        else:
            raise FileNotFoundError(f"File {s3_path} does not exist in local file service.")
