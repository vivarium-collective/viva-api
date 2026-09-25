"""Stream every object under an S3 prefix as one ``tar.gz``, without touching disk.

The prefix is a run's output directory (``viva_core.storage.layout``); the archive holds each
object under ``<name>/<key relative to the prefix>``. The file service is HANDED IN (P3d-4d-2):
this is core's and reads no application state.
"""

import asyncio
import gzip
import io
import logging
import os
import tarfile
from collections.abc import AsyncIterator
from pathlib import Path

from viva_core.storage.file_paths import S3FilePath
from viva_core.storage.file_service import FileService

logger = logging.getLogger(__name__)


async def fetch_s3_file_entries(
    file_service: FileService, name: str, download_keys: list[str], prefix: str
) -> list[tuple[str, bytes]]:
    """Fetch S3 objects in-memory as (arcname, content) pairs for tar creation."""
    entries: list[tuple[str, bytes]] = []
    for key in download_keys:
        try:
            content = await file_service.get_file_contents(S3FilePath(s3_path=Path(key)))
            if content is not None:
                relative = str(Path(key).relative_to(prefix))
                entries.append((f"{name}/{relative}", content))
        except Exception:
            logger.warning(f"Failed to fetch {key}, skipping")
    return entries


def write_tar_entries(write_file: io.BufferedWriter, file_entries: list[tuple[str, bytes]]) -> None:
    """Write (arcname, content) pairs into a streaming tar archive."""
    try:
        with tarfile.open(fileobj=write_file, mode="w|") as tar:
            for arcname, data in file_entries:
                info = tarfile.TarInfo(name=arcname)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
    finally:
        write_file.close()


async def gzip_pipe_stream(
    read_file: io.BufferedReader, loop: asyncio.AbstractEventLoop, chunk_size: int
) -> AsyncIterator[bytes]:
    """Read raw tar data from a pipe, gzip-compress, and yield chunks."""
    gzip_buffer = io.BytesIO()
    gzip_file = gzip.GzipFile(fileobj=gzip_buffer, mode="wb")
    try:
        while True:
            raw = await loop.run_in_executor(None, read_file.read, chunk_size)
            if not raw:
                break
            gzip_file.write(raw)
            if gzip_buffer.tell() > 0:
                gzip_buffer.seek(0)
                compressed = gzip_buffer.read()
                gzip_buffer.seek(0)
                gzip_buffer.truncate()
                yield compressed
        gzip_file.close()
        gzip_buffer.seek(0)
        final = gzip_buffer.read()
        if final:
            yield final
    finally:
        read_file.close()


async def stream_prefix_tar_gz(
    file_service: FileService, prefix: str, name: str, chunk_size: int = 64 * 1024
) -> AsyncIterator[bytes]:
    """Every object under ``prefix`` (bucket-relative), as a ``tar.gz`` whose entries sit under ``name/``.

    Nothing is written to disk: the objects are fetched, tarred into a pipe on a thread, and the
    pipe is gzipped in chunks as it fills.
    """
    listing = await file_service.get_listing(S3FilePath(s3_path=Path(prefix)))
    download_keys = [item.Key for item in listing]
    logger.info(f"Streaming {len(download_keys)} objects from S3 under {prefix}")

    file_entries = await fetch_s3_file_entries(file_service, name, download_keys, prefix)

    read_fd, write_fd = os.pipe()
    read_file = os.fdopen(read_fd, "rb")
    write_file = os.fdopen(write_fd, "wb")
    loop = asyncio.get_event_loop()

    tar_future = loop.run_in_executor(None, write_tar_entries, write_file, file_entries)

    async for chunk in gzip_pipe_stream(read_file, loop, chunk_size):
        yield chunk

    await tar_future
