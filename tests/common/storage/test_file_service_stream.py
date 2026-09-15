"""``FileService.open_file_stream``: the base default and ``FileServiceS3``'s streamed GET.

The S3 test drives the real ``FileServiceS3`` method against an in-memory stand-in for the
aioboto3 session, so nothing reaches AWS.
"""

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service import FileService
from viva_api.common.storage.file_service_s3 import FileServiceS3


def _path(key: str) -> S3FilePath:
    return S3FilePath(s3_path=Path(key))


async def _drain(stream: AsyncIterator[bytes]) -> list[bytes]:
    return [chunk async for chunk in stream]


class _Whole:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    async def get_file_contents(self, s3_path: S3FilePath) -> bytes | None:
        return self.objects.get(str(s3_path.s3_path))


@pytest.mark.asyncio
async def test_the_default_stream_chunks_a_whole_read_and_reports_a_missing_object() -> None:
    service = _Whole({"a/b.tsv": b"0123456789", "a/empty.tsv": b""})
    stream = await FileService.open_file_stream(service, _path("a/b.tsv"), chunk_size=4)  # type: ignore[arg-type]
    assert stream is not None
    assert await _drain(stream) == [b"0123", b"4567", b"89"]
    empty = await FileService.open_file_stream(service, _path("a/empty.tsv"))  # type: ignore[arg-type]
    assert empty is not None and await _drain(empty) == []
    assert await FileService.open_file_stream(service, _path("a/missing.tsv")) is None  # type: ignore[arg-type]


class _Body:
    def __init__(self, data: bytes, log: list[str]) -> None:
        self.data = data
        self.log = log

    async def __aenter__(self) -> "_Body":
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.log.append("body closed")

    async def iter_chunks(self, chunk_size: int) -> AsyncIterator[bytes]:
        for start in range(0, len(self.data), chunk_size):
            yield self.data[start : start + chunk_size]


class _Client:
    def __init__(self, objects: dict[str, bytes], log: list[str]) -> None:
        self.objects = objects
        self.log = log

    async def __aenter__(self) -> "_Client":
        self.log.append("client opened")
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.log.append("client closed")

    async def get_object(self, **kwargs: str) -> dict[str, Any]:
        key = kwargs["Key"]
        if key == "denied":
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "GetObject")
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")
        return {"Body": _Body(self.objects[key], self.log)}


class _Session:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.log: list[str] = []

    def client(self, name: str) -> _Client:
        assert name == "s3"
        return _Client(self.objects, self.log)


def _s3(objects: dict[str, bytes]) -> tuple[FileServiceS3, _Session]:
    service = FileServiceS3.__new__(FileServiceS3)  # skip credential setup; only the session is used
    session = _Session(objects)
    service.session = session  # type: ignore[assignment]
    return service, session


@pytest.mark.asyncio
async def test_s3_streams_in_chunks_and_holds_the_client_until_the_stream_ends() -> None:
    service, session = _s3({"vecoli-output/x/ptools/a.tsv": b"abcdefghij"})
    stream = await service.open_file_stream(_path("vecoli-output/x/ptools/a.tsv"), chunk_size=3)
    assert stream is not None
    assert session.log == ["client opened"]  # open, not yet read
    assert await _drain(stream) == [b"abc", b"def", b"ghi", b"j"]
    assert session.log == ["client opened", "body closed", "client closed"]


@pytest.mark.asyncio
async def test_s3_reports_a_missing_object_and_raises_other_errors_closing_the_client_either_way() -> None:
    service, session = _s3({})
    assert await service.open_file_stream(_path("vecoli-output/x/gone.tsv")) is None
    assert session.log == ["client opened", "client closed"]

    session.log.clear()
    with pytest.raises(ClientError):
        await service.open_file_stream(_path("denied"))
    assert session.log == ["client opened", "client closed"]
