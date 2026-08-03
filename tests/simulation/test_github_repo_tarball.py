"""Tests for the simulator workspace export (repo@commit tarball) helpers."""

from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import HTTPException

from viva_api.simulation.github_repo import open_repo_tarball_stream, repo_tarball_url
from viva_api.simulation.models import SimulatorVersion


def _sim() -> SimulatorVersion:
    return SimulatorVersion(
        git_commit_hash="abc1234",
        git_repo_url="https://github.com/org/repo",
        git_branch="main",
        database_id=5,
    )


def test_repo_tarball_url_builds_github_tarball_endpoint() -> None:
    assert repo_tarball_url(_sim()) == "https://api.github.com/repos/org/repo/tarball/abc1234"


def test_repo_tarball_url_strips_dot_git_suffix() -> None:
    sv = SimulatorVersion(
        git_commit_hash="def5678",
        git_repo_url="https://github.com/org/repo.git",
        git_branch="main",
        database_id=6,
    )
    assert repo_tarball_url(sv) == "https://api.github.com/repos/org/repo/tarball/def5678"


class _FakeResp:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        self.closed = False

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        for c in (b"tar", b"ball"):
            yield c

    async def aread(self) -> bytes:
        return b"error body"

    async def aclose(self) -> None:
        self.closed = True


class _FakeClient:
    captured: dict[str, object] = {}
    status_code: int = 200  # set per-test before instantiation

    def __init__(self, *args: object, **kwargs: object) -> None:
        _FakeClient.captured["follow_redirects"] = kwargs.get("follow_redirects")

    def build_request(self, method: str, url: str, headers: dict[str, str] | None = None) -> object:
        _FakeClient.captured.update(method=method, url=url, headers=headers)
        return object()

    async def send(self, request: object, stream: bool = False) -> _FakeResp:
        _FakeClient.captured["stream"] = stream
        return _FakeResp(_FakeClient.status_code)

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_open_repo_tarball_stream_streams_github_tarball(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeClient.status_code = 200
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)

    body = await open_repo_tarball_stream(_sim(), token="tok")  # noqa: S106
    chunks = [c async for c in body]

    assert b"".join(chunks) == b"tarball"
    # Followed GitHub's tarball redirect, streamed, with the auth header, GET to the tarball URL.
    assert _FakeClient.captured["follow_redirects"] is True
    assert _FakeClient.captured["stream"] is True
    assert _FakeClient.captured["method"] == "GET"
    assert _FakeClient.captured["url"] == "https://api.github.com/repos/org/repo/tarball/abc1234"
    headers = _FakeClient.captured["headers"]
    assert isinstance(headers, dict)
    assert headers["Authorization"] == "token tok"


@pytest.mark.asyncio
async def test_open_repo_tarball_stream_raises_on_non_2xx(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-2xx upstream must raise HTTPException BEFORE any body is yielded —
    so the endpoint never commits a 200 that truncates mid-stream."""
    _FakeClient.status_code = 404
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)

    with pytest.raises(HTTPException) as exc_info:
        await open_repo_tarball_stream(_sim(), token="tok")  # noqa: S106
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_open_repo_tarball_stream_collapses_5xx_to_502(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeClient.status_code = 503
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)

    with pytest.raises(HTTPException) as exc_info:
        await open_repo_tarball_stream(_sim(), token="tok")  # noqa: S106
    assert exc_info.value.status_code == 502
