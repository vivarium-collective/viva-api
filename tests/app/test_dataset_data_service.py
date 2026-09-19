"""``E2EDataService``'s dataset and analysis-listing calls, against an in-process transport.

Pins the HTTP each call makes (path, query parameters, body) and how ``fetch_dataset``
writes a download. No server: ``httpx.MockTransport`` answers.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.app_data_service import BaseUrl, E2EDataService

URI = "s3://bucket/vecoli-output/exp/analyses/a/ptools/ptools_rna_multiseed__variant=0.tsv"
DATASET: dict[str, Any] = {"database_id": 41, "kind": "ptools-analysis", "uri": URI, "analysis_id": 7}
PAGE: dict[str, Any] = {"datasets": [DATASET], "limit": 100, "offset": 0, "next_offset": None, "total": 1}


def _service(handler: Callable[[httpx.Request], httpx.Response]) -> tuple[E2EDataService, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    service = E2EDataService(base_url=BaseUrl.LOCAL_8080)
    service.client = httpx.Client(transport=httpx.MockTransport(record), base_url="http://testserver")
    return service, seen


def test_list_datasets_repeats_attr_lowercases_booleans_and_drops_unset_filters() -> None:
    service, seen = _service(lambda request: httpx.Response(200, json=PAGE))
    page = service.list_datasets(kind="ptools-analysis", attrs=["variant=0", "protocol=multiseed"], simulation_id=3)
    assert page.datasets[0].uri == URI
    request = seen[-1]
    assert request.method == "GET" and request.url.path == "/api/v1/datasets"
    assert request.url.params.get_list("attr") == ["variant=0", "protocol=multiseed"]
    assert request.url.params["simulation_id"] == "3" and request.url.params["available"] == "true"
    assert "view" not in request.url.params and "source" not in request.url.params

    service.list_simulation_datasets(1002, include_analyses=True, available="any")
    assert seen[-1].url.path == "/api/v1/simulations/1002/datasets"
    assert seen[-1].url.params["include_analyses"] == "true" and seen[-1].url.params["available"] == "any"

    service.list_analysis_datasets(7, kind="figure")
    assert seen[-1].url.path == "/api/v1/analyses/7/datasets" and seen[-1].url.params["kind"] == "figure"


def test_list_analyses_sends_only_the_filters_given() -> None:
    row = {
        "database_id": 7,
        "name": "a",
        "config": {"analysis_options": {"experiment_id": ["exp"]}},
        "last_updated": "2026-09-15",
        "status": "completed",
    }
    service, seen = _service(lambda request: httpx.Response(200, json=[row]))
    analyses = service.list_analyses(status="completed", source="sim:1002", limit=10)
    assert analyses[0].database_id == 7
    assert seen[-1].url.path == "/api/v1/analyses"
    assert dict(seen[-1].url.params) == {"status": "completed", "source": "sim:1002", "limit": "10"}


def test_single_dataset_calls_and_tagging() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/provenance"):
            return httpx.Response(200, json={"dataset": DATASET, "producer": {"kind": "analysis", "id": 7}})
        if request.url.path == "/api/v1/datasets/tags":
            return httpx.Response(200, json={"cd2": 3})
        if request.url.path == "/api/v1/datasets/attributes":
            return httpx.Response(200, json={"variant": [0, 1]})
        return httpx.Response(200, json={**DATASET, "tags": ["cd2"]})

    service, seen = _service(handler)
    assert service.get_dataset(41).database_id == 41
    assert service.get_dataset_provenance(41).producer is not None
    assert service.list_dataset_tags(kind="figure") == {"cd2": 3}
    assert seen[-1].url.params["kind"] == "figure"
    assert service.list_dataset_attributes() == {"variant": [0, 1]}
    assert service.tag_dataset(41, ["cd2"]).tags == ["cd2"]
    assert seen[-1].method == "POST" and seen[-1].url.path == "/api/v1/datasets/41/tags"
    assert json.loads(seen[-1].content) == {"tags": ["cd2"]}


def test_a_server_error_carries_status_and_detail() -> None:
    service, _ = _service(lambda request: httpx.Response(404, json={"detail": "Dataset 9 not found"}))
    with pytest.raises(httpx.HTTPError, match="404.*Dataset 9 not found"):
        service.get_dataset(9)


def _content(body: bytes, filename: str = "ptools_rna_multiseed__variant=0.tsv") -> httpx.Response:
    return httpx.Response(200, content=body, headers={"content-disposition": f'inline; filename="{filename}"'})


def test_fetch_saves_under_the_server_filename_in_a_directory_or_to_a_named_file(tmp_path: Path) -> None:
    service, seen = _service(lambda request: _content(b"$\t0m\nEG1\t0.25\n"))

    into_dir = service.fetch_dataset(41, f"{tmp_path}/downloads/")
    assert into_dir == tmp_path / "downloads" / "ptools_rna_multiseed__variant=0.tsv"
    assert into_dir.read_bytes() == b"$\t0m\nEG1\t0.25\n"
    assert seen[-1].url.path == "/api/v1/datasets/41/content"

    named = service.fetch_dataset(41, tmp_path / "rna.tsv")
    assert named == tmp_path / "rna.tsv" and named.read_bytes() == b"$\t0m\nEG1\t0.25\n"
    assert not list(tmp_path.rglob("*.part"))


def test_fetch_never_writes_outside_dest_or_leaves_a_partial_file(tmp_path: Path) -> None:
    service, _ = _service(lambda request: _content(b"abc", filename="../../escape.tsv"))
    assert service.fetch_dataset(41, tmp_path) == tmp_path / "escape.tsv"

    refused, _ = _service(lambda request: httpx.Response(409, json={"detail": "a parquet store"}))
    with pytest.raises(httpx.HTTPError, match="409"):
        refused.fetch_dataset(40, tmp_path / "store")
    assert not (tmp_path / "store").exists()

    class _Broken(httpx.SyncByteStream):
        def __iter__(self) -> Iterator[bytes]:
            yield b"half"
            raise httpx.ReadError("connection dropped")

    broken, _ = _service(
        lambda request: httpx.Response(200, stream=_Broken(), headers={"content-disposition": 'filename="x.tsv"'})
    )
    with pytest.raises(httpx.ReadError):
        broken.fetch_dataset(41, tmp_path)
    assert not (tmp_path / "x.tsv").exists() and not (tmp_path / "x.tsv.part").exists()
