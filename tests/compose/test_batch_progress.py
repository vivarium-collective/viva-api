"""Unit tests for compose batch-progress computation (handlers.get_batch_progress).

Exercises the pure parsing helpers and the bounded S3 hive walk against a fake
FileService, plus the end-to-end DTO assembly with a mocked database. No network.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import override
from unittest.mock import AsyncMock, MagicMock

import pytest

from sms_api.common.storage.data_layout import RayLayout
from sms_api.common.storage.file_paths import S3FilePath
from sms_api.common.storage.file_service import FileService, ListingItem
from sms_api.compose.handlers import (
    _extract_batch_dims,
    _find_first_int,
    _generation_indices,
    _leaf,
    _scan_batch_output,
    get_batch_progress,
)


class FakeFileService(FileService):
    """FileService whose ``list_prefixes`` replays a canned ``prefix -> children`` tree."""

    def __init__(self, tree: dict[str, list[str]]) -> None:
        self._tree = tree

    @override
    async def list_prefixes(self, s3_path: S3FilePath) -> list[str]:
        key = str(s3_path.s3_path)
        if not key.endswith("/"):
            key += "/"
        return list(self._tree.get(key, []))

    # --- unused abstract surface (progress only ever calls list_prefixes) ---
    @override
    async def download_file(self, s3_path: S3FilePath, file_path: Path | None = None) -> tuple[S3FilePath, str]:
        raise NotImplementedError

    @override
    async def upload_file(self, file_path: Path, s3_path: S3FilePath) -> S3FilePath:
        raise NotImplementedError

    @override
    async def upload_bytes(self, file_contents: bytes, s3_path: S3FilePath) -> S3FilePath:
        raise NotImplementedError

    @override
    async def get_modified_date(self, s3_path: S3FilePath) -> datetime:
        raise NotImplementedError

    @override
    async def get_listing(self, s3_path: S3FilePath) -> list[ListingItem]:
        raise NotImplementedError

    @override
    async def get_file_contents(self, s3_path: S3FilePath) -> bytes | None:
        raise NotImplementedError

    @override
    async def delete_file(self, s3_path: S3FilePath) -> None:
        raise NotImplementedError

    @override
    async def close(self) -> None:
        raise NotImplementedError


def _hive_tree(experiment_id: str, lineage_gens: dict[int, int], *, nested_column: bool = False) -> dict[str, list[str]]:
    """Build a canned hive tree mirroring the real Ray/Batch output layout.

    ``lineage_gens`` maps ``lineage_seed -> generations_present`` (so seed→gens 3
    yields ``generation=0../generation=2/``). ``nested_column`` inserts a
    ``<column>/`` level between ``lineage_seed=`` and ``generation=`` to exercise the
    column-nested layout branch.
    """
    root = RayLayout.experiment_prefix(experiment_id)  # e.g. "vecoli-output/<exp>"
    runner = f"{root}/batch_baseline"
    substore = f"{runner}/batch_baseline"
    variant = f"{substore}/history/experiment_id=batch_baseline/variant=0"

    tree: dict[str, list[str]] = {
        f"{root}/": [f"{runner}/"],
        # runner level: the parquet substore + per-seed zarr stores (which must be skipped)
        f"{runner}/": [f"{substore}/"] + [f"{runner}/batch_baseline_v0_s{s}.zarr/" for s in lineage_gens],
        f"{substore}/": [f"{substore}/configuration/", f"{substore}/history/", f"{substore}/success/"],
        f"{substore}/history/": [f"{substore}/history/experiment_id=batch_baseline/"],
        f"{substore}/history/experiment_id=batch_baseline/": [f"{variant}/"],
        f"{variant}/": [f"{variant}/lineage_seed={s}/" for s in lineage_gens],
    }
    for seed, gens in lineage_gens.items():
        lineage = f"{variant}/lineage_seed={seed}"
        gen_dirs = [f"{lineage}{'/cell_mass' if nested_column else ''}/generation={g}/" for g in range(gens)]
        if nested_column:
            tree[f"{lineage}/"] = [f"{lineage}/cell_mass/"]
            tree[f"{lineage}/cell_mass/"] = gen_dirs
        else:
            tree[f"{lineage}/"] = gen_dirs
    return tree


def test_leaf() -> None:
    assert _leaf("a/b/lineage_seed=7/") == "lineage_seed=7"
    assert _leaf("a/b/generation=3") == "generation=3"


def test_generation_indices_filters_and_parses() -> None:
    prefixes = ["p/generation=0/", "p/generation=2/", "p/configuration/", "p/generation=x/"]
    assert sorted(_generation_indices(prefixes)) == [0, 2]


def test_find_first_int_nested() -> None:
    doc = {"state": {"nested": [{"n_seeds": "1000"}], "other": 5}}
    assert _find_first_int(doc, "n_seeds") == 1000
    assert _find_first_int(doc, "missing") == 0


def test_extract_batch_dims_prefers_canonical_then_aliases() -> None:
    assert _extract_batch_dims(json.dumps({"n_seeds": 1000, "n_generations": 10})) == (1000, 10)
    # falls back to vEcoli aliases when the canonical keys are absent
    assert _extract_batch_dims(json.dumps({"config": {"n_init_sims": 8, "generations": 4}})) == (8, 4)
    assert _extract_batch_dims(None) == (0, 0)
    assert _extract_batch_dims("not json") == (0, 0)


@pytest.mark.asyncio
async def test_scan_batch_output_direct_layout() -> None:
    fs = FakeFileService(_hive_tree("exp1", {0: 10, 1: 5, 2: 2}))
    started, deepest, mean = await _scan_batch_output(fs, "exp1")
    assert started == 3
    assert deepest == 10  # seed 0 reached generation index 9 -> 10 generations
    assert mean == pytest.approx((10 + 5 + 2) / 3)


@pytest.mark.asyncio
async def test_scan_batch_output_nested_column_layout() -> None:
    fs = FakeFileService(_hive_tree("exp2", {0: 3, 1: 1}, nested_column=True))
    started, deepest, mean = await _scan_batch_output(fs, "exp2")
    assert started == 2
    assert deepest == 3
    assert mean == pytest.approx((3 + 1) / 2)


@pytest.mark.asyncio
async def test_scan_batch_output_no_output_yet() -> None:
    assert await _scan_batch_output(FakeFileService({}), "exp3") == (0, 0, 0.0)


@pytest.mark.asyncio
async def test_get_batch_progress_assembles_dto() -> None:
    fs = FakeFileService(_hive_tree("exp1", {0: 10, 1: 5, 2: 2}))
    db = MagicMock()
    db.get_simulator_db.return_value.get_simulations_experiment_id = AsyncMock(return_value="exp1")
    db.get_simulator_db.return_value.get_simulation_document = AsyncMock(
        return_value=json.dumps({"n_seeds": 3, "n_generations": 10})
    )
    db.get_hpc_db.return_value.get_hpcrun_by_ref = AsyncMock(return_value=None)

    progress = await get_batch_progress(1, db, fs)

    assert progress.lineages == "3:3"
    assert progress.generations == "10:10"
    # overall = started * mean_gen / (n_seeds * n_generations) * 100 = 3 * 5.667 / 30 * 100
    assert progress.overall == pytest.approx(56.67, abs=0.05)
    assert progress.time_elapsed == 0.0
    assert progress.status is None
