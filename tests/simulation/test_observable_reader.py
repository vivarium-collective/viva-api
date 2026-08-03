import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from viva_api.simulation.observable_reader import (
    StoreIndex,
    detect_store_kind,
    list_observables,
    read_observables,
)


def _write_fixture_zarr(tmp_path: Path) -> str:
    """Write a tiny XArray zarr store and return its file:// URI."""
    ds = xr.Dataset(
        data_vars={
            "mass": ("time", np.array([1.0, 2.0, 3.0])),
            "volume": ("time", np.array([0.1, 0.2, 0.3])),
        },
        coords={"time": np.array([0.0, 1.0, 2.0])},
    )
    store_path = tmp_path / "store.zarr"
    ds.to_zarr(store_path, mode="w")
    return f"file://{store_path}"


def test_detect_store_kind_zarr(tmp_path: Path) -> None:
    uri = _write_fixture_zarr(tmp_path)
    assert detect_store_kind(uri) == "zarr"


def test_list_observables_zarr(tmp_path: Path) -> None:
    uri = _write_fixture_zarr(tmp_path)
    idx = list_observables(uri)
    assert isinstance(idx, StoreIndex)
    assert idx.store == "zarr"
    names = {o.name for o in idx.observables}
    assert names == {"mass", "volume"}
    mass = next(o for o in idx.observables if o.name == "mass")
    assert mass.dims == ["time"]
    assert mass.shape == [3]


def test_read_observables_zarr_selected(tmp_path: Path) -> None:
    uri = _write_fixture_zarr(tmp_path)
    kind, time, series = read_observables(uri, names=["mass"])
    assert kind == "zarr"
    assert time == [0.0, 1.0, 2.0]
    assert series == {"mass": [1.0, 2.0, 3.0]}


def test_read_observables_zarr_all(tmp_path: Path) -> None:
    uri = _write_fixture_zarr(tmp_path)
    kind, time, series = read_observables(uri, names=[])
    assert kind == "zarr"
    assert set(series) == {"mass", "volume"}
    assert series["volume"] == [0.1, 0.2, 0.3]


def test_read_observables_zarr_multidim_raises(tmp_path: Path) -> None:
    """A 2-D variable (time, mol) must raise ValueError, not silently flatten."""
    ds = xr.Dataset(
        data_vars={
            "bulk": (["time", "mol"], np.ones((3, 5))),
        },
        coords={"time": np.array([0.0, 1.0, 2.0])},
    )
    store_path = tmp_path / "store.zarr"
    ds.to_zarr(store_path, mode="w")
    uri = f"file://{store_path}"

    with pytest.raises(ValueError, match="observable 'bulk' is not a 1-D timeseries"):
        read_observables(uri, names=["bulk"])


def test_read_observables_zarr_nan_sanitized(tmp_path: Path) -> None:
    """NaN values must be sanitized to None for JSON-safe output."""
    ds = xr.Dataset(
        data_vars={
            "mass": ("time", np.array([1.0, float("nan"), 3.0])),
        },
        coords={"time": np.array([0.0, 1.0, 2.0])},
    )
    store_path = tmp_path / "store.zarr"
    ds.to_zarr(store_path, mode="w")
    uri = f"file://{store_path}"

    _, _, series = read_observables(uri, names=["mass"])
    assert series["mass"][0] == 1.0
    assert series["mass"][1] is None
    assert series["mass"][2] == 3.0


def _write_fixture_parquet(tmp_path: Path) -> str:
    df = pd.DataFrame({"time": [0.0, 1.0, 2.0], "mass": [1.0, 2.0, 3.0], "volume": [0.1, 0.2, 0.3]})
    p = tmp_path / "store.parquet"
    df.to_parquet(p)
    return f"file://{p}"


def test_detect_store_kind_parquet(tmp_path: Path) -> None:
    uri = _write_fixture_parquet(tmp_path)
    assert detect_store_kind(uri) == "parquet"


def test_list_observables_parquet(tmp_path: Path) -> None:
    uri = _write_fixture_parquet(tmp_path)
    idx = list_observables(uri)
    assert idx.store == "parquet"
    assert {o.name for o in idx.observables} == {"mass", "volume"}  # time excluded


def test_read_observables_parquet(tmp_path: Path) -> None:
    uri = _write_fixture_parquet(tmp_path)
    kind, time, series = read_observables(uri, names=["volume"])
    assert kind == "parquet"
    assert time == [0.0, 1.0, 2.0]
    assert series == {"volume": [0.1, 0.2, 0.3]}


# ── Column projection (review item b) ──────────────────────────────────────


def _write_wide_zarr(tmp_path: Path, npoints: int = 8) -> str:
    t = np.arange(npoints, dtype=float)
    ds = xr.Dataset(
        data_vars={
            "mass": ("time", t),
            "volume": ("time", t * 0.1),
            "growth": ("time", t * 2.0),
        },
        coords={"time": t},
    )
    store_path = tmp_path / "store.zarr"
    ds.to_zarr(store_path, mode="w")
    return f"file://{store_path}"


def _write_wide_parquet(tmp_path: Path, npoints: int = 8) -> str:
    t = np.arange(npoints, dtype=float)
    df = pd.DataFrame({"time": t, "mass": t, "volume": t * 0.1, "growth": t * 2.0})
    p = tmp_path / "store.parquet"
    df.to_parquet(p)
    return f"file://{p}"


def test_read_observables_parquet_projects_only_requested(tmp_path: Path) -> None:
    """Requesting a subset returns exactly that subset (+ time) — no other columns."""
    uri = _write_wide_parquet(tmp_path)
    _, _, series = read_observables(uri, names=["growth"])
    assert set(series) == {"growth"}
    assert series["growth"] == [0.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0]


def test_list_observables_parquet_lists_without_full_read(tmp_path: Path) -> None:
    """Listing reports names + row count (schema/footer only), excluding `time`."""
    uri = _write_wide_parquet(tmp_path, npoints=8)
    idx = list_observables(uri)
    assert {o.name for o in idx.observables} == {"mass", "volume", "growth"}
    assert all(o.shape == [8] for o in idx.observables)


# ── Decimation (review item c) ─────────────────────────────────────────────


@pytest.mark.parametrize("writer", [_write_wide_zarr, _write_wide_parquet])
def test_read_observables_stride(tmp_path: Path, writer) -> None:  # type: ignore[no-untyped-def]
    uri = writer(tmp_path, 8)
    _, time, series = read_observables(uri, names=["mass"], stride=2)
    assert time == [0.0, 2.0, 4.0, 6.0]
    assert series["mass"] == [0.0, 2.0, 4.0, 6.0]


@pytest.mark.parametrize("writer", [_write_wide_zarr, _write_wide_parquet])
def test_read_observables_max_points(tmp_path: Path, writer) -> None:  # type: ignore[no-untyped-def]
    uri = writer(tmp_path, 8)
    _, time, series = read_observables(uri, names=["mass"], max_points=3)
    # ceil(8/3) = 3 → indices 0,3,6 → 3 points (≤ max_points)
    assert len(time) <= 3
    assert time == [0.0, 3.0, 6.0]
    assert series["mass"] == [0.0, 3.0, 6.0]


@pytest.mark.parametrize("writer", [_write_wide_zarr, _write_wide_parquet])
def test_read_observables_max_points_overrides_smaller_stride(tmp_path: Path, writer) -> None:  # type: ignore[no-untyped-def]
    uri = writer(tmp_path, 8)
    # stride=1 but max_points=2 → coarser step wins (ceil(8/2)=4) → indices 0,4
    _, time, _series = read_observables(uri, names=["mass"], stride=1, max_points=2)
    assert time == [0.0, 4.0]


# ── Hive-partitioned datatree (real Ray XArrayEmitter layout) ──────────────


def _write_partitioned_zarr(tmp_path: Path) -> str:
    """Write a tiny hive-partitioned datatree matching the real S3 layout:
    leaf observables carry generation={G}; the parent carries time_gen={G}."""
    from xarray import DataTree

    parent = xr.Dataset({"time_gen=1": ("t1", [0.0, 1.0, 2.0]), "time_gen=2": ("t2", [3.0, 4.0])})
    cell_mass = xr.Dataset({"generation=1": ("t1", [10.0, 11.0, 12.0]), "generation=2": ("t2", [13.0, 14.0])})
    growth = xr.Dataset({"generation=1": ("t1", [0.1, 0.2, 0.3]), "generation=2": ("t2", [0.4, 0.5])})
    base = "experiment_id=cmp/variant=0/lineage_seed=0"
    dt = DataTree.from_dict({base: parent, f"{base}/cell_mass": cell_mass, f"{base}/growth": growth})
    store = tmp_path / "v2ecoli_seed00.zarr"
    dt.to_zarr(store, mode="w")
    return f"file://{store}"


def test_list_observables_partitioned(tmp_path: Path) -> None:
    uri = _write_partitioned_zarr(tmp_path)
    idx = list_observables(uri)
    assert idx.store == "zarr"
    assert {o.name for o in idx.observables} == {"cell_mass", "growth"}
    # concatenated length across both generations (3 + 2)
    assert all(o.shape == [5] for o in idx.observables)


def test_read_observables_partitioned_concatenates_generations(tmp_path: Path) -> None:
    uri = _write_partitioned_zarr(tmp_path)
    kind, time, series = read_observables(uri, names=["cell_mass"])
    assert kind == "zarr"
    assert time == [0.0, 1.0, 2.0, 3.0, 4.0]
    assert series["cell_mass"] == [10.0, 11.0, 12.0, 13.0, 14.0]


def test_read_observables_partitioned_all(tmp_path: Path) -> None:
    uri = _write_partitioned_zarr(tmp_path)
    _, _, series = read_observables(uri, names=[])
    assert set(series) == {"cell_mass", "growth"}
    assert series["growth"] == [0.1, 0.2, 0.3, 0.4, 0.5]


def test_read_observables_partitioned_decimation(tmp_path: Path) -> None:
    uri = _write_partitioned_zarr(tmp_path)
    _, time, series = read_observables(uri, names=["cell_mass"], stride=2)
    assert time == [0.0, 2.0, 4.0]
    assert series["cell_mass"] == [10.0, 12.0, 14.0]


def test_read_observables_partitioned_bad_name(tmp_path: Path) -> None:
    uri = _write_partitioned_zarr(tmp_path)
    with pytest.raises(KeyError):
        read_observables(uri, names=["nope"])


# ── Remote store (review item: include remote tests if .dev_env vars present) ──

_REMOTE_STORE_URI = os.environ.get("TEST_OBSERVABLE_STORE_URI", "")


@pytest.mark.skipif(
    not _REMOTE_STORE_URI,
    reason="TEST_OBSERVABLE_STORE_URI not set (provide a real s3:// store via .dev_env) — skipping remote reader test",
)
def test_remote_store_round_trip() -> None:
    """Round-trip against a real emitter store (S3) when one is configured.

    Set ``TEST_OBSERVABLE_STORE_URI`` to a real store URI (e.g.
    ``s3://<bucket>/<prefix>/<experiment_id>/<store>``) in ``.dev_env``; AWS
    credentials are loaded by config.py's ``load_dotenv()``. Decoupled from
    ``data_layout.ray_seed_store_uri`` so it validates the actual on-S3 layout directly.
    """
    idx = list_observables(_REMOTE_STORE_URI)
    assert idx.store in ("zarr", "parquet")
    assert idx.observables, "remote store exposed no observables"
    first = idx.observables[0].name
    kind, time, series = read_observables(_REMOTE_STORE_URI, names=[first], max_points=50)
    assert kind == idx.store
    assert len(time) <= 50
    assert first in series
    assert len(series[first]) == len(time)
