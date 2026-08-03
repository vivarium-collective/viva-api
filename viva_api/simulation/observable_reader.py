"""Read a simulation's emitter store (XArray/zarr or Parquet) into plain Python.

The reader is store-agnostic over fsspec URIs: ``file://`` paths work in tests,
``s3://`` paths work in production (s3fs handles credentials via the pod's role).
It has one job — turn a store URI into observable metadata and timeseries — so it
can be unit-tested without S3 or a database.

Two zarr layouts are supported:

* **Flat** — a single ``xarray.Dataset`` whose ``data_vars`` are the observables
  (used by simple/legacy emitters and the unit-test fixtures).
* **Hive-partitioned XArrayEmitter datatree** — what the Ray comparison engine
  actually writes to S3 (``data_layout.ray_seed_store_uri``): ``…/v2ecoli_seed{NN}.zarr`` with the
  interior ``experiment_id=…/variant=…/lineage_seed=…/{observable}/generation={G}``.
  Each observable is a leaf node whose ``generation={G}`` data-vars are the
  per-generation segments; the matching ``time_gen={G}`` arrays live on the
  parent node. Observables are reconstructed by concatenating the segments in
  generation order (mirrors the dashboard's ``study_charts`` reader).

Read performance (per review):
- The Parquet path uses **Polars** with **column projection** — listing reads
  only the schema + the parquet footer row count (no column data), and fetching
  reads only the requested columns + ``time``.
- ``read_observables`` supports **decimation** (``stride`` / ``max_points``).
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import Any, Literal

import fsspec

# The real XArrayEmitter stores aren't consolidated; opening with
# ``consolidated=False`` silences xarray's fallback warning and skips a probe.


@dataclass
class ObservableInfo:
    name: str
    dims: list[str]
    shape: list[int]


@dataclass
class StoreIndex:
    store: Literal["zarr", "parquet"]
    observables: list[ObservableInfo]


def detect_store_kind(store_uri: str) -> Literal["zarr", "parquet"]:
    """Return 'zarr' if the store is a zarr group (has a .zgroup/.zattrs), else 'parquet'."""
    fs, path = fsspec.core.url_to_fs(store_uri)
    if fs.exists(f"{path}/.zgroup") or fs.exists(f"{path}/zarr.json"):
        return "zarr"
    return "parquet"


def _effective_stride(n: int, stride: int, max_points: int | None) -> int:
    """Combine an explicit ``stride`` with a ``max_points`` cap into one step >= 1.

    ``max_points`` wins when it implies a coarser step than ``stride`` (so the
    response never exceeds ``max_points`` points); otherwise ``stride`` is used.
    """
    step = max(1, stride)
    if max_points is not None and max_points >= 1 and n > 0:
        step = max(step, math.ceil(n / max_points))
    return step


def _sanitize(value: float) -> float | None:
    """Convert NaN/Inf to None for JSON-safe output."""
    if math.isfinite(value):
        return value
    return None


# ── Hive-partitioned XArrayEmitter datatree ────────────────────────────────


def _gen_order(parent: Any) -> list[int]:
    """Sorted generation indices ``G`` for which the parent has a ``time_gen={G}``."""
    gens: list[int] = []
    for v in parent.data_vars or {}:
        s = str(v)
        if s.startswith("time_gen="):
            try:
                gens.append(int(s.split("=", 1)[1]))
            except ValueError:
                continue
    return sorted(gens)


def _partition_leaves(dt: Any) -> tuple[dict[str, Any], Any] | None:
    """Return ``({observable_name: leaf_node}, time_parent)`` for a partitioned
    datatree, or ``None`` if the store isn't hive-partitioned by generation.

    A leaf is an observable iff it has ``generation={G}`` data-vars and its parent
    carries the matching ``time_gen={G}`` arrays. All observable leaves under one
    lineage share that parent (and thus a single time axis); the first such parent
    found is used.
    """
    leaves: dict[str, Any] = {}
    parent: Any = None
    for node in dt.subtree:
        if node.parent is None:
            continue
        has_gen = any(str(v).startswith("generation=") for v in (node.data_vars or {}))
        if not has_gen:
            continue
        if not _gen_order(node.parent):
            continue
        if parent is None:
            parent = node.parent
        # Keep only leaves sharing the first parent (one seed → one lineage).
        if node.parent is parent:
            leaves[str(node.name)] = node
    if not leaves or parent is None:
        return None
    return leaves, parent


def _read_partitioned_zarr(
    store_uri: str, names: list[str], stride: int, max_points: int | None
) -> tuple[list[float], dict[str, list[float | None]]] | None:
    """Read observables from a hive-partitioned datatree, or ``None`` if the store
    isn't partitioned (caller falls back to the flat path)."""
    import numpy as np
    import xarray as xr

    try:
        dt = xr.open_datatree(store_uri, engine="zarr", consolidated=False)
    except Exception:
        return None
    part = _partition_leaves(dt)
    if part is None:
        return None
    leaves, parent = part

    wanted = names or sorted(leaves)
    missing = [n for n in wanted if n not in leaves]
    if missing:
        raise KeyError(f"observables not in store: {missing}")

    gens = _gen_order(parent)
    raw_time: list[float] = []
    for g in gens:
        raw_time.extend(float(t) for t in np.asarray(parent[f"time_gen={g}"].values).ravel())
    step = _effective_stride(len(raw_time), stride, max_points)
    time = raw_time[::step]

    series: dict[str, list[float | None]] = {}
    for nm in wanted:
        node = leaves[nm]
        vals: list[float | None] = []
        for g in gens:
            var_name = f"generation={g}"
            if var_name not in (node.data_vars or {}):
                continue
            arr = np.asarray(node[var_name].values)
            if arr.ndim != 1:
                raise ValueError(
                    f"observable {nm!r} is not a 1-D timeseries (shape {tuple(arr.shape)}); "
                    "multi-dimensional observables are not supported"
                )
            vals.extend(_sanitize(float(v)) for v in arr)
        series[nm] = vals[::step]
    return time, series


def _list_partitioned_zarr(store_uri: str) -> list[ObservableInfo] | None:
    """List observables in a hive-partitioned datatree (concatenated length per
    observable), or ``None`` if the store isn't partitioned."""
    import xarray as xr

    try:
        dt = xr.open_datatree(store_uri, engine="zarr", consolidated=False)
    except Exception:
        return None
    part = _partition_leaves(dt)
    if part is None:
        return None
    leaves, parent = part
    gens = _gen_order(parent)
    total = sum(int(parent[f"time_gen={g}"].shape[0]) for g in gens)
    return [ObservableInfo(name=name, dims=["time"], shape=[total]) for name in sorted(leaves)]


# ── Public API ─────────────────────────────────────────────────────────────


def list_observables(store_uri: str) -> StoreIndex:
    """Open the emitter store and return its observable variables (name, dims, shape).

    Metadata-only: the Parquet path reads the schema and the footer row count, not
    any column data.
    """
    kind = detect_store_kind(store_uri)
    if kind == "zarr":
        partitioned = _list_partitioned_zarr(store_uri)
        if partitioned is not None:
            return StoreIndex(store="zarr", observables=partitioned)

        import xarray as xr

        ds = xr.open_zarr(store_uri)
        try:
            obs = [
                ObservableInfo(name=str(name), dims=[str(d) for d in var.dims], shape=[int(s) for s in var.shape])
                for name, var in ds.data_vars.items()
            ]
        finally:
            ds.close()
        return StoreIndex(store="zarr", observables=obs)

    import polars as pl

    lf = pl.scan_parquet(store_uri)
    names = lf.collect_schema().names()
    nrows = int(lf.select(pl.len()).collect().item())
    obs = [ObservableInfo(name=str(c), dims=["time"], shape=[nrows]) for c in names if c != "time"]
    return StoreIndex(store="parquet", observables=obs)


def read_observables(
    store_uri: str,
    names: list[str],
    *,
    stride: int = 1,
    max_points: int | None = None,
) -> tuple[Literal["zarr", "parquet"], list[float], dict[str, list[float | None]]]:
    """Return (store_kind, time, {name: values}) for the requested observables.

    ``names=[]`` returns every observable in the store. The time axis is taken from
    the ``time`` coordinate if present, else a 0..N index.

    ``stride`` returns every Nth point; ``max_points`` caps the total points (and
    overrides ``stride`` when it implies a coarser step). Decimation is applied
    *before* materialization where possible — a lazy row-slice (Parquet) / ``isel``
    (flat zarr) — so only the kept points are read and serialized.

    Raises ``KeyError`` if a requested observable is absent, and ``ValueError`` if
    an observable's values are not a 1-D timeseries. Non-finite float values
    (NaN, ±Inf) are sanitized to ``None``.
    """
    kind = detect_store_kind(store_uri)
    if kind == "zarr":
        partitioned = _read_partitioned_zarr(store_uri, names, stride, max_points)
        if partitioned is not None:
            return "zarr", partitioned[0], partitioned[1]

        import numpy as np
        import xarray as xr

        ds = xr.open_zarr(store_uri)
        try:
            wanted = names or [str(n) for n in ds.data_vars]
            missing = [n for n in wanted if n not in ds.data_vars]
            if missing:
                raise KeyError(f"observables not in store: {missing}")
            n = int(ds["time"].shape[0]) if "time" in ds.coords else int(ds[wanted[0]].shape[0])
            step = _effective_stride(n, stride, max_points)
            row_slice = slice(None, None, step)
            if "time" in ds.coords:
                time_da = ds["time"]
                raw_time = np.asarray(time_da.isel({time_da.dims[0]: row_slice}).values).ravel()
                time = [float(t) for t in raw_time]
            else:
                time = [float(i) for i in range(0, n, step)]
            series: dict[str, list[float | None]] = {}
            for nm in wanted:
                var = ds[nm]
                if var.ndim != 1 or var.shape[0] != n:
                    raise ValueError(
                        f"observable {nm!r} is not a 1-D timeseries (shape {tuple(var.shape)}); "
                        "multi-dimensional observables are not supported"
                    )
                arr = np.asarray(var.isel({var.dims[0]: row_slice}).values)
                series[nm] = [_sanitize(float(v)) for v in arr]
        finally:
            ds.close()
        return kind, time, series

    import polars as pl

    lf = pl.scan_parquet(store_uri)
    schema_names = lf.collect_schema().names()
    wanted = names or [c for c in schema_names if c != "time"]
    missing = [n for n in wanted if n not in schema_names]
    if missing:
        raise KeyError(f"observables not in store: {missing}")
    has_time = "time" in schema_names
    nrows = int(lf.select(pl.len()).collect().item())
    step = _effective_stride(nrows, stride, max_points)
    cols = ["time", *wanted] if has_time else list(wanted)
    df = lf.select(cols).gather_every(step).collect()
    time = [float(t) for t in df["time"].to_list()] if has_time else [float(i) for i in range(0, nrows, step)]
    series_p: dict[str, list[float | None]] = {n: [_sanitize(float(v)) for v in df[n].to_list()] for n in wanted}
    return kind, time, series_p


# ── Async API (for FastAPI routes) ─────────────────────────────────────────
#
# ``list_observables`` / ``read_observables`` wrap synchronous libraries
# (fsspec + xarray/zarr + polars) that have no true-async read path, so they
# must run off the event loop. These thin wrappers offload to a worker thread
# (``asyncio.to_thread``) and expose an awaitable surface, so async callers
# write ``await read_observables_async(...)`` instead of threading by hand.


async def list_observables_async(store_uri: str) -> StoreIndex:
    """Async wrapper over :func:`list_observables` (offloaded to a thread)."""
    return await asyncio.to_thread(list_observables, store_uri)


async def read_observables_async(
    store_uri: str,
    names: list[str],
    *,
    stride: int = 1,
    max_points: int | None = None,
) -> tuple[Literal["zarr", "parquet"], list[float], dict[str, list[float | None]]]:
    """Async wrapper over :func:`read_observables` (offloaded to a thread)."""
    return await asyncio.to_thread(read_observables, store_uri, names, stride=stride, max_points=max_points)
