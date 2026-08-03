"""Generic process-bigraph runner executed inside the compose container.

CLI contract (fixed by sms-api's ``_build_run_command``)::

    python run_pbg.py <input-file> -o <outdir> -n <steps>

Writes results into ``/experiment/output`` (the bind-mounted, zipped dir).
The ``-o`` value is accepted for CLI compatibility but output always lands in
``RESULTS_DIR`` so it matches sms-api's ``zip -r ../results.zip`` collection.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

RESULTS_DIR = Path(os.environ.get("PBG_RESULTS_DIR", "/experiment/output"))


def _workspace_core() -> Any | None:
    """The *workspace's own* core builder, when the deployment names one.

    ``PBG_CORE_BUILDER`` is a ``"module.path:callable"`` string (e.g.
    ``"v2ecoli.core:build_core"``) that the compose container sets. This matters
    because the generic core below registers only process-bigraph's base types plus
    the pbg-emitters links — a workspace's own ``build_core`` typically registers
    much more (v2ecoli's registers ``ECOLI_TYPES`` plus several process/step links).
    Process *addresses* (``local:…``) resolve dynamically via importlib, but
    registered *types* do not, so a document referencing a workspace type fails to
    resolve against the generic core.

    Kept generic on purpose: any workspace names its own builder rather than this
    runner hardcoding one. Returns None (falling back to the generic core) when the
    var is unset or the target can't be imported.
    """
    spec = os.environ.get("PBG_CORE_BUILDER", "").strip()
    if not spec:
        return None
    if ":" not in spec:
        print(f"run_pbg: ignoring malformed PBG_CORE_BUILDER={spec!r} (want 'module:callable')")
        return None
    mod_name, _, fn_name = spec.partition(":")
    try:
        import importlib

        core = getattr(importlib.import_module(mod_name), fn_name)()
    except Exception as e:
        print(f"run_pbg: PBG_CORE_BUILDER={spec!r} failed ({e}); falling back to the generic core")
        return None
    print(f"run_pbg: using workspace core from {spec}")
    return core


def _build_core() -> Any:
    """A process-bigraph Core (process-bigraph's own base types) with pbg-emitters'
    link classes registered.

    ``Composite`` requires a ``core`` (``bigraph_schema.edge.Edge.__init__`` raises
    ``"must provide a core"`` when it's ``None``) — the baseline construction is
    ``allocate_core()`` + ``process_bigraph.register_types()``, the same pair
    ``process_bigraph``'s own package init uses. ``pbg-emitters`` ships in every
    compose container (``container_def.py``) but a document's
    ``{"address": "local:ParquetEmitter", ...}`` step only resolves if the Core
    it's built against has that address registered, so it's added the same way
    v2ecoli's own ``build_core()`` does (``v2ecoli/core.py``) — any uploaded
    document that wires a pbg-emitters step (zarr/parquet, matching what
    ``observable_reader.py`` expects) resolves the same way it would in a
    workspace that authored it.
    """
    from bigraph_schema import allocate_core
    from process_bigraph import register_types

    core = register_types(allocate_core())

    try:
        from pbg_emitters import ParquetEmitter

        core.register_link("ParquetEmitter", ParquetEmitter)
    except ImportError:
        pass  # [parquet] extra not installed in this image
    try:
        from pbg_emitters import SQLiteEmitter

        core.register_link("SQLiteEmitter", SQLiteEmitter)
    except ImportError:
        pass  # [sqlite] extra not installed in this image
    try:
        from pbg_emitters import XArrayEmitter

        core.register_link("XArrayEmitter", XArrayEmitter)
    except ImportError:
        pass  # [xarray] extra not installed in this image
    return core


# Emitter config keys that name WHERE output is written, by emitter kind.
# ParquetEmitter -> out_dir (emitter_presets.parquet_vecoli), XArrayEmitter -> out_uri.
_EMITTER_OUT_KEYS = ("out_dir", "out_uri")


def _redirect_emitters(node: Any, results_dir: Path) -> int:
    """Point every emitter step's output location at *results_dir*, recursively.

    A document's emitter usually resolves its own output location relative to the
    authoring WORKSPACE — v2ecoli's baseline omits ``out_dir`` on purpose so it
    lands in ``<workspace>/.pbg/parquet-runs``. That is correct locally and wrong
    here: the Batch entrypoint syncs only ``RAY_OUT_DIR`` (this ``results_dir``) to
    S3, so a workspace-relative emitter writes real output that never leaves the
    container — the run "succeeds" and produces nothing readable.

    So we rewrite the location key in the loaded document before constructing the
    Composite. Any pre-existing value is overridden rather than preserved: it was
    computed in the authoring environment, and inside this container the ONLY
    directory that reaches S3 is ``results_dir``. Returns the number of emitters
    redirected (0 is a legitimate answer — a document need not declare one).
    """
    redirected = 0
    if isinstance(node, dict):
        address = node.get("address")
        if isinstance(address, str) and "emitter" in address.lower():
            config = node.get("config")
            if not isinstance(config, dict):
                config = {}
                node["config"] = config
            # Reuse whichever key this emitter already speaks; default to out_dir.
            key = next((k for k in _EMITTER_OUT_KEYS if k in config), "out_dir")
            before = config.get(key)
            config[key] = str(results_dir)
            redirected += 1
            print(f"run_pbg: redirected emitter {address} {key}: {before!r} -> {results_dir}")
        for value in node.values():
            redirected += _redirect_emitters(value, results_dir)
    elif isinstance(node, list):
        for item in node:
            redirected += _redirect_emitters(item, results_dir)
    return redirected


def run(input_file: str, steps: int, results_dir: Path = RESULTS_DIR) -> Path:
    """Load a ``.pbg`` document, run it ``steps`` times, write ``final_state.json``.

    Any pbg-emitters step the document itself wires (``local:ParquetEmitter`` etc.)
    resolves via ``_build_core()`` and writes its own zarr/parquet output alongside
    this snapshot — ``final_state.json`` stays as the always-present fallback so a
    document with no emitter step still produces *something* under ``results_dir``.
    """
    from process_bigraph import Composite  # imported lazily so tests can stub it

    results_dir.mkdir(parents=True, exist_ok=True)
    document = json.loads(Path(input_file).read_text())
    # Prefer the workspace's own core (it registers types the generic one can't know
    # about); fall back to the generic core when no builder is named. Test against
    # None explicitly — a Core is a registry-ish object that may well define
    # __bool__/__len__, and `or` would silently discard a valid-but-empty one.
    core = _workspace_core()
    if core is None:
        core = _build_core()
    # Emitters must write where the entrypoint syncs from, not where the authoring
    # workspace would have put them.
    _redirect_emitters(document, results_dir)
    composite = Composite(document, core=core)  # full-path local:! addresses resolve via importlib
    composite.run(steps)
    out = results_dir / "final_state.json"
    out.write_text(json.dumps(composite.serialize_state(), default=str))
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file")
    parser.add_argument("-o", "--output", default=str(RESULTS_DIR))
    parser.add_argument("-n", "--steps", type=int, default=1)
    args = parser.parse_args(argv)
    run(args.input_file, args.steps)


if __name__ == "__main__":
    main()
