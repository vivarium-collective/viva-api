"""Generic process-bigraph runner executed inside the compose container.

CLI contract (fixed by sms-api's ``_build_run_command``)::

    python run_pbg.py <input-file> -o <outdir> -n <steps>

Writes results into ``/experiment/output`` (the bind-mounted, zipped dir).
The ``-o`` value is accepted for CLI compatibility but output always lands in
``RESULTS_DIR`` so it matches sms-api's ``zip -r ../results.zip`` collection.

A second, equally generic mode builds the document itself rather than reading
one from disk::

    python run_pbg.py --composite-id <id> --overrides '<json>' -n <steps>

``<id>`` is any id resolvable by ``process_bigraph.composite_spec.get()`` (the
single registry every ``@composite_generator``/``@composite_spec`` decorator
registers into) — not specific to any one workspace. This is what lets a
model-specific dispatcher (e.g. viva-api's vEcoli ensemble endpoint) submit a
config-driven run through the exact same execution mechanism a hand-authored
``.pbg`` document uses, instead of shelling out to a bespoke per-workspace CLI
script. Exactly one of ``input_file`` or ``--composite-id`` is required.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

RESULTS_DIR = Path(os.environ.get("PBG_RESULTS_DIR", "/experiment/output"))


@contextlib.contextmanager
def _v2ecoli_parquet_emitter_override(out_dir: Path, overrides: dict[str, Any] | None) -> Iterator[None]:
    """Best-effort: make v2ecoli's generator-declared default ParquetEmitter
    write real, correctly-partitioned output to *out_dir* instead of
    resolving a workspace-relative default.

    v2ecoli composites built via ``@composite_generator(emitters=[...])``
    (``ecoli_baseline``/``batch_baseline``) don't leave their default emitter
    as a document node ``Composite`` resolves lazily -- ``to_document()``
    eagerly constructs the actual ``ParquetEmitter`` instance itself, inside
    ``v2ecoli.composites._helpers._build_declared_emitter``, and embeds the
    already-built instance in the returned document. By the time that
    document reaches this runner, ``_redirect_emitters()``'s raw-document
    mutation below has nothing to act on -- the instance already exists,
    with its ``out_dir`` already resolved (to ``<workspace>/.pbg/parquet-runs``
    when a ``workspace.yaml`` is found by walking up from cwd, or a bare
    ``out/parquet`` relative path otherwise) via v2ecoli's own
    ``_find_workspace_root()`` fallback -- never synced to S3, since a compose
    container's cwd (``/app/v2ecoli``) isn't a real workspace checkout.
    Confirmed directly: a real local composite run with no override produced
    zero readable output; the identical run with this override produced real
    ``history/*.pq`` files, with the real hive-partition columns intact, at
    the target directory.

    v2ecoli's own ``set_parquet_emitter_override`` is the documented, existing
    hook for exactly this case (its priority is checked before the eager
    declared-default path -- see ``_build_declared_emitter``'s callers in
    ``v2ecoli/composites/_helpers.py`` -- "external overrides still win", per
    ``ecoli_baseline.py``'s own docstring). Set it before building the
    document, clear it once built -- mirrors v2ecoli's own
    ``parquet_emitter()`` context manager's try/finally shape.

    The override dict is passed straight through to ``ParquetEmitter(cfg,
    core)`` verbatim (confirmed by reading ``_helpers.py``'s own
    ``parquet_override is not None`` branch: ``cfg = {'emit': ..., **
    parquet_override}``) -- it is NOT run through ``parquet_vecoli()``'s own
    preset-building the way the generator-declared default is, so this
    builds that same preset itself (real hive-partition ``partitioning_keys``,
    dtype overrides, matching vEcoli's own layout) rather than passing a bare
    ``out_dir`` and losing the partition columns every downstream DuckDB query
    filters on (``variant``/``lineage_seed``/``generation``/``agent_id`` --
    confirmed empirically: a bare ``{"out_dir": ...}`` override wrote real
    data but with none of those as either real columns or hive path segments).
    ``seed``/``initial_generation_index`` are this runner's own real per-job
    identity (see ``simulation_service_ray.py::_seed_generation_command``) --
    threading them through is what makes multiple real generations of the
    same seed distinguishable in the aggregated history instead of colliding
    on the same partition.

    A no-op (nothing to override) for any composite that isn't v2ecoli's, or
    any v2ecoli image without the [parquet] extra installed.
    """
    try:
        from v2ecoli.composites._helpers import set_parquet_emitter_override
        from v2ecoli.library.emitter_presets import parquet_vecoli
    except ImportError:
        yield
        return
    overrides = overrides or {}
    seed = overrides.get("seed")
    generation = overrides.get("initial_generation_index")
    override = parquet_vecoli(
        out_dir=str(out_dir),
        experiment_id=overrides.get("experiment_id") or "default",
        lineage_seed=int(seed) if seed is not None else 0,
        generation=int(generation) if generation is not None else None,
    )
    set_parquet_emitter_override(override)
    try:
        yield
    finally:
        set_parquet_emitter_override(None)


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


def _flush_emitters(composite: Any) -> None:
    """Flush any ParquetEmitter steps' buffered rows before the process exits.

    A ParquetEmitter is typically constructed deep inside a composite's step
    factory (see ``viva_emitters.lifecycle``'s own docstring) — this generic
    runner never sees the instance directly, so it cannot call ``close()`` on
    it itself. Without an explicit flush, the trailing partial batch (rows
    since the last ``batch_size`` flush) stays in memory and is lost when the
    process exits: ``ParquetEmitter.__del__``'s finalizer is a best-effort,
    non-blocking safety net (by its own docstring), not a guarantee, and
    interpreter shutdown does not reliably run `__del__` on module-level
    instances. Mirrors v2ecoli's own ``composites._helpers.flush_parquet()``,
    reimplemented generically here since ``run_pbg.py`` has no v2ecoli-specific
    knowledge — same ``viva_emitters.ParquetEmitter.flush_all_in_composite``
    call, guarded the same way ``_build_core()`` guards every pbg-emitters
    import (the ``[parquet]`` extra need not be installed in every image).
    """
    try:
        from viva_emitters import ParquetEmitter
    except ImportError:
        return  # [parquet] extra not installed in this image
    ParquetEmitter.flush_all_in_composite(composite, success=True)


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


def _resolve_document(
    input_file: str | None, composite_id: str | None, overrides: dict[str, Any] | None, core: Any
) -> dict[str, Any]:
    """Load a static ``.pbg`` document, or build one from a registered composite.

    The composite-id branch resolves through ``process_bigraph.composite_spec`` —
    the same registry ``vivarium_workbench.lib.pbg_export.export_composite_pbg``
    already resolves composites through for the Composites-tab / remote-run path.
    Building the document HERE (in-process, same call that constructs ``Composite``
    below) never crosses a serialization boundary, so unlike a document loaded from
    disk, realized-edge fields (a live ``instance``, resolved port schemas) are
    exactly what the generator function returns — nothing to strip or rewrite.
    """
    if composite_id:
        from process_bigraph.composite_spec import discover_specs
        from process_bigraph.composite_spec import get as get_spec

        spec = get_spec(composite_id)
        if spec is None:
            # The defining module may not have been imported yet (its
            # @composite_generator/@composite_spec decorator only fires on
            # import) — discover_specs() walks every installed
            # bigraph-schema-dependent package to force that, then retry once.
            discover_specs()
            spec = get_spec(composite_id)
        if spec is None:
            raise SystemExit(f"run_pbg: no composite registered as {composite_id!r}")
        document: dict[str, Any] = spec.to_document(overrides=overrides or {}, core=core)
        return document
    if input_file is None:
        raise ValueError("_resolve_document: input_file is required when composite_id is not given")
    loaded: dict[str, Any] = json.loads(Path(input_file).read_text())
    return loaded


def run(
    input_file: str | None,
    steps: int,
    results_dir: Path = RESULTS_DIR,
    *,
    composite_id: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> Path:
    """Get a document (static file, or built from ``composite_id`` + ``overrides``),
    run it ``steps`` times, write ``final_state.json``.

    Any pbg-emitters step the document itself wires (``local:ParquetEmitter`` etc.)
    resolves via ``_build_core()`` and writes its own zarr/parquet output alongside
    this snapshot — ``final_state.json`` stays as the always-present fallback so a
    document with no emitter step still produces *something* under ``results_dir``.
    """
    from process_bigraph import Composite  # imported lazily so tests can stub it

    results_dir.mkdir(parents=True, exist_ok=True)
    # Prefer the workspace's own core (it registers types the generic one can't know
    # about); fall back to the generic core when no builder is named. Test against
    # None explicitly — a Core is a registry-ish object that may well define
    # __bool__/__len__, and `or` would silently discard a valid-but-empty one.
    core = _workspace_core()
    if core is None:
        core = _build_core()
    with _v2ecoli_parquet_emitter_override(results_dir, overrides):
        document = _resolve_document(input_file, composite_id, overrides, core)
        # Emitters must write where the entrypoint syncs from, not where the authoring
        # workspace would have put them.
        _redirect_emitters(document, results_dir)
        composite = Composite(document, core=core)  # full-path local:! addresses resolve via importlib
    composite.run(steps)
    _flush_emitters(composite)
    out = results_dir / "final_state.json"
    out.write_text(json.dumps(composite.serialize_state(), default=str))
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", nargs="?", default=None)
    parser.add_argument("--composite-id", dest="composite_id", default=None)
    parser.add_argument("--overrides", default=None, help="JSON object of composite-generator parameter overrides")
    parser.add_argument("-o", "--output", default=str(RESULTS_DIR))
    parser.add_argument("-n", "--steps", type=int, default=1)
    args = parser.parse_args(argv)
    if bool(args.input_file) == bool(args.composite_id):
        parser.error("exactly one of input_file or --composite-id is required")
    overrides = json.loads(args.overrides) if args.overrides else None
    run(args.input_file, args.steps, composite_id=args.composite_id, overrides=overrides)


if __name__ == "__main__":
    main()
