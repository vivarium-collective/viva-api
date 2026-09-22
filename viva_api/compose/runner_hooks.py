"""SMS's runner hooks: what its model needs of the generic runner, staged beside it (P3d-4c-2).

``run_pbg.py`` (core) is the generic process-bigraph runner, staged into a job and run inside the
science image -- where ``viva_api`` is not installed and never will be. Two things in it were one
model's: making v2ecoli's generator-declared ParquetEmitter write real, correctly-partitioned output
to the job's results directory, and knowing which composite ids are the batch-baseline Step. They
live here, verbatim, and reach the runner the way ``render_nf`` reaches ``run_pbg``: as a sibling
file in the job's ``/tmp``, named by ``PBG_RUNNER_HOOKS=runner_hooks``. A runner with no hooks is
the generic runner.

Stdlib-only at module scope, for the same reason ``run_pbg`` is: this file is executed where only
the science image's packages exist, and imports them lazily, inside the hook.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator, Mapping
from pathlib import Path

# The composite whose batch route (n_seeds > 1 or n_generations > 1) is a single
# Step -- see _check_required_run_interval. Mirrors
# simulation_service_ray.V2ECOLI_BATCH_BASELINE_COMPOSITE_ID without importing
# the service into this runner (which is staged into the container on its own).
batch_baseline_composite_ids: frozenset[str] = frozenset({
    "v2ecoli.composites.ecoli_baseline.ecoli_baseline",
    "v2ecoli.composites.ecoli_baseline",
})


@contextlib.contextmanager
def emitter_override(out_dir: Path, overrides: Mapping[str, object] | None) -> Iterator[None]:
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
        experiment_id=str(overrides.get("experiment_id") or "default"),
        lineage_seed=int(str(seed)) if seed is not None else 0,
        generation=int(str(generation)) if generation is not None else None,
    )
    set_parquet_emitter_override(override)
    try:
        yield
    finally:
        set_parquet_emitter_override(None)
