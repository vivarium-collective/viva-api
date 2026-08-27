"""Single owner of the S3 data layout for simulation outputs and ParCa caches.

Every place that constructs a path for a simulation's outputs or its ParCa cache
derives it from here, so the writer, the reader (observables), and the downloader
can never drift apart (the failure mode behind #152/#153 — where the observables
reader and the tar.gz downloader had disagreed on the per-seed store path).

Two backends, two DIFFERENT layouts under the same output prefix, kept as separate
namespaces so "which layout" is a typed choice rather than a naming convention —
you can't call a Ray-only path (e.g. ``seed_store_uri``) in a Nextflow context:

    RayLayout (v2ecoli, XArray/zarr) — single-nested
        {prefix}/{experiment_id}/                      <- results sync dir
        {prefix}/{experiment_id}/v2ecoli_seed{NN}.zarr <- per-seed store (read)
        {prefix}/{experiment_id}/summary.json
      ParCa cache (keyed by image commit, not experiment):
        ray-parca-cache/{commit}/          <- v2ecoli cache
        ray-upstream-parca-cache/{commit}/ <- pristine upstream cache

    NextflowLayout (vEcoli, parquet)
        {prefix}/{experiment_id}                   <- workflow emitter out_uri (write)
        {prefix}/{experiment_id}/{experiment_id}/  <- DOWNLOAD prefix (DOUBLE-nested;
                                                      vEcoli nests the run dir under
                                                      the experiment prefix)

The single-nested Ray layout vs the double-nested Nextflow download layout is a
real, load-bearing asymmetry. ``layout_for(backend)`` binds the choice to the
authoritative ``ComputeBackend`` discriminator (from ``compute_backend_for_repo``),
so dynamic call sites select a layout from the same enum they already branch on.

Paths read ``get_settings()`` internally so call sites stay thin. All sim-data
paths use the ``s3_work_bucket``.
"""

from viva_api.config import ComputeBackend, get_settings


def _bucket() -> str:
    return get_settings().s3_work_bucket


def _prefix() -> str:
    return get_settings().s3_output_prefix


def s3_uri(key: str) -> str:
    """Prepend the sim-data bucket (``s3_work_bucket``) to a bucket-relative key."""
    return f"s3://{_bucket()}/{key}"


def key_from_uri(uri: str) -> str:
    """Strip a ``s3://<bucket>/`` prefix, returning the bucket-relative key.

    The inverse of ``s3_uri``. ``S3FilePath.s3_path`` is documented as bucket-relative
    (``FileServiceS3`` resolves the bucket separately from settings) -- passing a full
    ``s3://...`` URI into it double-prefixes the bucket into the key AND, via
    ``Path()``'s slash-collapsing, mangles ``s3://`` into ``s3:/``, so the resulting
    "key" never matches a real object and every existence check silently 404s. Any
    caller building an ``S3FilePath`` from a stored full URI (e.g. a DB record's
    ``result_uri``) must go through this first.
    """
    if not uri.startswith("s3://"):
        return uri
    return uri.removeprefix("s3://").split("/", 1)[1]


class RayLayout:
    """v2ecoli / XArray-zarr output layout (single-nested), plus the Ray ParCa caches."""

    @staticmethod
    def experiment_prefix(experiment_id: str) -> str:
        """Bucket-relative key prefix for a Ray run's outputs (single-nested)."""
        return f"{_prefix()}/{experiment_id}"

    @staticmethod
    def results_uri(experiment_id: str) -> str:
        """Where the Ray ensemble syncs its outputs (trailing slash = sync dir)."""
        return s3_uri(f"{RayLayout.experiment_prefix(experiment_id)}/")

    @staticmethod
    def seed_store_uri(experiment_id: str, seed: int) -> str:
        """Per-seed XArray/zarr store the observables reader opens (Ray-only).

        Verified against real smsvpctest ``sim{N}-v2c-*`` runs (see #152).
        """
        return s3_uri(f"{RayLayout.experiment_prefix(experiment_id)}/v2ecoli_seed{seed:02d}.zarr")

    @staticmethod
    def summary_key(experiment_id: str) -> str:
        """Bucket-relative key for a Ray run's ensemble ``summary.json``."""
        return f"{RayLayout.experiment_prefix(experiment_id)}/summary.json"

    @staticmethod
    def seed_results_prefix(experiment_id: str, seed: int) -> str:
        """Bucket-relative key prefix for ONE seed's own results (backlog item
        33/35: per-generation chain-dispatch). Every generation's job for a
        given seed shares this prefix, so the parquet sweep / zarr store /
        summary.json accumulate under one seed-scoped location instead of the
        ensemble-wide ``experiment_prefix`` every seed's job would otherwise
        collide on. ``seed_{seed:02d}`` (zero-padded, underscored) matches the
        multiseed analysis step's own real, confirmed lookup convention (
        ``v2ecoli.workflow.analysis_runner``'s per-seed sweep_dir) — a
        DIFFERENT format from this class's other two per-seed conventions
        (``seed_store_uri``'s ``v2ecoli_seed{NN}``, ``daughter_state_uri``'s
        unpadded ``seed{N}``); each pre-dates this one and is owned by a
        different reader, so unifying them is out of scope here.
        """
        return f"{RayLayout.experiment_prefix(experiment_id)}/seed_{seed:02d}"

    @staticmethod
    def seed_results_uri(experiment_id: str, seed: int) -> str:
        """Where ONE seed's chain-dispatch jobs write their results (trailing
        slash = sync/prefix dir). See ``seed_results_prefix``."""
        return s3_uri(f"{RayLayout.seed_results_prefix(experiment_id, seed)}/")

    @staticmethod
    def parca_cache_uri(commit: str, *, upstream: bool = False, variant: str | None = None) -> str:
        """S3 URI for a commit's ParCa cache (trailing slash = 'directory').

        ``upstream=True`` selects the SEPARATE pristine-upstream-vEcoli cache used by
        the ``--composite vecoli`` wrapper (an upstream-built ``simData.cPickle``);
        ``upstream=False`` is the v2ecoli cache. Both the ParCa job (writes) and the
        sim job (stages) derive the same URI, so the hand-off needs no runtime wiring.

        ``variant`` (item 87) is None for every existing caller -- returns the exact
        same commit-only key as before this param existed. This cache is a SHARED
        resource: any concurrent dispatch on the same commit stages from this one
        key, so a config-driven build (e.g. a custom-strain ``new_genes`` ParCa)
        must NEVER write here -- it would silently overwrite the plain baseline
        cache every other concurrent dispatch on that commit relies on. Pass an
        explicit label (e.g. ``"custom-strain"``) to get a nested, non-colliding
        key instead
        (``<kind>/<commit>/<variant>/``) -- still commit-scoped (a variant build
        from a different commit is still a different cache), just never the bare
        commit-only path a plain baseline build/stage would ever read.
        """
        kind = "ray-upstream-parca-cache" if upstream else "ray-parca-cache"
        if variant:
            return s3_uri(f"{kind}/{commit}/{variant}/")
        return s3_uri(f"{kind}/{commit}/")

    @staticmethod
    def daughter_state_prefix(experiment_id: str) -> str:
        """Bucket-relative key prefix under which per-seed chain-dispatch
        daughter-state checkpoints live (backlog item 33: per-generation task
        decomposition)."""
        return f"{RayLayout.experiment_prefix(experiment_id)}/daughter-state"

    @staticmethod
    def daughter_state_uri(experiment_id: str, seed: int, generation: int) -> str:
        """Per-seed, per-generation daughter-state checkpoint URI.

        Mirrors vEcoli-private's own Nextflow task I/O hand-off (``sim.nf``):
        each seed's per-generation Batch job writes THIS generation's daughter
        state here (``LineageProcess.daughter_state_out_path``) so the NEXT
        generation's job (chained via that job's own ``dependsOn``) can load it
        as its carry-state in (``initial_carry_state_path``) -- task retry at
        generation granularity IS checkpoint/resume, no in-process pickling
        needed. Ephemeral hand-off, never browsed/sorted by a human, so unlike
        ``seed_store_uri`` this deliberately skips zero-padding: the plain
        ``seed{seed}``/``gen{generation}`` form is what the submitting Python
        code (via this function) and the v2ecoli container's own command
        (embedded verbatim at submission time -- see ``_seed_generation_command``)
        both reference, so keeping the format trivial keeps the two independent
        call sites trivially in sync.

        Nothing has been dispatched against the old ``wave-state`` key prefix
        yet (this whole capability is still unwired from any HTTP router --
        see ``SimulationServiceRay.submit_chain_dispatch_job``), so renaming
        the prefix alongside the accessor methods costs nothing: there is no
        already-landed S3 data under the old name to migrate.
        """
        return s3_uri(f"{RayLayout.daughter_state_prefix(experiment_id)}/seed{seed}/gen{generation}.pkl")


class NextflowLayout:
    """vEcoli / parquet output layout (Batch + SLURM)."""

    @staticmethod
    def output_uri(experiment_id: str) -> str:
        """Emitter ``out_uri`` the Batch/Nextflow workflow writes to (single-nested).

        NOTE: the download side reads one level deeper — see ``experiment_prefix`` —
        because the workflow nests the run dir under this prefix.
        """
        return s3_uri(f"{_prefix()}/{experiment_id}")

    @staticmethod
    def experiment_prefix(experiment_id: str) -> str:
        """Bucket-relative key prefix the Nextflow DOWNLOAD reads. DOUBLE-nested
        (``{prefix}/{experiment_id}/{experiment_id}``) — intentionally distinct from
        the single-nested ``RayLayout.experiment_prefix``."""
        return f"{_prefix()}/{experiment_id}/{experiment_id}"


def layout_for(backend: ComputeBackend) -> type[RayLayout] | type[NextflowLayout]:
    """Select the output layout for a backend (the authoritative discriminator).

    Returns the layout *class*; the union return means a Ray-only method
    (``seed_store_uri``) isn't callable until the caller narrows to ``RayLayout``.
    """
    if backend == ComputeBackend.RAY:
        return RayLayout
    if backend in (ComputeBackend.BATCH, ComputeBackend.SLURM):
        return NextflowLayout
    raise ValueError(f"no data layout defined for backend {backend!r}")
