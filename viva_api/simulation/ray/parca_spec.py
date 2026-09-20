"""ParCa and the caches a simulation stages from: WHERE they live in S3 and the COMMANDS
that build them.

Split out of ``ray/parca.py`` (``docs/plan-core.md`` P2.1, PR 4 of the 2026-09-20 sequence).
These six were methods of ``RayParcaMixin`` that never touched ``self``: each body below is
what it was there, dedented, with the ``self`` parameter dropped and the leading underscore
gone (``_parca_command`` -> ``parca_command``, ``_build_new_gene_cache_command`` ->
``new_gene_cache_command`` ...). Functions of their arguments -- plus, in ``parca_command``
alone, the two ParCa settings (mode, cpus) it has always read through ``_seams``.

This is the half of "ParCa" that is a domain SPECIFICATION: every dispatch mechanism needs
``cache_s3_uri`` and ``parca_command``, and needs nothing else from ParCa. ParCa itself is
NOT submitted one way -- it is a container job in the mbp-tracked and chain mechanisms and
an MNP job in the ensemble and multi-node composite mechanisms -- so its submission stays
with each mechanism. The half that IS one way, the three cache jobs, is
``RayParcaService`` in ``ray/parca.py``.

SMS code, and it stays SMS code: the parameter calculator, new-gene and variant caches are
domain knowledge. Call these as ``parca_spec.cache_s3_uri(...)`` -- through the module, not
by ``from ... import`` -- so a test can wrap one and see every caller.
"""

import json
import logging
import shlex

from viva_api.common.storage import data_layout
from viva_api.simulation.ray import _seams
from viva_api.simulation.ray.image_paths import (
    NEW_GENE_INDUCED_CACHE_DIR,
    PARCA_CACHE_DIR,
    PARCA_SIMDATA_DIR,
    V2ECOLI_DIR,
    VARIANT_CACHE_DIR,
)

logger = logging.getLogger(__name__)


def cache_s3_uri(commit: str, *, variant: str | None = None) -> str:
    """Deterministic S3 URI for a commit's v2ecoli ParCa cache.

    Both the ParCa job (writes here) and the simulation job (stages from
    here) derive the same URI, so the cache hand-off needs no runtime wiring.

    ``variant`` (backlog item 105): None for every existing caller --
    unchanged commit-only key. See ``RayLayout.parca_cache_uri``'s own
    docstring for why a derived cache (e.g. ``submit_new_gene_cache_job``'s
    induced-expression build) MUST pass a real label here rather than ever
    writing to the shared bare-commit path.
    """
    return data_layout.RayLayout.parca_cache_uri(commit, variant=variant)


def upstream_cache_s3_uri(commit: str, *, variant: str | None = None) -> str:
    """S3 URI for the PRISTINE upstream-vEcoli ParCa cache (``--composite vecoli``).

    Kept SEPARATE from ``cache_s3_uri`` (the v2ecoli cache): the external
    upstream wrapper needs an UPSTREAM-MASTER-built ``simData.cPickle``, not
    the v2ecoli one (whose TCS ``modified_molecules`` skew makes upstream's
    two-component-system ODE go negative). Keyed by the same image commit so
    both engines' parca→sim hand-offs derive their URI with no runtime wiring.

    ``variant`` (item 87): None for every existing caller -- unchanged
    commit-only key. See ``RayLayout.parca_cache_uri``'s own docstring for why
    a config-driven build (e.g. a custom strain's ``new_genes``) MUST pass a
    real label here rather than ever writing to the shared bare-commit path.
    """
    return data_layout.RayLayout.parca_cache_uri(commit, upstream=True, variant=variant)


def parca_command(
    *,
    new_genes: str | None = None,
    bundle_overrides: str | list[str] | None = None,
    rnaseq_source: str | None = None,
    bundle_manifest_path: str | None = None,
    build_combined_bundle_manifest: bool = False,
    include_violacein_bundle: bool = False,
    deterministic_hash_seed: bool = False,
) -> str:
    """Run ParCa, then hydrate the sim-input bundle into PARCA_CACHE_DIR (out/cache).

    v2ecoli's sim loads ``out/cache/{initial_state.json, sim_data_cache.dill, ...}`` via
    ``build_composite(cache_dir=out/cache)``. ``v2ecoli-parca`` only emits the raw
    ``parca_state.pkl`` (+ a Km cache), so ``scripts/build_cache.py`` must hydrate that
    into the bundle. build_cache.py/load_parca_state read a GZIPPED fixture, so gzip the
    parca output first (the round-trip bridges v2ecoli's .pkl→.pkl.gz mismatch). Only
    PARCA_CACHE_DIR is synced to S3 (RAY_OUT_DIR), and that is exactly what the sim stages.

    ``new_genes`` (backlog item 93): a legacy config's own ``parca_options.new_genes``
    (e.g. a custom strain's new-gene insertion subdir) -- generic passthrough to
    ``v2ecoli-parca``'s own ``--new-genes SUBDIR`` flag (default ``"off"``, so omitting
    or passing ``"off"`` builds byte-for-byte the same command as before this param
    existed). No caller-side change required for any dispatch that doesn't set it.

    ``bundle_overrides`` (backlog item 104, extended item 106): a legacy
    config's own ``parca_options.bundle_overrides`` -- generic passthrough to
    ``v2ecoli-parca``'s own ``--bundle-overrides PATH`` flag (``cli/parca.py``,
    ``action="append"``). Same "missed in the new_genes pass" class of gap
    this mirrors exactly (sms-ecoli#184 / viva-api#365): the value survived on
    the stored request but was never read here, so ParCa silently built from
    defaults and any keys the overrides supply were absent. Accepts a single
    string (one flag, byte-for-byte the original behavior) OR a list of
    strings (one ``--bundle-overrides PATH`` per entry, IN ORDER) -- real,
    confirmed need: sms-ecoli's own declared recipe for the CD2 J3/K4
    candidate chassis (``cd2-pnnl-01-bundle-scenarios/sims/run_scenarios.sh``,
    scenario ``rung5_lam075``) stacks TWO overrides in one command
    (``--bundle-overrides .../vio-gfp/overrides.tsv --bundle-overrides
    .../rung5-lambda-075/overrides.tsv``); a single-string field could only
    ever carry one of the two layers. Default ``None`` builds byte-for-byte
    the same command as before this param existed.

    ``rnaseq_source`` (backlog item 106/#166 chassis-provenance thread): a legacy
    config's own ``parca_options.rnaseq_source`` -- generic passthrough to
    ``v2ecoli-parca``'s own ``--rnaseq-source {reference,experimental}`` flag
    (default ``"reference"``). Real, confirmed gap this closes: at least one
    real ``bundle_overrides`` manifest (sms-ecoli's
    ``cd2-pnnl-01-bundle-scenarios/bundles/rung5-lambda-075/overrides.tsv``)
    is a no-op without it -- its own header: "READ BY NOTHING without that
    flag... the scenario silently becomes its own control". Same silent-
    wrong-build failure mode as new_genes/bundle_overrides being dropped.
    Default ``None`` builds byte-for-byte the same command as before this
    param existed.

    ``bundle_manifest_path`` (item 451/#166, Run 4 founder-chassis rebuild):
    generic passthrough to ``v2ecoli-parca``'s own ``--bundle-manifest-path
    PATH`` flag -- the BASE reference-bundle manifest, distinct from
    ``bundle_overrides`` above (which layers on top of whatever base is in
    effect; this flag replaces the base itself). Real, confirmed need:
    sms-ecoli's own declared recipe for Run 4's violacein founder chassis
    (``scripts/build_run4_founder_caches.py``'s own module docstring) is
    ``--bundle-manifest-path out/combined_violacein.tsv --new-genes
    violacein_MG1655_M5`` -- unreachable from a remote dispatch without this
    flag. Default ``None`` builds byte-for-byte the same command as before
    this param existed.

    ``build_combined_bundle_manifest``/``include_violacein_bundle`` (item
    451/#166): when the first is set, prepends a real invocation of
    sms-ecoli's own ``scripts/build_combined_bundle_manifest.py``
    (``--include-violacein`` when the second is also set) ahead of the
    ``v2ecoli-parca`` call, so the combined manifest -- a deliberately
    machine-local, gitignored build artifact per that script's own notes,
    never committed -- gets generated fresh inside THIS remote container
    before ParCa reads it, then points ``--bundle-manifest-path`` at the
    generator's own known default output
    (``out/combined_bundle_manifest.tsv``). Mutually exclusive with passing
    ``bundle_manifest_path`` directly -- set one or the other, not both.
    Default ``False`` is a pure no-op.

    ``deterministic_hash_seed`` (item 451/#166): when set, prepends
    ``PYTHONHASHSEED=0`` to the ``v2ecoli-parca`` invocation. Real, confirmed
    need: Run 4's own founder-chassis recipe explicitly requires it (same
    module docstring as above) -- Python's hash randomization can otherwise
    perturb dict/set iteration order inside ParCa's own reconstruction code,
    which the recipe treats as a determinism requirement for a chassis meant
    to be deterministically re-derived. Opt-in rather than unconditional:
    changing hash-seed behavior for every existing ParCa dispatch is a
    bigger, untested behavioral change than this fix's own scope calls for.
    Default ``False`` is a pure no-op.

    Also copies the chassis-provenance sidecar (``parca_state.provenance.json``,
    item 106/#166, v2ecoli#735) alongside the raw state, non-fatally -- a
    pre-#735 v2ecoli image never writes this file, so the ``|| true`` keeps
    every dispatch working unchanged until that lands; once it does, the
    sidecar rides the same S3 sync ``parca_state.pkl.gz`` already does,
    without this command needing to change again.

    Also copies the gzipped RAW fitted state (``parca_state.pkl.gz``) into
    ``PARCA_CACHE_DIR`` itself (backlog item 105), so it rides along in the
    existing ``out_dir``/``out_s3`` sync instead of being discarded with the
    rest of ``PARCA_SIMDATA_DIR`` when the job's container exits.
    ``build_new_gene_cache.py`` needs exactly this raw file, not the
    hydrated ``out/cache`` bundle ``build_cache.py`` produces from it one
    line below -- see ``submit_new_gene_cache_job``. Unconditional: the file
    is small next to the rest of the cache, and every existing consumer of
    this cache dir already tolerates unknown files (nothing here globs or
    rejects extras).

    ``new_genes``/``bundle_overrides`` do NOT ride onto the ``build_cache.py``
    step below (confirmed 2026-09-04 against a real crash: its current CLI has
    neither flag, ``unrecognized arguments``). Not a gap -- its own bundle-write
    (``save_sim_input``) already produces a complete, correct ``cache_version.json``
    straight from ``sim_data``, which is already strain-specific because
    ``v2ecoli-parca`` received both flags one command earlier in this same chain.
    Restamping here would be redundant even where it was once supported.
    """
    if bundle_manifest_path and build_combined_bundle_manifest:
        raise ValueError(
            "bundle_manifest_path and build_combined_bundle_manifest are mutually exclusive -- "
            "set one or the other, not both."
        )
    settings = _seams.get_settings()
    new_genes_flag = f" --new-genes {shlex.quote(new_genes)}" if new_genes and new_genes != "off" else ""
    bundle_overrides_list = (
        [bundle_overrides] if isinstance(bundle_overrides, str) else list(bundle_overrides or [])
    )
    bundle_overrides_flag = "".join(f" --bundle-overrides {shlex.quote(path)}" for path in bundle_overrides_list)
    rnaseq_source_flag = f" --rnaseq-source {shlex.quote(rnaseq_source)}" if rnaseq_source else ""
    combined_manifest_prefix = ""
    effective_bundle_manifest_path = bundle_manifest_path
    if build_combined_bundle_manifest:
        violacein_flag = " --include-violacein" if include_violacein_bundle else ""
        combined_manifest_prefix = f"python scripts/build_combined_bundle_manifest.py{violacein_flag} && "
        effective_bundle_manifest_path = "out/combined_bundle_manifest.tsv"
    bundle_manifest_path_flag = (
        f" --bundle-manifest-path {shlex.quote(effective_bundle_manifest_path)}"
        if effective_bundle_manifest_path
        else ""
    )
    hash_seed_prefix = "PYTHONHASHSEED=0 " if deterministic_hash_seed else ""
    command = (
        f"cd {V2ECOLI_DIR}"
        f" && {combined_manifest_prefix}{hash_seed_prefix}v2ecoli-parca --mode {settings.ray_parca_mode}"
        f" --cpus {settings.ray_parca_cpus}"
        f" -o {PARCA_SIMDATA_DIR} --cache-dir {PARCA_CACHE_DIR}"
        f"{new_genes_flag}{bundle_overrides_flag}{rnaseq_source_flag}{bundle_manifest_path_flag}"
        f" && gzip -f -k {PARCA_SIMDATA_DIR}/parca_state.pkl"
        f" && python scripts/build_cache.py"
        f" --fixture {PARCA_SIMDATA_DIR}/parca_state.pkl.gz"
        f" --cache {PARCA_CACHE_DIR}"
        f" && cp {PARCA_SIMDATA_DIR}/parca_state.pkl.gz {PARCA_CACHE_DIR}/parca_state.pkl.gz"
        f" && (cp {PARCA_SIMDATA_DIR}/parca_state.provenance.json"
        f" {PARCA_CACHE_DIR}/parca_state.provenance.json 2>/dev/null || true)"
    )
    # A config that requests a real strain (new_genes != "off") MUST produce a
    # command that carries the flag — otherwise ParCa silently builds wild-type
    # and the run "succeeds" with the wrong genotype (CD2 audit §2.1 / P0-2).
    if new_genes and new_genes != "off":
        assert new_genes_flag and new_genes_flag in command, (  # noqa: S101  internal invariant, not input validation
            f"new_genes={new_genes!r} requested but the ParCa command does not carry --new-genes: {command!r}"
        )
    return command


def upstream_parca_command(*, config_path: str | None = None) -> str:
    """Build a PRISTINE upstream-vEcoli ParCa simData for the ``--composite vecoli`` wrapper.

    Runs once on the 1-node parca job from the image's bundled upstream
    checkout (``$V2E_VECOLI_DIR=/app/vEcoli``), dropping a flat
    ``simData.cPickle`` into ``PARCA_CACHE_DIR``. The entrypoint then syncs
    that dir to ``_upstream_cache_s3_uri``; the N-node sim stages the SAME
    cache to every node (Ray workers must read identical sim_data — a
    per-node refit would diverge since ParCa is not bit-reproducible).

    ``--cpus 1`` is REQUIRED, not a perf knob: upstream's fit_sim_data_1 only
    spawns a worker Pool when cpus>1, and those workers re-import
    wholecell.utils.polymerize from the source-only /app/vEcoli checkout —
    which has no compiled Cython (.so) and hard-raises "Failed to import
    Cython module", looping forever. The serial path (cpus==1) runs entirely
    in the main process, where the wrapper's import shim has pinned the
    INSTALLED compiled wholecell into sys.modules, so the import resolves.

    ``config_path`` (item 87): None for every existing caller -- identical
    command to before this param existed (``build_upstream_parca.py``'s own
    ``--config`` defaults to the pristine baseline build). When set (an
    in-image path to a config declaring ``parca_options.new_genes``, e.g. a
    custom strain), threads it through so the built cache is config-driven.
    The CALLER is responsible for pairing this with a matching ``variant``
    label on ``_upstream_cache_s3_uri`` -- this method has no way to enforce
    that pairing itself.
    """
    config_flag = f" --config {config_path}" if config_path else ""
    return (
        f"cd {V2ECOLI_DIR} && python scripts/build_upstream_parca.py"
        f" --outdir {V2ECOLI_DIR}/out/upstream --cpus 1"
        f" --copy-to {PARCA_CACHE_DIR}{config_flag}"
    )


def new_gene_cache_command(
    *,
    expression: float,
    translation_efficiency: float,
    rel_exp_adj: str | None = None,
    rel_trl_eff_adj: str | None = None,
    seed: int = 0,
    media_condition: str | None = None,
    fixed_media: str | None = None,
) -> str:
    """Run ``scripts/build_new_gene_cache.py`` against an ALREADY-STAGED
    commit cache (backlog item 105 -- the ``build_new_gene_cache.py``
    remote-reachability gap Chris/cplong90 flagged: "we have no idea
    whether step two is reachable remotely at all").

    ParCa inserts a new gene SILENT (``new_genes`` is presence/absence
    only, item 93). This is the OTHER half: it hydrates the raw
    ``parca_state.pkl.gz`` a prior ``_parca_command`` run left in
    ``PARCA_CACHE_DIR`` (staged into this job via ``stage_s3``/
    ``stage_dir`` -- see ``submit_new_gene_cache_job``) and writes a NEW,
    derived cache bundle to ``NEW_GENE_INDUCED_CACHE_DIR`` with the given
    gene(s) actually expressed at a caller-chosen level. The strain
    identity itself lives in THIS cache, not in any per-dispatch
    injection (Chris's own framing, sms-ecoli#166) -- ``expression``/
    ``translation_efficiency`` are the two required knobs the script
    itself requires; the rest are its own optional per-gene/media knobs,
    passed straight through.

    Caller (``submit_new_gene_cache_job``) is responsible for staging the
    SOURCE commit's cache (which must itself have been built with
    ``new_genes`` set -- an all-zero-expression source has nothing to
    induce) and for writing the output to a ``variant``-labeled S3 key,
    never the bare commit-only path a plain baseline stage would read.
    """
    rel_exp_flag = f" --rel-exp-adj {shlex.quote(rel_exp_adj)}" if rel_exp_adj else ""
    rel_trl_flag = f" --rel-trl-eff-adj {shlex.quote(rel_trl_eff_adj)}" if rel_trl_eff_adj else ""
    media_flag = f" --media-condition {shlex.quote(media_condition)}" if media_condition else ""
    fixed_media_flag = f" --fixed-media {shlex.quote(fixed_media)}" if fixed_media else ""
    return (
        f"cd {V2ECOLI_DIR}"
        f" && python scripts/build_new_gene_cache.py"
        f" --state {PARCA_CACHE_DIR}/parca_state.pkl.gz"
        f" --cache {NEW_GENE_INDUCED_CACHE_DIR}"
        f" --expression {expression} --translation-efficiency {translation_efficiency}"
        f" --seed {seed}"
        f"{rel_exp_flag}{rel_trl_flag}{media_flag}{fixed_media_flag}"
    )


def variant_cache_command(
    *,
    perturbations: dict[str, float],
    seed: int = 0,
    fixed_media: str | None = None,
) -> str:
    """Run ``scripts/build_variant_cache.py`` against an ALREADY-STAGED commit
    cache (backlog item 451 -- the other half of a design screen from
    ``_build_new_gene_cache_command`` above: NATIVE-gene translation-efficiency
    perturbations, not a new-gene induction level).

    Sibling of ``_build_new_gene_cache_command`` in every structural respect:
    hydrates the raw ``parca_state.pkl.gz`` a prior ``_parca_command`` run left
    in ``PARCA_CACHE_DIR`` (staged into this job via ``stage_s3``/``stage_dir``
    -- see ``submit_variant_cache_job``) and writes a NEW, derived cache bundle
    to ``VARIANT_CACHE_DIR`` with the given native genes perturbed via
    ``v2ecoli.perturbations.build_variant_cache`` (see that script's own
    docstring for why this cache route, not a config-level override, is
    required for a multi-generation study). ``perturbations`` is the one
    required knob the script itself requires (a gene-id -> multiplier JSON
    object); ``seed``/``fixed_media`` are its own optional knobs.

    Caller (``submit_variant_cache_job``) is responsible for staging the
    SOURCE commit's cache and for writing the output to a ``variant``-labeled
    S3 key, never the bare commit-only path a plain baseline stage would read
    -- identical contract to ``submit_new_gene_cache_job``.
    """
    fixed_media_flag = f" --fixed-media {shlex.quote(fixed_media)}" if fixed_media else ""
    return (
        f"cd {V2ECOLI_DIR}"
        f" && python scripts/build_variant_cache.py"
        f" --state {PARCA_CACHE_DIR}/parca_state.pkl.gz"
        f" --cache {VARIANT_CACHE_DIR}"
        f" --perturbations {shlex.quote(json.dumps(perturbations))}"
        f" --seed {seed}"
        f"{fixed_media_flag}"
    )
