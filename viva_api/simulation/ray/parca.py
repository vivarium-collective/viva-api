"""ParCa and the caches a simulation stages from: where they live in S3, the commands that
build them, and the jobs that run those commands.

Carved out of simulation_service_ray.py (docs/plan-core.md P2.1, cut 5) as a pure
move -- every method below is byte-for-byte what it was in SimulationServiceRay, which
now inherits them from this mixin.

This is SMS through and through -- the parameter calculator, new-gene and variant caches,
per-seed founder caches -- and it stays SMS. What core sees of it is a container job with a
command and an output URI (_submit_container), and nothing else.

The dispatch mixins that follow (chain, multi-node composite, mbp-tracked, analysis) all
need cache_s3_uri and _parca_command. They will inherit THIS mixin rather than find
those in the base layer: the dependency is real, so it is written down as one.
"""

import json
import logging
import shlex
from typing import Any, override

from viva_api.common.models import JobId
from viva_api.common.storage import data_layout
from viva_api.simulation.models import ParcaDataset
from viva_api.simulation.ray import _seams
from viva_api.simulation.ray.batch_layer import RayBatchLayer, _rand_suffix
from viva_api.simulation.ray.image_paths import (
    NEW_GENE_INDUCED_CACHE_DIR,
    PARCA_CACHE_DIR,
    PARCA_SIMDATA_DIR,
    V2ECOLI_DIR,
    VARIANT_CACHE_DIR,
)

logger = logging.getLogger(__name__)


class RayParcaMixin(RayBatchLayer):
    def cache_s3_uri(self, commit: str, *, variant: str | None = None) -> str:
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

    def _upstream_cache_s3_uri(self, commit: str, *, variant: str | None = None) -> str:
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

    def _parca_command(
        self,
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

    def _upstream_parca_command(self, *, config_path: str | None = None) -> str:
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

    def _build_new_gene_cache_command(
        self,
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

    def _build_variant_cache_command(
        self,
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

    @override
    async def submit_parca_job(self, parca_dataset: ParcaDataset) -> JobId:
        """Submit ParCa as a standalone container job (backlog item 71), capturing
        the cache to S3. Was a 1-node Ray MNP job; ParCa has no real inter-node
        traffic, so it moves to the plain container-type path -- see
        ``_submit_container``."""
        simulator_version = parca_dataset.parca_dataset_request.simulator_version
        commit = simulator_version.environment_key
        job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        job_id = self._submit_container(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            job_cmd=self._parca_command(),
            out_s3=self.cache_s3_uri(commit),
            out_dir=PARCA_CACHE_DIR,
        )
        return JobId.ray(job_id)

    async def submit_new_gene_cache_job(
        self,
        *,
        commit: str,
        variant: str,
        expression: float,
        translation_efficiency: float,
        rel_exp_adj: str | None = None,
        rel_trl_eff_adj: str | None = None,
        seed: int = 0,
        media_condition: str | None = None,
        fixed_media: str | None = None,
        source_variant: str | None = None,
    ) -> JobId:
        """Submit ``build_new_gene_cache.py`` as a standalone container job
        (backlog item 105), stamping a new-gene INDUCTION LEVEL onto a
        commit's already-built ParCa cache and capturing the result to a
        ``variant``-labeled S3 key. Sibling of ``submit_parca_job`` -- same
        1-node container shape, same job-def, same image; the only structural
        difference is this job STAGES IN a prior cache (``stage_s3``/
        ``stage_dir``) before running, since it composes on top of ParCa's
        output rather than producing it from scratch.

        ``variant`` is REQUIRED (not optional, unlike ``_upstream_parca_command``'s
        own ``config_path``): every caller of this method is, by construction,
        building a derived cache, so there is no "default" call that should
        ever land on the bare commit-only key -- see ``RayLayout.parca_cache_uri``'s
        own docstring for why writing there would silently corrupt every other
        concurrent dispatch on this commit.

        The source commit's cache MUST already have been built with
        ``new_genes`` set (``_parca_command``'s own param, item 93) -- an
        all-zero-expression source has nothing for this job to induce; that
        precondition is the CALLER's responsibility (e.g. a completed
        ``ParcaDataset`` whose own request set ``parca_options.new_genes``),
        not re-validated here, matching this class's existing pure-passthrough
        philosophy for ``injected_processes``/``variants``/``composite_id``.
        """
        job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        job_id = self._submit_container(
            job_name=f"new-gene-cache-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            job_cmd=self._build_new_gene_cache_command(
                expression=expression,
                translation_efficiency=translation_efficiency,
                rel_exp_adj=rel_exp_adj,
                rel_trl_eff_adj=rel_trl_eff_adj,
                seed=seed,
                media_condition=media_condition,
                fixed_media=fixed_media,
            ),
            # ``source_variant`` (sms-ecoli#166, 2026-09-09): stage the chassis from
            # a variant slot instead of the shared bare commit slot, which any
            # chain dispatch's ``run_parca`` on this commit rewrites (last writer
            # wins). None keeps the bare-slot source byte-for-byte.
            stage_s3=self.cache_s3_uri(commit, variant=source_variant),
            stage_dir=PARCA_CACHE_DIR,
            out_s3=self.cache_s3_uri(commit, variant=variant),
            out_dir=NEW_GENE_INDUCED_CACHE_DIR,
        )
        return JobId.ray(job_id)

    async def submit_variant_cache_job(
        self,
        *,
        commit: str,
        variant: str,
        perturbations: dict[str, float],
        seed: int = 0,
        fixed_media: str | None = None,
    ) -> JobId:
        """Submit ``build_variant_cache.py`` as a standalone container job
        (backlog item 451), stamping NATIVE-gene translation-efficiency
        perturbations onto a commit's already-built ParCa cache and capturing
        the result to a ``variant``-labeled S3 key. Sibling of
        ``submit_new_gene_cache_job`` -- identical 1-node container shape,
        same job-def, same image, same stage-in-then-run structure; the only
        difference is which script runs and what it perturbs (native genes
        here, a new gene's own induction level there).

        ``variant`` is REQUIRED for the same reason as ``submit_new_gene_cache_job``'s
        own docstring: every caller is, by construction, building a derived
        cache, so there is no default call that should land on the bare
        commit-only key.

        The source commit's cache MUST already exist (any ``_parca_command``
        run for this commit) -- that precondition is the CALLER's
        responsibility, not re-validated here, matching this class's existing
        pure-passthrough philosophy.
        """
        job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        job_id = self._submit_container(
            job_name=f"variant-cache-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            job_cmd=self._build_variant_cache_command(
                perturbations=perturbations,
                seed=seed,
                fixed_media=fixed_media,
            ),
            stage_s3=self.cache_s3_uri(commit),
            stage_dir=PARCA_CACHE_DIR,
            out_s3=self.cache_s3_uri(commit, variant=variant),
            out_dir=VARIANT_CACHE_DIR,
        )
        return JobId.ray(job_id)

    def _stage_seed_override_caches(
        self,
        *,
        seed_overrides: dict[Any, dict[str, Any]],
        cache_s3: str,
        stage_dir: str,
    ) -> dict[Any, dict[str, Any]]:
        """Server-side copy each ``seed_overrides[*].cache_dir`` S3 prefix under
        this dispatch's own ``cache_s3`` prefix, then rewrite ``cache_dir`` to the
        LOCAL path it resolves to once the existing single ``stage_s3``->``stage_dir``
        sync (``ray-batch-entrypoint.sh``'s ``stage_inputs``, a recursive
        ``aws s3 sync``) pulls it down on every node.

        Real bug this fixes (backlog item 106, 2026-09-09): ``seed_overrides[*].
        cache_dir`` is a raw ``s3://`` URI, but ``_submit_mnp`` only ever stages ONE
        ``(stage_s3, stage_dir)`` pair -- the dispatch's own base/chassis cache.
        v2ecoli's ``build_lineage_ray_batch_document`` passes an override's
        ``cache_dir`` straight through to ``LineageProcess.config["cache_dir"]``
        unmodified (``v2ecoli/workflow/batch_lineage_ray.py`` line ~247), and
        ``read_cache_version`` does a plain ``os.path.exists()`` on it
        (``v2ecoli/library/cache_version.py`` line ~575) -- unconditionally False
        for an ``s3://`` string regardless of whether the real object exists,
        raising ``StaleCacheError``. Confirmed via direct source trace plus two
        independent real dispatches (database_id 733/738) each hitting this on a
        different seed (Ray's own non-deterministic task ordering picks whichever
        seed's generation-build task runs first before the job aborts).

        Copying each override's own prefix INTO the already-staged ``cache_s3``
        prefix (under a ``_seed_overrides/<seed>/`` subpath) means the EXISTING
        recursive sync already pulls it down — no new env var, no
        entrypoint-script change, no image rebuild. The only new work is a
        server-side S3->S3 copy plus rewriting each override's own ``cache_dir``
        to the resulting local path. A ``cache_dir`` that isn't an ``s3://`` URI
        (already local, or absent) passes through unchanged.
        """
        dest_bucket = _seams.get_settings().s3_work_bucket
        dest_prefix = data_layout.key_from_uri(cache_s3).rstrip("/")
        s3_client = _seams.boto3.client("s3", region_name=_seams.get_settings().storage_s3_region)
        rewritten: dict[Any, dict[str, Any]] = {}
        for seed, seed_override in seed_overrides.items():
            seed_override = dict(seed_override)
            cache_dir = seed_override.get("cache_dir")
            if isinstance(cache_dir, str) and cache_dir.startswith("s3://"):
                src_bucket, _, src_prefix = cache_dir.removeprefix("s3://").partition("/")
                src_prefix = src_prefix.rstrip("/")
                dest_seed_prefix = f"{dest_prefix}/_seed_overrides/{seed}"
                paginator = s3_client.get_paginator("list_objects_v2")
                copied = 0
                for page in paginator.paginate(Bucket=src_bucket, Prefix=f"{src_prefix}/"):
                    for obj in page.get("Contents", []):
                        rel_key = obj["Key"][len(src_prefix) + 1 :]
                        s3_client.copy_object(
                            Bucket=dest_bucket,
                            Key=f"{dest_seed_prefix}/{rel_key}",
                            CopySource={"Bucket": src_bucket, "Key": obj["Key"]},
                        )
                        copied += 1
                logger.info(
                    "Staged seed_overrides[%s].cache_dir (%d objects) from %s to s3://%s/%s",
                    seed,
                    copied,
                    cache_dir,
                    dest_bucket,
                    dest_seed_prefix,
                )
                seed_override["cache_dir"] = f"{stage_dir}/_seed_overrides/{seed}"
            rewritten[seed] = seed_override
        return rewritten
