"""The ParCa cache jobs: build a commit's ParCa cache, a new-gene cache, a variant cache.

``RayParcaService`` is a SERVICE, not a mixin of ``SimulationServiceRay`` (it was
``RayParcaMixin`` from P2.1 cut 5 until PR 4 of the 2026-09-20 sequence; ``docs/plan-core.md``
decision log). The test for a service is Jim's: a common requirement that works ONE way
whatever the dispatch mechanism. These three jobs pass it -- each is one container job that
stages a cache in and captures a cache out, and no mechanism does it differently.

What did NOT pass, and is therefore not here:

* where caches live and the commands that build them -- pure functions, ``ray/parca_spec.py``;
* submitting ParCa as part of a run -- a container job in two mechanisms and an MNP job in two
  others, so it stays with each mechanism;
* ``_stage_seed_override_caches`` -- only the multi-node composite mechanism uses it; it is
  back in the class and moves with that mechanism.

It is handed what it needs, a ``ParcaDispatch``: resolve an image, get a job definition for
it, submit one container job. Today ``SimulationServiceRay`` answers (through its Batch
layer), which is why the member names are that class's private ones; PR 5 replaces this
Protocol and ``TaskDispatch`` with one ``ContainerSubmitter``.
"""

from typing import Protocol

from viva_api.common.models import JobId
from viva_api.simulation.models import ParcaDataset
from viva_api.simulation.ray import parca_spec
from viva_api.simulation.ray.batch_layer import _rand_suffix
from viva_api.simulation.ray.image_paths import (
    NEW_GENE_INDUCED_CACHE_DIR,
    PARCA_CACHE_DIR,
    VARIANT_CACHE_DIR,
)


class ParcaDispatch(Protocol):
    """What a cache job needs from whatever dispatches container jobs for it. Only the
    keyword arguments the three jobs actually pass are declared."""

    def _image_uri(self, commit: str) -> str: ...

    def _ensure_container_job_def(self, image: str, commit: str) -> str: ...

    def _submit_container(
        self,
        *,
        job_name: str,
        job_definition: str,
        job_cmd: str,
        out_s3: str,
        out_dir: str,
        stage_s3: str | None = ...,
        stage_dir: str | None = ...,
    ) -> str: ...


class RayParcaService:
    def __init__(self, dispatch: ParcaDispatch) -> None:
        # Held, not copied: every call below looks the method up on ``dispatch`` when it
        # runs, so a test that swaps ``service._submit_container`` is what a cache job gets.
        self._dispatch = dispatch

    async def submit_parca_job(self, parca_dataset: ParcaDataset) -> JobId:
        """Submit ParCa as a standalone container job (backlog item 71), capturing
        the cache to S3. Was a 1-node Ray MNP job; ParCa has no real inter-node
        traffic, so it moves to the plain container-type path -- see
        ``_submit_container``."""
        simulator_version = parca_dataset.parca_dataset_request.simulator_version
        commit = simulator_version.environment_key
        job_def = self._dispatch._ensure_container_job_def(self._dispatch._image_uri(commit), commit)
        job_id = self._dispatch._submit_container(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            job_cmd=parca_spec.parca_command(),
            out_s3=parca_spec.cache_s3_uri(commit),
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
        job_def = self._dispatch._ensure_container_job_def(self._dispatch._image_uri(commit), commit)
        job_id = self._dispatch._submit_container(
            job_name=f"new-gene-cache-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            job_cmd=parca_spec.new_gene_cache_command(
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
            stage_s3=parca_spec.cache_s3_uri(commit, variant=source_variant),
            stage_dir=PARCA_CACHE_DIR,
            out_s3=parca_spec.cache_s3_uri(commit, variant=variant),
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
        job_def = self._dispatch._ensure_container_job_def(self._dispatch._image_uri(commit), commit)
        job_id = self._dispatch._submit_container(
            job_name=f"variant-cache-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            job_cmd=parca_spec.variant_cache_command(
                perturbations=perturbations,
                seed=seed,
                fixed_media=fixed_media,
            ),
            stage_s3=parca_spec.cache_s3_uri(commit),
            stage_dir=PARCA_CACHE_DIR,
            out_s3=parca_spec.cache_s3_uri(commit, variant=variant),
            out_dir=VARIANT_CACHE_DIR,
        )
        return JobId.ray(job_id)
