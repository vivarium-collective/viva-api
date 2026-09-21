"""The ensemble dispatch mechanism: ParCa as a one-node MNP job, then the simulation ensemble as an
N-node MNP job that depends on it -- the single-generation run and the two-engine comparison.

The fourth dispatch mechanism to become a strategy object (``docs/plan-core.md`` P2.1, PR 10), and the
one that had no name. It was not a method: it was the last 217 lines of the router,
``SimulationServiceRay.submit_ecoli_simulation_job``, reached by falling through every other
mechanism's check. The plan audit called it the ensemble path; ``EnsembleStrategy.submit`` is those
lines, verbatim but for how they spell what used to be on ``self`` -- and the router is now what
its name says: precedence, then a hand-over.

This IS a Ray mechanism: the simulation job is a Ray head plus workers, fanning seeds out to actors.

* ``EnsembleStrategy(batch, stage_runner=)``. ``batch`` is an ``MnpSubmitter`` and nothing else.
* The three locals the old tail read from the router's head (``config``, ``composite``,
  ``n_generations``) are derived again at the top of ``submit``, by the same three statements.
* ``sim_command`` was ``_sim_command``, a method that never touched ``self``: a function.

SMS code, and it stays SMS code.
"""

import json
import logging
import shlex
from collections.abc import Awaitable, Callable
from typing import Any

from viva_api.common.dispatch_validation import resolve_task_env
from viva_api.common.models import JobId
from viva_api.common.storage import data_layout
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import CompositeEngine, Simulation, VecoliSource
from viva_api.simulation.ray import _seams, parca_spec
from viva_api.simulation.ray.batch_layer import MnpSubmitter, _rand_suffix
from viva_api.simulation.ray.config_interpretation import (
    _batch_domain_overrides,
    _is_upstream_vecoli,
    injected_processes_from_config,
)
from viva_api.simulation.ray.image_paths import PARCA_CACHE_DIR, SIM_OUT_DIR, V2ECOLI_DIR
from viva_api.simulation.ray.run_records import record_run_with_companions
from viva_api.simulation.ray.runner_env import PBG_RUNNER_ENV, V2ECOLI_BATCH_BASELINE_COMPOSITE_ID
from viva_core.events.events_env import with_events_env

logger = logging.getLogger(__name__)


def sim_command(
    n_seeds: int,
    n_steps: int,
    chunk: int,
    *,
    composite: CompositeEngine | None = None,
    condition: str | None = None,
    max_generations: int | None = None,
    vecoli_source: VecoliSource | None = None,
    n_generations: int = 1,
    experiment_id: str | None = None,
    runner_s3_uri: str | None = None,
    injected_processes: dict[str, Any] | None = None,
    variants: dict[str, Any] | None = None,
    config_overrides: dict[str, Any] | None = None,
    features: list[Any] | None = None,
    exchange_fluxes: dict[str, Any] | None = None,
    exchange_flux_basis: str | None = None,
) -> str:
    # When ``composite`` is set, run the two-engine comparison driver — both
    # engines (v2ecoli port + vEcoli imported via build_composite_native)
    # as bigraph composites on Ray, emitting only the compact XArray view →
    # zarr/S3. Otherwise: single-generation phase0 by default, or the real
    # multi-generation LineageProcess/batch_baseline_runner pipeline when
    # the caller actually requested more than one generation.
    #
    # Engine selection is decoupled from generation count: ``max_generations``
    # defaults to 1 (the phase0 single-gen baseline), so picking an engine
    # never implies a multi-generation comparison-ensemble run by itself —
    # callers opt into more generations explicitly.
    if composite:
        # vecoli_source selects HOW the genuine vEcoli side runs (only meaningful
        # for --composite vecoli): "upstream" (default, ~50 pbg steps) or
        # "vivarium-process" (vEcoli as ONE pbg node with vivarium-core's Engine
        # inside — faithful by construction). Both stage the SAME upstream ParCa
        # cache (is_upstream routing is unchanged), so only the driver flag differs.
        src = f" --vecoli-source {vecoli_source}" if (_is_upstream_vecoli(composite) and vecoli_source) else ""
        return (
            f"cd {V2ECOLI_DIR} && python scripts/run_comparison_ensemble.py"
            f" --composite {composite} --condition {condition or 'basal'}"
            f" --n-seeds {n_seeds} --max-generations {int(max_generations or 1)}"
            f" --chunk {chunk} --out-root {SIM_OUT_DIR} --mode ray{src}"
        )
    if int(n_generations) > 1:
        # Real multi-generation lineage (cell division across generations) --
        # scripts/run_phase0_xarray_ensemble.py below silently ignores this
        # entirely and only ever runs one generation per seed.
        #
        # Dispatched as a registered process-bigraph composite (sms-ecoli's
        # v2ecoli/composites/ecoli_baseline.py's `baseline()` — n_seeds/
        # n_generations > 1 switches it into the batch/lineage shape that used
        # to be the standalone batch_baseline composite before it was folded in
        # here, backlog item 55) run through the SAME generic run_pbg.py every
        # compose-on-Batch job already uses — not a v2ecoli-specific CLI script.
        # See backlog items 26/27: this is the one execution mechanism both the
        # ensemble endpoint and the generic compose endpoint dispatch through;
        # only the composite id + overrides differ per caller.
        if not experiment_id:
            raise RuntimeError("experiment_id is required for multi-generation batch dispatch")
        if not runner_s3_uri:
            raise RuntimeError(
                "runner_s3_uri is required for multi-generation batch dispatch "
                "(the generic run_pbg.py runner must be staged to S3 first)"
            )
        overrides: dict[str, Any] = {
            "n_seeds": int(n_seeds),
            "n_generations": int(n_generations),
            "cache_dir": PARCA_CACHE_DIR,
            "out_dir": SIM_OUT_DIR,
            "experiment_id": experiment_id,
            # The analysis DAG node (see _analysis_command) runs the same
            # analysis_runner.run_analyses() over the LANDED S3 sweep once this
            # job has succeeded -- skip the composite's own inline flush here so
            # the analyses run exactly once, over the whole sweep.
            "analyses": "none",
            "parallel": "ray",
        }
        # Thread the submitted config's DOMAIN fields into the batch composite's
        # own overrides so the native ``ecoli_baseline.baseline()`` batch run
        # actually carries the metabolism-redux/violacein swap (the CD2 native
        # seam). Without these keys the composite runs a plain basal baseline
        # even though the config requested a swap -- the composite never sees it.
        #
        # These are exactly ``ecoli_baseline.baseline()``'s own batch-mode kwargs
        # (v2ecoli #640 threaded injected_processes/features/exchange_fluxes/
        # exchange_flux_basis through ``_build_batch_document``; config_overrides
        # and variants already existed). Each is added ONLY when non-empty, so a
        # config with no injection/variant intent produces the exact overrides
        # dict this path built before -- the byte-for-byte-unchanged regression
        # property (mirrors ``injected_processes_from_config``'s own contract and
        # the 0.9.79 chain-dispatch passthrough).
        #
        # ``injected_processes`` is the mapped shape ``baseline()`` expects
        # ({swap_processes, add_processes, exclude_processes, fork_repo}), built
        # by ``injected_processes_from_config`` from the legacy config's
        # ``swap_processes`` -- the SAME mapping ``_seed_generation_command`` and
        # the chain-dispatch/JobScheduler path already use.
        overrides.update(
            _batch_domain_overrides(
                injected_processes=injected_processes,
                variants=variants,
                config_overrides=config_overrides,
                features=features,
                exchange_fluxes=exchange_fluxes,
                exchange_flux_basis=exchange_flux_basis,
            )
        )
        env = PBG_RUNNER_ENV
        return (
            f"cd {V2ECOLI_DIR}"
            f" && aws s3 cp {runner_s3_uri} /tmp/run_pbg.py"
            f" && {env} python /tmp/run_pbg.py"
            f" --composite-id {V2ECOLI_BATCH_BASELINE_COMPOSITE_ID}"
            f" --overrides {shlex.quote(json.dumps(overrides))} -n 1"
        )
    return (
        f"cd {V2ECOLI_DIR} && python scripts/run_phase0_xarray_ensemble.py"
        f" --n-seeds {n_seeds} --n-steps {n_steps} --chunk {chunk} --parallel ray"
    )


class EnsembleStrategy:
    def __init__(self, batch: MnpSubmitter, *, stage_runner: Callable[[str], Awaitable[str]]) -> None:
        self._batch = batch
        self._stage_runner = stage_runner

    async def submit(
        self, ecoli_simulation: Simulation, database_service: DatabaseService, *, correlation_id: str
    ) -> JobId:
        """Submit ParCa (1 node) + the simulation ensemble (N nodes), gated by a Batch dependency.

        The tracked job id is the *simulation* job. Batch will not start it until the ParCa job
        SUCCEEDED, so the cache is in S3 before the sim stages it. Reached by the router only when
        no other mechanism claimed the request: no dispatch block, and not the canonical
        multi-generation sweep (which is the chain mechanism's).
        """
        config = ecoli_simulation.config
        composite = getattr(config, "composite", None)
        # config.generations is a real (non-"extra") SimulationConfig field, unlike
        # the comparison knobs read further below -- read directly, not via getattr.
        n_generations = int(config.generations or 1)

        parca_dataset = await database_service.get_parca_dataset(parca_dataset_id=ecoli_simulation.parca_dataset_id)
        if parca_dataset is None:
            raise ValueError(f"ParcaDataset with ID {ecoli_simulation.parca_dataset_id} not found.")
        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {ecoli_simulation.simulator_id} not found")

        settings = _seams.get_settings()
        commit = simulator.environment_key
        experiment_id = ecoli_simulation.config.experiment_id

        # Run the TRUE commit image: derive a per-commit MNP job-def revision pointing at
        # v2ecoli:<commit> (both ParCa and the sim run the same image).
        job_def = self._batch.ensure_mnp_job_def(self._batch.image_uri(commit), commit)

        # SimulationConfig is a vEcoli passthrough (extra="allow"); the comparison
        # knobs are validated at the API boundary (Literal Query params) and ride
        # in as extra keys, so they're read here via getattr (present only when the
        # caller set them). ``vecoli_source`` is already constrained by the
        # endpoint's VecoliSource type; ``composite`` was already read above.
        n_seeds = ecoli_simulation.num_seeds or getattr(config, "n_init_sims", None) or 1
        n_steps = getattr(config, "ray_n_steps", None) or settings.ray_n_steps
        chunk = getattr(config, "ray_chunk", None) or settings.ray_chunk
        condition = getattr(config, "condition", None)
        max_generations = getattr(config, "max_generations", None)
        vecoli_source = getattr(config, "vecoli_source", None)

        # Engine-specific ParCa source: the pristine upstream wrapper (--composite
        # vecoli) stages an UPSTREAM-built simData (separate cache + build cmd);
        # every other engine stages the v2ecoli cache. Both ParCa and the sim use
        # the matching pair so the staged simData is consistent across all nodes.
        is_upstream = _is_upstream_vecoli(composite)
        # Backlog item 105: same generic cache_variant passthrough already proven
        # for the chain-dispatch/multi-node-composite paths -- irrelevant to the
        # upstream-vEcoli engine (its own config_path-driven mechanism is separate).
        # This is the whole fix for the comparison-ensemble path's own real gap: the
        # driver's `--cache-dir` default already resolves to PARCA_CACHE_DIR (the
        # exact path staged below), so redirecting the STAGED cache via `variant`
        # is sufficient -- no command-line change needed, confirmed the two paths
        # are byte-identical (`REPO_ROOT/out/cache` == `/app/v2ecoli/out/cache`).
        cache_variant = None if is_upstream else (getattr(config, "cache_variant", None) or None)
        cache_s3 = (
            parca_spec.upstream_cache_s3_uri(commit)
            if is_upstream
            else parca_spec.cache_s3_uri(commit, variant=cache_variant)
        )
        # Backlog item 93: same generic new_genes passthrough as
        # submit_chain_dispatch_job -- irrelevant to the upstream-vEcoli
        # engine (its own config_path-driven mechanism, item 87, is separate).
        new_genes = None if is_upstream else getattr(config.parca_options, "new_genes", None)
        # Backlog item 104: same generic bundle_overrides passthrough, same
        # upstream-vEcoli exemption as new_genes above.
        bundle_overrides = None if is_upstream else getattr(config.parca_options, "bundle_overrides", None)
        # Same generic rnaseq_source passthrough (item 106/#166 chassis-provenance
        # thread) -- a bundle_overrides manifest can itself require this to have
        # any effect at all; same upstream-vEcoli exemption as the two above.
        rnaseq_source = None if is_upstream else getattr(config.parca_options, "rnaseq_source", None)
        # Same generic require_clean_chain passthrough (item 106/#166, v2ecoli#735) --
        # opt-in only (default False emits nothing, see _stage_out_env's own
        # docstring): most existing callers don't pass v2ecoli's own `sources=` yet.
        require_clean_chain = (
            False if is_upstream else bool(getattr(config.parca_options, "require_clean_chain", False))
        )
        # Same generic bundle_manifest_path/build_combined_bundle_manifest/
        # include_violacein_bundle/deterministic_hash_seed passthrough (item
        # 451/#166, Run 4 founder-chassis rebuild) -- same upstream-vEcoli
        # exemption as new_genes/bundle_overrides/rnaseq_source above.
        bundle_manifest_path = None if is_upstream else getattr(config.parca_options, "bundle_manifest_path", None)
        build_combined_bundle_manifest = (
            False if is_upstream else bool(getattr(config.parca_options, "build_combined_bundle_manifest", False))
        )
        include_violacein_bundle = (
            False if is_upstream else bool(getattr(config.parca_options, "include_violacein_bundle", False))
        )
        deterministic_hash_seed = (
            False if is_upstream else bool(getattr(config.parca_options, "deterministic_hash_seed", False))
        )
        parca_command = (
            parca_spec.upstream_parca_command()
            if is_upstream
            else parca_spec.parca_command(
                new_genes=new_genes,
                bundle_overrides=bundle_overrides,
                rnaseq_source=rnaseq_source,
                bundle_manifest_path=bundle_manifest_path,
                build_combined_bundle_manifest=build_combined_bundle_manifest,
                include_violacein_bundle=include_violacein_bundle,
                deterministic_hash_seed=deterministic_hash_seed,
            )
        )

        # Only the composite-driven comparison-ensemble path can still reach here
        # with n_generations > 1 (the non-composite canonical shape is routed to
        # chain-dispatch above, before this line); its own _sim_command branch
        # never reads runner_s3_uri, but staging it costs nothing and this stays
        # unconditional on generations alone, matching pre-existing behavior.
        runner_s3_uri = await self._stage_runner(experiment_id) if n_generations > 1 else None

        # Cost-allocation tags (propagate to ECS tasks → payer-account Cost
        # Explorer attributes spend per run/engine/condition). Values must be
        # tag-safe strings.
        base_tags = {
            "Project": "v2ecoli-comparison",
            "ExperimentId": str(experiment_id)[:255],
            "Engine": str(composite or "v2ecoli"),
            "Condition": str(condition or "basal"),
            "Commit": str(commit)[:12],
            "Team": getattr(settings, "cost_team_tag", None) or "covertlab",
        }

        # 1. ParCa job (1 node) → cache to S3.
        #
        # The events identity (D4a) is merged UNDER the request's own task_env on
        # BOTH submits, per phase. Omitting it here was a live gap: the HpcRun row
        # got a trace_id (the API derives it from correlation_id) while the Batch
        # job carried no PBG_* at all, so the run recorded an identity no task was
        # ever told about and emitted nothing. Caught by running a 1-gen/1-seed
        # verification campaign and reading the submitted job's environment.
        request_env = resolve_task_env(config)

        def _events_env(phase: str) -> dict[str, str]:
            return with_events_env(
                request_env,
                correlation_id=correlation_id,
                experiment_id=str(experiment_id),
                sim_id=ecoli_simulation.database_id,
                backend="mnp",
                tags={"phase": phase},
                settings=settings,
            )

        parca_job_id = self._batch.submit_mnp(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            num_nodes=1,
            ray_job_cmd=parca_command,
            out_s3=cache_s3,
            out_dir=PARCA_CACHE_DIR,
            tags={**base_tags, "Phase": "parca"},
            task_env=_events_env("parca"),
        )

        # 2. Simulation ensemble (N-node Ray cluster), gated on ParCa, staging the
        # cache. Always MNP now: the ONE shape that used to need Array jobs here
        # (canonical batch_baseline, composite is None + multi-generation) is
        # routed to submit_chain_dispatch_job before this method does ANY of the
        # setup above (see the routing check at the top) -- every request that
        # still reaches this point either sets composite (the comparison
        # ensemble, which genuinely fans out via Ray actors) or requests a
        # single generation (the phase0 ensemble).
        sim_job_id = self._batch.submit_mnp(
            job_name=f"ray-sim-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            num_nodes=settings.ray_num_nodes,
            ray_job_cmd=sim_command(
                int(n_seeds),
                int(n_steps),
                int(chunk),
                composite=composite,
                condition=condition,
                max_generations=max_generations,
                vecoli_source=vecoli_source,
                n_generations=n_generations,
                experiment_id=str(experiment_id),
                runner_s3_uri=runner_s3_uri,
                # CD2 native seam: thread the submitted config's domain fields so the
                # --composite-id batch run carries the metabolism-redux/violacein swap.
                # injected_processes maps the legacy config's swap_processes ->
                # ecoli_baseline.baseline()'s injected_processes kwarg (same helper the
                # chain-dispatch/JobScheduler path uses); the rest are ecoli_baseline
                # batch-mode kwargs read straight off the config (extra="allow"), all
                # no-ops when the config sets none of them.
                injected_processes=injected_processes_from_config(config),
                variants=getattr(config, "variants", None),
                config_overrides=getattr(config, "config_overrides", None),
                features=getattr(config, "features", None),
                exchange_fluxes=getattr(config, "exchange_fluxes", None),
                exchange_flux_basis=getattr(config, "exchange_flux_basis", None),
            ),
            out_s3=data_layout.RayLayout.results_uri(experiment_id),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            depends_on=[parca_job_id],
            tags={**base_tags, "Phase": "sim"},
            task_env=_events_env("sim"),
            # Wrong-strain guard (sms-ecoli#210 / #215): tell the entrypoint which
            # strain this run staged so it rejects a cache built for a different one.
            # off/None (wild-type) emits nothing, so this is inert for non-strain runs.
            expect_new_genes=new_genes,
            expect_bundle_overrides=bundle_overrides,
            require_clean_chain=require_clean_chain,
        )
        logger.info(
            "Ray simulation %s: parca job %s -> sim job %s (%d nodes)",
            experiment_id,
            parca_job_id,
            sim_job_id,
            settings.ray_num_nodes,
        )

        # No inline analysis submission: the ONE shape that used to need it here
        # (canonical batch_baseline) is entirely handled by chain-dispatch's own
        # poller-triggered submit_campaign_analysis now (see
        # JobScheduler.update_chain_campaigns / _advance_chain_campaign). The
        # comparison-ensemble and phase0 paths that still reach this point write
        # no cd1_*/ptools_*-ready sweep and never got inline analysis either --
        # unaffected by this rework.
        job_id = JobId.ray(sim_job_id)
        await record_run_with_companions(
            database_service,
            job_id=job_id,
            simulation_id=ecoli_simulation.database_id,
            correlation_id=correlation_id,
            companion_job_ids=[parca_job_id],
        )
        return job_id
