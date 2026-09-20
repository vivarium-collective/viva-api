"""Analysis: the post-simulation analysis DAG node, for a chain campaign and for a
multi-node composite run -- what to run, how big a box it needs, and the container job.

RayAnalysisService is a SERVICE (docs/plan-core.md decision log, 2026-09-20), carved
out of simulation_service_ray.py in P2.1 cut 6. Analysis is a leaf of that class: the
scheduler, the handlers and compose call INTO it, and nothing inside the class calls it.
It reaches back for six things, and AnalysisDispatch names them. So it is handed a
dispatcher rather than inheriting one.

Every method body below is what it was in SimulationServiceRay with self.<helper>
rewritten to self._dispatch.<helper> for those six names, and nothing else changed.

It stays SMS: which analyses exist, the scales, the memory sizing copied from the model
repo, the analysis table -- all of it is this application's science. What core sees is
a container job with a command and an output URI.
"""

import functools
import json
import logging
import shlex
from typing import Any, Protocol

from pydantic import BaseModel

from viva_api.common import analysis_dag
from viva_api.common.dispatch_validation import resolve_task_env
from viva_api.common.storage import data_layout
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import Simulation
from viva_api.simulation.ray import _seams
from viva_api.simulation.ray.batch_layer import _rand_suffix
from viva_api.simulation.ray.image_paths import ANALYSIS_OUT_DIR, V2ECOLI_DIR
from viva_api.simulation.tables_orm import AnalysisStatusDB
from viva_core.events.events_env import with_events_env

logger = logging.getLogger(__name__)

# The analysis scales a v2ecoli ``analysis_options`` map can carry. Everything else
# in that (extra="allow") model — ``cpus``, ``memory_gb``, vEcoli-Nextflow-only keys —
# is not a scale and must not be forwarded as one.
ANALYSIS_SCALES = ("single", "multidaughter", "multigeneration", "multiseed", "multivariant")
# The composite's own "every analysis this batch's shape has the cells for" keyword
# (v2ecoli.steps.batch_baseline_runner.build_analysis_options). Used when the caller
# named no modules: sms-api runs outside the model image and has no ANALYSIS_REGISTRY
# to enumerate, so it asks the image to resolve the set with its own resolver rather
# than carrying a second, drift-prone copy of the list.
APPLICABLE_ANALYSES = "applicable"

# --- Analysis memory-class routing (viva-api#625 / v2ecoli#788) --------------
#
# An analysis container runs on one of two Batch instance classes: "standard"
# (the 60 GB box the analysis job def defaults to, which OOMed on CD2 --
# v2ecoli#786) and "large" (a 200 GB r7i queue). The class is picked HERE, at
# submission time, because the queue is chosen before the container runs -- so
# sms-api cannot ask the model image, where v2ecoli's own analysis_memory_class
# lives. This is a deliberate small local copy of that sizing (sms-api does not
# import v2ecoli at runtime; the analysis SCALES it reads are already carried in
# analysis_options). The constants below MUST stay in step with
# v2ecoli.workflow.analysis_runner: a per-lineage multiseed/multigeneration group
# peaks ~8 GB/generation (~78 GB over 10 generations, #786), past the 60 GB
# standard box; single/multidaughter read one cell and stay standard.
#
# DRIFT TRAP: v2ecoli's copy also honors an analysis class that DECLARES
# `memory_class = "large"` (analysis_runner._declared_memory_class); this copy
# does NOT -- it derives purely from scale x generations, because sms-api has no
# ANALYSIS_REGISTRY to read a class attribute from. So declaring memory_class on
# an analysis currently has NO effect on which queue the API picks. Latent today
# (nothing declares it). If a module ever needs to force "large" regardless of
# scale, thread the declared class through analysis_options / task_env so this
# function can see it; until then a declaration would route large in v2ecoli's
# in-image reasoning but STANDARD here -- do not let that gap go silent.
_ANALYSIS_STANDARD_INSTANCE_GB = 60
_ANALYSIS_GB_PER_GENERATION = 8.0
_ANALYSIS_MULTI_CELL_SCALES = frozenset({"multigeneration", "multiseed"})


def analysis_memory_class(
    analysis_options: dict[str, Any] | str | None,
    *,
    n_seeds: int | None = None,
    n_generations: int | None = None,
) -> str:
    """The Batch instance memory class an analysis submission needs: ``"standard"``
    or ``"large"``.

    Derived from the scales named in ``analysis_options`` (the ``{scale: {name:
    params}}`` shape, e.g. from ``analysis_modules_for``) and the sweep's
    ``n_generations``. Generations drive the per-lineage peak; ``n_seeds`` is
    accepted for interface parity with v2ecoli's function but the chunked readers
    make it a non-factor. Any multiseed/multigeneration analysis over enough
    generations routes the whole job to the large instance; everything else stays
    standard. Mirrors ``v2ecoli.workflow.analysis_runner.analysis_memory_class``
    -- see the note above on why sms-api keeps a local copy."""
    if not n_generations or not isinstance(analysis_options, dict):
        # No generation count, or the "applicable" keyword / any non-scale-map
        # (the image resolves the set itself) -- nothing to size on here.
        return "standard"
    over_standard = _ANALYSIS_GB_PER_GENERATION * int(n_generations) > _ANALYSIS_STANDARD_INSTANCE_GB
    for scale, entries in analysis_options.items():
        if scale in _ANALYSIS_MULTI_CELL_SCALES and isinstance(entries, dict) and entries and over_standard:
            return "large"
    return "standard"


# The SubmitJob pacer, its 50-TPS rationale and the DescribeJobs chunk size moved to
# ``viva_core.backends.batch`` with the rest of the Batch engine (docs/plan-core.md P2.1,
# cut 2). A canonical 1000-seed x 10-generation chain campaign submits N*G=10,000
# individual per-seed-per-generation jobs upfront (see ``submit_chain_dispatch_job``),
# which is why that loop is the pacer's main customer.


def analysis_modules_for(config: Any) -> dict[str, dict[str, Any]] | str:
    """The analyses the analysis DAG node should run for this simulation.

    Reads the simulation's OWN ``config.analysis_options`` — the field the run
    endpoint already populates from the caller's ``--analysis-options`` (and that
    the workbench already fills from a study's ``spec.analyses``), and which this
    backend previously ignored entirely, so a remote dispatch's configured
    analyses never ran.

    Only real scale keys are forwarded, and only non-empty ones: the endpoint's
    own fallback default is ``{"multiseed": {}}`` — "no modules named", not "run
    nothing" — which resolves to the ``applicable`` keyword like any other
    unset case.
    """
    options: Any = getattr(config, "analysis_options", None)
    raw: dict[str, Any] = options.model_dump() if isinstance(options, BaseModel) else dict(options or {})
    modules = {
        scale: dict(entries)
        for scale, entries in raw.items()
        if scale in ANALYSIS_SCALES and isinstance(entries, dict) and entries
    }
    return modules or APPLICABLE_ANALYSES


class AnalysisDispatch(Protocol):
    """What analysis needs from whatever dispatches container jobs for it. Only the keyword
    arguments analysis actually passes are declared; the implementation may accept more."""

    def _image_uri(self, commit: str) -> str: ...
    def _ensure_container_job_def(self, image: str, commit: str) -> str: ...
    def _results_s3_uri(self, experiment_id: str) -> str: ...
    def cache_s3_uri(self, commit: str, *, variant: str | None = ...) -> str: ...
    def chain_base_tags(self, *, simulation: Simulation, commit: str) -> dict[str, str]: ...
    def _submit_container(
        self,
        *,
        job_name: str,
        job_definition: str,
        job_cmd: str,
        out_s3: str,
        out_dir: str,
        depends_on: list[str] | None = ...,
        depends_type: str | None = ...,
        tags: dict[str, str] | None = ...,
        task_env: dict[str, str] | None = ...,
        memory_class: str = ...,
    ) -> str: ...


class RayAnalysisService:
    def __init__(self, dispatch: AnalysisDispatch) -> None:
        # Held, not copied: each call looks the method up on ``dispatch`` when it runs, so a
        # test that swaps ``service._submit_container`` is what analysis gets.
        self._dispatch = dispatch

    def _analysis_command(
        self,
        *,
        experiment_id: str,
        modules: dict[str, dict[str, Any]] | str,
        analysis_name: str,
        commit: str,
        cache_variant: str | None = None,
    ) -> str:
        """Build the analysis DAG node's command: the ported analyses over the S3 sweep.

        WHY A THIRD DAG NODE, not the composite's own inline flush. The composite
        (``v2ecoli.composites.ecoli_baseline``, formerly the standalone
        ``batch_baseline`` — backlog item 55) does ship a post-simulation flush that
        runs exactly these analyses, and the sim overrides deliberately disable it
        (``"analyses": "none"``). That is not a workaround for a broken flush — it is
        forced by the sweep's SHAPE on this backend:

          * The canonical dispatch is an AWS Batch ARRAY job: N independent children,
            one seed each, no shared filesystem. Each child's composite run sees 1/N
            of the sweep, so an inline flush there would run the cross-seed scales
            (multiseed/multivariant) against a single seed — N times over, racing on
            the same output prefix. The sweep only becomes whole once every child's
            output has landed in S3.
          * The whole-sweep analysis is therefore a GATHER node, and the DAG edge that
            expresses "after every child succeeded" is the same Batch ``dependsOn``
            the ParCa→sim edge already uses. No poller, no webhook, no external
            watcher: completion is an edge in the pipeline graph.

        The node itself reuses the model image's own ``v2ecoli-analyze`` console
        script (``v2ecoli.workflow.analysis_runner:main``) — the SAME function the
        composite's inline flush calls (``run_analyses``), reading the hive-parquet
        in place through DuckDB/httpfs. No new analysis logic. See
        ``viva_api.common.analysis_dag`` for the real CLI surface this conforms to
        (positional ``sweep_dir`` + ``--config`` — nothing else) and why the old
        ``--out-uri``/``--n-seeds``/``--n-generations``/``--modules``/
        ``--analysis-name`` flags folded into the ``--config`` JSON instead.

        ``sim_data`` points at the commit's ParCa cache in S3 (both via
        ``V2ECOLI_SIM_DATA`` and ``config["sim_data_path"]``) because an S3 sweep
        has no co-located pickle to glob (``analysis_runner.resolve_sim_data`` only
        globs local paths) — identical to how ``SimulationServiceK8s.
        submit_ray_native_analysis`` provisions the same script. Both this job and
        the ParCa job derive that URI from the commit independently, so it needs no
        hand-off plumbing.

        ``cache_variant`` (viva-api#448): the dispatch's own variant, if any — an
        analysis without it silently reads the plain per-commit (stock) simData
        even when the real simulation ran against a real strain's own cache,
        grading a candidate strain against stock data with no error. None (every
        caller before #448) is unaffected, matching the dispatch-side guard's own
        `cache_variant=None` default.
        """
        out_uri = self._dispatch._results_s3_uri(experiment_id).rstrip("/")
        sim_data_uri = f"{data_layout.RayLayout.parca_cache_uri(commit, variant=cache_variant)}simData.cPickle"
        result_out_dir = f"{out_uri}/analyses/{analysis_name}"
        config = analysis_dag.build_analysis_config(
            analysis_options=modules,
            out_dir=result_out_dir,
            sim_data_path=sim_data_uri,
        )
        return analysis_dag.analysis_dag_command(
            v2ecoli_dir=V2ECOLI_DIR,
            sweep_dir=out_uri,
            sim_data_uri=sim_data_uri,
            config=config,
        )

    async def _submit_analysis_job(
        self,
        *,
        simulation: Simulation,
        database_service: DatabaseService,
        job_definition: str,
        commit: str,
        sim_job_id: str | None,
        n_seeds: int,
        n_generations: int,
        depends_type: str | None,
        tags: dict[str, str],
        cache_variant: str | None = None,
        correlation_id: str | None = None,
    ) -> str | None:
        """Submit the analysis DAG node and record it, returning its Batch job id.

        The analysis is tracked in the SAME ``analyses`` table (and therefore the same
        ``GET /analyses/{id}/status`` S3-manifest probe) the on-demand
        ``POST /simulations/{id}/analysis`` trigger already writes to — an
        auto-triggered analysis must be exactly as discoverable as a hand-triggered
        one, not an invisible side effect.

        Thin wrapper around ``viva_api.common.analysis_dag.submit_analysis_dag_node``
        (backlog: made reusable so a compose backend can share the exact same node
        instead of re-deriving its own argv) — this method owns only what's specific
        to the study Batch path: resolving the sim-data/results URIs from ``commit``/
        ``experiment_id``, and the ``analyses`` table's ``config`` record shape.

        ``cache_variant`` (viva-api#448): forwarded to the sim_data URI so a
        strain-specific dispatch's analysis reads its own cache, not the plain
        per-commit stock one. None (every caller before #448) is unaffected.

        ``sim_job_id`` is the single Batch job this analysis should natively
        ``dependsOn`` (item 24's original single-DAG-edge shape, still used by the
        single-shot dispatch paths). Pass ``None`` for the chain-dispatch campaign
        path (backlog item 33 rework), where by construction everything this
        analysis depends on has ALREADY finished by the time it's submitted — the
        analysis-fan-in poller's own "all tracked jobs terminal" check (see
        ``JobScheduler.update_chain_campaigns``) provides the "wait for all"
        semantics a native ``dependsOn`` can't express at 1000-seed scale (Batch
        caps a job at 20 dependencies), so no ``dependsOn`` is needed at all here.
        """
        experiment_id = simulation.config.experiment_id
        analysis_name = f"analysis-{experiment_id[:20]}-{_rand_suffix()}"
        out_uri = self._dispatch._results_s3_uri(experiment_id).rstrip("/")
        result_uri = f"{out_uri}/analyses/{analysis_name}"
        modules = analysis_modules_for(simulation.config)
        sim_data_uri = f"{data_layout.RayLayout.parca_cache_uri(commit, variant=cache_variant)}simData.cPickle"
        params: dict[str, Any] = {
            "out_uri": out_uri,
            "n_seeds": int(n_seeds),
            "n_generations": int(n_generations),
            "modules": modules,
            "analysis_name": analysis_name,
            "trigger": "dispatch-dag",
            # ORMAnalysis.to_dto() unconditionally reads config["analysis_options"]
            # (AnalysisConfigOptions requires experiment_id) -- mirror the shape the
            # existing producers write so to_dto() doesn't KeyError.
            "analysis_options": {
                "experiment_id": [experiment_id],
                **(modules if isinstance(modules, dict) else {}),
            },
        }
        # Backlog item 71: the analysis DAG node has no real inter-node traffic
        # either (was a 1-node MNP job) -- moves to the plain container-type
        # path. `job_definition` must now be a container job def (see
        # submit_campaign_analysis's _ensure_container_job_def call).
        # Route a heavy multiseed/multigeneration gather to the large-memory
        # queue by declaration instead of OOM-then-hand-rerun (viva-api#625).
        memory_class = analysis_memory_class(modules, n_seeds=n_seeds, n_generations=n_generations)
        return await analysis_dag.submit_analysis_dag_node(
            sweep_dir=out_uri,
            analysis_options=modules,
            sim_data_uri=sim_data_uri,
            result_out_dir=result_uri,
            v2ecoli_dir=V2ECOLI_DIR,
            # task_env (sms-ecoli#166): the campaign's own env reaches its gather too,
            # WITH the events identity (D4a) merged under it so the gather lands in the
            # campaign's trace rather than a trace of its own. ``correlation_id`` is
            # threaded from the scheduler's HpcRun row; when it is None ``events_env``
            # seeds from experiment_id instead, which still groups the run's own tasks.
            submit_container=functools.partial(
                self._dispatch._submit_container,
                task_env=with_events_env(
                    resolve_task_env(simulation.config),
                    correlation_id=correlation_id,
                    experiment_id=str(experiment_id),
                    sim_id=simulation.database_id,
                    backend="analysis",
                    tags={"phase": "analysis"},
                    settings=_seams.get_settings(),
                ),
                memory_class=memory_class,
            ),
            job_definition=job_definition,
            job_name=f"ray-analysis-{experiment_id}-{_rand_suffix()}"[:128],
            out_s3=self._dispatch._results_s3_uri(experiment_id),
            container_out_dir=ANALYSIS_OUT_DIR,
            depends_on_job_id=sim_job_id,
            depends_type=depends_type,
            tags=tags,
            database_service=database_service,
            experiment_id=experiment_id,
            analysis_name=analysis_name,
            simulation_id=simulation.database_id,
            backend="ray",
            db_config=params,
            result_uri=result_uri,
        )

    async def submit_campaign_analysis(
        self,
        *,
        simulation: Simulation,
        database_service: DatabaseService,
        commit: str,
        total_n_seeds: int,
        n_generations: int,
        correlation_id: str | None = None,
    ) -> str | None:
        """Submit the analysis DAG node for a chain-dispatch campaign that the
        analysis-fan-in poller (``JobScheduler._advance_chain_campaign``) has
        just confirmed all-terminal — called exactly once per campaign, after
        every tracked seed chain (whether it fully succeeded or was permanently
        failed/truncated) has resolved. By construction everything this
        analysis depends on has ALREADY finished by the time this runs, so it
        reuses item 24's existing analysis-job submission code
        (``_submit_analysis_job``) completely as-is, just with ``sim_job_id=
        None`` — no native ``dependsOn`` needed; the poller's own "all tracked
        jobs terminal" check already provided the "wait for all" semantics.

        ``total_n_seeds`` is the campaign's ORIGINALLY REQUESTED seed count
        (not however many chains actually succeeded) — matches the superseded
        design's own resolved semantics: the analysis resolves "applicable"
        modules against the campaign's INTENDED shape.
        """
        base_tags = self._dispatch.chain_base_tags(simulation=simulation, commit=commit)
        # Backlog item 71: _submit_analysis_job now submits via _submit_container,
        # so this must resolve a container job def, not an MNP one.
        container_job_def = self._dispatch._ensure_container_job_def(self._dispatch._image_uri(commit), commit)
        # viva-api#448: same getattr(simulation.config, "cache_variant", ...)
        # pattern job_scheduler.py's own chain-dispatch cache-staging already uses
        # — without it, a strain-specific campaign's analysis silently reads the
        # plain per-commit stock simData instead of its own real cache.
        cache_variant = getattr(simulation.config, "cache_variant", None)
        return await self._submit_analysis_job(
            simulation=simulation,
            database_service=database_service,
            job_definition=container_job_def,
            commit=commit,
            sim_job_id=None,
            n_seeds=total_n_seeds,
            n_generations=n_generations,
            depends_type=None,
            tags={**base_tags, "Phase": "analysis"},
            cache_variant=cache_variant,
            correlation_id=correlation_id,
        )

    def _multi_node_analysis_command(
        self,
        *,
        experiment_id: str,
        composite_id: str,
        history_uri: str,
        out_uri: str,
        n_seeds: int | None = None,
        n_generations: int = 1,
        modules: dict[str, dict[str, Any]] | str | None = None,
        sim_data_uri: str | None = None,
    ) -> str:
        """Build the "Analysis flush" DAG node's command for a generic
        multi-node process-bigraph composite dispatch (backlog item 88).

        Unlike ``_analysis_command`` (a fixed hive-parquet seed x generation
        sweep, v2ecoli-specific analysis modules), this points at a separate,
        generic entrypoint (``scripts/run_multi_node_analysis.py``) that
        tries TWO read paths in order (item 109's own fix): (1) when
        ``n_seeds`` is given, a hive-parquet sweep read via the same
        DuckDB-httpfs mechanism ``run_standalone_analysis.py`` already uses --
        the shape a ``lineage_ray_batch`` pbg-native dispatch actually
        produces; (2) the original flat-file fallback (``emitter_history
        .json``/``final_state.json`` -> ``v2ecoli.workflow.flush.run_flush``),
        colony's own shape (item 88), unconditionally tried when path 1 is
        not applicable or finds nothing. Nothing in either path branches on
        ``composite_id`` -- a composite-specific renderer, if one is ever
        needed, is a new registered post-sim step, never a per-composite
        branch here. Writes whichever path succeeds + ``_manifest.json`` to
        ``out_uri``, matching the same S3-manifest contract
        ``GET /analyses/{id}/status`` already probes for every other
        analysis kind.

        ``modules`` mirrors ``_analysis_command``'s own encoding exactly (same
        real bug class to avoid): the ``"applicable"`` keyword must ride as a
        bare, unquoted-in-JSON-sense token, not ``json.dumps``'d -- JSON-
        encoding it would produce the 12-character string ``'"applicable"'``
        (quotes included), which the receiving script's own ``.strip().lower()
        == "applicable"`` check would silently miss, falling through to
        ``json.loads`` and handing back the bare word as if it were a real
        module mapping -- a real bug caught here before shipping, not a
        theoretical one.

        ``sim_data_uri`` (viva-api#448): exported as ``V2ECOLI_SIM_DATA`` before
        the script runs, the SAME fallback ``_analysis_command``'s own analysis
        node relies on (``analysis_runner.resolve_sim_data``'s resolution order:
        a co-located pickle, then this env var, then the image's own stock
        knowledge-base build). This node's own analysis container never stages
        a ParCa cache locally (no ``stage_s3``/``stage_dir`` on its dispatch),
        so without this a candidate strain's analysis silently falls through to
        the stock knowledge-base build. None (every caller before #448)
        preserves the exact prior fallback-only behavior.
        """
        cmd = f"cd {V2ECOLI_DIR}"
        if sim_data_uri:
            cmd += f" && export V2ECOLI_SIM_DATA={shlex.quote(sim_data_uri)}"
        cmd += (
            f" && python scripts/run_multi_node_analysis.py"
            f" --composite-id {shlex.quote(composite_id)}"
            f" --history-uri {shlex.quote(history_uri)}"
            f" --out-uri {shlex.quote(out_uri)}"
            f" --experiment-id {shlex.quote(experiment_id)}"
        )
        if n_seeds:
            modules_arg = modules if isinstance(modules, str) else json.dumps(modules or {})
            cmd += f" --n-seeds {int(n_seeds)} --n-generations {int(n_generations)}"
            cmd += f" --modules {shlex.quote(modules_arg)}"
        return cmd

    async def submit_multi_node_analysis(
        self,
        *,
        simulation: Simulation,
        database_service: DatabaseService,
        commit: str,
        composite_id: str,
    ) -> str | None:
        """Submit the "Analysis flush" node for a multi-node composite dispatch
        (backlog item 88) that ``JobScheduler.update_multi_node_jobs`` has just
        confirmed COMPLETED -- the multi-node-composite analogue of
        ``submit_campaign_analysis``, deliberately NOT a shared function with
        it (different command, different params, different DB fields) to keep
        the chain-dispatch analysis-fan-in path this mirrors completely
        untouched. Same best-effort-but-never-silent contract as
        ``_submit_analysis_job``: a submission failure is recorded as a FAILED
        row, not just logged, so it's visible through the same
        ``GET /analyses/{id}/status`` surface a successful submission uses.

        Item 109: also extracts ``n_seeds``/``n_generations`` from the
        ORIGINAL dispatch's own stored ``multi_node_dispatch.params`` (already
        on ``simulation.config`` -- no new DB column needed) and the analysis
        module selection via ``analysis_modules_for`` -- the SAME already-
        tested resolver ``_analysis_command`` (chain-dispatch's own analysis
        node) already uses, reused rather than re-derived, so an unset/empty
        ``analysis_options`` resolves to the ``"applicable"`` keyword here
        too, not a silently-different "run nothing" -- both threaded into the
        command builder so a ``lineage_ray_batch``-shaped dispatch's real
        hive-parquet output is actually read, not just colony's own
        flat-file fallback.
        """
        experiment_id = str(simulation.config.experiment_id)
        mnp_dispatch = getattr(simulation.config, "multi_node_dispatch", None) or {}
        dispatch_params = mnp_dispatch.get("params") or {} if isinstance(mnp_dispatch, dict) else {}
        n_seeds = dispatch_params.get("n_seeds")
        n_generations = int(dispatch_params.get("n_generations") or 1)
        modules = analysis_modules_for(simulation.config)
        # viva-api#448: cache_variant is a SIBLING of params on multi_node_dispatch
        # (the same shape _submit_multi_node_composite's own guard reads) -- without
        # threading it here too, a candidate strain's analysis silently reads the
        # image's own stock knowledge-base build instead of the real dispatch's cache.
        cache_variant = mnp_dispatch.get("cache_variant") if isinstance(mnp_dispatch, dict) else None
        sim_data_uri = f"{self._dispatch.cache_s3_uri(commit, variant=cache_variant)}simData.cPickle"
        analysis_name = f"analysis-mnp-{experiment_id[:20]}-{_rand_suffix()}"
        results_uri = self._dispatch._results_s3_uri(experiment_id).rstrip("/")
        result_uri = f"{results_uri}/analyses/{analysis_name}"
        settings = _seams.get_settings()
        container_job_def = self._dispatch._ensure_container_job_def(self._dispatch._image_uri(commit), commit)
        tags = {
            "Project": "v2ecoli-multi-node-composite",
            "ExperimentId": experiment_id[:255],
            "CompositeId": str(composite_id)[:255],
            "Commit": str(commit)[:12],
            "Team": getattr(settings, "cost_team_tag", None) or "covertlab",
            "Phase": "analysis",
        }
        params: dict[str, Any] = {
            "composite_id": composite_id,
            "history_uri": results_uri,
            "analysis_name": analysis_name,
            "trigger": "multi-node-dispatch-flush",
            # ORMAnalysis.to_dto() unconditionally reads config["analysis_options"]
            # (AnalysisConfigOptions requires experiment_id) -- mirror the shape
            # _submit_analysis_job's own params dict already writes, so to_dto()
            # doesn't KeyError for this analysis kind either.
            "analysis_options": {"experiment_id": [experiment_id]},
        }
        try:
            analysis_job_id = self._dispatch._submit_container(
                job_name=f"ray-mnp-analysis-{experiment_id}-{_rand_suffix()}"[:128],
                job_definition=container_job_def,
                job_cmd=self._multi_node_analysis_command(
                    experiment_id=experiment_id,
                    composite_id=composite_id,
                    history_uri=results_uri,
                    out_uri=result_uri,
                    n_seeds=n_seeds,
                    n_generations=n_generations,
                    modules=modules,
                    sim_data_uri=sim_data_uri,
                ),
                out_s3=self._dispatch._results_s3_uri(experiment_id),
                out_dir=ANALYSIS_OUT_DIR,
                depends_on=None,
                depends_type=None,
                tags=tags,
                memory_class=analysis_memory_class(modules, n_seeds=n_seeds, n_generations=n_generations),
            )
        except Exception as e:
            logger.exception("Multi-node analysis submission failed for %s", experiment_id)
            await database_service.record_analysis(
                experiment_id=experiment_id,
                n_tp=None,
                status=AnalysisStatusDB.FAILED,
                config=params,
                name=analysis_name,
                simulation_id=simulation.database_id,
                backend="ray",
                result_uri=result_uri,
                error_message=f"multi-node analysis submission failed: {type(e).__name__}: {e}",
            )
            return None
        await database_service.record_analysis(
            experiment_id=experiment_id,
            n_tp=None,
            status=AnalysisStatusDB.COMPUTING,
            config=params,
            name=analysis_name,
            simulation_id=simulation.database_id,
            backend="ray",
            job_id_ext=str(analysis_job_id),
            result_uri=result_uri,
        )
        logger.info(
            "Multi-node composite %s (%s): analysis flush -> job %s",
            experiment_id,
            composite_id,
            analysis_job_id,
        )
        return analysis_job_id
