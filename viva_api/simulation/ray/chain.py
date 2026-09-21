"""The chain dispatch mechanism: the canonical multi-generation sweep as INDIVIDUAL AWS Batch container
jobs -- ParCa, then one job per seed per lineage, each depending on the one before -- and the analysis
job that gathers the campaign when its seeds have finished.

The fifth and last dispatch mechanism to become a strategy object (``docs/plan-core.md`` P2.1, PR 11),
and the largest: a thousand lines, because a campaign of 1000 seeds x 10 generations is 10,000 jobs
submitted under Batch's 50-TPS limit, in the background, resumably.

Not a Ray mechanism: each link is one plain container job. What orchestrates a chain is Batch's
``dependsOn`` plus the scheduler's poll (``job_scheduler.py``), which submits the next lineage batch.

* ``ChainStrategy(batch, local)``. ``batch`` is a ``ContainerSubmitter``. ``local`` is the in-process
  task service: ``submit`` hands the long submission loop to it and returns a ``JobId.local`` at once
  -- a constructor argument of THIS strategy, as ``k8s`` is of Nextflow's, and of no Protocol.
* ``submit`` is what the router calls (it was ``_submit_chain_dispatch_background``).
  ``submit_chain_dispatch_job`` is the synchronous campaign submit it wraps, still public: the
  integration tests and the capability probe reach it through a delegate on the service.
* ``submit_campaign_analysis`` / ``_submit_analysis_job`` travel with the mechanism, as the audit
  decided for every mechanism's analysis submitter.
* Four methods never touched ``self`` and are functions: the two seed commands, the analysis command,
  and ``chain_base_tags``.

What stays on the service until P6, because it is progress and cancel, not dispatch:
``get_chain_campaign_result``, ``cancel_chain_campaign``, ``cancel_companion_jobs``.

SMS code, and it stays SMS code.
"""

import asyncio
import functools
import json
import logging
import shlex
from collections.abc import Mapping
from typing import TYPE_CHECKING

from botocore.config import Config

from viva_api.common import analysis_dag
from viva_api.common.dispatch_validation import resolve_task_env
from viva_api.common.hpc.local_task_service import LocalTaskService
from viva_api.common.models import JobId
from viva_api.common.storage import data_layout
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import JobType, Simulation
from viva_api.simulation.ray import _seams, parca_spec
from viva_api.simulation.ray.analysis_spec import analysis_memory_class, analysis_modules_for
from viva_api.simulation.ray.batch_layer import ContainerSubmitter, _rand_suffix
from viva_api.simulation.ray.image_paths import ANALYSIS_OUT_DIR, PARCA_CACHE_DIR, SIM_OUT_DIR, V2ECOLI_DIR
from viva_api.simulation.ray.runner_env import PBG_RUNNER_ENV, V2ECOLI_BATCH_BASELINE_COMPOSITE_ID
from viva_core.backends.batch import SUBMIT_JOB_MAX_ATTEMPTS, SubmitJobPacer
from viva_core.events.events_env import with_events_env

if TYPE_CHECKING:
    # ``types-boto3`` is a dev dependency (annotations only): never imported at runtime.
    from types_boto3_batch import BatchClient

logger = logging.getLogger(__name__)


def seed_generation_command(
    *,
    seed: int,
    generation_index: int,
    experiment_id: str,
    runner_s3_uri: str,
    injected_processes: Mapping[str, object] | None = None,
    variants: Mapping[str, object] | None = None,
    composite_id: str | None = None,
    exchange_fluxes: Mapping[str, object] | None = None,
    exchange_flux_basis: str | None = None,
) -> str:
    """Build ONE seed's ONE generation's command (backlog item 33 rework —
    per-seed independent job chains, mirroring vEcoli-private's own
    Nextflow task granularity, ``runscripts/nextflow/sim.nf``, where task
    retry at generation granularity IS checkpoint/resume).

    SIMPLER than the per-generation-array design this superseded (formerly
    ``_wave_sim_command``): this job is submitted as its own standalone
    Batch job (see ``submit_chain_dispatch_job``), not one array child
    sharing a command across many indices, so BOTH ``seed`` and
    ``generation_index`` are already known Python-side at SUBMISSION time
    — no ``AWS_BATCH_JOB_ARRAY_INDEX``, no lookup table, no container-start
    shell/python3 merge step at all. The full ``--overrides`` payload is
    computed once, here, and embedded as a single static JSON blob.

    ``initial_carry_state_path``/``daughter_state_out_path`` are exactly
    ``RayLayout.daughter_state_uri``'s own deterministic per-seed,
    per-generation S3 path — generation 0 has no prior generation, so
    ``initial_carry_state_path`` is "" (``LineageProcess`` defaults it to
    "", matching a fresh cell — the validated, non-error case). Every
    generation writes its own daughter state out, including the final
    one — a harmless no-op read by nobody if the chain ends there, cheaper
    than a special case to skip it.

    ``injected_processes``/``variants`` (backlog item 93): generic
    passthrough of ``ecoli_baseline.baseline()``'s own same-named kwargs —
    built by ``injected_processes_from_config``/read off the legacy config
    by this method's callers, never by this method itself. Both default to
    ``None`` and are omitted from ``overrides`` entirely when absent, so
    any caller not passing them builds the exact same command as before
    these params existed.

    ``stop_at_division: True`` (backlog item 103) is unconditional, always
    set: with ``n_seeds=1, n_generations=1`` (this method's own fixed
    values) and no ``stop_at_division``, ``ecoli_baseline.baseline()``'s own
    dispatch gate (``n_seeds>1 or n_generations>1 or stop_at_division``)
    evaluates False, routing every generation through the PLAIN single-cell
    build the composite's own docs call "NO division-stop" -- each job ran
    for exactly 1 simulated second (``-n 1`` below) regardless of whether
    the cell divided, and ``initial_carry_state_path``/
    ``daughter_state_out_path`` (this method's own checkpoint/resume
    mechanism, set above) were silently never consumed, since they only
    apply inside the gated branch. ``stop_at_division=True`` routes through
    the SAME batch/lineage path via ``LineageProcess`` (``generations=1``
    stops the lineage after the first real division, Option A / issue #495)
    -- that path's own checkpoint/resume handling is real and unconditional
    on the daughter-state write (``lineage.py``'s own comment: "a
    one-wave-per-invocation caller (generations=1) always takes the
    'complete' branch below, but still needs THIS generation's daughter
    written out"), so no other change here is needed for the hand-off to
    start working correctly. Confirmed empirically: campaign 171's own real
    production output showed generation 0/5/9 of the same lineage as
    MD5-identical files and ``global_time: 1.0`` after 10 chained
    "generations" -- this fixes that.

    CROSS-REPO CONTRACT: overrides threading these 3 keys through to
    ``v2ecoli/composites/ecoli_baseline.py``'s ``baseline()`` signature (the
    composite this command dispatches through by default, via
    ``V2ECOLI_BATCH_BASELINE_COMPOSITE_ID`` — formerly a dedicated
    ``composites/batch_baseline.py``, folded into ``ecoli_baseline.py`` by
    v2ecoli #373 and finally synced into sms-ecoli by PR #56, backlog item 55)
    is v2ecoli's own responsibility; nothing about that contract is affected by
    this per-seed rework, only WHICH viva-api command builder emits the same
    keys.

    ``composite_id`` (backlog item 105): caller-selectable override for
    ``--composite-id``, defaulting to ``V2ECOLI_BATCH_BASELINE_COMPOSITE_ID``
    when omitted — every existing caller keeps building the exact same
    command as before this param existed. Exists because chain-dispatch
    was previously hardcoded to ``ecoli_baseline`` only; some real
    composites (e.g. ``reactor_bird_coupled``) that ALSO support the
    ``n_seeds``/``n_generations``/``injected_processes``/``variants``
    overrides shape above (v2ecoli #648) were unreachable through this
    entrypoint purely because nothing could ever select them. Not
    validated here — same philosophy as ``injected_processes``/
    ``variants`` above: pure passthrough, v2ecoli's own
    ``composite_spec`` resolution (inside ``run_pbg.py``) is what fails
    loudly on a bad id, not this layer.

    ``out_dir`` is ``RayLayout.seed_results_uri`` (an ``s3://`` URI), not a
    local path: every generation's job for this seed shares it, so the
    composite's own parquet sweep / zarr store / summary.json (sms-ecoli's
    ``v2ecoli/cache.py``, ``workflow/lineage.py``, ``workflow/run.py`` —
    all made S3-URI-aware alongside this) accumulate under one seed-scoped
    S3 prefix instead of colliding on the flat, ensemble-wide
    ``experiment_prefix`` every seed's every generation would otherwise
    share (the real bug item 35's pilot found — every job's ``summary.json``
    clobbering the last one's). ``daughter_state_out_path``/
    ``initial_carry_state_path`` already lived under a per-seed prefix
    (``daughter_state_uri``), unaffected by this.

    ``exchange_fluxes``/``exchange_flux_basis`` (backlog item 105, the K4
    cell-only ensemble): the SAME two params ``_submit_multi_node_composite``
    already threads for pbg-native (item106) -- chain-dispatch never had
    them. Without ``exchange_fluxes`` the composite's own ExchangeFluxListener
    never mounts, so no ``listeners__exchange_flux__*`` columns are written
    AT ALL, with no refusal -- a real, silent measurement gap this project's
    own sms-ecoli collaborators (cplong90) documented precisely:
    "an absent product column has the same artifact signature as a run that
    never worked, and costs real time to tell apart." Pure passthrough,
    both default ``None`` and are omitted from ``overrides`` entirely when
    absent -- every existing caller builds the exact same command as before
    these params existed.
    """
    daughter_state_out_path = data_layout.RayLayout.daughter_state_uri(experiment_id, seed, generation_index)
    initial_carry_state_path = (
        data_layout.RayLayout.daughter_state_uri(experiment_id, seed, generation_index - 1)
        if generation_index > 0
        else ""
    )
    seed_out_dir = data_layout.RayLayout.seed_results_uri(experiment_id, seed).rstrip("/")
    overrides = {
        "n_seeds": 1,
        "n_generations": 1,
        # Backlog item 103: unconditional, always on -- see this method's
        # own docstring. Without it every generation silently ran a plain,
        # non-division-gated 1-simulated-second build regardless of
        # generation_index, and the checkpoint/resume fields below were
        # never actually consumed.
        "stop_at_division": True,
        "cache_dir": PARCA_CACHE_DIR,
        "out_dir": seed_out_dir,
        "experiment_id": experiment_id,
        "analyses": "none",
        "parallel": "",
        # ecoli_baseline.baseline()'s own param is `seed`, not `base_seed` --
        # the latter was correct for the old, now-deleted batch_baseline
        # composite (backlog item 55) but is an unexpected-kwarg TypeError here.
        "seed": int(seed),
        "initial_generation_index": int(generation_index),
        "initial_carry_state_path": initial_carry_state_path,
        "daughter_state_out_path": daughter_state_out_path,
    }
    if injected_processes:
        overrides["injected_processes"] = injected_processes
    if variants:
        overrides["variants"] = variants
    if exchange_fluxes:
        overrides["exchange_fluxes"] = exchange_fluxes
        if exchange_flux_basis:
            overrides["exchange_flux_basis"] = exchange_flux_basis
    env = PBG_RUNNER_ENV
    return (
        f"cd {V2ECOLI_DIR}"
        f" && aws s3 cp {runner_s3_uri} /tmp/run_pbg.py"
        f" && {env} python /tmp/run_pbg.py"
        f" --composite-id {composite_id or V2ECOLI_BATCH_BASELINE_COMPOSITE_ID}"
        f" --overrides {shlex.quote(json.dumps(overrides))} -n 1"
    )


def seed_lineage_command(
    *,
    seed: int,
    n_generations: int,
    experiment_id: str,
    runner_s3_uri: str,
    injected_processes: Mapping[str, object] | None = None,
    variants: Mapping[str, object] | None = None,
    composite_id: str | None = None,
    exchange_fluxes: Mapping[str, object] | None = None,
    exchange_flux_basis: str | None = None,
) -> str:
    """Build ONE seed's WHOLE-LINEAGE command: all ``n_generations`` in a
    single ``LineageProcess`` (one Batch job per seed), not one job per
    generation.

    This is the Run-3 fix. ``_seed_generation_command`` runs one generation
    per job (``n_generations=1``, ``stop_at_division=True``) with an S3
    daughter-state checkpoint handed between jobs — so every generation job
    is a *fresh* ``LineageProcess`` whose ``lineage_time_offset`` restarts at
    0.0, and a ``field_timeline`` dose scheduled at a cumulative-lineage time
    (e.g. ``DOSE_ONSET_TIME_S=10000``) is compared against a per-generation
    clock that only reaches ~3000 s — so it NEVER fires (silent no-dose
    control; see docs/design-chain-one-lineageprocess.md).

    Running the whole lineage in ONE ``LineageProcess`` (exactly as the
    Nextflow path already does, and as the ``n_generations>1`` batch shape in
    ``_sim_command`` does) makes ``lineage_time_offset`` accumulate across
    generations, so the dose fires at its intended cumulative time. Division
    is in-process, so NO ``initial_generation_index`` /
    ``initial_carry_state_path`` / ``daughter_state_out_path`` and NO
    ``stop_at_division`` — ``baseline()``'s own ``n_seeds>1 or
    n_generations>1`` gate routes ``n_generations>1`` through the
    batch/lineage shape. ``seed``/``injected_processes``/``variants``/
    ``exchange_fluxes`` are threaded identically to
    ``_seed_generation_command`` (same per-seed S3 out layout).
    """
    seed_out_dir = data_layout.RayLayout.seed_results_uri(experiment_id, seed).rstrip("/")
    overrides: dict[str, object] = {
        "n_seeds": 1,
        "n_generations": int(n_generations),
        "cache_dir": PARCA_CACHE_DIR,
        "out_dir": seed_out_dir,
        "experiment_id": experiment_id,
        # The campaign's analysis DAG node runs analyses once over the landed
        # sweep -- skip the composite's own inline flush (matches the batch
        # and per-generation paths).
        "analyses": "none",
        "parallel": "",
        # ecoli_baseline.baseline()'s own per-seed param (see
        # _seed_generation_command): run exactly this seed's lineage.
        "seed": int(seed),
    }
    if injected_processes:
        overrides["injected_processes"] = injected_processes
    if variants:
        overrides["variants"] = variants
    if exchange_fluxes:
        overrides["exchange_fluxes"] = exchange_fluxes
        if exchange_flux_basis:
            overrides["exchange_flux_basis"] = exchange_flux_basis
    env = PBG_RUNNER_ENV
    return (
        f"cd {V2ECOLI_DIR}"
        f" && aws s3 cp {runner_s3_uri} /tmp/run_pbg.py"
        f" && {env} python /tmp/run_pbg.py"
        f" --composite-id {composite_id or V2ECOLI_BATCH_BASELINE_COMPOSITE_ID}"
        f" --overrides {shlex.quote(json.dumps(overrides))} -n 1"
    )


def analysis_command(
    *,
    experiment_id: str,
    modules: dict[str, dict[str, object]] | str,
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
    out_uri = data_layout.RayLayout.results_uri(experiment_id).rstrip("/")
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


def chain_base_tags(*, simulation: Simulation, commit: str) -> dict[str, str]:
    """Cost-allocation tag base shared by every per-seed chain job + the
    ParCa job that precedes them, mirroring ``submit_ecoli_simulation_job``'s
    ``base_tags`` (composite/condition don't apply — chain dispatch is
    v2ecoli-only)."""
    settings = _seams.get_settings()
    return {
        "Project": "v2ecoli-comparison",
        "ExperimentId": str(simulation.config.experiment_id)[:255],
        "Engine": "v2ecoli",
        "Commit": str(commit)[:12],
        "Team": getattr(settings, "cost_team_tag", None) or "covertlab",
    }


class ChainStrategy:
    def __init__(self, batch: ContainerSubmitter, local: LocalTaskService) -> None:
        self._batch = batch
        self._local = local

    def submit_chain_generation(
        self,
        *,
        seed: int,
        generation_index: int,
        experiment_id: str,
        commit: str,
        cache_s3: str,
        runner_s3_uri: str,
        tags: dict[str, str],
        batch_client: "BatchClient | None" = None,
        injected_processes: Mapping[str, object] | None = None,
        variants: Mapping[str, object] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: Mapping[str, object] | None = None,
        exchange_flux_basis: str | None = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | None = None,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
    ) -> str:
        """Submit ONE seed's ONE generation as a standalone container-type job
        (backlog item 71 Phase 4) — the app-level-gated replacement for the
        superseded design's per-generation submission inside
        ``submit_chain_dispatch_job``'s own loop, which submitted every
        generation for every seed upfront via native Batch ``dependsOn``
        chains (item 68's own scaling-stall root cause). No ``depends_on``
        here: ``JobScheduler`` itself now decides WHEN to call this — only
        after confirming the previous generation (or ParCa, for generation 0)
        actually SUCCEEDED — so Batch's own dependency resolution is no longer
        part of the sequencing at all. Mirrors ``_seed_generation_command``'s
        own per-seed S3 layout exactly (unchanged by this migration — see that
        method's docstring); only the job TYPE and dependency model change.

        ``injected_processes``/``variants``/``composite_id``/``exchange_fluxes``/
        ``exchange_flux_basis`` (backlog items 93, 105): passed straight through
        to ``_seed_generation_command`` — see that method's own docstring.
        ``JobScheduler`` is the real caller, re-deriving all five from the
        campaign's own ``Simulation.config`` every tick (restart-safe, same as
        every other piece of per-tick state here).
        """
        job_def = self._batch.ensure_container_job_def(self._batch.image_uri(commit), commit)
        return self._batch.submit_container(
            job_name=f"chain-seed{seed}-gen{generation_index}-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            job_cmd=seed_generation_command(
                seed=seed,
                generation_index=generation_index,
                experiment_id=experiment_id,
                runner_s3_uri=runner_s3_uri,
                injected_processes=injected_processes,
                variants=variants,
                composite_id=composite_id,
                exchange_fluxes=exchange_fluxes,
                exchange_flux_basis=exchange_flux_basis,
            ),
            out_s3=data_layout.RayLayout.seed_results_uri(experiment_id, seed),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            tags={**tags, "Seed": str(seed), "Generation": str(generation_index)},
            batch_client=batch_client,
            expect_new_genes=expect_new_genes,
            expect_bundle_overrides=expect_bundle_overrides,
            lineage_debug_division=lineage_debug_division,
            task_env=task_env,
        )

    async def submit_chain_generation_batch(
        self,
        *,
        seeds: list[int],
        generation_index: int,
        experiment_id: str,
        commit: str,
        cache_s3: str,
        runner_s3_uri: str,
        tags: dict[str, str],
        injected_processes: Mapping[str, object] | None = None,
        variants: Mapping[str, object] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: Mapping[str, object] | None = None,
        exchange_flux_basis: str | None = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | None = None,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
    ) -> dict[int, str]:
        """Submit the SAME generation index for MULTIPLE seeds at once,
        TPS-paced below the account-wide ``SubmitJob`` rate limit (reuses
        ``SubmitJobPacer`` + a dedicated retry-configured client — the same
        mechanism the superseded upfront-chain design used for its own N*G
        burst). Still needed for the one remaining genuine burst moment under
        the per-seed app-level-gated model: every seed's generation 0, fanned
        out the instant ParCa succeeds. Every OTHER generation-index step from
        then on submits at most one job per seed per campaign per poll
        interval — naturally spread out by the 30s tick cadence, no pacing
        needed there (``submit_chain_generation`` alone is used for those).

        A per-seed submission failure (even after retry-on-throttle) is logged
        and that seed is simply omitted from the returned mapping — mirrors
        the superseded design's own "truncate just this seed's chain" failure
        semantics; other seeds are unaffected.

        ``injected_processes``/``variants``/``composite_id``/``exchange_fluxes``/
        ``exchange_flux_basis`` (backlog items 93, 105): the SAME values for
        every seed in this batch — one campaign, one config — forwarded to each
        seed's own ``submit_chain_generation`` call below.
        """
        pacer = SubmitJobPacer()
        submit_client = _seams.boto3.client(
            "batch",
            region_name=_seams.get_settings().batch_region,
            config=Config(retries={"mode": "standard", "max_attempts": SUBMIT_JOB_MAX_ATTEMPTS}),
        )
        submitted: dict[int, str] = {}
        for seed in seeds:
            await pacer.wait()
            try:
                submitted[seed] = self.submit_chain_generation(
                    seed=seed,
                    generation_index=generation_index,
                    experiment_id=experiment_id,
                    commit=commit,
                    cache_s3=cache_s3,
                    runner_s3_uri=runner_s3_uri,
                    tags=tags,
                    batch_client=submit_client,
                    injected_processes=injected_processes,
                    variants=variants,
                    composite_id=composite_id,
                    exchange_fluxes=exchange_fluxes,
                    exchange_flux_basis=exchange_flux_basis,
                    expect_new_genes=expect_new_genes,
                    expect_bundle_overrides=expect_bundle_overrides,
                    lineage_debug_division=lineage_debug_division,
                    task_env=task_env,
                )
            except Exception:
                logger.exception(
                    "Chain dispatch %s: seed %d generation %d submission failed "
                    "(even after retry-on-throttle) -- this seed's chain ends here; "
                    "other seeds are unaffected",
                    experiment_id,
                    seed,
                    generation_index,
                )
                continue
        return submitted

    def submit_chain_lineage(
        self,
        *,
        seed: int,
        n_generations: int,
        experiment_id: str,
        commit: str,
        cache_s3: str,
        runner_s3_uri: str,
        tags: dict[str, str],
        batch_client: "BatchClient | None" = None,
        injected_processes: Mapping[str, object] | None = None,
        variants: Mapping[str, object] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: Mapping[str, object] | None = None,
        exchange_flux_basis: str | None = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | None = None,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
    ) -> str:
        """Submit ONE seed's WHOLE lineage as a single standalone container job:
        all ``n_generations`` in one ``LineageProcess``.

        Replaces the per-generation ``submit_chain_generation`` on the chain
        path. Because the whole lineage now runs in one process,
        ``lineage_time_offset`` accumulates across generations and a
        ``field_timeline`` dose scheduled at a cumulative time actually fires
        (the Run-3 fix; see ``_seed_lineage_command``). Per-seed independence is
        preserved — one job per seed, no cross-seed barrier — so this keeps the
        scheduler's fully-asynchronous per-seed model.
        """
        # Same container job def as the per-generation jobs: peak memory of a
        # whole-lineage LineageProcess matches one generation (advance_generation
        # flushes + consolidates each generation, so no whole-lineage buffer
        # growth), and _submit_container sets no attemptDurationSeconds, so the
        # longer wall-clock of N generations simply runs. (Spot interruption of a
        # long lineage loses the partial run's job success even though gens
        # 0..N-1 are durably on disk; resume-from-generation is a possible
        # follow-up -- see docs/design-chain-one-lineageprocess.md.)
        job_def = self._batch.ensure_container_job_def(self._batch.image_uri(commit), commit)
        return self._batch.submit_container(
            job_name=f"chain-seed{seed}-lineage-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            job_cmd=seed_lineage_command(
                seed=seed,
                n_generations=n_generations,
                experiment_id=experiment_id,
                runner_s3_uri=runner_s3_uri,
                injected_processes=injected_processes,
                variants=variants,
                composite_id=composite_id,
                exchange_fluxes=exchange_fluxes,
                exchange_flux_basis=exchange_flux_basis,
            ),
            out_s3=data_layout.RayLayout.seed_results_uri(experiment_id, seed),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            tags={**tags, "Seed": str(seed), "Generations": str(n_generations)},
            batch_client=batch_client,
            expect_new_genes=expect_new_genes,
            expect_bundle_overrides=expect_bundle_overrides,
            lineage_debug_division=lineage_debug_division,
            task_env=task_env,
        )

    async def submit_chain_lineage_batch(
        self,
        *,
        seeds: list[int],
        n_generations: int,
        experiment_id: str,
        commit: str,
        cache_s3: str,
        runner_s3_uri: str,
        tags: dict[str, str],
        injected_processes: Mapping[str, object] | None = None,
        variants: Mapping[str, object] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: Mapping[str, object] | None = None,
        exchange_flux_basis: str | None = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | None = None,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
    ) -> dict[int, str]:
        """Fan out ONE whole-lineage job per seed, TPS-paced below the
        account-wide ``SubmitJob`` rate limit (same ``SubmitJobPacer`` +
        retry-configured client as ``submit_chain_generation_batch``).

        Replaces the per-generation ``submit_chain_generation_batch`` on the
        chain path. This is the ONLY submission burst now: every seed's lineage
        is fanned out once, the instant ParCa succeeds; there is no per-seed
        per-generation follow-up submission (the scheduler only polls each
        lineage job to resolution). A per-seed submission failure is logged and
        that seed omitted from the returned mapping — other seeds unaffected.
        """
        pacer = SubmitJobPacer()
        submit_client = _seams.boto3.client(
            "batch",
            region_name=_seams.get_settings().batch_region,
            config=Config(retries={"mode": "standard", "max_attempts": SUBMIT_JOB_MAX_ATTEMPTS}),
        )
        submitted: dict[int, str] = {}
        for seed in seeds:
            await pacer.wait()
            try:
                submitted[seed] = self.submit_chain_lineage(
                    seed=seed,
                    n_generations=n_generations,
                    experiment_id=experiment_id,
                    commit=commit,
                    cache_s3=cache_s3,
                    runner_s3_uri=runner_s3_uri,
                    tags=tags,
                    batch_client=submit_client,
                    injected_processes=injected_processes,
                    variants=variants,
                    composite_id=composite_id,
                    exchange_fluxes=exchange_fluxes,
                    exchange_flux_basis=exchange_flux_basis,
                    expect_new_genes=expect_new_genes,
                    expect_bundle_overrides=expect_bundle_overrides,
                    lineage_debug_division=lineage_debug_division,
                    task_env=task_env,
                )
            except Exception:
                logger.exception(
                    "Chain dispatch %s: seed %d lineage submission failed "
                    "(even after retry-on-throttle) -- this seed's lineage is "
                    "omitted; other seeds are unaffected",
                    experiment_id,
                    seed,
                )
                continue
        return submitted

    async def submit(
        self,
        ecoli_simulation: Simulation,
        database_service: DatabaseService,
        *,
        correlation_id: str,
    ) -> JobId:
        """Run ``submit_chain_dispatch_job`` as a background task; return a trackable id at once.

        A chain-dispatch campaign issues ``n_seeds * n_generations`` individual
        AWS Batch ``SubmitJob`` calls, paced below the account-wide TPS cap. For
        the canonical 1000x10 shape that is 10,000 calls and roughly 15 minutes
        of wall time — all of it inside the single ``POST /api/v1/simulations``
        request while the submission loop runs inline. A real production
        dispatch (2026-08-14) proved the consequence: the calling client's HTTP
        timeout fired long before the loop finished, so the user was told the
        dispatch had FAILED while viva-api went right on submitting the real,
        AWS-billed campaign. The obvious reaction to that message — retry —
        would have started a second, duplicate, paid campaign on top of the
        first.

        The fix reuses the pattern this service already uses for the other
        multi-minute operation it owns, the DooD image build
        (``submit_build_image_job``): hand the slow coroutine to
        ``LocalTaskService``, return its ``JobId.local(...)`` immediately, let
        the caller poll. No new machinery is involved — because every backend
        service shares ONE process-wide ``LocalTaskService`` (see
        ``viva_api.dependencies._init_simulation_service``), ``get_job_status``
        — and therefore ``GET /api/v1/simulations/{id}/status`` — already
        resolves such an id, reporting RUNNING while the submission loop is
        still going and FAILED if it crashes outright. ``cancel_job`` already
        routes LOCAL ids to ``LocalTaskService.cancel``, so cancelling a
        still-submitting campaign comes along for free.

        ``submit_chain_dispatch_job`` itself is UNCHANGED and still runs
        synchronously to completion for its direct callers (its own unit tests
        and the real-AWS integration test); only this one call site is
        asynchronous.

        THE PLACEHOLDER ROW, and why its chain fields must stay ``None``: the
        campaign's REAL ``HpcRun`` row is inserted by
        ``submit_chain_dispatch_job`` itself, as its very last action — minutes
        from now. Until then a status lookup would find nothing at all, so this
        method records a placeholder row carrying the LOCAL task id. It
        deliberately leaves BOTH ``chain_n_generations`` and
        ``chain_final_job_ids`` unset:

          - ``DatabaseService.list_active_chain_campaigns`` (the scheduler's
            poll set) discriminates on ``chain_n_generations IS NOT NULL``
            ALONE. Setting it here would enroll the placeholder in that poll set
            before a single per-seed job exists.
          - ``get_chain_campaign_result([])`` returns ``terminal=True`` with
            zero successes by definition, so ``JobScheduler._advance_chain_campaign``
            would then immediately mark the campaign FAILED — recreating, inside
            viva-api this time, the very false-failure this method exists to
            eliminate.

        With both left ``None``, ``get_simulation_status`` takes its ordinary
        non-campaign branch and reports the LOCAL task's own status, which is
        the honest answer while submission is in flight. Once the background
        task finishes, the campaign row it inserts (a SECOND real row, under the
        same ``correlation_id``) supersedes this placeholder for every later
        read: ``get_hpcrun_by_ref`` resolves the highest ``id``. The handler's
        own idempotent-insert guard (``viva_api.common.handlers.simulations``,
        keyed on ``correlation_id``) sees this placeholder and correctly skips
        adding a third, generic row.
        """
        # Gate the background task on the placeholder being committed. Without
        # the gate, `create_task` followed by `await insert_hpcrun(...)` lets the
        # campaign coroutine run during that await, and the two inserts can land
        # in either order. The wrong order is NOT benign: `get_hpcrun_by_ref`
        # resolves the highest id, so a placeholder written AFTER the real
        # campaign row would shadow it permanently, and the plain status path
        # would report the whole campaign COMPLETED the moment the submission
        # loop finished — while every one of its N*G real jobs was still queued
        # or running. An asyncio.Event makes the ordering a guarantee instead of
        # a race the mocked-out test environment happens to usually win.
        placeholder_recorded = asyncio.Event()

        async def _run() -> JobId:
            await placeholder_recorded.wait()
            return await self.submit_chain_dispatch_job(
                ecoli_simulation, database_service, correlation_id=correlation_id
            )

        task_job_id = self._local.submit(_run(), name=f"chain-dispatch-{ecoli_simulation.config.experiment_id}")
        try:
            placeholder = await database_service.insert_hpcrun(
                job_id=task_job_id,
                job_type=JobType.SIMULATION,
                ref_id=ecoli_simulation.database_id,
                correlation_id=correlation_id,
            )
        except Exception:
            # Nothing was recorded, so nothing may run: releasing the gate here
            # would dispatch a real, billed campaign for a request whose caller
            # is about to be handed an error.
            self._local.cancel(task_job_id.value)
            raise
        # viva-api#414: bind the placeholder to its task so the row is finalized
        # from the task's own outcome (COMPLETED once the real campaign row has
        # superseded it; FAILED, with the exception, if submission crashed).
        # Before this the placeholder stayed `running` in the DB forever, and a
        # submission crash was visible only in this process's memory.
        await self._local.bind_hpcrun(task_job_id.value, placeholder.database_id, database_service)
        placeholder_recorded.set()
        logger.info(
            "Chain dispatch %s: submitting the campaign in the background as local task %s "
            "(request returns now; poll GET /simulations/{id}/status for progress)",
            ecoli_simulation.config.experiment_id,
            task_job_id.value,
        )
        return task_job_id

    async def submit_chain_dispatch_job(
        self,
        ecoli_simulation: Simulation,
        database_service: DatabaseService,
        correlation_id: str | None = None,
    ) -> JobId:
        """Kick off a per-seed chain-dispatch campaign (backlog item 33 rework,
        further reworked by item 71 Phase 4): submit ONLY ParCa here, as a
        plain container-type job (backlog item 71 — no real inter-node traffic
        to protect, same reasoning as ``submit_parca_job``). The N*G per-seed
        generation jobs are NOT submitted upfront anymore — that upfront-
        ``dependsOn`` design was item 68's own scaling-stall root cause (AWS
        Batch's compute-environment scaling reconciliation never engaged for a
        huge MNP+dependsOn backlog, confirmed via CloudTrail showing zero
        scaling API activity despite ~1000 RUNNABLE jobs). Generation
        submission moves to ``JobScheduler``'s existing 30s poll loop
        (``_advance_chain_campaign``, DB-driven, restart-safe), which submits
        exactly ONE generation per seed at a time, only once the previous one
        (or ParCa, for generation 0) is confirmed SUCCEEDED — app-level gating
        instead of native Batch dependency chains. Still a true v2 analogy of
        vEcoli-private's own fully-asynchronous per-seed Nextflow execution:
        seed 5 can be on generation 8 while seed 800 is on generation 1,
        throttled only by available compute, never by a cross-seed barrier —
        that property now comes from the scheduler's own per-seed
        independence, not from Batch dependsOn.

        This method returns as soon as ParCa is submitted and the campaign's
        initial tracking row is written — no more N*G-submission wall time to
        wait out inline. ``_submit_chain_dispatch_background`` still wraps it
        in a background task (cheap now, but keeps that caller's contract
        unchanged rather than special-casing "fast" vs "slow" chain-dispatch
        calls).

        ``correlation_id``: unchanged from before — this method always records
        its OWN ``HpcRun`` row (the campaign-tracking row needs
        ``chain_n_generations``/the ``chain_current_*``/``chain_final_job_ids``
        fields, which a generic caller has no way to populate). One is
        generated fresh here when called with none (e.g. directly by tests);
        the real dispatch entrypoint threads its own request-scoped id through
        instead, so a status lookup by that id resolves to this exact row (see
        ``viva_api.common.handlers.simulations``'s idempotent-insert guard).

        Unlike the array-job design predating item 33, ``n_seeds >= 2`` is not
        required (no AWS Batch array-size floor applies — every seed's chain is
        independent standalone jobs). ``n_generations >= 2`` is still required
        — a single-generation request has nothing to chain; use
        ``submit_ecoli_simulation_job``.

        The initial campaign row's per-seed tracking fields all start "empty":
        ``chain_current_job_ids``/``chain_current_generation`` are
        ``[None] * n_seeds`` (no generation submitted yet, gated on ParCa),
        ``chain_parca_done=False``, ``chain_final_job_ids=[]`` (filled
        incrementally by the scheduler as each seed's chain resolves — see
        that method for why this keeps the existing analysis-fan-in consumer,
        ``get_chain_campaign_result``, working unchanged once every seed has
        contributed its entry). Returns the ParCa job's ``JobId`` — the one
        well-defined "campaign kickoff" marker, unchanged from before.
        """
        parca_dataset = await database_service.get_parca_dataset(parca_dataset_id=ecoli_simulation.parca_dataset_id)
        if parca_dataset is None:
            raise ValueError(f"ParcaDataset with ID {ecoli_simulation.parca_dataset_id} not found.")
        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {ecoli_simulation.simulator_id} not found")

        commit = simulator.environment_key
        config = ecoli_simulation.config
        n_seeds = int(ecoli_simulation.num_seeds or getattr(config, "n_init_sims", None) or 1)
        n_generations = int(config.generations or 1)
        if n_generations < 2:
            raise ValueError(
                "submit_chain_dispatch_job requires generations > 1 "
                "(use submit_ecoli_simulation_job for single-generation runs)"
            )

        experiment_id = str(ecoli_simulation.config.experiment_id)
        cache_s3 = parca_spec.cache_s3_uri(commit)
        base_tags = chain_base_tags(simulation=ecoli_simulation, commit=commit)
        container_job_def = self._batch.ensure_container_job_def(self._batch.image_uri(commit), commit)

        # Backlog item 93: a legacy config's own parca_options.new_genes (e.g.
        # a custom strain's new-gene insertion) is a real SimulationConfig
        # field, not an extra -- read directly, matching config.generations
        # above (extra="allow" only applies to genuinely undeclared keys).
        new_genes = getattr(config.parca_options, "new_genes", None)
        # Backlog item 104 (sms-ecoli#184 / viva-api#365): same generic
        # passthrough, missed in the item-93 pass -- parca_options.bundle_overrides
        # survived on the stored request but was never forwarded, so ParCa built
        # from defaults only and any keys the overrides supply were absent.
        bundle_overrides = getattr(config.parca_options, "bundle_overrides", None)
        # Same generic rnaseq_source passthrough (item 106/#166 chassis-provenance
        # thread), same reasoning as new_genes/bundle_overrides above.
        rnaseq_source = getattr(config.parca_options, "rnaseq_source", None)
        # Fixed BEFORE the ParCa job is submitted so the job's PBG_* identity env
        # and the campaign row's correlation_id derive the same trace id.
        campaign_correlation_id = correlation_id or f"chain-campaign-{experiment_id}-{_rand_suffix()}"
        parca_job_id = self._batch.submit_container(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=container_job_def,
            job_cmd=parca_spec.parca_command(
                new_genes=new_genes, bundle_overrides=bundle_overrides, rnaseq_source=rnaseq_source
            ),
            out_s3=cache_s3,
            out_dir=PARCA_CACHE_DIR,
            tags={**base_tags, "Phase": "parca"},
            task_env=with_events_env(
                resolve_task_env(config),
                correlation_id=campaign_correlation_id,
                experiment_id=experiment_id,
                sim_id=ecoli_simulation.database_id,
                backend="chain",
                tags={"phase": "parca"},
                settings=_seams.get_settings(),
            ),
        )

        await database_service.insert_hpcrun(
            job_id=JobId.ray(parca_job_id),
            job_type=JobType.SIMULATION,
            ref_id=ecoli_simulation.database_id,
            correlation_id=campaign_correlation_id,
            chain_n_generations=n_generations,
            chain_final_job_ids=[],
            chain_current_job_ids=[None] * n_seeds,
            chain_current_generation=[None] * n_seeds,
            chain_parca_done=False,
        )
        logger.info(
            "Chain dispatch %s: parca job %s submitted; %d seeds x %d generations "
            "will be advanced incrementally by JobScheduler once ParCa succeeds",
            experiment_id,
            parca_job_id,
            n_seeds,
            n_generations,
        )
        return JobId.ray(parca_job_id)

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
        base_tags = chain_base_tags(simulation=simulation, commit=commit)
        # Backlog item 71: _submit_analysis_job now submits via _submit_container,
        # so this must resolve a container job def, not an MNP one.
        container_job_def = self._batch.ensure_container_job_def(self._batch.image_uri(commit), commit)
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
        out_uri = data_layout.RayLayout.results_uri(experiment_id).rstrip("/")
        result_uri = f"{out_uri}/analyses/{analysis_name}"
        modules = analysis_modules_for(simulation.config)
        sim_data_uri = f"{data_layout.RayLayout.parca_cache_uri(commit, variant=cache_variant)}simData.cPickle"
        params: dict[str, object] = {
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
                self._batch.submit_container,
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
            out_s3=data_layout.RayLayout.results_uri(experiment_id),
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
