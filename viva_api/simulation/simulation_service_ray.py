"""AWS Batch multi-node-parallel (MNP) Ray implementation of SimulationService.

The v2ecoli whole-cell sim runs distributed on a *transient* Ray cluster: one
Batch MNP job gang-schedules N nodes, the Ray-on-Batch entrypoint
(``ray-batch-entrypoint.sh``, bundled in the v2ecoli image) forms the Ray
cluster, stages a ParCa cache from S3, exports ``RAY_ADDRESS``, and runs
``RAY_JOB_CMD`` (the v2ecoli ensemble) on the head — no Nextflow. This service
submits those MNP jobs.

Data flow (no shared filesystem):
  - ParCa runs first as its own 1-node MNP job; its cache is captured to a
    deterministic S3 URI (``RAY_OUT_S3``).
  - The simulation MNP job ``dependsOn`` the ParCa job (Batch gates it until
    ParCa SUCCEEDED), stages that cache (``RAY_STAGE_S3``), runs the ensemble,
    and captures the zarr/summary outputs to S3 (``RAY_OUT_S3``).
  - For the multi-generation batch_baseline sweep, an ANALYSIS job ``dependsOn``
    the simulation job and runs the ported cd1_*/ptools_* analyses over the
    landed S3 sweep. The whole pipeline is therefore one Batch dependency DAG
    (parca -> sim -> analysis); nothing external has to notice a completion and
    react to it. See ``_analysis_command`` for why this is a third DAG node and
    not the composite's own inline flush.

The image is the **workload-owned**, self-contained ``v2ecoli:<sha>`` (bundles
the AWS CLI + the Ray entrypoint), built by ``submit_build_image_job`` via a DooD
Batch job — symmetric with how ``SimulationServiceK8s`` builds ``vecoli:{commit}``.
Each run uses its simulator's TRUE commit image: since Batch MNP can't override the
image per submission, we derive a per-commit MNP job-def revision from the sms-cdk
base (cloning its node properties, swapping the image to ``v2ecoli:<commit>``).
"""

import asyncio
import functools
import importlib.resources as _res
import json
import logging
import shlex
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, override

from botocore.config import Config

from viva_api.common import analysis_dag
from viva_api.common.dispatch_validation import resolve_task_env
from viva_api.common.events_env import with_events_env
from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.hpc.k8s_job_service import K8sJobService
from viva_api.common.hpc.local_task_service import LocalTaskService
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.common.simulator_defaults import DEFAULT_BRANCH, DEFAULT_REPO
from viva_api.common.storage import data_layout
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.github_repo import (
    fetch_config_template,
    fetch_latest_commit_hash,
    fetch_repo_discovery,
)
from viva_api.simulation.models import (
    CompositeEngine,
    HpcRun,
    JobType,
    ParcaDataset,
    RepoDiscovery,
    Simulation,
    SimulatorVersion,
    VecoliSource,
)
from viva_api.simulation.ray import _seams, parca_spec
from viva_api.simulation.ray.analysis_spec import analysis_memory_class, analysis_modules_for
from viva_api.simulation.ray.batch_layer import RayBatchLayer, _rand_suffix
from viva_api.simulation.ray.build import RayImageBuilder
from viva_api.simulation.ray.config_interpretation import (
    _batch_domain_overrides,
    _is_upstream_vecoli,
    injected_processes_from_config,
)
from viva_api.simulation.ray.image_paths import (
    ANALYSIS_OUT_DIR,
    PARCA_CACHE_DIR,
    SIM_OUT_DIR,
    V2ECOLI_DIR,
)
from viva_api.simulation.ray.mbp_tracked import MbpTrackedStrategy
from viva_api.simulation.ray.multi_node import MultiNodeCompositeStrategy
from viva_api.simulation.ray.nextflow import NextflowStrategy
from viva_api.simulation.ray.parca import RayParcaService
from viva_api.simulation.ray.run_records import record_run_with_companions
from viva_api.simulation.ray.runner_env import PBG_RUNNER_ENV
from viva_api.simulation.ray.tasks import RayTaskService
from viva_api.simulation.simulation_service import SimulationService
from viva_core.backends.batch import (
    SUBMIT_JOB_MAX_ATTEMPTS,
    BatchJobDetail,
    SubmitJobPacer,
    batch_exit_code,
)

if TYPE_CHECKING:
    # ``types-boto3`` is a dev dependency (annotations only): never imported at runtime.
    pass

logger = logging.getLogger(__name__)

# The generic runner every Ray-Batch job (ensemble or compose) executes. Read once as a
# resource (same source viva_api.compose.simulation_service_ray stages for compose jobs) so
# the multi-generation batch path below dispatches through the identical mechanism instead
# of a v2ecoli-specific CLI script — see backlog items 26/27.
_RUNNER_SRC = (_res.files("viva_api.compose") / "run_pbg.py").read_text()

# Registered composite id (process_bigraph.composite_spec) for the multi-generation
# batch orchestrator, and the workspace core-builder that resolves its registered
# types (e.g. "inplace_dict"). Both are inherent facts about what THIS endpoint
# dispatches — this file already hardcodes v2ecoli-specific paths (V2ECOLI_DIR,
# PARCA_CACHE_DIR below); what item 27 removes is the bespoke EXECUTION MECHANISM (a
# CLI script), not this identity.
#
# The id is `f"{fn.__module__}.{name}"` (process_bigraph.composite_spec's own
# registration scheme, mirrored by pbg_superpowers.composite_generator). Two real
# pilot dispatches (2026-08-06) failed chasing wrong values for this constant before
# it was verified directly against the DEPLOYED sms-ecoli image (commit e38f742,
# `git show`/`git grep` against that exact commit — never the local v2ecoli
# checkout, a separate, structurally-diverged repo that is NOT a mirror of what's
# actually in this simulator image). At that commit the real module was
# v2ecoli/composites/batch_baseline.py (decorated function `batch_baseline`,
# name="batch_baseline") — this constant was correctly set to
# "v2ecoli.composites.batch_baseline.batch_baseline" and worked through build 62
# (commit 8d50ff0, item 1's real 1000x10 campaign).
#
# UPDATED 2026-08-16 (backlog item 55): sms-ecoli PR #56 (the sync that also
# carried item 52's wall-time fix) finally synced a v2ecoli upstream refactor that
# had been sitting unsynced since 2026-07-25 (v2ecoli #373, "Unify composites into
# baseline: knockouts + media + batch (n_seeds)") — it deleted
# composites/batch_baseline.py and folded its batch/lineage behavior into
# composites/ecoli_baseline.py's `baseline()` function (n_seeds/n_generations > 1
# switches it into what used to be the standalone batch_baseline composite).
# v2ecoli/composites/__init__.py deliberately registers NO legacy-id alias for the
# old name ("a stale `baseline` id resolving silently would only hide a missed
# reference") — so the old id now fails LOUDLY (confirmed via a real dispatch,
# sim 152, 2026-08-16: "no composite registered as
# 'v2ecoli.composites.batch_baseline.batch_baseline'"), exactly as its authors
# intended, rather than silently drifting. Re-verified the SAME way the 2026-08-06
# incident above did — `git show`/`git grep` directly against the real deployed
# commit (sms-ecoli c44b69a, build 63), never the separately-diverged local v2ecoli
# checkout. `baseline()`'s real signature (checked directly) is a strict superset
# of the old `batch_baseline` params EXCEPT one rename: `base_seed` -> `seed`.
V2ECOLI_BATCH_BASELINE_COMPOSITE_ID = "v2ecoli.composites.ecoli_baseline.ecoli_baseline"

# The SubmitJob pacer, its 50-TPS rationale and the DescribeJobs chunk size moved to
# ``viva_core.backends.batch`` with the rest of the Batch engine (docs/plan-core.md P2.1,
# cut 2). A canonical 1000-seed x 10-generation chain campaign submits N*G=10,000
# individual per-seed-per-generation jobs upfront (see ``submit_chain_dispatch_job``),
# which is why that loop is the pacer's main customer.


@dataclass
class ChainCampaignPollResult:
    """A chain-dispatch campaign's analysis-fan-in poll outcome — backlog item 33.

    ``terminal`` means every one of the campaign's tracked final-generation job
    ids (``HpcRun.chain_final_job_ids`` — one per seed, each seed's own LAST
    successfully-submitted generation job) has reached a Batch-terminal state
    (SUCCEEDED or FAILED); a campaign with any tracked job still
    SUBMITTED/PENDING/RUNNABLE/STARTING/RUNNING is NOT terminal and must be
    polled again next interval. A job id that hasn't shown up in ``describe_jobs``
    yet (e.g. brief eventual-consistency lag right after submission) is treated
    as not-yet-terminal, not an error — the caller just polls again.

    Unlike the per-generation-array design this superseded, there is no local
    array position to remap: each tracked id is already a real, independent AWS
    Batch job id (one seed's final chain link), so ``succeeded_job_ids`` /
    ``failed_job_ids`` are real job ids directly, usable as-is.
    """

    terminal: bool
    succeeded_job_ids: list[str] = field(default_factory=list)
    failed_job_ids: list[str] = field(default_factory=list)


class SimulationServiceRay(SimulationService):
    """Ray-on-Batch (MNP) implementation of SimulationService."""

    def __init__(
        self,
        local_task_service: LocalTaskService | None = None,
        k8s_job_service: "K8sJobService | None" = None,
        batch: RayBatchLayer | None = None,
    ) -> None:
        self._local = local_task_service or LocalTaskService()
        # Only the Nextflow dispatch uses this: its HEAD runs as a K8s Job so it
        # inherits the `batch-submit` ServiceAccount's IRSA identity. Every other
        # path here submits to Batch directly and needs no cluster access.
        self._k8s = k8s_job_service
        # The Batch layer, COMPOSED (it was this class's base until P2.1 PR 5). One instance
        # for the service's lifetime: everything that submits -- this class, the ParCa and
        # task services, soon the dispatch strategies -- is handed THIS object, so a test
        # that patches ``service.batch.submit_container`` is what all of them get.
        self.batch = batch or RayBatchLayer()

    def get_batch_job_statuses(self, job_ids: list[str]) -> dict[str, JobStatus]:
        """``self.batch.get_batch_job_statuses``, kept on the service because the scheduler
        asks the SERVICE for it (that ends in P6, with the scheduler's split)."""
        return self.batch.get_batch_job_statuses(job_ids)

    def get_batch_job_details(self, job_ids: list[str]) -> dict[str, BatchJobDetail]:
        """``self.batch.get_batch_job_details`` -- the scheduler and the chain-progress handler
        ask the SERVICE for it; see ``get_batch_job_statuses``."""
        return self.batch.get_batch_job_details(job_ids)

    async def stage_runner(self, experiment_id: str) -> str:
        """Upload the generic run_pbg.py runner to S3 for this experiment; return its URI.

        Mirrors ``viva_api.compose.simulation_service_ray.ComposeSimulationServiceRay``'s
        own runner staging exactly (same source, same per-experiment S3 layout) -- the
        multi-generation batch path below downloads and runs it the identical way a
        compose job does. Staged (not embedded via heredoc) because AWS Batch caps a
        container override command at 8192 bytes.

        The returned URI is fully DETERMINISTIC from ``experiment_id`` alone (the S3
        key has no random component) — safe, cheap, and idempotent to call again
        rather than cache: ``JobScheduler``'s per-tick chain-dispatch advance
        (backlog item 71 Phase 4) calls this fresh whenever it's about to submit a
        generation, rather than persisting the URI anywhere.
        """
        from viva_api.dependencies import get_file_service

        file_service = get_file_service()
        if file_service is None:
            raise RuntimeError("FileService not initialized; cannot stage run_pbg.py to S3.")
        exp_prefix = data_layout.RayLayout.experiment_prefix(experiment_id)
        runner_key = f"{exp_prefix}/run_pbg.py"
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
            tmp.write(_RUNNER_SRC)
            runner_local = tmp.name
        try:
            await file_service.upload_file(Path(runner_local), S3FilePath(s3_path=Path(runner_key)))
        finally:
            Path(runner_local).unlink(missing_ok=True)
        return data_layout.s3_uri(runner_key)

    def _sim_command(
        self,
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

    def _seed_generation_command(
        self,
        *,
        seed: int,
        generation_index: int,
        experiment_id: str,
        runner_s3_uri: str,
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
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

    def _seed_lineage_command(
        self,
        *,
        seed: int,
        n_generations: int,
        experiment_id: str,
        runner_s3_uri: str,
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
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
        overrides: dict[str, Any] = {
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
                self.batch.submit_container,
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

    @property
    def parca(self) -> RayParcaService:
        """The ParCa cache jobs (a commit's cache, a new-gene cache, a variant cache), composed:
        handed this service as the thing that dispatches container jobs for them. Built per
        access; it holds no state of its own."""
        return RayParcaService(self.batch)

    @override
    async def submit_parca_job(self, parca_dataset: ParcaDataset) -> JobId:
        """``SimulationService``'s contract; the work is ``RayParcaService``'s."""
        return await self.parca.submit_parca_job(parca_dataset)

    def cache_s3_uri(self, commit: str, *, variant: str | None = None) -> str:
        """Where a commit's ParCa cache lives -- ``parca_spec.cache_s3_uri``, kept on the
        service because the scheduler and the handlers ask the SERVICE for it."""
        return parca_spec.cache_s3_uri(commit, variant=variant)

    @property
    def tasks(self) -> RayTaskService:
        """The task service (``POST /api/v1/tasks`` and friends), composed: it is handed this
        service as the thing that dispatches container jobs for it. Built per access; it
        holds no state of its own."""
        return RayTaskService(
            self.batch,
            latest_commit=lambda: self.get_latest_commit_hash(),
            results_uri=data_layout.RayLayout.results_uri,
        )

    def _nextflow(self) -> NextflowStrategy:
        """The Nextflow dispatch mechanism, as a strategy. Built per call around ``self._k8s`` and a
        late-bound ``stage_runner``, so a test (or anything else) that swaps either is what it gets."""
        return NextflowStrategy(
            self.batch, self._k8s, stage_runner=lambda experiment_id: self.stage_runner(experiment_id)
        )

    async def reap_cancelled_campaign(self, head_job_name: str) -> int | None:
        """``NextflowStrategy.reap_cancelled_campaign``, kept on the service because the scheduler
        asks the SERVICE for it (that ends in P6, with the scheduler's split)."""
        return await self._nextflow().reap_cancelled_campaign(head_job_name)

    def _multi_node(self) -> MultiNodeCompositeStrategy:
        """The multi-node composite dispatch mechanism, as a strategy handed this service's Batch layer
        and a late-bound ``stage_runner``. Built per call; it holds no state of its own."""
        return MultiNodeCompositeStrategy(
            self.batch, stage_runner=lambda experiment_id: self.stage_runner(experiment_id)
        )

    async def submit_multi_node_analysis(
        self,
        *,
        simulation: Simulation,
        database_service: DatabaseService,
        commit: str,
        composite_id: str,
    ) -> str | None:
        """``MultiNodeCompositeStrategy.submit_analysis``, kept on the service because the scheduler
        asks the SERVICE for it (that ends in P6, with the scheduler's split)."""
        return await self._multi_node().submit_analysis(
            simulation=simulation, database_service=database_service, commit=commit, composite_id=composite_id
        )

    def _mbp_tracked(self) -> MbpTrackedStrategy:
        """The mbp-tracked dispatch mechanism, as a strategy handed this service's Batch layer.
        Built per call; it holds no state of its own."""
        return MbpTrackedStrategy(self.batch)

    def _image_builder(self) -> RayImageBuilder:
        """The build service, composed. Built per call around ``self._local`` so that a test
        (or anything else) that swaps the local task service is what the builder gets --
        the same late binding as ``_batch_jobs``."""
        return RayImageBuilder(self._local)

    @override
    async def submit_build_image_job(
        self,
        simulator_version: SimulatorVersion,
        *,
        include_new_gene_data: bool = False,
        include_submit_image: bool = False,
        stage_private_fork: bool = False,
        vecoli_private_commit: str | None = None,
    ) -> JobId:
        """Build the simulator image (``RayImageBuilder.submit``). The keyword arguments are
        spelled out, not ``**kwargs``: ``handlers.simulators`` inspects this signature to
        decide which of them this backend supports."""
        return await self._image_builder().submit(
            simulator_version,
            include_new_gene_data=include_new_gene_data,
            include_submit_image=include_submit_image,
            stage_private_fork=stage_private_fork,
            vecoli_private_commit=vecoli_private_commit,
        )

    @override
    async def get_latest_commit_hash(
        self,
        git_repo_url: str = DEFAULT_REPO,
        git_branch: str = DEFAULT_BRANCH,
    ) -> str:
        return await fetch_latest_commit_hash(git_repo_url, git_branch, _seams.get_settings().github_token)

    @override
    async def submit_ecoli_simulation_job(
        self, ecoli_simulation: Simulation, database_service: DatabaseService, correlation_id: str
    ) -> JobId:
        """Submit ParCa (1 node) + the simulation ensemble (N nodes), gated by a Batch dependency.

        The tracked job id is the *simulation* job. Batch will not start it until
        the ParCa job SUCCEEDED, so the cache is in S3 before the sim stages it.

        ROUTING (backlog item 33 rework): the canonical batch_baseline sweep
        (``composite`` is None, more than one generation) is delegated ENTIRELY
        to ``submit_chain_dispatch_job`` — individual per-seed AWS Batch job
        chains, a true v2 analogy of vEcoli-private's own fully-asynchronous
        per-seed Nextflow execution (Alex's explicit decision). This check runs
        BEFORE any of the MNP/single-shot setup below, so a canonical request
        never touches that setup, and never needs Array-job machinery at all —
        ``_submit_array``/``_array_sim_command``/``_ensure_array_job_def`` were
        REMOVED as part of this rework once this routing landed made them dead
        code (their only caller was the array branch this replaced; confirmed
        via a fresh repo-wide grep before deleting them, not assumed).

        That delegation runs in the BACKGROUND (via
        ``_submit_chain_dispatch_background``), so this method returns in
        seconds no matter how large the campaign is, and the returned id is a
        ``JobId.local(...)`` rather than the ParCa job's ``JobId.ray(...)``.
        Submitting a campaign's N*G jobs takes minutes of real wall time
        (~15 for the canonical 1000x10 shape) and used to happen inline, inside
        the ``POST /api/v1/simulations`` request — see that method's docstring
        for the real production failure that caused. Progress stays trackable
        through the unchanged ``GET /api/v1/simulations/{id}/status``.
        ``submit_chain_dispatch_job`` itself is untouched and still runs
        synchronously to completion for its direct callers.

        Every OTHER shape still reaches the MNP path below exactly as before:
        the composite-driven two-engine comparison ensemble (genuinely fans out
        via Ray actors, at ANY generation count — chain-dispatch is v2ecoli-only
        and does not apply), and the single-generation phase0 ensemble.
        """
        if database_service is None:
            raise RuntimeError("DatabaseService is not available. Cannot submit Ray simulation job.")

        config = ecoli_simulation.config

        # Backlog item 88: a generic multi-node process-bigraph composite dispatch
        # (e.g. a colony composite distributed across N Ray-cluster nodes) arrives
        # as an extra key (SimulationConfig's extra="allow") rather than a declared
        # field -- checked FIRST, before the chain-dispatch routing below, since a
        # multi-node request may otherwise also satisfy that check's own
        # composite-is-None/generations>1 condition and would silently misroute.
        # A Nextflow dispatch is checked BEFORE multi_node_dispatch for the same
        # reason that one is checked before chain-dispatch: it carries a
        # composite_id too, so a later check would silently claim it first and the
        # request would run on the wrong mechanism while looking like it worked.
        #
        # Deliberately a per-request extra rather than a new ComputeBackend:
        # compute_backend_for_repo maps repo -> backend, so a NEXTFLOW member would
        # either reroute every v2ecoli request or be dead configuration. This is the
        # same axis multi_node_dispatch already uses to pick pbg-native over
        # chain-dispatch for the same repo, same image, one config field apart.
        nf_dispatch = getattr(config, "nextflow_dispatch", None)
        if nf_dispatch is not None:
            return await self._nextflow().submit(
                ecoli_simulation, database_service, nf_dispatch, correlation_id=correlation_id
            )

        # Run 1's real missing-output fix (Alex's Option 1 decision, 2026-09-06):
        # a run_mbp_tracked.py variant dispatch, e.g. reactor_bird_coupled. Checked
        # here for the same reason nextflow_dispatch/multi_node_dispatch are --
        # it carries no composite_id/generations shape that would otherwise
        # satisfy a different branch's own routing condition, but checking early
        # keeps every extra-field dispatch shape's own precedence explicit rather
        # than incidental.
        mbp_dispatch = getattr(config, "mbp_dispatch", None)
        if mbp_dispatch is not None:
            return await self._mbp_tracked().submit(
                ecoli_simulation, database_service, mbp_dispatch, correlation_id=correlation_id
            )

        mnp_dispatch = getattr(config, "multi_node_dispatch", None)
        if mnp_dispatch is not None:
            return await self._multi_node().submit(
                ecoli_simulation, database_service, mnp_dispatch, correlation_id=correlation_id
            )

        composite = getattr(config, "composite", None)
        # config.generations is a real (non-"extra") SimulationConfig field, unlike
        # the comparison knobs read further below -- read directly, not via getattr.
        n_generations = int(config.generations or 1)
        if composite is None and n_generations > 1:
            return await self._submit_chain_dispatch_background(
                ecoli_simulation, database_service, correlation_id=correlation_id
            )

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
        job_def = self.batch.ensure_mnp_job_def(self.batch.image_uri(commit), commit)

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
            else self.cache_s3_uri(commit, variant=cache_variant)
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
        runner_s3_uri = await self.stage_runner(experiment_id) if n_generations > 1 else None

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

        parca_job_id = self.batch.submit_mnp(
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
        sim_job_id = self.batch.submit_mnp(
            job_name=f"ray-sim-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            num_nodes=settings.ray_num_nodes,
            ray_job_cmd=self._sim_command(
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

    def chain_base_tags(self, *, simulation: Simulation, commit: str) -> dict[str, str]:
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
        batch_client: Any = None,
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
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
        job_def = self.batch.ensure_container_job_def(self.batch.image_uri(commit), commit)
        return self.batch.submit_container(
            job_name=f"chain-seed{seed}-gen{generation_index}-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            job_cmd=self._seed_generation_command(
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
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
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
        batch_client: Any = None,
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
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
        job_def = self.batch.ensure_container_job_def(self.batch.image_uri(commit), commit)
        return self.batch.submit_container(
            job_name=f"chain-seed{seed}-lineage-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            job_cmd=self._seed_lineage_command(
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
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
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

    async def _submit_chain_dispatch_background(
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
        cache_s3 = self.cache_s3_uri(commit)
        base_tags = self.chain_base_tags(simulation=ecoli_simulation, commit=commit)
        container_job_def = self.batch.ensure_container_job_def(self.batch.image_uri(commit), commit)

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
        parca_job_id = self.batch.submit_container(
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
        base_tags = self.chain_base_tags(simulation=simulation, commit=commit)
        # Backlog item 71: _submit_analysis_job now submits via _submit_container,
        # so this must resolve a container job def, not an MNP one.
        container_job_def = self.batch.ensure_container_job_def(self.batch.image_uri(commit), commit)
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

    @override
    async def read_config_template(
        self,
        simulator_version: SimulatorVersion,
        config_filename: str,
        allow_default_fallback: bool = False,
    ) -> str:
        return await fetch_config_template(
            simulator_version, config_filename, _seams.get_settings().github_token, allow_default_fallback
        )

    @override
    async def discover_repo_contents(self, simulator_version: SimulatorVersion) -> RepoDiscovery:
        return await fetch_repo_discovery(simulator_version, _seams.get_settings().github_token)

    @override
    async def get_job_status(self, job_id: JobId) -> JobStatusInfo | None:
        """Status — LOCAL, a K8s-hosted Nextflow head, or AWS Batch describe_jobs."""
        if job_id.backend == JobBackend.LOCAL:
            return self._local.get_status(job_id.value)
        if job_id.backend == JobBackend.K8S_NEXTFLOW:
            if self._k8s is None:
                raise RuntimeError(
                    f"job {job_id.value} is a K8s-hosted Nextflow head, but this service has no "
                    f"K8sJobService (k8s_job_namespace unset)"
                )
            # The value is a Job NAME, not a Batch job id -- describe_jobs would
            # return an empty list and this would report None rather than fail.
            return self._k8s.get_job_status(job_id.value)

        job = self.batch.engine().describe_job(job_id.value)
        if job is None:
            logger.warning("No Batch job found with id %s", job_id.value)
            return None
        status = JobStatus.from_batch_state(job.get("status", ""))
        started = job.get("startedAt")
        stopped = job.get("stoppedAt")
        return JobStatusInfo(
            job_id=job_id,
            status=status,
            start_time=str(started) if started else None,
            end_time=str(stopped) if stopped else None,
            exit_code=batch_exit_code(job),
            error_message=job.get("statusReason") if status == JobStatus.FAILED else None,
        )

    def get_chain_campaign_result(self, job_ids: list[str]) -> ChainCampaignPollResult:
        """Poll a chain-dispatch campaign's tracked final-generation job ids —
        one per seed, each seed's own last successfully-submitted generation
        job (``HpcRun.chain_final_job_ids``) — for the analysis-fan-in
        condition (backlog item 33 rework, replacing the per-generation-array
        "wave" design's own single-array-job ``get_wave_result``).

        "Terminal" means EVERY tracked job has reached a Batch-terminal state
        (SUCCEEDED or FAILED, via the same ``JobStatus.from_batch_state``
        mapping ``get_job_status`` already uses) — a seed's chain ending in
        FAILED (that job's own retries exhausted, or an earlier generation in
        its chain having failed and auto-propagated via Batch's own dependsOn)
        is expected economics, not an orchestrator error; the caller
        (``JobScheduler._advance_chain_campaign``) decides whether the
        campaign as a whole produced anything worth analyzing. A tracked id
        that hasn't appeared in a ``describe_jobs`` response yet (brief
        eventual-consistency lag right after submission) is treated as
        not-yet-terminal, not a hard failure — this poller runs on an
        interval, it just checks again next time.

        ``describe_jobs`` accepts at most 100 job ids per call (verified
        against the real API model this session) — a campaign's up to 1000
        tracked ids are chunked accordingly, unlike the array-job design this
        superseded (which polled ONE array parent's own
        ``arrayProperties.statusSummary`` plus paginated ``list_jobs`` calls).
        """
        if not job_ids:
            # Nothing tracked at all -- every seed failed even generation 0's
            # submission. Trivially "terminal" (nothing left to wait for); the
            # caller's own zero-succeeded handling covers marking the campaign
            # FAILED without submitting an analysis over an empty sweep.
            return ChainCampaignPollResult(terminal=True)

        statuses = self.get_batch_job_statuses(job_ids)
        succeeded = [jid for jid in job_ids if statuses.get(jid) == JobStatus.COMPLETED]
        failed = [jid for jid in job_ids if statuses.get(jid) == JobStatus.FAILED]
        # A missing id (not in `statuses` at all) or one still queued/running is
        # simply neither succeeded nor failed above -- either way, not yet terminal.

        if len(succeeded) + len(failed) < len(job_ids):
            return ChainCampaignPollResult(terminal=False)
        return ChainCampaignPollResult(terminal=True, succeeded_job_ids=succeeded, failed_job_ids=failed)

    @override
    async def cancel_job(self, job_id: JobId) -> None:
        """Cancel — LOCAL task or AWS Batch terminate_job (also kills child MNP nodes)."""
        if job_id.backend == JobBackend.LOCAL:
            self._local.cancel(job_id.value)
            logger.info("Cancelled local task %s", job_id.value)
            return
        if job_id.backend == JobBackend.K8S_NEXTFLOW:
            if self._k8s is None:
                raise RuntimeError(f"cannot cancel {job_id.value}: no K8sJobService configured")
            # Deleting the Job SIGTERMs the head, which asks Nextflow to shut
            # down; its hook then terminates the tasks it submitted. That is the
            # correct mechanism -- Nextflow is the only party that knows exactly
            # which tasks belong to this run -- and the pod now gets
            # NF_HEAD_TERMINATION_GRACE_SECONDS to do it.
            #
            # But it was OBSERVED not to happen (viva-api#472): on simulation 441,
            # 8 of 10 lineage tasks outlived the cancel by ~100 minutes and filled
            # host disk until a later campaign started failing. A cancel that
            # leaves work running is worse than one that refuses, because the
            # operator believes the resources are free. So we verify rather than
            # assume, and say what was actually stopped.
            #
            # The reap that guarantees it is NOT here any more. Done inline it was
            # synchronous inside the cancel request (a pod restart mid-cancel lost
            # the intent), scanned one queue, did not paginate, and -- worst --
            # ran while the head was still in its grace period, so Nextflow saw
            # each termination as a task failure and RESUBMITTED it (measured:
            # terminating 10 tasks produced 9 fresh jobs). The handler writes
            # CANCELLED after this returns; ``JobScheduler``'s
            # ``reconcile_cancelled_nextflow_campaigns`` then reaps from that
            # row every tick, only once the head is actually gone.
            self._k8s.delete_job(job_id.value)
            logger.info("Deleted Nextflow head Job %s", job_id.value)
            return
        self.batch.engine().terminate(job_id.value, reason="cancelled via sms-api")
        logger.info("Terminated Ray Batch job %s", job_id.value)

    async def cancel_companion_jobs(self, hpc_run: HpcRun) -> int:
        """Terminate the Batch jobs a run owns besides its tracked one; returns how many.

        Called BEFORE the tracked job is cancelled: that job depends on these, and Batch keeps
        a terminated job PENDING until its dependency ends -- so stopping the dependency
        first is also what lets the tracked job leave the queue.

        Only a Batch-backed run has companions of this kind. A LOCAL row's
        ``external_job_ids`` are its build's jobs, which ``LocalTaskService`` owns.
        """
        if hpc_run.job_id.backend != JobBackend.RAY:
            return 0
        companions = [j for j in hpc_run.external_job_ids or [] if j and j != hpc_run.job_id.value]
        for companion in companions:
            await self.cancel_job(JobId.ray(companion))
        return len(companions)

    async def cancel_chain_campaign(self, campaign: HpcRun) -> None:
        """Cancel every seed's current in-flight job for a chain-dispatch
        campaign (backlog item 71 Phase 4, folding in backlog item 53's
        cancellation design). Simpler than item 53's original walk-back-through-
        dependsOn proposal: under the per-seed app-level-gated model there is at
        most ONE in-flight job per seed at any time, directly readable from
        ``chain_current_job_ids`` — no dependsOn chain to walk. Reuses
        ``cancel_job``'s existing ``terminate_job`` call unchanged, which item
        53's own empirical testing already validated works correctly across
        every non-terminal Batch state (RUNNING, RUNNABLE, PENDING) — no
        state-dependent branching needed. Idempotent: a seed whose chain
        already resolved (its ``chain_current_job_ids`` entry already ``None``)
        is simply skipped.

        This only terminates the AWS-side jobs — writing the campaign's own
        CANCELLED status is the caller's responsibility (mirrors the existing
        single-job ``cancel_job``/``cancel_simulation`` split: this service
        talks to AWS, the handler owns the DB write).
        """
        # The campaign's OWN job id is its ParCa job, and until ParCa succeeds no seed has a
        # current job at all -- so a campaign cancelled in that phase terminated nothing and
        # left ParCa running under a CANCELLED row (viva-api#709). Terminating a job that has
        # already finished is accepted by Batch, so there is no state to check first.
        if not campaign.chain_parca_done and campaign.job_id.backend == JobBackend.RAY:
            await self.cancel_job(campaign.job_id)
        for job_id in campaign.chain_current_job_ids or []:
            if job_id is None:
                continue
            await self.cancel_job(JobId.ray(job_id))

    @override
    async def close(self) -> None:
        pass
