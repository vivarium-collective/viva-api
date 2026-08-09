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
import copy
import importlib.resources as _res
import json
import logging
import random
import shlex
import string
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, override

import boto3
from botocore.config import Config
from pydantic import BaseModel

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.hpc.local_task_service import LocalTaskService
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.common.simulator_defaults import DEFAULT_BRANCH, DEFAULT_REPO
from viva_api.common.storage import data_layout
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.config import get_settings
from viva_api.simulation import batch_build
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.github_repo import (
    fetch_config_template,
    fetch_latest_commit_hash,
    fetch_repo_discovery,
)
from viva_api.simulation.models import (
    CompositeEngine,
    JobType,
    ParcaDataset,
    RepoDiscovery,
    Simulation,
    SimulatorVersion,
    VecoliSource,
)
from viva_api.simulation.simulation_service import SimulationService
from viva_api.simulation.tables_orm import AnalysisStatusDB

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
# actually in this simulator image):
#   1st: "v2ecoli.composites.ecoli_baseline" — missing the id scheme's trailing
#     name-repeat.
#   2nd: "...ecoli_baseline.ecoli_baseline" — correctly SHAPED, but sms-ecoli has
#     no "ecoli_baseline" module at all (zero matches anywhere in that repo at the
#     deployed commit) — it was chasing a module name from a different codebase.
# The real module is v2ecoli/composites/batch_baseline.py: decorated function
# `batch_baseline`, name="batch_baseline". Its declared parameters (n_seeds,
# n_generations, cache_dir, out_dir, experiment_id, analyses, parallel) match the
# `overrides` dict below exactly. sms-ecoli's separate baseline.py is single-run
# only (no n_seeds/n_generations param at all) — not a candidate for this path.
V2ECOLI_BATCH_BASELINE_COMPOSITE_ID = "v2ecoli.composites.batch_baseline.batch_baseline"
V2ECOLI_CORE_BUILDER = "v2ecoli.core:build_core"

# Absolute paths inside the v2ecoli Ray image (WORKDIR=/app/v2ecoli). The
# entrypoint runs RAY_JOB_CMD on the head; v2ecoli reads the cache from
# CACHE_DIR and writes the ensemble outputs under OUT_DIR.
V2ECOLI_DIR = "/app/v2ecoli"
PARCA_CACHE_DIR = f"{V2ECOLI_DIR}/out/cache"
PARCA_SIMDATA_DIR = f"{V2ECOLI_DIR}/out/sim_data"
SIM_OUT_DIR = f"{V2ECOLI_DIR}/.pbg/runs/phase0-xarray"
# The analysis DAG node writes its outputs straight to S3 (see _analysis_command),
# so this local dir normally never exists and the entrypoint's RAY_OUT_DIR sync is a
# documented no-op ("no <dir>; nothing to upload"). It is still declared so anything
# the analysis does drop locally lands under the run's own S3 prefix.
ANALYSIS_OUT_DIR = f"{V2ECOLI_DIR}/.pbg/runs/analysis"
# Where the head writes the entrypoint's metrics report (uploaded as report.json).
REPORT_PATH = "/tmp/report.json"  # noqa: S108

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

# ── Chain-dispatch campaign submission (backlog item 33) ────────────────────
#
# AWS Batch's SubmitJob is capped at 50 TPS per account, fixed -- not
# adjustable via a quota increase (AWS Batch service quotas, verified this
# session). A canonical 1000-seed x 10-generation campaign submits N*G=10,000
# individual per-seed-per-generation jobs upfront (see
# ``submit_chain_dispatch_job``), so that loop must stay safely under the cap.
_SUBMIT_JOB_SAFE_RATE = 40.0  # jobs/sec; headroom below the 50 TPS account cap
#                               for other concurrent Batch traffic in the same
#                               account (ParCa/analysis jobs, other campaigns).
_SUBMIT_JOB_MAX_ATTEMPTS = 5  # botocore "standard" retry attempts per submit_job
#                               call, for whatever transient/throttling errors
#                               proactive pacing alone doesn't fully prevent.
# Per-generation-job retry, matching the (Spot-tolerant) Array job definition's
# own already-tuned ``arrayRetryAttempts`` default (sms-cdk/lib/ray-batch-
# stack.ts) -- restores the "checkpoint/resume via the job's own retry" property
# the per-seed chain design assumes, on the MNP job definition that per-seed
# jobs actually submit through (see ``_seed_generation_command``'s module-level
# docstring note for why MNP, not Array, and the cost tradeoff that leaves open).
_CHAIN_JOB_RETRY_STRATEGY = {"attempts": 2}
# AWS Batch DescribeJobs accepts at most 100 job ids per call (verified against
# the real API model this session) -- the analysis-fan-in poller must chunk a
# campaign's up-to-1000 tracked job ids into batches this size.
_DESCRIBE_JOBS_MAX_BATCH = 100


def _rand_suffix() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


class _SubmitJobPacer:
    """Proactive client-side pacer for AWS Batch SubmitJob.

    Caps outbound ``submit_job`` calls to ``max_per_second``, computed from
    REAL elapsed wall-clock time since the previous call (not a fixed
    per-call sleep, which either over-throttles once call latency is added on
    top, or under-throttles if the guessed interval is even slightly off).
    Deliberately proactive rather than reactive: botocore's own "adaptive"
    retry mode only starts throttling client-side AFTER it has already
    observed a real throttling response, so pacing every call up front is
    what keeps a fresh several-thousand-call burst from front-loading
    avoidable 429s in the first place. This pacer and the "standard" retry
    mode configured on the submitting client (see
    ``submit_chain_dispatch_job``) are complementary, not redundant: this
    caps the steady-state rate; retry-on-throttle is the backstop for
    whatever pacing alone doesn't prevent (concurrent campaigns, other Batch
    traffic in the same account -- the 50 TPS cap is account-wide, not
    per-campaign).
    """

    def __init__(self, max_per_second: float = _SUBMIT_JOB_SAFE_RATE) -> None:
        self._min_interval = 1.0 / max_per_second
        self._last_call_at: float | None = None

    async def wait(self) -> None:
        now = time.monotonic()
        if self._last_call_at is not None:
            deficit = self._min_interval - (now - self._last_call_at)
            if deficit > 0:
                await asyncio.sleep(deficit)
                now = time.monotonic()
        self._last_call_at = now


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


def _is_upstream_vecoli(composite: CompositeEngine | None) -> bool:
    """The pristine upstream-vEcoli engine (``--composite vecoli``).

    The single source of truth for the routing question "does this run need the
    separate upstream ParCa cache + ``--vecoli-source`` flag?" — used both to
    select the ParCa cache/command in ``submit_ecoli_simulation_job`` and to gate
    the ``--vecoli-source`` arg in ``_sim_command``.
    """
    return composite == "vecoli"


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

    def __init__(self, local_task_service: LocalTaskService | None = None) -> None:
        self._local = local_task_service or LocalTaskService()

    def _batch(self) -> Any:
        return boto3.client("batch", region_name=get_settings().batch_region)

    def _cache_s3_uri(self, commit: str) -> str:
        """Deterministic S3 URI for a commit's v2ecoli ParCa cache.

        Both the ParCa job (writes here) and the simulation job (stages from
        here) derive the same URI, so the cache hand-off needs no runtime wiring.
        """
        return data_layout.RayLayout.parca_cache_uri(commit)

    def _upstream_cache_s3_uri(self, commit: str) -> str:
        """S3 URI for the PRISTINE upstream-vEcoli ParCa cache (``--composite vecoli``).

        Kept SEPARATE from ``_cache_s3_uri`` (the v2ecoli cache): the external
        upstream wrapper needs an UPSTREAM-MASTER-built ``simData.cPickle``, not
        the v2ecoli one (whose TCS ``modified_molecules`` skew makes upstream's
        two-component-system ODE go negative). Keyed by the same image commit so
        both engines' parca→sim hand-offs derive their URI with no runtime wiring.
        """
        return data_layout.RayLayout.parca_cache_uri(commit, upstream=True)

    def _results_s3_uri(self, experiment_id: str) -> str:
        return data_layout.RayLayout.results_uri(experiment_id)

    def _image_uri(self, commit: str) -> str:
        """The TRUE commit image for a run: <account>.dkr.ecr.<region>/v2ecoli:<commit>."""
        settings = get_settings()
        registry = f"{settings.ecr_account_id}.dkr.ecr.{settings.batch_region}.amazonaws.com"
        return f"{registry}/{settings.ray_ecr_repository}:{commit}"

    def _ensure_mnp_job_def(self, image: str, commit: str) -> str:
        """Return an MNP job definition (name:revision) whose image is the commit's image.

        Batch MNP can't override the image per-submission, so — symmetric with how K8s
        sets the image per-Job — we derive a per-commit job-def revision: describe the
        CDK base job def (``ray_mnp_job_definition``: roles, resources, shm, log config,
        node count), swap ONLY every node range's container image to ``image``, and
        register it as ``<base>-<commit>``. An existing active revision already pointing
        at this image is reused, so resubmits don't churn revisions.
        """
        settings = get_settings()
        batch = self._batch()
        name = f"{settings.ray_mnp_job_definition}-{commit}"

        # Reuse an existing active revision that already targets this exact image.
        existing = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
        for jd in existing.get("jobDefinitions", []):
            images = {
                nr.get("container", {}).get("image")
                for nr in jd.get("nodeProperties", {}).get("nodeRangeProperties", [])
            }
            if images == {image}:
                return f"{name}:{jd['revision']}"

        # Otherwise clone the base job def's node properties and swap the image.
        base = batch.describe_job_definitions(jobDefinitionName=settings.ray_mnp_job_definition, status="ACTIVE")
        base_defs = base.get("jobDefinitions", [])
        if not base_defs:
            raise RuntimeError(f"Base Ray MNP job definition {settings.ray_mnp_job_definition!r} not found")
        node_properties = copy.deepcopy(max(base_defs, key=lambda d: d["revision"])["nodeProperties"])
        for nr in node_properties.get("nodeRangeProperties", []):
            nr.setdefault("container", {})["image"] = image

        response = batch.register_job_definition(
            jobDefinitionName=name,
            type="multinode",
            nodeProperties=node_properties,
        )
        logger.info("Registered Ray MNP job def %s:%s for image %s", name, response["revision"], image)
        return f"{name}:{response['revision']}"

    def _submit_mnp(
        self,
        *,
        job_name: str,
        job_definition: str,
        num_nodes: int,
        ray_job_cmd: str,
        out_s3: str,
        out_dir: str,
        stage_s3: str | None = None,
        stage_dir: str | None = None,
        depends_on: list[str] | None = None,
        depends_type: str | None = "SEQUENTIAL",
        tags: dict[str, str] | None = None,
        retry_strategy: dict[str, Any] | None = None,
        batch_client: Any = None,
    ) -> str:
        """Submit a Ray MNP job via boto3, mirroring sms-cdk scripts/ray_batch_submit.sh.

        Env targeting matters: the entrypoint runs ``stage_inputs`` and the periodic
        output sync on EVERY node, so the staging/output/log knobs must reach all
        nodes — the workers need the ParCa cache to run seeds and must ship their own
        zarr to S3. Only ``RAY_JOB_CMD`` (the driver) and ``RAY_REPORT_PATH`` are
        head-only. So the shared env goes on node 0 (``0:0``) and, when there are
        workers, also on the worker range (``1:``). Returns the AWS Batch job id.

        ``depends_type`` selects the ``dependsOn`` shape. The default keeps the
        long-standing ParCa→sim edge byte-identical (``{"jobId": …, "type":
        "SEQUENTIAL"}``, live-verified). Pass ``None`` for a plain ``{"jobId": …}``
        wait — required when the DEPENDENCY is an Array job, whose parent id AWS
        Batch will not accept under a SEQUENTIAL type (real API rejection, hit live
        2026-08-06; see ``_submit_array``).

        ``retry_strategy``, passed through verbatim as ``SubmitJob.retryStrategy``,
        overrides whatever the job definition itself declares (per the real AWS
        Batch API — confirmed this session) — used by the per-seed chain-dispatch
        path (backlog item 33) to restore per-job retry on the MNP job definition,
        which (unlike the Array job definition) declares none of its own; omitted
        (``None``) everywhere else, unchanged from existing behavior.

        ``batch_client``, when given, is used INSTEAD of ``self._batch()`` for this
        one call — lets a caller submitting many jobs in a tight loop (chain
        dispatch) supply its own retry-configured client without changing what
        every other existing call site in this class gets from the shared
        ``self._batch()`` factory.
        """
        settings = get_settings()
        # Per-node knobs every node acts on (stage cache in, sync results out, ship logs).
        shared_env: list[dict[str, str]] = [
            {"name": "RAY_OUT_DIR", "value": out_dir},
            {"name": "RAY_OUT_S3", "value": out_s3},
        ]
        if stage_s3 and stage_dir:
            shared_env.append({"name": "RAY_STAGE_S3", "value": stage_s3})
            shared_env.append({"name": "RAY_STAGE_DIR", "value": stage_dir})
        if settings.ray_log_s3_prefix:
            shared_env.append({"name": "RAY_LOG_S3_PREFIX", "value": settings.ray_log_s3_prefix})

        # The head additionally runs the workload (RAY_JOB_CMD) and writes the report.
        # Workers receive these too but never act on them — the entrypoint branches on
        # AWS_BATCH_JOB_NODE_INDEX and only the head executes RAY_JOB_CMD/writes the report.
        head_env: list[dict[str, str]] = [
            {"name": "RAY_JOB_CMD", "value": ray_job_cmd},
            {"name": "RAY_REPORT_PATH", "value": REPORT_PATH},
            *shared_env,
        ]

        # The CDK base job definition declares a SINGLE node range ("0:") — the entrypoint
        # self-branches head vs. worker — so the submit-time override must target that same
        # range. (Splitting into "0:0"/"1:" makes Batch reject: "NodeOverride targets should
        # match job definition".) One override on "0:" with the full env reaches every node;
        # the per-node staging/output knobs in shared_env are what workers need.
        node_property_overrides: list[dict[str, Any]] = [
            {"targetNodes": "0:", "containerOverrides": {"environment": head_env}},
        ]

        node_overrides: dict[str, Any] = {
            "numNodes": num_nodes,
            "nodePropertyOverrides": node_property_overrides,
        }
        kwargs: dict[str, Any] = {
            "jobName": job_name,
            "jobQueue": settings.ray_mnp_queue,
            "jobDefinition": job_definition,
            "nodeOverrides": node_overrides,
        }
        if depends_on:
            kwargs["dependsOn"] = [
                ({"jobId": jid, "type": depends_type} if depends_type else {"jobId": jid}) for jid in depends_on
            ]
        if tags:
            # Cost-allocation tags: propagate to the underlying ECS tasks so the
            # payer account's Cost Explorer can attribute compute per run/engine.
            kwargs["tags"] = tags
            kwargs["propagateTags"] = True
        if retry_strategy:
            kwargs["retryStrategy"] = retry_strategy

        batch = batch_client if batch_client is not None else self._batch()
        response = batch.submit_job(**kwargs)
        batch_job_id = str(response["jobId"])
        logger.info(
            "Submitted Ray MNP job %s (id=%s, nodes=%d) to %s",
            job_name,
            batch_job_id,
            num_nodes,
            settings.ray_mnp_queue,
        )
        return batch_job_id

    def _parca_command(self) -> str:
        """Run ParCa, then hydrate the sim-input bundle into PARCA_CACHE_DIR (out/cache).

        v2ecoli's sim loads ``out/cache/{initial_state.json, sim_data_cache.dill, ...}`` via
        ``build_composite(cache_dir=out/cache)``. ``v2ecoli-parca`` only emits the raw
        ``parca_state.pkl`` (+ a Km cache), so ``scripts/build_cache.py`` must hydrate that
        into the bundle. build_cache.py/load_parca_state read a GZIPPED fixture, so gzip the
        parca output first (the round-trip bridges v2ecoli's .pkl→.pkl.gz mismatch). Only
        PARCA_CACHE_DIR is synced to S3 (RAY_OUT_DIR), and that is exactly what the sim stages.
        """
        settings = get_settings()
        return (
            f"cd {V2ECOLI_DIR}"
            f" && v2ecoli-parca --mode {settings.ray_parca_mode} --cpus {settings.ray_parca_cpus}"
            f" -o {PARCA_SIMDATA_DIR} --cache-dir {PARCA_CACHE_DIR}"
            f" && gzip -f -k {PARCA_SIMDATA_DIR}/parca_state.pkl"
            f" && python scripts/build_cache.py"
            f" --fixture {PARCA_SIMDATA_DIR}/parca_state.pkl.gz --cache {PARCA_CACHE_DIR}"
        )

    def _upstream_parca_command(self) -> str:
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
        """
        return (
            f"cd {V2ECOLI_DIR} && python scripts/build_upstream_parca.py"
            f" --outdir {V2ECOLI_DIR}/out/upstream --cpus 1"
            f" --copy-to {PARCA_CACHE_DIR}"
        )

    async def _stage_runner(self, experiment_id: str) -> str:
        """Upload the generic run_pbg.py runner to S3 for this experiment; return its URI.

        Mirrors ``viva_api.compose.simulation_service_ray.ComposeSimulationServiceRay``'s
        own runner staging exactly (same source, same per-experiment S3 layout) -- the
        multi-generation batch path below downloads and runs it the identical way a
        compose job does. Staged (not embedded via heredoc) because AWS Batch caps a
        container override command at 8192 bytes.
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
            # v2ecoli/composites/batch_baseline.py, which wires in BatchBaselineRunner
            # directly — a dedicated always-batch-shaped composite, not a conditional
            # branch of the single-run baseline.py) run through the SAME generic
            # run_pbg.py every compose-on-Batch job already uses — not a
            # v2ecoli-specific CLI script. See backlog items 26/27: this is the one
            # execution mechanism both the ensemble endpoint and the generic compose
            # endpoint dispatch through; only the composite id + overrides differ per
            # caller.
            if not experiment_id:
                raise RuntimeError("experiment_id is required for multi-generation batch dispatch")
            if not runner_s3_uri:
                raise RuntimeError(
                    "runner_s3_uri is required for multi-generation batch dispatch "
                    "(the generic run_pbg.py runner must be staged to S3 first)"
                )
            overrides = {
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
            env = f"PBG_RESULTS_DIR={SIM_OUT_DIR} PBG_CORE_BUILDER={V2ECOLI_CORE_BUILDER}"
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

        CROSS-REPO CONTRACT: overrides threading these 3 keys through to
        ``v2ecoli/composites/batch_baseline.py``'s own ``parameters={...}``
        declaration (the composite this command dispatches through, via
        ``V2ECOLI_BATCH_BASELINE_COMPOSITE_ID``) is sms-ecoli PR #39's
        responsibility, already applied there — see that repo's own history;
        nothing about that contract is affected by this per-seed rework, only
        WHICH viva-api command builder emits the same 3 keys.

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
            "cache_dir": PARCA_CACHE_DIR,
            "out_dir": seed_out_dir,
            "experiment_id": experiment_id,
            "analyses": "none",
            "parallel": "",
            "base_seed": int(seed),
            "initial_generation_index": int(generation_index),
            "initial_carry_state_path": initial_carry_state_path,
            "daughter_state_out_path": daughter_state_out_path,
        }
        env = f"PBG_RESULTS_DIR={SIM_OUT_DIR} PBG_CORE_BUILDER={V2ECOLI_CORE_BUILDER}"
        return (
            f"cd {V2ECOLI_DIR}"
            f" && aws s3 cp {runner_s3_uri} /tmp/run_pbg.py"
            f" && {env} python /tmp/run_pbg.py"
            f" --composite-id {V2ECOLI_BATCH_BASELINE_COMPOSITE_ID}"
            f" --overrides {shlex.quote(json.dumps(overrides))} -n 1"
        )

    def _analysis_command(
        self,
        *,
        experiment_id: str,
        n_seeds: int,
        n_generations: int,
        modules: dict[str, dict[str, Any]] | str,
        analysis_name: str,
        commit: str,
    ) -> str:
        """Build the analysis DAG node's command: the ported analyses over the S3 sweep.

        WHY A THIRD DAG NODE, not the composite's own inline flush. The composite
        (``v2ecoli.composites.batch_baseline``) does ship a post-simulation flush that
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

        The node itself reuses the model image's existing, S3-native entrypoint
        (``scripts/run_standalone_analysis.py`` → ``v2ecoli.workflow.analysis_runner.
        run_analyses``) — the SAME function the composite's inline flush calls, reading
        the hive-parquet in place through DuckDB/httpfs. No new analysis logic.

        ``V2ECOLI_SIM_DATA`` points at the commit's ParCa cache in S3 because an S3
        sweep has no co-located pickle to glob (``analysis_runner.resolve_sim_data``
        only globs local paths) — identical to how ``SimulationServiceK8s.
        submit_ray_native_analysis`` provisions the same script. Both this job and the
        ParCa job derive that URI from the commit independently, so it needs no
        hand-off plumbing.
        """
        out_uri = self._results_s3_uri(experiment_id).rstrip("/")
        sim_data_uri = f"{data_layout.RayLayout.parca_cache_uri(commit)}simData.cPickle"
        modules_arg = modules if isinstance(modules, str) else json.dumps(modules)
        # --n-generations exists only to let the image resolve the "applicable"
        # keyword; an explicit module mapping doesn't need it. Emitting it only in
        # the keyword case keeps the explicit path runnable against ANY image that
        # already ships the script, so a simulator built before the keyword landed
        # still gets its configured analyses instead of dying on an unrecognized
        # argument. Only the keyword default requires the newer image.
        gens = f" --n-generations {int(n_generations)}" if isinstance(modules, str) else ""
        return (
            f"cd {V2ECOLI_DIR}"
            f" && V2ECOLI_SIM_DATA={shlex.quote(sim_data_uri)}"
            f" python scripts/run_standalone_analysis.py"
            f" --out-uri {shlex.quote(out_uri)}"
            f" --n-seeds {int(n_seeds)}"
            f"{gens}"
            f" --modules {shlex.quote(modules_arg)}"
            f" --analysis-name {shlex.quote(analysis_name)}"
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
    ) -> str | None:
        """Submit the analysis DAG node and record it, returning its Batch job id.

        The analysis is tracked in the SAME ``analyses`` table (and therefore the same
        ``GET /analyses/{id}/status`` S3-manifest probe) the on-demand
        ``POST /simulations/{id}/analysis`` trigger already writes to — an
        auto-triggered analysis must be exactly as discoverable as a hand-triggered
        one, not an invisible side effect.

        Best-effort by design, but never SILENT: the simulation job(s) this depends
        on are already submitted and running by the time this is reached, so
        raising would orphan real, expensive jobs. A submission failure is logged
        AND written to the analyses table as a FAILED row, so "the analysis never
        ran" is a visible state rather than an absence.

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
        out_uri = self._results_s3_uri(experiment_id).rstrip("/")
        result_uri = f"{out_uri}/analyses/{analysis_name}"
        modules = analysis_modules_for(simulation.config)
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
        try:
            analysis_job_id = self._submit_mnp(
                job_name=f"ray-analysis-{experiment_id}-{_rand_suffix()}"[:128],
                job_definition=job_definition,
                num_nodes=1,
                ray_job_cmd=self._analysis_command(
                    experiment_id=experiment_id,
                    n_seeds=n_seeds,
                    n_generations=n_generations,
                    modules=modules,
                    analysis_name=analysis_name,
                    commit=commit,
                ),
                out_s3=self._results_s3_uri(experiment_id),
                out_dir=ANALYSIS_OUT_DIR,
                depends_on=[sim_job_id] if sim_job_id else None,
                depends_type=depends_type,
                tags=tags,
            )
        except Exception as e:
            logger.exception("Analysis DAG node submission failed for %s", experiment_id)
            await database_service.record_analysis(
                experiment_id=experiment_id,
                n_tp=None,
                status=AnalysisStatusDB.FAILED,
                config=params,
                name=analysis_name,
                simulation_id=simulation.database_id,
                backend="ray",
                result_uri=result_uri,
                error_message=f"analysis job submission failed: {type(e).__name__}: {e}",
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
        return analysis_job_id

    @override
    async def get_latest_commit_hash(
        self,
        git_repo_url: str = DEFAULT_REPO,
        git_branch: str = DEFAULT_BRANCH,
    ) -> str:
        return await fetch_latest_commit_hash(git_repo_url, git_branch, get_settings().github_token)

    @override
    async def submit_build_image_job(self, simulator_version: SimulatorVersion) -> JobId:
        """Build the self-contained v2ecoli Ray image via a DooD Batch job.

        Symmetric with SimulationServiceK8s.submit_build_image_job: a LOCAL task submits a
        DooD Batch build job that clones the workload repo at the commit and runs its own
        build-and-push recipe (v2ecoli/docker/build-and-push-ecr.sh) → v2ecoli:<commit>
        (plus the :latest deploy tag the Ray-MNP job def references). Returns immediately
        with a LOCAL JobId; _run_build polls the Batch job to completion.
        """
        commit = simulator_version.git_commit_hash
        return self._local.submit(self._run_build(simulator_version), name=f"ray-build-{commit}")

    def _build_command(self, simulator_version: SimulatorVersion) -> list[str]:
        """DooD build command: clone v2ecoli@commit, run its build-and-push recipe.

        Mirrors SimulationServiceK8s._build_command (apk deps, PAT clone, in-repo recipe),
        but the workload repo is v2ecoli and the recipe is the v2ecoli image's own
        docker/build-and-push-ecr.sh → v2ecoli:<sha> (+ :latest).
        """
        settings = get_settings()
        commit = simulator_version.git_commit_hash
        branch = simulator_version.git_branch
        repo_url = simulator_version.git_repo_url
        script = f"""\
set -ex
export USER=${{USER:-sms-api}}
apk add --no-cache aws-cli git bash

# Docker daemon runs on the host (DooD) — verify the mounted socket.
docker info >/dev/null 2>&1 || {{ echo "ERROR: Docker socket not available"; exit 1; }}

# GitHub PAT (Secrets Manager) for the clone; x-access-token is GitHub's HTTPS convention.
# Disable xtrace around the secret so the PAT (and the clone URL embedding it) never lands
# in the build logs (CloudWatch). Re-enable tracing once the clone is done.
set +x
GH_PAT=$(aws secretsmanager get-secret-value \
    --secret-id {settings.build_git_secret_arn} --query SecretString --output text)
CLONE_URL=$(echo "{repo_url}" | sed "s|https://github.com/|https://x-access-token:${{GH_PAT}}@github.com/|")
export GIT_TERMINAL_PROMPT=0
git clone --branch {branch} --single-branch "$CLONE_URL" /build/v2ecoli
unset GH_PAT CLONE_URL
set -x
cd /build/v2ecoli
git checkout {commit}

# The v2ecoli image is self-contained (bundles the AWS CLI + Ray entrypoint); its own
# recipe builds + pushes v2ecoli:<sha> and the :latest deploy tag the MNP job def uses.
bash docker/build-and-push-ecr.sh -i {commit} -r {settings.ray_ecr_repository} -R {settings.batch_region}
"""
        return ["sh", "-c", script]

    async def _run_build(self, simulator_version: SimulatorVersion) -> None:
        """Submit the DooD v2ecoli image build to Batch (amd64 queue) and poll it."""
        settings = get_settings()
        commit = simulator_version.git_commit_hash
        job_id = await batch_build.submit_batch_build(
            job_name=f"v2ecoli-ray-build-{commit}",
            queue=settings.build_amd64_queue,
            command=self._build_command(simulator_version),
        )
        await batch_build.poll_batch_jobs([job_id])
        logger.info("v2ecoli Ray image build complete: %s:%s", settings.ray_ecr_repository, commit)

    @override
    async def submit_parca_job(self, parca_dataset: ParcaDataset) -> JobId:
        """Submit ParCa as a 1-node Ray MNP job, capturing the cache to S3."""
        simulator_version = parca_dataset.parca_dataset_request.simulator_version
        commit = simulator_version.git_commit_hash
        job_def = self._ensure_mnp_job_def(self._image_uri(commit), commit)
        job_id = self._submit_mnp(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            num_nodes=1,
            ray_job_cmd=self._parca_command(),
            out_s3=self._cache_s3_uri(commit),
            out_dir=PARCA_CACHE_DIR,
        )
        return JobId.ray(job_id)

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

        Every OTHER shape still reaches the MNP path below exactly as before:
        the composite-driven two-engine comparison ensemble (genuinely fans out
        via Ray actors, at ANY generation count — chain-dispatch is v2ecoli-only
        and does not apply), and the single-generation phase0 ensemble.
        """
        if database_service is None:
            raise RuntimeError("DatabaseService is not available. Cannot submit Ray simulation job.")

        config = ecoli_simulation.config
        composite = getattr(config, "composite", None)
        # config.generations is a real (non-"extra") SimulationConfig field, unlike
        # the comparison knobs read further below -- read directly, not via getattr.
        n_generations = int(config.generations or 1)
        if composite is None and n_generations > 1:
            return await self.submit_chain_dispatch_job(
                ecoli_simulation, database_service, correlation_id=correlation_id
            )

        parca_dataset = await database_service.get_parca_dataset(parca_dataset_id=ecoli_simulation.parca_dataset_id)
        if parca_dataset is None:
            raise ValueError(f"ParcaDataset with ID {ecoli_simulation.parca_dataset_id} not found.")
        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {ecoli_simulation.simulator_id} not found")

        settings = get_settings()
        commit = simulator.git_commit_hash
        experiment_id = ecoli_simulation.config.experiment_id

        # Run the TRUE commit image: derive a per-commit MNP job-def revision pointing at
        # v2ecoli:<commit> (both ParCa and the sim run the same image).
        job_def = self._ensure_mnp_job_def(self._image_uri(commit), commit)

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
        cache_s3 = self._upstream_cache_s3_uri(commit) if is_upstream else self._cache_s3_uri(commit)
        parca_command = self._upstream_parca_command() if is_upstream else self._parca_command()

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
        parca_job_id = self._submit_mnp(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            num_nodes=1,
            ray_job_cmd=parca_command,
            out_s3=cache_s3,
            out_dir=PARCA_CACHE_DIR,
            tags={**base_tags, "Phase": "parca"},
        )

        # 2. Simulation ensemble (N-node Ray cluster), gated on ParCa, staging the
        # cache. Always MNP now: the ONE shape that used to need Array jobs here
        # (canonical batch_baseline, composite is None + multi-generation) is
        # routed to submit_chain_dispatch_job before this method does ANY of the
        # setup above (see the routing check at the top) -- every request that
        # still reaches this point either sets composite (the comparison
        # ensemble, which genuinely fans out via Ray actors) or requests a
        # single generation (the phase0 ensemble).
        sim_job_id = self._submit_mnp(
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
            ),
            out_s3=self._results_s3_uri(experiment_id),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            depends_on=[parca_job_id],
            tags={**base_tags, "Phase": "sim"},
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
        return JobId.ray(sim_job_id)

    def _chain_base_tags(self, *, simulation: Simulation, commit: str) -> dict[str, str]:
        """Cost-allocation tag base shared by every per-seed chain job + the
        ParCa job that precedes them, mirroring ``submit_ecoli_simulation_job``'s
        ``base_tags`` (composite/condition don't apply — chain dispatch is
        v2ecoli-only)."""
        settings = get_settings()
        return {
            "Project": "v2ecoli-comparison",
            "ExperimentId": str(simulation.config.experiment_id)[:255],
            "Engine": "v2ecoli",
            "Commit": str(commit)[:12],
            "Team": getattr(settings, "cost_team_tag", None) or "covertlab",
        }

    async def submit_chain_dispatch_job(
        self,
        ecoli_simulation: Simulation,
        database_service: DatabaseService,
        correlation_id: str | None = None,
    ) -> JobId:
        """Kick off a per-seed chain-dispatch campaign (backlog item 33 rework
        — individual per-seed job chains, replacing the per-generation-array
        "wave" design): submit ParCa (1 node), then EVERY seed's full
        G-generation ``dependsOn`` chain, all N*G jobs submitted upfront,
        TPS-paced. A true v2 analogy of vEcoli-private's own fully-asynchronous
        per-seed Nextflow execution (Alex's explicit decision, 2026-08-08) —
        seed 5 can be on generation 8 while seed 800 is still on generation 1,
        throttled only by available compute, never by a cross-seed barrier.

        ``correlation_id``: this method (unlike a single-shot dispatch) always
        records its OWN ``HpcRun`` row internally — see the ``insert_hpcrun``
        call at the end — because the campaign-tracking row needs
        ``chain_n_generations``/``chain_final_job_ids``, fields a generic
        caller has no way to populate. When called directly (e.g. by tests, or
        a future dedicated campaign-kickoff route) with no ``correlation_id``,
        one is generated fresh here, exactly as before. When called FROM
        ``submit_ecoli_simulation_job`` (the real dispatch entrypoint), that
        method's own caller-supplied ``correlation_id`` is threaded through
        instead, so the ONE row this method inserts uses the SAME
        correlation_id the router/handler already generated for the request —
        the row a status lookup by that id resolves to is the real campaign
        row, not a second, generic one a caller might otherwise insert after
        the fact (see ``viva_api.common.handlers.simulations``'s own
        idempotent-insert guard, added alongside this parameter).

        For each seed independently: generation 0 ``dependsOn`` ParCa
        (SEQUENTIAL, matching the long-standing ParCa→sim edge shape exactly —
        both ends are MNP jobs here); generation g>0 ``dependsOn`` generation
        g-1's own job id (same SEQUENTIAL shape). All jobs for every seed are
        submitted immediately, back to back, without waiting for any to
        actually run — AWS Batch holds a job in ``PENDING`` (no compute, no
        cost) until its dependency reaches ``SUCCEEDED``; a dependency that
        permanently FAILS auto-propagates failure to what depends on it, no
        orchestrator action needed (see ``_SubmitJobPacer``/``_submit_mnp``'s
        ``retry_strategy`` for the two things this DOES still need to handle
        itself: staying under the account-wide SubmitJob rate limit, and
        restoring per-job retry — see below).

        WHY MNP (``_submit_mnp``, ``num_nodes=1``), not a "singleton array job":
        this session confirmed directly against the real, shipped mechanism
        (sms-cdk's ``batch-array-entrypoint.sh`` and AWS's own
        ``job_env_vars.html``) that NEITHER of viva-api's two entrypoint
        scripts supports a genuinely standalone job with its own independent
        job id. ``batch-array-entrypoint.sh`` hard-requires
        ``AWS_BATCH_JOB_ARRAY_INDEX``, which AWS Batch only sets for children
        of a REAL array job (and ``arrayProperties.size`` has a hard floor of
        2 — no size-1 "singleton array" exists). A true per-seed dependsOn
        chain structurally needs each generation to be its OWN job with its
        OWN id anyway (array children can't dependsOn each other individually
        — dependsOn operates at the array PARENT level only), so array jobs
        were never a fit for this design regardless. MNP with ``num_nodes=1``
        is the one mechanism already proven to submit a genuinely standalone
        job (ParCa and the analysis job already run this way) — reused as-is,
        no new job type, no sms-cdk change.

        KNOWN, FLAGGED COST TRADEOFF (not silently absorbed): the MNP queue's
        compute environment (``RayBatchOnDemandCE``, confirmed directly against
        ``sms-cdk/lib/ray-batch-stack.ts``) is ON-DEMAND ONLY — unlike the Array
        job definition's queue, which is Spot-tolerant and already carries its
        own ``retryStrategy`` (``arrayRetryAttempts``, default 2) for exactly
        the "a Spot reclaim IS the job's own retry" property item 34 assumes.
        The MNP job definition declares NO ``retryStrategy`` of its own, so
        every per-seed-per-generation submission below passes an explicit
        ``retry_strategy`` override (``_CHAIN_JOB_RETRY_STRATEGY``, matching
        the Array job definition's own already-tuned value) to restore that
        property — real, working, per-job retry, achieved from viva-api alone.
        What can NOT be restored from viva-api alone is Spot PRICING itself
        (a property of the compute environment bound to the queue, not
        anything a submission-time parameter can change) — a real cost-shape
        difference from the superseded array-child design, left open for a
        companion sms-cdk change (e.g. a Spot-capable container-type job
        definition with a relaxed entrypoint), not this PR's scope.

        Unlike the superseded per-generation-array design, ``n_seeds >= 2`` is
        NOT required: that floor was AWS Batch's own array-size minimum, which
        doesn't apply here (each seed's chain is independent standalone jobs,
        no array involved at all). ``n_generations >= 2`` is still required —
        a single-generation request has nothing to chain; use
        ``submit_ecoli_simulation_job``.

        A submission failure partway through one seed's chain (even after
        real retry-on-throttle is exhausted) truncates JUST that seed's chain
        — its already-submitted generations still run normally on Batch, but
        nothing later is submitted for it, and its last successfully-submitted
        job id (not necessarily generation G-1) is what gets tracked for the
        analysis-fan-in poll. OTHER seeds are unaffected. This mirrors how a
        RUNTIME failure is handled (Batch's own dependency propagation, no
        orchestrator involvement) as closely as a SUBMISSION-time failure can.

        Returns the ParCa job's ``JobId`` — the one well-defined "campaign
        kickoff" marker (no single job represents N*G independent chains as a
        whole). The real per-seed tracking lives in the campaign's own
        ``HpcRun`` row (``chain_final_job_ids``), inserted once at the end of
        this method — analogous to how the superseded design's every wave
        recorded its own row, just collapsed to ONE row per campaign now that
        Batch's own dependsOn (not orchestrator polling) advances each chain.
        """
        parca_dataset = await database_service.get_parca_dataset(parca_dataset_id=ecoli_simulation.parca_dataset_id)
        if parca_dataset is None:
            raise ValueError(f"ParcaDataset with ID {ecoli_simulation.parca_dataset_id} not found.")
        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {ecoli_simulation.simulator_id} not found")

        commit = simulator.git_commit_hash
        config = ecoli_simulation.config
        n_seeds = int(ecoli_simulation.num_seeds or getattr(config, "n_init_sims", None) or 1)
        n_generations = int(config.generations or 1)
        if n_generations < 2:
            raise ValueError(
                "submit_chain_dispatch_job requires generations > 1 "
                "(use submit_ecoli_simulation_job for single-generation runs)"
            )

        experiment_id = str(ecoli_simulation.config.experiment_id)
        job_def_mnp = self._ensure_mnp_job_def(self._image_uri(commit), commit)
        cache_s3 = self._cache_s3_uri(commit)
        base_tags = self._chain_base_tags(simulation=ecoli_simulation, commit=commit)
        runner_s3_uri = await self._stage_runner(experiment_id)

        pacer = _SubmitJobPacer()
        # A dedicated, retry-configured client for this bulk-submission loop only
        # -- keeps every OTHER existing call site's behavior (which uses the
        # shared self._batch() factory, unconfigured) completely unchanged.
        submit_client = boto3.client(
            "batch",
            region_name=get_settings().batch_region,
            config=Config(retries={"mode": "standard", "max_attempts": _SUBMIT_JOB_MAX_ATTEMPTS}),
        )

        await pacer.wait()
        parca_job_id = self._submit_mnp(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=job_def_mnp,
            num_nodes=1,
            ray_job_cmd=self._parca_command(),
            out_s3=cache_s3,
            out_dir=PARCA_CACHE_DIR,
            tags={**base_tags, "Phase": "parca"},
            batch_client=submit_client,
        )

        final_job_ids: list[str] = []
        for seed in range(n_seeds):
            prev_job_id = parca_job_id
            seed_final_job_id: str | None = None
            for generation_index in range(n_generations):
                await pacer.wait()
                try:
                    job_id = self._submit_mnp(
                        job_name=f"chain-seed{seed}-gen{generation_index}-{experiment_id}-{_rand_suffix()}"[:128],
                        job_definition=job_def_mnp,
                        num_nodes=1,
                        ray_job_cmd=self._seed_generation_command(
                            seed=seed,
                            generation_index=generation_index,
                            experiment_id=experiment_id,
                            runner_s3_uri=runner_s3_uri,
                        ),
                        # Per-seed prefix, not the flat ensemble one: matches the
                        # composite's own out_dir override in
                        # _seed_generation_command (see there for why) — RAY_OUT_S3
                        # is now mostly a safety-net catch-all (the emitters/
                        # summary.json/daughter-state all write directly to S3
                        # under this same prefix), not the primary mechanism, but
                        # it must still point at the SAME prefix so anything that
                        # DOES still land in the local RAY_OUT_DIR scratch dir
                        # syncs to the right place rather than the old flat one.
                        out_s3=data_layout.RayLayout.seed_results_uri(experiment_id, seed),
                        out_dir=SIM_OUT_DIR,
                        stage_s3=cache_s3,
                        stage_dir=PARCA_CACHE_DIR,
                        depends_on=[prev_job_id],
                        depends_type="SEQUENTIAL",
                        tags={**base_tags, "Phase": "sim", "Seed": str(seed), "Generation": str(generation_index)},
                        retry_strategy=_CHAIN_JOB_RETRY_STRATEGY,
                        batch_client=submit_client,
                    )
                except Exception:
                    logger.exception(
                        "Chain dispatch %s: seed %d generation %d submission failed "
                        "(even after retry-on-throttle) -- truncating this seed's chain here; "
                        "other seeds are unaffected",
                        experiment_id,
                        seed,
                        generation_index,
                    )
                    break
                seed_final_job_id = job_id
                prev_job_id = job_id
            if seed_final_job_id is not None:
                final_job_ids.append(seed_final_job_id)
            else:
                logger.error(
                    "Chain dispatch %s: seed %d contributed NO jobs to the campaign (generation 0 submission failed)",
                    experiment_id,
                    seed,
                )

        await database_service.insert_hpcrun(
            job_id=JobId.ray(parca_job_id),
            job_type=JobType.SIMULATION,
            ref_id=ecoli_simulation.database_id,
            correlation_id=correlation_id or f"chain-campaign-{experiment_id}-{_rand_suffix()}",
            chain_n_generations=n_generations,
            chain_final_job_ids=final_job_ids,
        )
        logger.info(
            "Chain dispatch %s: parca job %s -> %d/%d seed chains submitted (%d generations each)",
            experiment_id,
            parca_job_id,
            len(final_job_ids),
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
        base_tags = self._chain_base_tags(simulation=simulation, commit=commit)
        mnp_job_def = self._ensure_mnp_job_def(self._image_uri(commit), commit)
        return await self._submit_analysis_job(
            simulation=simulation,
            database_service=database_service,
            job_definition=mnp_job_def,
            commit=commit,
            sim_job_id=None,
            n_seeds=total_n_seeds,
            n_generations=n_generations,
            depends_type=None,
            tags={**base_tags, "Phase": "analysis"},
        )

    @override
    async def read_config_template(
        self,
        simulator_version: SimulatorVersion,
        config_filename: str,
        allow_default_fallback: bool = False,
    ) -> str:
        return await fetch_config_template(
            simulator_version, config_filename, get_settings().github_token, allow_default_fallback
        )

    @override
    async def discover_repo_contents(self, simulator_version: SimulatorVersion) -> RepoDiscovery:
        return await fetch_repo_discovery(simulator_version, get_settings().github_token)

    @override
    async def get_job_status(self, job_id: JobId) -> JobStatusInfo | None:
        """Status — LOCAL (prebuilt-image placeholder) or AWS Batch describe_jobs."""
        if job_id.backend == JobBackend.LOCAL:
            return self._local.get_status(job_id.value)

        response = self._batch().describe_jobs(jobs=[job_id.value])
        jobs = response.get("jobs", [])
        if not jobs:
            logger.warning("No Batch job found with id %s", job_id.value)
            return None
        job = jobs[0]
        status = JobStatus.from_batch_state(job.get("status", ""))
        started = job.get("startedAt")
        stopped = job.get("stoppedAt")
        return JobStatusInfo(
            job_id=job_id,
            status=status,
            start_time=str(started) if started else None,
            end_time=str(stopped) if stopped else None,
            exit_code=None,
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

        batch = self._batch()
        statuses: dict[str, str] = {}
        for i in range(0, len(job_ids), _DESCRIBE_JOBS_MAX_BATCH):
            chunk = job_ids[i : i + _DESCRIBE_JOBS_MAX_BATCH]
            response = batch.describe_jobs(jobs=chunk)
            for job in response.get("jobs", []):
                job_id = job.get("jobId")
                if job_id is not None:
                    statuses[str(job_id)] = str(job.get("status", ""))

        succeeded: list[str] = []
        failed: list[str] = []
        for jid in job_ids:
            mapped = JobStatus.from_batch_state(statuses.get(jid, ""))
            if mapped == JobStatus.COMPLETED:
                succeeded.append(jid)
            elif mapped == JobStatus.FAILED:
                failed.append(jid)
            # else: still queued/running, or missing from the response
            # entirely (not yet visible) -- either way, not yet terminal.

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
        self._batch().terminate_job(jobId=job_id.value, reason="cancelled via sms-api")
        logger.info("Terminated Ray Batch job %s", job_id.value)

    @override
    async def close(self) -> None:
        pass
