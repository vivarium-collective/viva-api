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
    HpcRun,
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
V2ECOLI_CORE_BUILDER = "v2ecoli.core:build_core"

# Absolute paths inside the v2ecoli Ray image (WORKDIR=/app/v2ecoli). The
# entrypoint runs RAY_JOB_CMD on the head; v2ecoli reads the cache from
# CACHE_DIR and writes the ensemble outputs under OUT_DIR.
V2ECOLI_DIR = "/app/v2ecoli"
PARCA_CACHE_DIR = f"{V2ECOLI_DIR}/out/cache"
PARCA_SIMDATA_DIR = f"{V2ECOLI_DIR}/out/sim_data"
SIM_OUT_DIR = f"{V2ECOLI_DIR}/.pbg/runs/phase0-xarray"
# ecoli_baseline.baseline()'s injection branch (taken whenever injected_processes
# is passed) does `from scripts._compare.inject import (...)` -- a bare absolute
# import that only resolves when V2ECOLI_DIR (which DOES contain scripts/, copied
# in by sms-ecoli's Dockerfile `COPY . .`) is on sys.path. Every run_pbg.py
# invocation below runs it via an absolute /tmp path, which makes CPython put
# /tmp on sys.path[0] instead of the cwd -- the `cd {V2ECOLI_DIR}` alone doesn't
# fix this; PYTHONPATH does. Found live 2026-09-01 (backlog item 93): a real
# chain-dispatch run with a non-empty injected_processes failed
# ModuleNotFoundError('scripts') despite the cd already being correct.
PBG_RUNNER_ENV = f"PBG_RESULTS_DIR={SIM_OUT_DIR} PBG_CORE_BUILDER={V2ECOLI_CORE_BUILDER} PYTHONPATH={V2ECOLI_DIR}"
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
# (Per-generation-job retry no longer needs a manual override here as of item 71
# Phase 4: chain-dispatch generations now submit as container-type jobs, whose
# job definition already bakes in retryStrategy.attempts=2 -- see sms-cdk's
# RayContainerJobDef -- unlike the MNP job definition this superseded, which
# declared none of its own.)
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


def injected_processes_from_config(config: Any) -> dict[str, Any] | None:
    """Build ``ecoli_baseline.baseline()``'s own ``injected_processes`` kwarg
    (backlog item 93) from a legacy config's ``swap_processes``/
    ``add_processes``/``exclude_processes`` -- real ``ExperimentRequest``
    fields (``viva_api/simulation/models.py``) that ride through
    ``SimulationConfig`` as extras (``extra="allow"``), so ``getattr`` is the
    correct read whether or not the field was ever declared on the model.

    Returns ``None`` when none of the three are set, so a config with no
    injection intent produces the exact ``overrides`` dict this dispatch path
    already built before this existed -- the byte-for-byte-unchanged
    regression property backlog items 86/88's own ``extra_params`` passthrough
    established for this same class of fix.

    ``fork_repo`` is always ``""``: every caller of this helper dispatches
    through ``ecoli_baseline``, the NATIVE (fork-free) composite --
    ``assert_injection_sourcing`` (v2ecoli's ``composites/ecoli_baseline.py``)
    rejects a non-empty ``fork_repo`` for a native composite outright, and
    ``scripts._compare.inject.resolve_injections`` indexes
    ``injected_processes["fork_repo"]`` directly (not ``.get``), so the key
    must be present even though it is always empty on this path.
    """
    swap_processes = getattr(config, "swap_processes", None) or {}
    add_processes = getattr(config, "add_processes", None) or []
    exclude_processes = getattr(config, "exclude_processes", None) or []
    if not (swap_processes or add_processes or exclude_processes):
        return None
    return {
        "swap_processes": swap_processes,
        "add_processes": add_processes,
        "exclude_processes": exclude_processes,
        "fork_repo": "",
    }


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

    def cache_s3_uri(self, commit: str) -> str:
        """Deterministic S3 URI for a commit's v2ecoli ParCa cache.

        Both the ParCa job (writes here) and the simulation job (stages from
        here) derive the same URI, so the cache hand-off needs no runtime wiring.
        """
        return data_layout.RayLayout.parca_cache_uri(commit)

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
        shared_env = self._stage_out_env(
            prefix="RAY",
            out_dir=out_dir,
            out_s3=out_s3,
            stage_s3=stage_s3,
            stage_dir=stage_dir,
            log_s3_prefix=settings.ray_log_s3_prefix,
        )

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
        # Backlog item 65: a standalone (numNodes=1) submission has no inter-node
        # traffic to protect, so it gains nothing from ray_mnp_queue's cluster-
        # placement-group compute environment and pays its full concurrency cost
        # for nothing -- route it to the dedicated no-placement-group queue
        # instead, when one is configured. Automatic and transparent to every
        # caller (chain-dispatch, ParCa, compose): both already pass their real
        # num_nodes here, no call-site changes needed. Falls back to
        # ray_mnp_queue unchanged for a genuine multi-node request (num_nodes >
        # 1, e.g. colony sims) or when ray_mnp_standalone_queue isn't set yet.
        job_queue = (
            settings.ray_mnp_standalone_queue
            if num_nodes == 1 and settings.ray_mnp_standalone_queue
            else settings.ray_mnp_queue
        )
        kwargs: dict[str, Any] = {
            "jobName": job_name,
            "jobQueue": job_queue,
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
            job_queue,
        )
        return batch_job_id

    def _stage_out_env(
        self,
        *,
        prefix: str,
        out_dir: str,
        out_s3: str,
        stage_s3: str | None = None,
        stage_dir: str | None = None,
        log_s3_prefix: str | None = None,
    ) -> list[dict[str, str]]:
        """Shared stage/output/log env-var construction for both the MNP (``RAY_*``)
        and container (``CONTAINER_*``) submission paths (backlog item 71) -- same
        conditional logic (only emit STAGE_*/LOG_S3_PREFIX when configured), a
        different env-var prefix per job shape, since each entrypoint script only
        reads its own prefix -- the values can't literally share one env list.
        """
        env: list[dict[str, str]] = [
            {"name": f"{prefix}_OUT_DIR", "value": out_dir},
            {"name": f"{prefix}_OUT_S3", "value": out_s3},
        ]
        if stage_s3 and stage_dir:
            env.append({"name": f"{prefix}_STAGE_S3", "value": stage_s3})
            env.append({"name": f"{prefix}_STAGE_DIR", "value": stage_dir})
        if log_s3_prefix:
            env.append({"name": f"{prefix}_LOG_S3_PREFIX", "value": log_s3_prefix})
        return env

    def _ensure_container_job_def(self, image: str, commit: str) -> str:
        """Return a container job definition (name:revision) whose image is the commit's image.

        Mirrors ``_ensure_mnp_job_def`` exactly, for the plain (non-MNP, non-array)
        standalone container job shape (backlog item 71 -- ParCa, the analysis DAG
        node, and eventually chain-dispatch's per-seed-per-generation jobs, none of
        which have any real inter-node traffic to protect). Plain container jobs
        can't override the image at submission time either -- same limitation as
        MNP -- so a per-commit job-def revision is derived the same way: describe
        the CDK base container job def (``ray_container_job_definition``: roles,
        resources, retry strategy, log config -- provisioned by sms-cdk's
        RayContainerJobDef), swap ONLY its image, and register it as
        ``<base>-<commit>``. An existing active revision already pointing at this
        image is reused, so resubmits don't churn revisions.
        """
        settings = get_settings()
        if not settings.ray_container_job_definition:
            # Matches this file's own compose_ray_image_tag precedent: fail loud with
            # the setting name rather than submit a doomed job with a blank job-def.
            raise RuntimeError("ray_container_job_definition is not set; cannot submit a container-type Batch job.")
        batch = self._batch()
        name = f"{settings.ray_container_job_definition}-{commit}"

        # Reuse an existing active revision that already targets this exact image.
        existing = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
        for jd in existing.get("jobDefinitions", []):
            if jd.get("containerProperties", {}).get("image") == image:
                return f"{name}:{jd['revision']}"

        # Otherwise clone the base job def's container properties and swap the image.
        base = batch.describe_job_definitions(jobDefinitionName=settings.ray_container_job_definition, status="ACTIVE")
        base_defs = base.get("jobDefinitions", [])
        if not base_defs:
            raise RuntimeError(f"Base container job definition {settings.ray_container_job_definition!r} not found")
        container_properties = copy.deepcopy(max(base_defs, key=lambda d: d["revision"])["containerProperties"])
        container_properties["image"] = image

        response = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties=container_properties,
        )
        logger.info("Registered container job def %s:%s for image %s", name, response["revision"], image)
        return f"{name}:{response['revision']}"

    def _submit_container(
        self,
        *,
        job_name: str,
        job_definition: str,
        job_cmd: str,
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
        """Submit a plain, standalone AWS Batch container-type job (backlog item 71).

        Sibling of ``_submit_mnp`` for the non-MNP, non-array job shape -- currently
        ParCa (``submit_parca_job``) and the analysis DAG node
        (``_submit_analysis_job``), both already ``num_nodes=1`` MNP jobs with no
        real inter-node traffic; chain-dispatch's per-seed-per-generation jobs
        migrate here too in a later phase. One task, one container: no node
        overrides, no head/worker split -- every env var goes in a single
        ``containerOverrides.environment`` list, matching
        ``docker/batch-container-entrypoint.sh``'s ``CONTAINER_*`` contract exactly
        (sms-ecoli). Returns the AWS Batch job id.

        Do NOT modify ``_submit_mnp`` -- this is a parallel path, not a
        replacement; genuinely multi-node Ray paths keep submitting through
        ``_submit_mnp`` unchanged.
        """
        settings = get_settings()
        if not settings.ray_container_queue:
            raise RuntimeError("ray_container_queue is not set; cannot submit a container-type Batch job.")

        env: list[dict[str, str]] = [
            {"name": "CONTAINER_JOB_CMD", "value": job_cmd},
            {"name": "CONTAINER_REPORT_PATH", "value": REPORT_PATH},
            *self._stage_out_env(
                prefix="CONTAINER",
                out_dir=out_dir,
                out_s3=out_s3,
                stage_s3=stage_s3,
                stage_dir=stage_dir,
                log_s3_prefix=settings.ray_log_s3_prefix,
            ),
        ]

        kwargs: dict[str, Any] = {
            "jobName": job_name,
            "jobQueue": settings.ray_container_queue,
            "jobDefinition": job_definition,
            "containerOverrides": {"environment": env},
        }
        if depends_on:
            kwargs["dependsOn"] = [
                ({"jobId": jid, "type": depends_type} if depends_type else {"jobId": jid}) for jid in depends_on
            ]
        if tags:
            kwargs["tags"] = tags
            kwargs["propagateTags"] = True
        if retry_strategy:
            kwargs["retryStrategy"] = retry_strategy

        batch = batch_client if batch_client is not None else self._batch()
        response = batch.submit_job(**kwargs)
        batch_job_id = str(response["jobId"])
        logger.info("Submitted container job %s (id=%s) to %s", job_name, batch_job_id, settings.ray_container_queue)
        return batch_job_id

    def _parca_command(self, *, new_genes: str | None = None, bundle_overrides: str | None = None) -> str:
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

        ``bundle_overrides`` (backlog item 104): a legacy config's own
        ``parca_options.bundle_overrides`` (a bundle-overrides manifest path, e.g. a composed
        new-gene overlay) -- generic passthrough to ``v2ecoli-parca``'s own
        ``--bundle-overrides PATH`` flag (``cli/parca.py``, ``action="append"``). Same
        "missed in the new_genes pass" class of gap this mirrors exactly (sms-ecoli#184 /
        viva-api#365): the value survived on the stored request but was never read here, so
        ParCa silently built from defaults and any keys the overrides supply were absent.
        Default ``None`` builds byte-for-byte the same command as before this param existed.
        """
        settings = get_settings()
        new_genes_flag = f" --new-genes {shlex.quote(new_genes)}" if new_genes and new_genes != "off" else ""
        bundle_overrides_flag = f" --bundle-overrides {shlex.quote(bundle_overrides)}" if bundle_overrides else ""
        return (
            f"cd {V2ECOLI_DIR}"
            f" && v2ecoli-parca --mode {settings.ray_parca_mode} --cpus {settings.ray_parca_cpus}"
            f" -o {PARCA_SIMDATA_DIR} --cache-dir {PARCA_CACHE_DIR}{new_genes_flag}{bundle_overrides_flag}"
            f" && gzip -f -k {PARCA_SIMDATA_DIR}/parca_state.pkl"
            f" && python scripts/build_cache.py"
            f" --fixture {PARCA_SIMDATA_DIR}/parca_state.pkl.gz --cache {PARCA_CACHE_DIR}"
        )

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
        composite this command dispatches through, via
        ``V2ECOLI_BATCH_BASELINE_COMPOSITE_ID`` — formerly a dedicated
        ``composites/batch_baseline.py``, folded into ``ecoli_baseline.py`` by
        v2ecoli #373 and finally synced into sms-ecoli by PR #56, backlog item 55)
        is v2ecoli's own responsibility; nothing about that contract is affected by
        this per-seed rework, only WHICH viva-api command builder emits the same
        keys.

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
        env = PBG_RUNNER_ENV
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
            # Backlog item 71: the analysis DAG node has no real inter-node traffic
            # either (was a 1-node MNP job) -- moves to the plain container-type
            # path. `job_definition` must now be a container job def (see
            # submit_campaign_analysis's _ensure_container_job_def call).
            analysis_job_id = self._submit_container(
                job_name=f"ray-analysis-{experiment_id}-{_rand_suffix()}"[:128],
                job_definition=job_definition,
                job_cmd=self._analysis_command(
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
    async def submit_build_image_job(
        self, simulator_version: SimulatorVersion, *, include_new_gene_data: bool = False
    ) -> JobId:
        """Build the self-contained v2ecoli Ray image via a DooD Batch job.

        Symmetric with SimulationServiceK8s.submit_build_image_job: a LOCAL task submits a
        DooD Batch build job that clones the workload repo at the commit and runs its own
        build-and-push recipe (v2ecoli/docker/build-and-push-ecr.sh) → v2ecoli:<commit>
        (plus the :latest deploy tag the Ray-MNP job def references). Returns immediately
        with a LOCAL JobId; _run_build polls the Batch job to completion.

        ``include_new_gene_data`` (item 87): False for every existing caller -- identical
        build to before this param existed. See ``_build_command``'s own docstring.
        """
        commit = simulator_version.git_commit_hash
        return self._local.submit(
            self._run_build(simulator_version, include_new_gene_data=include_new_gene_data),
            name=f"ray-build-{commit}",
        )

    def _build_command(self, simulator_version: SimulatorVersion, *, include_new_gene_data: bool = False) -> list[str]:
        """DooD build command: clone v2ecoli@commit, run its build-and-push recipe.

        Mirrors SimulationServiceK8s._build_command (apk deps, PAT clone, in-repo recipe),
        but the workload repo is v2ecoli and the recipe is the v2ecoli image's own
        docker/build-and-push-ecr.sh → v2ecoli:<sha> (+ :latest).

        ``include_new_gene_data`` (item 87): False for every existing caller -- identical
        command to before this param existed (the outer clone's PAT is unset immediately,
        as before; ``-g`` is never passed). When True, the SAME PAT this method already
        fetches to clone the workload repo (both under the CovertLabEcoli org) stays
        exported for the build-and-push recipe's own ``-g`` flag, which threads it through
        as a Docker BuildKit secret (never a plain env/build-arg baked into a layer) so the
        image can stage private new-gene data for a ``--composite vecoli`` ParCa build that
        declares one. No new credential -- reuses this same Secrets Manager entry.
        """
        settings = get_settings()
        commit = simulator_version.git_commit_hash
        branch = simulator_version.git_branch
        repo_url = simulator_version.git_repo_url
        build_flags = " -g" if include_new_gene_data else ""
        unset_pat = "" if include_new_gene_data else "unset GH_PAT\n"
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
export GH_PAT=$(aws secretsmanager get-secret-value \
    --secret-id {settings.build_git_secret_arn} --query SecretString --output text)
CLONE_URL=$(echo "{repo_url}" | sed "s|https://github.com/|https://x-access-token:${{GH_PAT}}@github.com/|")
export GIT_TERMINAL_PROMPT=0
git clone --branch {branch} --single-branch "$CLONE_URL" /build/v2ecoli
unset CLONE_URL
{unset_pat}set -x
cd /build/v2ecoli
git checkout {commit}

# The v2ecoli image is self-contained (bundles the AWS CLI + Ray entrypoint); its own
# recipe builds + pushes v2ecoli:<sha> and the :latest deploy tag the MNP job def uses.
bash docker/build-and-push-ecr.sh -i {commit} -r {settings.ray_ecr_repository} -R {settings.batch_region}{build_flags}
"""
        return ["sh", "-c", script]

    async def _run_build(self, simulator_version: SimulatorVersion, *, include_new_gene_data: bool = False) -> None:
        """Submit the DooD v2ecoli image build to Batch (amd64 queue) and poll it."""
        settings = get_settings()
        commit = simulator_version.git_commit_hash
        job_id = await batch_build.submit_batch_build(
            job_name=f"v2ecoli-ray-build-{commit}",
            queue=settings.build_amd64_queue,
            command=self._build_command(simulator_version, include_new_gene_data=include_new_gene_data),
        )
        await batch_build.poll_batch_jobs([job_id])
        logger.info("v2ecoli Ray image build complete: %s:%s", settings.ray_ecr_repository, commit)

    @override
    async def submit_parca_job(self, parca_dataset: ParcaDataset) -> JobId:
        """Submit ParCa as a standalone container job (backlog item 71), capturing
        the cache to S3. Was a 1-node Ray MNP job; ParCa has no real inter-node
        traffic, so it moves to the plain container-type path -- see
        ``_submit_container``."""
        simulator_version = parca_dataset.parca_dataset_request.simulator_version
        commit = simulator_version.git_commit_hash
        job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        job_id = self._submit_container(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            job_cmd=self._parca_command(),
            out_s3=self.cache_s3_uri(commit),
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
        mnp_dispatch = getattr(config, "multi_node_dispatch", None)
        if mnp_dispatch is not None:
            return await self._submit_multi_node_composite(
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
        cache_s3 = self._upstream_cache_s3_uri(commit) if is_upstream else self.cache_s3_uri(commit)
        # Backlog item 93: same generic new_genes passthrough as
        # submit_chain_dispatch_job -- irrelevant to the upstream-vEcoli
        # engine (its own config_path-driven mechanism, item 87, is separate).
        new_genes = None if is_upstream else getattr(config.parca_options, "new_genes", None)
        # Backlog item 104: same generic bundle_overrides passthrough, same
        # upstream-vEcoli exemption as new_genes above.
        bundle_overrides = None if is_upstream else getattr(config.parca_options, "bundle_overrides", None)
        parca_command = (
            self._upstream_parca_command()
            if is_upstream
            else self._parca_command(new_genes=new_genes, bundle_overrides=bundle_overrides)
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

    # A freshly-registered job definition (this method is always called right
    # after `_ensure_mnp_job_def` registers one) can briefly 404/come back
    # empty from `describe_job_definitions` due to AWS eventual consistency --
    # confirmed live 2026-08-25 on a commit's first-ever multi-node dispatch
    # (sim255): the identical job definition, queried again a few minutes
    # later, returned correctly. A few short retries covers this window
    # without meaningfully slowing down the common case (already-registered
    # job def, resolves on the first attempt).
    _VCPU_LOOKUP_RETRIES = 3
    _VCPU_LOOKUP_BACKOFF_SECONDS = 1.0

    def _mnp_node_vcpus(self, job_definition: str) -> int | None:
        """Real per-node vCPU count declared on an MNP job definition's own
        ``resourceRequirements`` (confirmed live 2026-08-24: ``VCPU: "16"`` on
        the real ``smsvpctest-ray-mnp`` base def) -- used to size
        ``RAY_SHARDS_DEFAULT`` for a multi-node composite dispatch. A
        ``--num-cpus=0`` head's own ``os.cpu_count()`` (``RayProtocolRuntime``'s
        default fallback) under-counts real aggregate cluster capacity; reading
        the job definition's own declared resources is the real, existing
        source of truth for this, not a new guessed config value. Returns
        ``None`` on any lookup failure so the caller can safely leave
        ``RAY_SHARDS_DEFAULT`` unset (process-bigraph's own fallback still
        applies) rather than fail the whole submission over a sizing nicety.

        Retries a few times on a transient empty/missing result -- see
        ``_VCPU_LOOKUP_RETRIES`` above -- before giving up.
        """
        name, _, revision = job_definition.partition(":")
        batch = self._batch()
        for attempt in range(self._VCPU_LOOKUP_RETRIES):
            defs: list[dict[str, Any]] = []
            try:
                described = batch.describe_job_definitions(jobDefinitionName=name, revision=int(revision))
                defs = described.get("jobDefinitions", [])
            except Exception as exc:
                # Treated the same as an empty result below -- both just mean "not visible yet".
                logger.debug("describe_job_definitions attempt %d for %s failed: %s", attempt, job_definition, exc)

            if defs:
                ranges = defs[0].get("nodeProperties", {}).get("nodeRangeProperties", [])
                for nr in ranges:
                    for req in nr.get("container", {}).get("resourceRequirements", []):
                        if req.get("type") == "VCPU":
                            return int(float(req["value"]))
                return None

            if attempt < self._VCPU_LOOKUP_RETRIES - 1:
                time.sleep(self._VCPU_LOOKUP_BACKOFF_SECONDS * (attempt + 1))

        logger.warning(
            "Could not determine per-node vCPUs for %s after %d attempts; RAY_SHARDS_DEFAULT left unset",
            job_definition,
            self._VCPU_LOOKUP_RETRIES,
        )
        return None

    def _multi_node_composite_command(
        self,
        *,
        composite_id: str,
        params: dict[str, Any],
        steps: int,
        runner_s3_uri: str,
        n_shards_default: int | None,
    ) -> str:
        """Head-node command for a multi-node process-bigraph composite dispatch
        (backlog item 88).

        Reuses ``run_pbg.py``'s existing, already-generic ``--composite-id``/
        ``--overrides`` mode (``viva_api/compose/run_pbg.py`` -- the SAME runner
        ``stage_runner`` stages for the multi-generation batch_baseline path
        above) rather than a new script; ``composite_id`` is resolved through
        ``process_bigraph.composite_spec``'s own registry, so this works for
        ANY registered composite, not a hardcoded one.

        No new Ray multi-node "pre-connect" code is needed either -- confirmed
        empirically 2026-08-24 (backlog item 88, local Ray test): the entrypoint
        already exports ``RAY_ADDRESS`` on the head node before this command
        runs (``ray-batch-entrypoint.sh``), and process-bigraph's own
        ``RayProtocolRuntime.__init__`` already falls back to a bare
        ``ray.init(ignore_reinit_error=True, ...)`` when no explicit address is
        passed -- which already respects ``RAY_ADDRESS`` from the environment
        per Ray's own SDK (log-confirmed: "Using address ... set in the
        environment variable RAY_ADDRESS"). So a composite built with
        ``transport='ray'`` on the head attaches to the real multi-node cluster
        with zero code changes anywhere in this chain.
        """
        env = PBG_RUNNER_ENV
        if n_shards_default:
            env = f"{env} RAY_SHARDS_DEFAULT={int(n_shards_default)}"
        return (
            f"cd {V2ECOLI_DIR}"
            f" && aws s3 cp {runner_s3_uri} /tmp/run_pbg.py"
            f" && {env} python /tmp/run_pbg.py"
            f" --composite-id {shlex.quote(composite_id)}"
            f" --overrides {shlex.quote(json.dumps(params))} -n {int(steps)}"
        )

    async def _submit_multi_node_composite(
        self,
        ecoli_simulation: Simulation,
        database_service: DatabaseService,
        mnp_dispatch: dict[str, Any],
        *,
        correlation_id: str,
    ) -> JobId:
        """Submit a generic multi-node process-bigraph composite dispatch
        (backlog item 88) -- e.g. a colony composite distributed across N
        Ray-cluster nodes. This method never references any one composite by
        name; ``composite_id`` is resolved generically at runtime by
        ``run_pbg.py`` via ``process_bigraph.composite_spec``.

        Reuses ``_ensure_mnp_job_def``/``_submit_mnp`` UNCHANGED -- the exact
        same MNP job definition/queue the comparison-ensemble path above uses
        (``RayBatchOnDemandCE``/``ray-mnp``, confirmed live 2026-08-24: a real
        code comment on the no-placement-group standalone-queue routing logic
        already anticipates "colony sims" falling back to this queue for any
        genuine multi-node request). No new CDK job definition, no new
        compute environment.

        Submits ParCa first (1 node), then the composite (N nodes), gated on
        ParCa via the same ``depends_on`` pattern ``submit_ecoli_simulation_job``
        already uses -- any composite embedding a whole-cell process needs the
        same staged ParCa cache every other dispatch shape does.
        """
        composite_id = mnp_dispatch.get("composite_id")
        if not composite_id:
            raise ValueError("multi_node_dispatch.composite_id is required")
        num_nodes = int(mnp_dispatch.get("num_nodes") or 1)
        params = dict(mnp_dispatch.get("params") or {})
        steps = int(mnp_dispatch.get("steps") or 1)

        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {ecoli_simulation.simulator_id} not found")

        settings = get_settings()
        commit = simulator.git_commit_hash
        experiment_id = ecoli_simulation.config.experiment_id

        job_def = self._ensure_mnp_job_def(self._image_uri(commit), commit)
        n_shards_default = self._mnp_node_vcpus(job_def)
        if n_shards_default:
            n_shards_default *= num_nodes

        cache_s3 = self.cache_s3_uri(commit)
        runner_s3_uri = await self.stage_runner(experiment_id)

        base_tags = {
            "Project": "v2ecoli-multi-node-composite",
            "ExperimentId": str(experiment_id)[:255],
            "CompositeId": str(composite_id)[:255],
            "Commit": str(commit)[:12],
            "Team": getattr(settings, "cost_team_tag", None) or "covertlab",
        }

        parca_job_id = self._submit_mnp(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            num_nodes=1,
            ray_job_cmd=self._parca_command(),
            out_s3=cache_s3,
            out_dir=PARCA_CACHE_DIR,
            tags={**base_tags, "Phase": "parca"},
        )

        composite_job_id = self._submit_mnp(
            job_name=f"ray-mnp-composite-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            num_nodes=num_nodes,
            ray_job_cmd=self._multi_node_composite_command(
                composite_id=composite_id,
                params=params,
                steps=steps,
                runner_s3_uri=runner_s3_uri,
                n_shards_default=n_shards_default,
            ),
            out_s3=self._results_s3_uri(experiment_id),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            depends_on=[parca_job_id],
            tags={**base_tags, "Phase": "composite"},
        )
        logger.info(
            "Multi-node composite %s (%s): parca job %s -> composite job %s (%d nodes)",
            experiment_id,
            composite_id,
            parca_job_id,
            composite_job_id,
            num_nodes,
        )
        job_id = JobId.ray(composite_job_id)
        # Backlog item 88: record this dispatch's OWN HpcRun row, under the SAME
        # correlation_id the generic caller (run_simulation_workflow) will use for
        # its own idempotent-insert guard -- mirrors submit_chain_dispatch_job's
        # identical pattern (simulation_service_ray.py, chain-dispatch background
        # task), for the identical reason: this row needs a field
        # (multi_node_composite_id) a generic caller has no way to populate.
        # JobScheduler.update_multi_node_jobs polls rows with this field set,
        # completely disjoint from list_active_chain_campaigns's own
        # chain_n_generations-based query.
        await database_service.insert_hpcrun(
            job_id=job_id,
            job_type=JobType.SIMULATION,
            ref_id=ecoli_simulation.database_id,
            correlation_id=correlation_id,
            multi_node_composite_id=composite_id,
        )
        return job_id

    def chain_base_tags(self, *, simulation: Simulation, commit: str) -> dict[str, str]:
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

        ``injected_processes``/``variants`` (backlog item 93): passed straight
        through to ``_seed_generation_command`` — see that method's own
        docstring. ``JobScheduler`` is the real caller, re-deriving both from
        the campaign's own ``Simulation.config`` every tick (restart-safe,
        same as every other piece of per-tick state here).
        """
        job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        return self._submit_container(
            job_name=f"chain-seed{seed}-gen{generation_index}-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            job_cmd=self._seed_generation_command(
                seed=seed,
                generation_index=generation_index,
                experiment_id=experiment_id,
                runner_s3_uri=runner_s3_uri,
                injected_processes=injected_processes,
                variants=variants,
            ),
            out_s3=data_layout.RayLayout.seed_results_uri(experiment_id, seed),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            tags={**tags, "Seed": str(seed), "Generation": str(generation_index)},
            batch_client=batch_client,
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
    ) -> dict[int, str]:
        """Submit the SAME generation index for MULTIPLE seeds at once,
        TPS-paced below the account-wide ``SubmitJob`` rate limit (reuses
        ``_SubmitJobPacer`` + a dedicated retry-configured client — the same
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

        ``injected_processes``/``variants`` (backlog item 93): the SAME dict
        for every seed in this batch — one campaign, one config — forwarded to
        each seed's own ``submit_chain_generation`` call below.
        """
        pacer = _SubmitJobPacer()
        submit_client = boto3.client(
            "batch",
            region_name=get_settings().batch_region,
            config=Config(retries={"mode": "standard", "max_attempts": _SUBMIT_JOB_MAX_ATTEMPTS}),
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
            await database_service.insert_hpcrun(
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
        cache_s3 = self.cache_s3_uri(commit)
        base_tags = self.chain_base_tags(simulation=ecoli_simulation, commit=commit)
        container_job_def = self._ensure_container_job_def(self._image_uri(commit), commit)

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
        parca_job_id = self._submit_container(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=container_job_def,
            job_cmd=self._parca_command(new_genes=new_genes, bundle_overrides=bundle_overrides),
            out_s3=cache_s3,
            out_dir=PARCA_CACHE_DIR,
            tags={**base_tags, "Phase": "parca"},
        )

        await database_service.insert_hpcrun(
            job_id=JobId.ray(parca_job_id),
            job_type=JobType.SIMULATION,
            ref_id=ecoli_simulation.database_id,
            correlation_id=correlation_id or f"chain-campaign-{experiment_id}-{_rand_suffix()}",
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
        container_job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
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
        )

    def _multi_node_analysis_command(
        self,
        *,
        experiment_id: str,
        composite_id: str,
        history_uri: str,
        out_uri: str,
    ) -> str:
        """Build the "Analysis flush" DAG node's command for a generic
        multi-node process-bigraph composite dispatch (backlog item 88).

        Unlike ``_analysis_command`` (a fixed hive-parquet seed x generation
        sweep, v2ecoli-specific analysis modules), this points at a separate,
        generic entrypoint (``scripts/run_multi_node_analysis.py``) that
        dispatches through v2ecoli's own generic post-run mechanism
        (``v2ecoli.workflow.flush.run_flush``, the SAME one every other
        composite's cd1_*/ptools_* analyses use) rather than branching on
        ``composite_id`` at all -- a composite-specific renderer, if one is
        ever needed, is a new registered post-sim step (e.g.
        ``EmitterHistorySummary``), never a per-composite branch here. Reads
        whatever ``run_pbg.py`` staged at ``history_uri`` (an
        ``emitter_history.json`` gathered from the composite's own in-memory
        emitter when no file-backed emitter already shipped its own output --
        see ``run_pbg.run``'s own docstring -- falling back to
        ``final_state.json`` when no history was captured), and writes
        whatever ``run_flush`` renders + ``_manifest.json`` to ``out_uri``,
        matching the same S3-manifest contract ``GET /analyses/{id}/status``
        already probes for every other analysis kind.
        """
        return (
            f"cd {V2ECOLI_DIR}"
            f" && python scripts/run_multi_node_analysis.py"
            f" --composite-id {shlex.quote(composite_id)}"
            f" --history-uri {shlex.quote(history_uri)}"
            f" --out-uri {shlex.quote(out_uri)}"
            f" --experiment-id {shlex.quote(experiment_id)}"
        )

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
        """
        experiment_id = str(simulation.config.experiment_id)
        analysis_name = f"analysis-mnp-{experiment_id[:20]}-{_rand_suffix()}"
        results_uri = self._results_s3_uri(experiment_id).rstrip("/")
        result_uri = f"{results_uri}/analyses/{analysis_name}"
        settings = get_settings()
        container_job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
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
            analysis_job_id = self._submit_container(
                job_name=f"ray-mnp-analysis-{experiment_id}-{_rand_suffix()}"[:128],
                job_definition=container_job_def,
                job_cmd=self._multi_node_analysis_command(
                    experiment_id=experiment_id,
                    composite_id=composite_id,
                    history_uri=results_uri,
                    out_uri=result_uri,
                ),
                out_s3=self._results_s3_uri(experiment_id),
                out_dir=ANALYSIS_OUT_DIR,
                depends_on=None,
                depends_type=None,
                tags=tags,
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

    def get_batch_job_statuses(self, job_ids: list[str]) -> dict[str, JobStatus]:
        """Batched ``describe_jobs`` status lookup for arbitrary AWS Batch job
        ids, chunked by ``_DESCRIBE_JOBS_MAX_BATCH`` (100/call, the real API
        limit). An id absent from the response (not yet visible — brief
        eventual-consistency lag right after submission, or simply unknown) is
        simply absent from the returned mapping rather than raising; callers
        should treat a missing id as not-yet-terminal, the same discipline
        ``get_chain_campaign_result`` already established (and now reuses this
        exact helper for). Shared by that method and
        ``JobScheduler._advance_chain_campaign``'s per-seed poll (backlog item
        71 Phase 4), which needs the same batching for a campaign's
        ``chain_current_job_ids`` on every tick.
        """
        if not job_ids:
            return {}
        batch = self._batch()
        statuses: dict[str, JobStatus] = {}
        for i in range(0, len(job_ids), _DESCRIBE_JOBS_MAX_BATCH):
            chunk = job_ids[i : i + _DESCRIBE_JOBS_MAX_BATCH]
            response = batch.describe_jobs(jobs=chunk)
            for job in response.get("jobs", []):
                jid = job.get("jobId")
                if jid is not None:
                    statuses[str(jid)] = JobStatus.from_batch_state(str(job.get("status", "")))
        return statuses

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
        self._batch().terminate_job(jobId=job_id.value, reason="cancelled via sms-api")
        logger.info("Terminated Ray Batch job %s", job_id.value)

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
        for job_id in campaign.chain_current_job_ids or []:
            if job_id is None:
                continue
            await self.cancel_job(JobId.ray(job_id))

    @override
    async def close(self) -> None:
        pass
