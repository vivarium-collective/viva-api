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

import copy
import importlib.resources as _res
import json
import logging
import random
import shlex
import string
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, override

import boto3
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


def _rand_suffix() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


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
class WavePollResult:
    """One wave's (array job's) poll outcome — backlog item 33.

    ``terminal`` means every child has reached a Batch-terminal state
    (SUCCEEDED or FAILED, per ``arrayProperties.statusSummary``); a wave with
    children still RUNNABLE/STARTING/RUNNING is NOT terminal and must be
    polled again next interval. Once terminal, ``succeeded_local_indices`` /
    ``failed_local_indices`` are the wave's own LOCAL array positions
    (0..array_size-1) — the caller (JobScheduler) remaps these to real seed
    numbers via the HpcRun row's ``wave_seed_indices``, since a wave's array
    positions are dense (0..len(seed_indices)-1) even though the seeds they
    represent may be sparse after a prior generation's attrition.
    """

    terminal: bool
    succeeded_local_indices: list[int] = field(default_factory=list)
    failed_local_indices: list[int] = field(default_factory=list)


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

    def _ensure_array_job_def(self, image: str, commit: str) -> str:
        """Return an Array job definition (name:revision) whose image is the commit's image.

        Verified directly against the real AWS Batch API (``aws batch submit-job
        help``): a plain container job's ``--container-overrides`` has no ``image``
        field (only EKS jobs' ``eksPropertiesOverride`` does) -- container jobs
        can't override the image per-submission either, same limitation as MNP,
        just for a different reason. So, symmetric with ``_ensure_mnp_job_def``,
        derive a per-commit job-def revision: describe the CDK base job def
        (``ray_array_job_definition``: roles, resources, retry strategy, log
        config), swap ONLY the container image to ``image``, and register it as
        ``<base>-<commit>``. An existing active revision already pointing at this
        image is reused, so resubmits don't churn revisions.
        """
        settings = get_settings()
        batch = self._batch()
        name = f"{settings.ray_array_job_definition}-{commit}"

        existing = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
        for jd in existing.get("jobDefinitions", []):
            if jd.get("containerProperties", {}).get("image") == image:
                return f"{name}:{jd['revision']}"

        base = batch.describe_job_definitions(jobDefinitionName=settings.ray_array_job_definition, status="ACTIVE")
        base_defs = base.get("jobDefinitions", [])
        if not base_defs:
            raise RuntimeError(f"Base Array job definition {settings.ray_array_job_definition!r} not found")
        base_def = max(base_defs, key=lambda d: d["revision"])
        container_properties = copy.deepcopy(base_def["containerProperties"])
        container_properties["image"] = image

        register_kwargs: dict[str, Any] = {
            "jobDefinitionName": name,
            "type": "container",
            "containerProperties": container_properties,
        }
        # Carry forward everything else the CDK base job def sets (retryStrategy,
        # platformCapabilities) -- register_job_definition does NOT inherit these
        # from an existing revision, it only creates exactly what's passed in.
        if base_def.get("retryStrategy"):
            register_kwargs["retryStrategy"] = base_def["retryStrategy"]
        if base_def.get("platformCapabilities"):
            register_kwargs["platformCapabilities"] = base_def["platformCapabilities"]

        response = batch.register_job_definition(**register_kwargs)
        logger.info("Registered Array job def %s:%s for image %s", name, response["revision"], image)
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

        response = self._batch().submit_job(**kwargs)
        batch_job_id = str(response["jobId"])
        logger.info(
            "Submitted Ray MNP job %s (id=%s, nodes=%d) to %s",
            job_name,
            batch_job_id,
            num_nodes,
            settings.ray_mnp_queue,
        )
        return batch_job_id

    def _submit_array(
        self,
        *,
        job_name: str,
        job_definition: str,
        array_size: int,
        array_job_cmd: str,
        out_s3: str,
        out_dir: str,
        stage_s3: str | None = None,
        stage_dir: str | None = None,
        depends_on: list[str] | None = None,
        tags: dict[str, str] | None = None,
    ) -> str:
        """Submit an AWS Batch ARRAY job: N independent single-seed children, no
        Ray cluster (see scripts/batch-array-entrypoint.sh, sms-cdk). Every child
        gets the SAME containerOverrides -- Batch injects only
        AWS_BATCH_JOB_ARRAY_INDEX differently per child, which ``array_job_cmd``
        resolves at container-start time (see ``_array_sim_command``). Env names
        are ``ARRAY_*`` (not ``RAY_*``) so the two dispatch paths' env vars can
        never be cross-wired if a caller mixes them up. Returns the AWS Batch job
        id (the array parent id; child status is queried per-index if needed).
        """
        settings = get_settings()
        env: list[dict[str, str]] = [
            {"name": "ARRAY_JOB_CMD", "value": array_job_cmd},
            {"name": "ARRAY_OUT_DIR", "value": out_dir},
            {"name": "ARRAY_OUT_S3", "value": out_s3},
            {"name": "ARRAY_REPORT_PATH", "value": REPORT_PATH},
        ]
        if stage_s3 and stage_dir:
            env.append({"name": "ARRAY_STAGE_S3", "value": stage_s3})
            env.append({"name": "ARRAY_STAGE_DIR", "value": stage_dir})
        if settings.ray_log_s3_prefix:
            env.append({"name": "ARRAY_LOG_S3_PREFIX", "value": settings.ray_log_s3_prefix})

        kwargs: dict[str, Any] = {
            "jobName": job_name,
            "jobQueue": settings.ray_array_queue,
            "jobDefinition": job_definition,
            "arrayProperties": {"size": array_size},
            "containerOverrides": {"environment": env},
        }
        if depends_on:
            # Plain job dependency (no "type") -- NOT "SEQUENTIAL", unlike _submit_mnp above.
            # AWS Batch rejects {"jobId": ..., "type": "SEQUENTIAL"} for a job that also sets
            # arrayProperties: real error, hit live 2026-08-06, "Job Id cannot be set when
            # dependency type is SEQUENTIAL". SEQUENTIAL is for an array job depending on
            # itself/other array-shaped dependents, not a targeted jobId wait -- it doesn't
            # apply here, we just need "don't start any array child until ParCa succeeds".
            kwargs["dependsOn"] = [{"jobId": jid} for jid in depends_on]
        if tags:
            kwargs["tags"] = tags
            kwargs["propagateTags"] = True

        response = self._batch().submit_job(**kwargs)
        batch_job_id = str(response["jobId"])
        logger.info(
            "Submitted Batch Array job %s (id=%s, size=%d) to %s",
            job_name,
            batch_job_id,
            array_size,
            settings.ray_array_queue,
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

    def _array_sim_command(
        self,
        n_generations: int,
        experiment_id: str,
        runner_s3_uri: str,
        base_seed_offset: int = 0,
    ) -> str:
        """Build one Array child's run_pbg.py command (the batch_baseline path).

        Every child runs exactly ONE seed (n_seeds=1), which v2ecoli's
        ``_resolve_parallel()`` deterministically routes to the sequential
        no-Ray code path (``len(branches) > 1`` is False for a 1-seed,
        no-variants request, regardless of the ``parallel`` setting) -- so an
        array child never needs a Ray cluster at all, verified directly
        against v2ecoli/workflow/run.py at the deployed commit.

        Each child's real seed is ``base_seed_offset + AWS_BATCH_JOB_ARRAY_INDEX``
        -- only known once the container starts (AWS Batch injects an identical
        containerOverrides.environment into every child; only
        AWS_BATCH_JOB_ARRAY_INDEX itself differs per child), so it can't be
        computed at submission time. The merge happens via a small ``python3 -c``
        invocation at container-start: BASE_SEED is arithmetic-only (digits, no
        quoting concerns); the static overrides are shlex-quoted (safe
        regardless of what ``experiment_id`` contains -- it is a caller-supplied,
        unconstrained string) and passed as argv, never string-spliced into the
        JSON -- json.loads/json.dumps do the merge exactly once, so there is no
        hand-rolled escaping to get wrong.
        """
        overrides = {
            "n_seeds": 1,
            "n_generations": int(n_generations),
            "cache_dir": PARCA_CACHE_DIR,
            "out_dir": SIM_OUT_DIR,
            "experiment_id": experiment_id,
            # A child sees only ITS OWN seed, so an inline flush here would run the
            # cross-seed scales against one seed, N times over. The whole-sweep
            # analysis is the DAG's third node instead -- see _analysis_command.
            "analyses": "none",
            "parallel": "",
        }
        static_overrides_json = shlex.quote(json.dumps(overrides))
        merge_py = 'import json,sys; d=json.loads(sys.argv[1]); d["base_seed"]=int(sys.argv[2]); print(json.dumps(d))'
        env = f"PBG_RESULTS_DIR={SIM_OUT_DIR} PBG_CORE_BUILDER={V2ECOLI_CORE_BUILDER}"
        return (
            f"BASE_SEED=$(({int(base_seed_offset)} + AWS_BATCH_JOB_ARRAY_INDEX))"
            f" && OVERRIDES=$(python3 -c '{merge_py}' {static_overrides_json} \"$BASE_SEED\")"
            f" && cd {V2ECOLI_DIR}"
            f" && aws s3 cp {runner_s3_uri} /tmp/run_pbg.py"
            f" && {env} python /tmp/run_pbg.py"
            f" --composite-id {V2ECOLI_BATCH_BASELINE_COMPOSITE_ID}"
            f' --overrides "$OVERRIDES" -n 1'
        )

    def _wave_sim_command(
        self,
        generation_index: int,
        experiment_id: str,
        runner_s3_uri: str,
        seed_indices: list[int],
    ) -> str:
        """Build one wave's Array child command: ONE seed's ONE generation
        (backlog item 33 — per-generation task decomposition, mirroring
        vEcoli-private's own Nextflow task granularity, ``runscripts/nextflow/
        sim.nf``). Sibling of ``_array_sim_command``, same inline-shell-plus-
        python3 per-child resolution pattern, extended for a SPARSE seed set:
        after a prior wave's attrition (Spot loss, OOM), a later wave's array
        positions are still dense (0..len(seed_indices)-1) but the REAL seeds
        they represent are not — so each child must look its real seed up in a
        lookup table (``seed_indices[AWS_BATCH_JOB_ARRAY_INDEX]``), not compute
        it via an offset (unlike ``_array_sim_command``'s BASE_SEED arithmetic,
        which only ever needs a contiguous range).

        CROSS-REPO CONTRACT GAP, confirmed against primary sources this
        session, NOT yet closed as of sms-ecoli PR #39 (``feat/per-generation-
        wave-dispatch``): the overrides below set ``initial_generation_index``
        (and the caller sets ``initial_carry_state_path``/``daughter_state_out
        _path`` via the merge script) as TOP-LEVEL ``--overrides`` keys, which
        ``run_pbg.py`` forwards to ``process_bigraph.composite_spec.
        CompositeSpec.to_document(overrides=...)`` -> ``_merged_params``. That
        method (verified directly, ``process_bigraph/composite_spec.py``)
        raises ``KeyError: "unknown override(s): [...]"`` for ANY key not in
        the composite's OWN declared ``@composite_generator(parameters={...})``
        dict — it is a strict allowlist, not a passthrough. PR #39 threaded
        the 3 new keys through ``BatchBaselineRunner.config_schema`` and
        ``meta_composite.py``'s ``_lineage_node`` (a DIFFERENT, multi-branch
        composite path), but NOT through ``v2ecoli/composites/batch_baseline.
        py``'s own ``parameters={...}`` dict / ``batch_baseline()`` signature /
        ``runner_config`` dict — which is the composite THIS command actually
        dispatches through (``V2ECOLI_BATCH_BASELINE_COMPOSITE_ID``). Until
        sms-ecoli adds the same 3 keys there (mirroring the exact pattern
        already applied to ``BatchBaselineRunner``/``meta_composite.py``), a
        real wave dispatch will fail fast with that KeyError at container
        start. This command's shape is correct against the INTENDED full
        contract; it is blocked on that small companion fix, not on anything
        wrong here.

        ``seed_indices`` is embedded as a bare JSON int array (at most 1000
        ints for the canonical dispatch shape — comfortably under Batch's
        8192-byte containerOverrides.command cap; see
        ``TestWaveSimCommand.test_stays_under_the_batch_command_size_cap`` for
        the actual byte count at n=1000). The per-seed checkpoint S3 URIs are
        NOT embedded (1000 full URIs would blow that cap) — the merge script
        reconstructs them itself using ``RayLayout.wave_state_uri``'s own
        format, mirrored here since the v2ecoli container has no import path
        back to this module.

        Generation 0 has no prior wave, so no ``initial_carry_state_path`` is
        set (LineageProcess defaults it to "", matching a fresh cell —
        ``initial_generation_index=0`` with no carry state is the validated,
        non-error case). Every generation writes its own daughter state out,
        including the final one — a harmless no-op read by nobody if the
        campaign ends there, cheaper than a special case to skip it.
        """
        overrides = {
            "n_seeds": 1,
            "n_generations": 1,
            "cache_dir": PARCA_CACHE_DIR,
            "out_dir": SIM_OUT_DIR,
            "experiment_id": experiment_id,
            "analyses": "none",
            "parallel": "",
            "initial_generation_index": int(generation_index),
        }
        static_overrides_json = shlex.quote(json.dumps(overrides))
        seed_indices_json = shlex.quote(json.dumps([int(s) for s in seed_indices]))
        state_prefix = shlex.quote(data_layout.RayLayout.wave_state_prefix(experiment_id))
        bucket = shlex.quote(get_settings().s3_work_bucket)
        # NOTE: every Python string literal inside this snippet MUST use double
        # quotes, never single — the whole snippet is embedded inside a
        # single-quoted shell argument (python3 -c '{merge_py}' below), so a
        # single quote in here would prematurely close the shell string.
        merge_py = (
            "import json,sys;"
            "d=json.loads(sys.argv[1]);"
            "seeds=json.loads(sys.argv[2]);"
            "seed=seeds[int(sys.argv[3])];"
            "bucket=sys.argv[4];"
            "prefix=sys.argv[5];"
            'gen=d["initial_generation_index"];'
            'd["base_seed"]=seed;'
            'd["daughter_state_out_path"]=f"s3://{bucket}/{prefix}/seed{seed}/gen{gen}.pkl";'
            'd["initial_carry_state_path"]=f"s3://{bucket}/{prefix}/seed{seed}/gen{gen - 1}.pkl" if gen>0 else "";'
            "print(json.dumps(d))"
        )
        env = f"PBG_RESULTS_DIR={SIM_OUT_DIR} PBG_CORE_BUILDER={V2ECOLI_CORE_BUILDER}"
        return (
            f"OVERRIDES=$(python3 -c '{merge_py}'"
            f' {static_overrides_json} {seed_indices_json} "$AWS_BATCH_JOB_ARRAY_INDEX" {bucket} {state_prefix})'
            f" && cd {V2ECOLI_DIR}"
            f" && aws s3 cp {runner_s3_uri} /tmp/run_pbg.py"
            f" && {env} python /tmp/run_pbg.py"
            f" --composite-id {V2ECOLI_BATCH_BASELINE_COMPOSITE_ID}"
            f' --overrides "$OVERRIDES" -n 1'
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
        sim_job_id: str,
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

        Best-effort by design, but never SILENT: the simulation job is already
        submitted and running by the time this is reached, so raising would orphan a
        real, expensive job. A submission failure is logged AND written to the
        analyses table as a FAILED row, so "the analysis never ran" is a visible state
        rather than an absence.
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
                depends_on=[sim_job_id],
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
        """
        if database_service is None:
            raise RuntimeError("DatabaseService is not available. Cannot submit Ray simulation job.")

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

        config = ecoli_simulation.config
        # SimulationConfig is a vEcoli passthrough (extra="allow"); the comparison
        # knobs are validated at the API boundary (Literal Query params) and ride
        # in as extra keys, so they're read here via getattr (present only when the
        # caller set them). ``composite``/``vecoli_source`` values are already
        # constrained by the endpoint's CompositeEngine/VecoliSource types.
        n_seeds = ecoli_simulation.num_seeds or getattr(config, "n_init_sims", None) or 1
        n_steps = getattr(config, "ray_n_steps", None) or settings.ray_n_steps
        chunk = getattr(config, "ray_chunk", None) or settings.ray_chunk
        # Optional two-engine comparison knobs (default phase0 ensemble when unset).
        composite = getattr(config, "composite", None)
        condition = getattr(config, "condition", None)
        max_generations = getattr(config, "max_generations", None)
        vecoli_source = getattr(config, "vecoli_source", None)
        # config.generations is a real (non-"extra") SimulationConfig field, unlike
        # the comparison knobs above -- read directly, not via getattr. Only the
        # non-composite path branches on it (see _sim_command).
        n_generations = int(config.generations or 1)

        # Engine-specific ParCa source: the pristine upstream wrapper (--composite
        # vecoli) stages an UPSTREAM-built simData (separate cache + build cmd);
        # every other engine stages the v2ecoli cache. Both ParCa and the sim use
        # the matching pair so the staged simData is consistent across all nodes.
        is_upstream = _is_upstream_vecoli(composite)
        cache_s3 = self._upstream_cache_s3_uri(commit) if is_upstream else self._cache_s3_uri(commit)
        parca_command = self._upstream_parca_command() if is_upstream else self._parca_command()

        # The multi-generation batch path dispatches through the generic run_pbg.py
        # runner (see _sim_command) instead of a hardcoded CLI script, so it alone
        # needs the runner staged to S3 first. Every other path is unaffected.
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

        # 2. Simulation ensemble, gated on ParCa, staging the cache.
        #
        # The canonical batch_baseline sweep (n_seeds independent seeds, only
        # the within-seed generation chain is sequential -- verified via
        # v2ecoli.workflow.run._resolve_parallel and run_seeds_parallel's pure
        # ray.remote() fan-out, zero actors/shared state) is Array-jobs-shaped:
        # dispatch it as N independent single-seed children instead of an MNP
        # Ray cluster. Everything else (phase0 ensemble, comparison-ensemble)
        # keeps using MNP -- those DO fan out via Ray actors internally. A
        # single-seed batch_baseline request (n_seeds<=1) also stays on MNP:
        # AWS Batch array jobs require size>=2, and there's no parallelism to
        # gain from Array-izing a single seed anyway. See the ray-vs-batch-
        # array-jobs-investigation decision: Array jobs for canonical, Ray-MNP
        # stays for colonies/anything needing real Ray coordination.
        is_array_eligible = composite is None and n_generations > 1 and int(n_seeds) > 1

        if is_array_eligible:
            if not runner_s3_uri:
                raise RuntimeError("runner_s3_uri is required for batch_baseline array dispatch")
            array_job_def = self._ensure_array_job_def(self._image_uri(commit), commit)
            sim_job_id = self._submit_array(
                job_name=f"array-sim-{experiment_id}-{_rand_suffix()}"[:128],
                job_definition=array_job_def,
                array_size=int(n_seeds),
                array_job_cmd=self._array_sim_command(n_generations, str(experiment_id), runner_s3_uri),
                out_s3=self._results_s3_uri(experiment_id),
                out_dir=SIM_OUT_DIR,
                stage_s3=cache_s3,
                stage_dir=PARCA_CACHE_DIR,
                depends_on=[parca_job_id],
                tags={**base_tags, "Phase": "sim"},
            )
            logger.info(
                "Array simulation %s: parca job %s -> sim job %s (%d array children)",
                experiment_id,
                parca_job_id,
                sim_job_id,
                int(n_seeds),
            )
        else:
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

        # 3. Analysis, gated on the simulation. The multi-generation batch_baseline
        #    sweep is the shape that emits the hive-parquet the ported cd1_*/ptools_*
        #    analyses read, so it is the shape that gets the third DAG node. The
        #    comparison-ensemble and phase0 paths write no such sweep and are
        #    deliberately untouched.
        #
        #    An Array sim job's parent id cannot be waited on under a SEQUENTIAL
        #    dependency type (real AWS Batch rejection) -- plain {"jobId": …} there.
        if composite is None and n_generations > 1:
            analysis_job_id = await self._submit_analysis_job(
                simulation=ecoli_simulation,
                database_service=database_service,
                job_definition=job_def,
                commit=commit,
                sim_job_id=sim_job_id,
                n_seeds=int(n_seeds),
                n_generations=n_generations,
                depends_type=None if is_array_eligible else "SEQUENTIAL",
                tags={**base_tags, "Phase": "analysis"},
            )
            logger.info(
                "Ray simulation %s: sim job %s -> analysis job %s",
                experiment_id,
                sim_job_id,
                analysis_job_id,
            )
        return JobId.ray(sim_job_id)

    def _wave_base_tags(self, *, simulation: Simulation, commit: str) -> dict[str, str]:
        """Cost-allocation tag base shared by every wave + the ParCa job that
        precedes them, mirroring ``submit_ecoli_simulation_job``'s ``base_tags``
        (composite/condition don't apply — wave dispatch is v2ecoli-only)."""
        settings = get_settings()
        return {
            "Project": "v2ecoli-comparison",
            "ExperimentId": str(simulation.config.experiment_id)[:255],
            "Engine": "v2ecoli",
            "Commit": str(commit)[:12],
            "Team": getattr(settings, "cost_team_tag", None) or "covertlab",
        }

    async def _submit_wave_and_maybe_analysis(
        self,
        *,
        simulation: Simulation,
        database_service: DatabaseService,
        commit: str,
        generation_index: int,
        seed_indices: list[int],
        total_n_seeds: int,
        n_generations: int,
        depends_on_parca: str | None,
        base_tags: dict[str, str],
    ) -> str:
        """Submit ONE wave (one generation's Array job) over ``seed_indices``,
        record it, and — if this is the FINAL generation — submit the analysis
        DAG node right behind it, depending on THIS wave's own array job id via
        the existing plain ``{"jobId": ...}`` shape (no ``"type"``; an array
        parent id is rejected by real AWS Batch under a SEQUENTIAL type, see
        ``_submit_array``). This piggybacks Batch's own ``dependsOn`` instead of
        polling to detect "did the last wave finish" — the analysis job simply
        waits on the wave that was JUST submitted, exactly like the existing
        single-shot Array path's sim -> analysis edge (item 24), just moved to
        fire at "final wave submitted" time instead of "the only wave
        submitted" time. Returns the wave's AWS Batch array job id.
        """
        experiment_id = simulation.config.experiment_id
        array_job_def = self._ensure_array_job_def(self._image_uri(commit), commit)
        runner_s3_uri = await self._stage_runner(experiment_id)
        cache_s3 = self._cache_s3_uri(commit)
        is_final_wave = generation_index >= n_generations - 1

        wave_job_id = self._submit_array(
            job_name=f"wave{generation_index}-sim-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=array_job_def,
            array_size=len(seed_indices),
            array_job_cmd=self._wave_sim_command(generation_index, experiment_id, runner_s3_uri, seed_indices),
            out_s3=self._results_s3_uri(experiment_id),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            depends_on=[depends_on_parca] if depends_on_parca else None,
            tags={**base_tags, "Phase": "sim", "Wave": str(generation_index)},
        )
        await database_service.insert_hpcrun(
            job_id=JobId.ray(wave_job_id),
            job_type=JobType.SIMULATION,
            ref_id=simulation.database_id,
            correlation_id=f"wave-{generation_index}-{experiment_id}-{_rand_suffix()}",
            wave_index=generation_index,
            wave_seed_indices=list(seed_indices),
        )
        logger.info(
            "Wave dispatch %s: generation %d submitted as array job %s (%d seeds)",
            experiment_id,
            generation_index,
            wave_job_id,
            len(seed_indices),
        )
        if is_final_wave:
            mnp_job_def = self._ensure_mnp_job_def(self._image_uri(commit), commit)
            analysis_job_id = await self._submit_analysis_job(
                simulation=simulation,
                database_service=database_service,
                job_definition=mnp_job_def,
                commit=commit,
                sim_job_id=wave_job_id,
                n_seeds=total_n_seeds,
                n_generations=n_generations,
                depends_type=None,
                tags={**base_tags, "Phase": "analysis"},
            )
            logger.info(
                "Wave dispatch %s: final wave %s -> analysis job %s",
                experiment_id,
                wave_job_id,
                analysis_job_id,
            )
        return wave_job_id

    async def submit_wave_dispatch_job(self, ecoli_simulation: Simulation, database_service: DatabaseService) -> JobId:
        """Kick off a wave-dispatch campaign (backlog item 33): submit ParCa (1
        node) + generation 0's Array wave over every requested seed.

        Sibling entrypoint to ``submit_ecoli_simulation_job``, same return
        contract (the tracked ``JobId`` is the just-submitted job — here,
        generation 0's array job) MINUS ``correlation_id``: unlike a single-shot
        dispatch (one job, one caller-supplied correlation_id, the caller's own
        follow-up ``insert_hpcrun`` call records it), a wave campaign spans
        MANY jobs over its lifetime that no single outer caller is present for
        (generations 1..N-1 are submitted later, from ``JobScheduler``'s poll
        loop) — so every wave, including generation 0, records its OWN HpcRun
        with its own freshly-generated correlation_id internally (see
        ``_submit_wave_and_maybe_analysis``), rather than splitting that
        bookkeeping between an external caller and this method.

        Subsequent generations are NOT submitted here: ``JobScheduler.
        update_wave_jobs`` polls generation 0 to terminal, computes survivors,
        and submits generation 1 itself (``submit_next_wave``) — repeating
        until the final generation, at which point the analysis DAG node rides
        along (see ``_submit_wave_and_maybe_analysis``). This mirrors AWS
        Batch's own confirmed limitation: an array job's ``dependsOn`` can't
        express "wait for all-terminal, not all-succeeded", so chaining
        generations natively would let one permanently-failed seed cascade-fail
        every later generation for the whole campaign.

        Only valid for the shape wave dispatch actually fixes: multiple seeds
        (AWS Batch array jobs require size>=2) across multiple generations. A
        single-generation or single-seed request has no attrition-across-
        generations problem to solve — use ``submit_ecoli_simulation_job``.
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
                "submit_wave_dispatch_job requires generations > 1 "
                "(use submit_ecoli_simulation_job for single-generation runs)"
            )
        if n_seeds < 2:
            raise ValueError("submit_wave_dispatch_job requires n_seeds > 1 (AWS Batch array jobs require size >= 2)")

        job_def_mnp = self._ensure_mnp_job_def(self._image_uri(commit), commit)
        cache_s3 = self._cache_s3_uri(commit)
        base_tags = self._wave_base_tags(simulation=ecoli_simulation, commit=commit)

        parca_job_id = self._submit_mnp(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=job_def_mnp,
            num_nodes=1,
            ray_job_cmd=self._parca_command(),
            out_s3=cache_s3,
            out_dir=PARCA_CACHE_DIR,
            tags={**base_tags, "Phase": "parca"},
        )
        wave0_job_id = await self._submit_wave_and_maybe_analysis(
            simulation=ecoli_simulation,
            database_service=database_service,
            commit=commit,
            generation_index=0,
            seed_indices=list(range(n_seeds)),
            total_n_seeds=n_seeds,
            n_generations=n_generations,
            depends_on_parca=parca_job_id,
            base_tags=base_tags,
        )
        logger.info(
            "Wave dispatch %s: parca job %s -> wave 0 job %s (%d seeds, %d generations)",
            ecoli_simulation.config.experiment_id,
            parca_job_id,
            wave0_job_id,
            n_seeds,
            n_generations,
        )
        return JobId.ray(wave0_job_id)

    async def submit_next_wave(
        self,
        *,
        simulation: Simulation,
        database_service: DatabaseService,
        commit: str,
        generation_index: int,
        seed_indices: list[int],
        total_n_seeds: int,
        n_generations: int,
    ) -> str:
        """Submit wave ``generation_index`` for the given SURVIVOR
        ``seed_indices`` (backlog item 33). No ParCa dependency — the cache is
        already in S3 from ``submit_wave_dispatch_job``'s generation-0 run, and
        ParCa already SUCCEEDED long before this call, so there is no ordering
        left to enforce. Called exclusively by ``JobScheduler.update_wave_jobs``
        once the PREVIOUS generation's wave reaches a terminal state. Returns
        the new wave's AWS Batch array job id.
        """
        base_tags = self._wave_base_tags(simulation=simulation, commit=commit)
        return await self._submit_wave_and_maybe_analysis(
            simulation=simulation,
            database_service=database_service,
            commit=commit,
            generation_index=generation_index,
            seed_indices=seed_indices,
            total_n_seeds=total_n_seeds,
            n_generations=n_generations,
            depends_on_parca=None,
            base_tags=base_tags,
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

    def _list_array_child_indices(self, array_job_id: str, status: str) -> list[int]:
        """Every child array position currently in ``status`` (paginated).

        Live AWS semantics verified against a real job history (sim139's parent
        array job, 2026-08-08): ``list_jobs(arrayJobId=..., jobStatus=
        "SUCCEEDED")`` returned exactly the real surviving indices,
        ``jobStatus="FAILED"`` returned exactly the rest, both cross-checked
        against ``describe_jobs``'s own ``arrayProperties.statusSummary``
        counts, union = the full contiguous index range with zero gaps or
        duplicates. Confirmed a second time on an all-failed job (a
        zero-match status returns a clean empty list, not an error).
        """
        batch = self._batch()
        indices: list[int] = []
        kwargs: dict[str, Any] = {"arrayJobId": array_job_id, "jobStatus": status}
        while True:
            response = batch.list_jobs(**kwargs)
            for job in response.get("jobSummaryList", []):
                index = job.get("arrayProperties", {}).get("index")
                if index is not None:
                    indices.append(int(index))
            next_token = response.get("nextToken")
            if not next_token:
                break
            kwargs["nextToken"] = next_token
        return sorted(indices)

    def get_wave_result(self, array_job_id: str) -> WavePollResult:
        """Poll ONE wave's array job: terminal-ness + which local array
        positions SUCCEEDED vs FAILED (backlog item 33).

        "Terminal" means every child has reached a Batch-terminal state — the
        array PARENT itself may report SUCCEEDED (every child succeeded) or
        FAILED (at least one child permanently exhausted retries — AWS Batch
        flips the parent to FAILED the instant that happens, per
        ``job_states.html``/``array_jobs.html``), but BOTH are "wave finished"
        from this orchestrator's perspective, not an error: some seeds dying to
        Spot reclamation or a transient OOM is expected economics, not a bug.
        The caller (``JobScheduler._advance_wave``) is what decides whether
        surviving seeds exist to carry forward.
        """
        response = self._batch().describe_jobs(jobs=[array_job_id])
        jobs = response.get("jobs", [])
        if not jobs:
            raise RuntimeError(f"Batch array job {array_job_id} not found")
        array_properties = jobs[0].get("arrayProperties", {})
        size = int(array_properties.get("size") or 0)
        status_summary: dict[str, int] = array_properties.get("statusSummary") or {}
        terminal_count = int(status_summary.get("SUCCEEDED", 0)) + int(status_summary.get("FAILED", 0))
        if size <= 0 or terminal_count < size:
            return WavePollResult(terminal=False)
        return WavePollResult(
            terminal=True,
            succeeded_local_indices=self._list_array_child_indices(array_job_id, "SUCCEEDED"),
            failed_local_indices=self._list_array_child_indices(array_job_id, "FAILED"),
        )

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
