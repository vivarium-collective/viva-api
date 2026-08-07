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
from pathlib import Path
from typing import Any, override

import boto3

from sms_api.common.hpc.job_service import JobStatusInfo
from sms_api.common.hpc.local_task_service import LocalTaskService
from sms_api.common.models import JobBackend, JobId, JobStatus
from sms_api.common.simulator_defaults import DEFAULT_BRANCH, DEFAULT_REPO
from sms_api.common.storage import data_layout
from sms_api.common.storage.file_paths import S3FilePath
from sms_api.config import get_settings
from sms_api.simulation import batch_build
from sms_api.simulation.database_service import DatabaseService
from sms_api.simulation.github_repo import (
    fetch_config_template,
    fetch_latest_commit_hash,
    fetch_repo_discovery,
)
from sms_api.simulation.models import (
    CompositeEngine,
    ParcaDataset,
    RepoDiscovery,
    Simulation,
    SimulatorVersion,
    VecoliSource,
)
from sms_api.simulation.simulation_service import SimulationService

logger = logging.getLogger(__name__)

# The generic runner every Ray-Batch job (ensemble or compose) executes. Read once as a
# resource (same source sms_api.compose.simulation_service_ray stages for compose jobs) so
# the multi-generation batch path below dispatches through the identical mechanism instead
# of a v2ecoli-specific CLI script — see backlog items 26/27.
_RUNNER_SRC = (_res.files("sms_api.compose") / "run_pbg.py").read_text()

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
# Where the head writes the entrypoint's metrics report (uploaded as report.json).
REPORT_PATH = "/tmp/report.json"  # noqa: S108


def _rand_suffix() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


def _is_upstream_vecoli(composite: CompositeEngine | None) -> bool:
    """The pristine upstream-vEcoli engine (``--composite vecoli``).

    The single source of truth for the routing question "does this run need the
    separate upstream ParCa cache + ``--vecoli-source`` flag?" — used both to
    select the ParCa cache/command in ``submit_ecoli_simulation_job`` and to gate
    the ``--vecoli-source`` arg in ``_sim_command``.
    """
    return composite == "vecoli"


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
        tags: dict[str, str] | None = None,
    ) -> str:
        """Submit a Ray MNP job via boto3, mirroring sms-cdk scripts/ray_batch_submit.sh.

        Env targeting matters: the entrypoint runs ``stage_inputs`` and the periodic
        output sync on EVERY node, so the staging/output/log knobs must reach all
        nodes — the workers need the ParCa cache to run seeds and must ship their own
        zarr to S3. Only ``RAY_JOB_CMD`` (the driver) and ``RAY_REPORT_PATH`` are
        head-only. So the shared env goes on node 0 (``0:0``) and, when there are
        workers, also on the worker range (``1:``). Returns the AWS Batch job id.
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
            kwargs["dependsOn"] = [{"jobId": jid, "type": "SEQUENTIAL"} for jid in depends_on]
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

        Mirrors ``sms_api.compose.simulation_service_ray.ComposeSimulationServiceRay``'s
        own runner staging exactly (same source, same per-experiment S3 layout) -- the
        multi-generation batch path below downloads and runs it the identical way a
        compose job does. Staged (not embedded via heredoc) because AWS Batch caps a
        container override command at 8192 bytes.
        """
        from sms_api.dependencies import get_file_service

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
                # Standalone post-hoc analysis (scripts/run_standalone_analysis.py)
                # runs the same analysis_runner.run_analyses() directly against the
                # landed S3 sweep -- skip the composite's own inline flush here.
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
        return JobId.ray(sim_job_id)

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
