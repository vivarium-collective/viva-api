"""The Nextflow dispatch mechanism: a Nextflow HEAD running as a Kubernetes Job, which submits one
AWS Batch task per lineage itself.

The second dispatch mechanism to become a strategy object (``docs/plan-core.md`` P2.1, PR 8), in the
shape ``MbpTrackedStrategy`` set, with the two things this mechanism needs that mbp-tracked did not:

* ``NextflowStrategy(batch, k8s, stage_runner=)``. ``k8s`` is a constructor argument of THIS
  strategy, not a member of any Protocol: it is the only mechanism whose head runs in the
  cluster (the head needs the ``batch-submit`` ServiceAccount's IRSA identity to submit its own
  tasks). ``None`` where no cluster access is configured; ``submit`` refuses then, as it always did.
* ``batch`` is a ``NextflowBatch``, not a ``ContainerSubmitter``: this mechanism submits NOTHING
  through the layer. It asks it for two image names (the science image for the tasks, the
  ``-submit`` image for the head) and for the engine, to terminate the tasks of a cancelled
  campaign -- children the head launched, which nobody here holds ids for.
* ``stage_runner`` is handed in as a callable: staging the generic runner is shared with two
  other mechanisms and the scheduler, so it is the service's; staging the Nextflow COMPILER is
  this mechanism's alone and is ``stage_render_nf`` below.

Five of the ten methods never touched ``self`` and are functions here. The other five are the
strategy's methods, verbatim but for how they spell what used to be on ``self``
(``scripts/prove_ray_carve_is_move_only.py``). ``reap_cancelled_campaign`` travels with the
mechanism because it IS the mechanism's -- it undoes what ``submit`` started; the scheduler still
reaches it through a one-line delegate on the service until P6.

SMS code, and it stays SMS code.
"""

import asyncio
import json
import logging
import re
import shlex
import tempfile
from collections.abc import Awaitable, Callable
from importlib import resources as _res
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, TypedDict

from viva_api.common.dispatch_validation import resolve_task_env, validate_nextflow_dispatch
from viva_api.common.hpc.k8s_job_service import K8sJobService
from viva_api.common.models import JobId
from viva_api.common.storage import data_layout
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import Simulation
from viva_api.simulation.ray import _seams
from viva_api.simulation.ray.batch_layer import _rand_suffix
from viva_api.simulation.ray.image_paths import V2ECOLI_CORE_BUILDER, V2ECOLI_DIR
from viva_core.backends.batch import BatchJobClient
from viva_core.events.events_env import with_events_env

if TYPE_CHECKING:
    # ``types-boto3`` and the kubernetes stubs are dev dependencies (annotations only): never
    # imported at runtime. (``kubernetes`` itself is imported where the head Job is built.)
    from kubernetes.client import V1Job
    from types_boto3_batch.type_defs import JobDetailTypeDef

logger = logging.getLogger(__name__)

# The Nextflow compiler, staged the same way and for the same reason (Batch caps a
# container override command at 8192 bytes).
_RENDER_NF_SRC = (_res.files("viva_api.compose") / "render_nf.py").read_text()


# Per-label resources for the Nextflow dispatch. NOT optional in practice: a
# dispatch that supplies none emits no `withLabel` block, so nf-amazon's
# auto-registered job definition takes ITS defaults (~1 GB, no timeout) and
# ParCa is killed with exit 137 before it does anything. Measured on simulation
# 355 -- three times, because `maxRetries` retried an OOM at the same size.
#
# `memory` is a Groovy CLOSURE (process-bigraph#205) that RAISES memory on 137
# specifically. Scaling on every failure would multiply memory for faults that
# have nothing to do with it; scaling on nothing makes the retries pointless.
# vEcoli's `scaledMemory` makes the same distinction.
#
# Sizes are anchored on what the chain/Ray paths actually run with -- their base
# job definitions request 16 vCPU / 60000 MB -- rather than invented. A caller
# may override any label via `nextflow_dispatch.resources`.
def _scaled_memory(base_gb: int) -> str:
    """Groovy: `base` normally, `base * attempt` after an OOM."""
    return f"{{ task.exitStatus == 137 ? {base_gb}.GB * task.attempt : {base_gb}.GB }}"


# Seconds the Nextflow head gets to shut down before SIGKILL. It terminates its
# own Batch tasks in that window -- the only party that knows exactly which
# tasks it submitted -- so this is the CORRECT path; the reap in cancel_job is
# the guarantee, not the mechanism.
NF_HEAD_TERMINATION_GRACE_SECONDS = 120

DEFAULT_NF_RESOURCES: dict[str, dict[str, object]] = {
    # ParCa is the memory-hungry one and the reason this default exists.
    "parca": {"cpus": 8, "memory": _scaled_memory(32), "time": "4 h"},
    # A lineage is the LONG one -- hours of simulated generations -- so `time`
    # matters here more than anywhere: it is the only bound on a runaway task
    # (plan-nextflow-dispatch §11.1), and Spot reclaim already retries 10x.
    "lineage": {"cpus": 4, "memory": _scaled_memory(16), "time": "12 h"},
    # The gather loads EVERY sweep's history into one DuckDB, so its memory
    # grows with N x M while a lineage's does not. Measured on simulation 574
    # (the first gather to complete, a 3x2): 16 GB was OOM-killed at 1 min
    # (exit 137) and the x-attempt retry at 32 GB finished in 2 min. A base that
    # only works through the retry is not a base -- and Run 4 is 336 lineages.
    "analysis": {"cpus": 4, "memory": _scaled_memory(32), "time": "2 h"},
}


def _merge_nf_resources(
    overrides: dict[str, dict[str, object]] | None,
) -> dict[str, dict[str, object]]:
    """DEFAULT_NF_RESOURCES with per-label overrides merged in.

    Merged rather than replaced, and merged per KEY within a label: overriding
    `lineage.time` must not drop `lineage.memory` and take the run back to
    nf-amazon's ~1 GB, which is the failure this whole default exists to stop.
    """
    merged = {label: dict(res) for label, res in DEFAULT_NF_RESOURCES.items()}
    for label, res in (overrides or {}).items():
        merged.setdefault(label, {}).update(res)
    return merged


def _command_belongs_to_campaign(command: str, campaign_stem: str) -> bool:
    """Is this Batch task in a work dir this run OWNS OUTRIGHT?

    EXACT match after sanitising, and that strictness is the point. The work-dir
    segment is the CAMPAIGN key; `campaign_stem` comes from the head Job name,
    which is the RUN. They are equal only when the run created the campaign --
    i.e. it was not a `resume_from`.

    A resumed run writes into the campaign it joined, whose tasks may belong to a
    DIFFERENT, still-running head. Reaping by campaign there would terminate
    another live run's work. So a resumed run reaps nothing and falls back to the
    grace period, which is the real fix anyway (viva-api#472): leaking a task is
    recoverable, destroying someone else's campaign is not.

    Sanitised both sides because `_nf_head_job_name` lowercases and replaces
    non-alphanumerics -- comparing raw finds nothing and the reap silently does
    nothing. A head name truncated at 63 chars also fails to match, and again
    skips rather than guesses.
    """
    if not campaign_stem:
        return False
    for segment in re.findall(r"/nextflow/work/([^/\s]+)", command):
        if re.sub(r"[^a-z0-9-]+", "-", segment.lower()).strip("-") == campaign_stem:
            return True
    return False


class NextflowDispatch(TypedDict, total=False):
    """The ``nextflow_dispatch`` block of a simulation config. Every key is optional to the type;
    ``validate_nextflow_dispatch`` (run at the API boundary and again in ``submit``) refuses a
    block without ``composite_id``, or a ``resume`` without ``resume_from``.

    Beyond those two rules and ``task_env`` this DECLARES the contract; nothing enforces it yet.
    ``params`` is the composite generator's own and is passed through unread.
    """

    composite_id: str
    params: dict[str, object] | None
    executor: str
    launch: bool
    resume: bool
    resume_from: str | None
    work_dir: str | None
    resources: dict[str, dict[str, object]] | None
    nextflow_args: list[str] | None
    task_env: dict[str, str]
    max_spot_attempts: int | None
    max_transfer_attempts: int | None
    max_retries: int | None


#: The retry counts a caller may tune (they ride into the awsbatch profile as they came).
_NF_RETRY_KEYS: tuple[Literal["max_spot_attempts", "max_transfer_attempts", "max_retries"], ...] = (
    "max_spot_attempts",
    "max_transfer_attempts",
    "max_retries",
)


class NextflowBatch(Protocol):
    """What the Nextflow mechanism asks of the Batch layer: two image names and the engine."""

    def image_uri(self, commit: str) -> str: ...

    def submit_image_uri(self, commit: str) -> str: ...

    def engine(self) -> BatchJobClient: ...


async def stage_render_nf(experiment_id: str) -> str:
    """Upload the Nextflow compiler beside the run_pbg runner; return its URI.

    Same staging idiom and the same reason as ``stage_runner``: Batch caps a
    container override command at 8192 bytes, so the script travels through S3
    rather than the command line. Deterministic from ``experiment_id``.
    """
    from viva_api.dependencies import get_file_service

    file_service = get_file_service()
    if file_service is None:
        raise RuntimeError("FileService not initialized; cannot stage render_nf.py to S3.")
    exp_prefix = data_layout.RayLayout.experiment_prefix(experiment_id)
    runner_key = f"{exp_prefix}/render_nf.py"
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
        tmp.write(_RENDER_NF_SRC)
        runner_local = tmp.name
    try:
        await file_service.upload_file(Path(runner_local), S3FilePath(s3_path=Path(runner_key)))
    finally:
        Path(runner_local).unlink(missing_ok=True)
    return data_layout.s3_uri(runner_key)


def nf_session_s3_uri(experiment_id: str) -> str:
    """Where this campaign's Nextflow session cache lives between dispatches.

    Beside the work dir and keyed the same way, because the two are only
    useful together: `-resume` matches a task by its hash in the SESSION and
    then reuses the outputs in the WORK DIR. Either one alone resumes
    nothing.
    """
    settings = _seams.get_settings()
    return f"s3://{settings.s3_work_bucket}/{settings.s3_work_prefix}/{experiment_id}/session"


def nf_generator_params(params: dict[str, object] | None, run_id: str) -> dict[str, object]:
    """The composite generator's parameters, with `experiment_id` defaulted to the run.

    `workflow_nf` defaults its own `experiment_id` to the literal string
    "workflow_nf", and that value is not cosmetic: it becomes the
    `experiment_id=` HIVE PARTITION the emitters write. Left unset, every
    campaign's parquet claims the same experiment_id -- observed on sim 392 as
    `sweep/workflow_nf/history/experiment_id=workflow_nf/...`.

    That is the same collision Chris and Alex spent a day chasing on the Ray
    path (sms-ecoli#235, viva-api#450), reappearing one layer down, inside the
    artifact rather than in its S3 prefix.

    A caller may still set it explicitly; this only supplies the default.
    """
    merged = dict(params or {})
    merged.setdefault("experiment_id", run_id)
    return merged


def render_nf_command(
    *,
    runner_s3_uri: str,
    composite_id: str,
    params: dict[str, object] | None,
    executor: str,
    launch: bool,
    outdir: str,
    pbg_runner_s3_uri: str,
    nf_params: dict[str, object] | None = None,
    resources: dict[str, dict[str, object]] | None = None,
    work_dir: str | None = None,
    resume: bool = False,
    stage_out_s3: str | None = None,
    session_s3: str | None = None,
    nextflow_args: list[str] | None = None,
) -> str:
    """The container command: fetch the compiler, render, optionally launch.

    ``--executor local`` is the intended FIRST check (Phase 3 of
    docs/plan-nextflow-dispatch.md): it answers "does render+launch work in our
    real image" separately from "does the awsbatch executor work", so a failure
    has one candidate cause rather than two.
    """
    overrides_flag = ""
    if params:
        overrides_flag = f" --overrides {shlex.quote(json.dumps(params))}"
    # ``params`` parameterizes the COMPOSITE generator; ``nf_params`` configures
    # NEXTFLOW itself (queue, image, region, work dir). Two different things that
    # both got called "params" upstream, so they are kept separate on the wire.
    nf_params_flag = ""
    if nf_params:
        nf_params_flag = f" --nf-params {shlex.quote(json.dumps(nf_params))}"
    # Verbatim passthrough to `nextflow run` -- `-dump-hashes` above all, which
    # prints each component of a task hash and is the only way to see WHY a
    # `-resume` did not match. A list, never a string, for the same reason
    # deploy() insists on one.
    nextflow_args_flag = ""
    if nextflow_args:
        nextflow_args_flag = f" --nextflow-args {shlex.quote(json.dumps(list(nextflow_args)))}"
    resources_flag = ""
    if resources:
        resources_flag = f" --resources {shlex.quote(json.dumps(resources))}"
    launch_flag = " --launch" if launch else ""
    resume_flag = " --resume" if resume else ""
    work_dir_flag = f" --work-dir {shlex.quote(work_dir)}" if work_dir else ""
    # On the Batch container path an entrypoint synced out_dir -> out_s3. A
    # K8s pod has no such entrypoint, so the command stages its own results
    # -- and MUST preserve the exit code, or a failed render would be
    # reported as a success by the trailing copy. `|| true` on the copy
    # keeps a staging hiccup from masking a run that actually worked.
    # `-resume` needs a durable SESSION, not just a durable work dir. Nextflow
    # keeps `.nextflow/history` and its cache DB in the LAUNCH directory --
    # here an ephemeral pod -- so a second dispatch starts with no record of
    # the first and reports, verbatim:
    #
    #   WARN: It appears you have never run this project before
    #         -- Option `-resume` is ignored
    #
    # and re-runs a ParCa whose output is sitting complete in the work dir.
    # Measured on simulation 359; plan-nextflow-dispatch risk 2 called it.
    # So the session is restored before the run and saved after, keyed by the
    # same experiment the work dir is.
    session_restore = ""
    session_save = ""
    if session_s3:
        session_restore = (
            f" && (aws s3 cp --recursive {shlex.quote(session_s3)} {shlex.quote(outdir)}/.nextflow"
            f" --only-show-errors 2>/dev/null || true)"
        )
        # Saved unconditionally -- a FAILED run's session is exactly the one a
        # `-resume` needs, so guarding this on success would defeat the point.
        session_save = (
            f" ; aws s3 cp --recursive {shlex.quote(outdir)}/.nextflow {shlex.quote(session_s3)}"
            f" --only-show-errors || true"
        )

    stage_out = ""
    if stage_out_s3:
        stage_out = (
            f" ; NF_EXIT=$?"
            f"{session_save}"
            f" ; aws s3 cp --recursive {shlex.quote(outdir)} {shlex.quote(stage_out_s3)}"
            f" --only-show-errors || true"
            f" ; exit $NF_EXIT"
        )
    # A trace is how a resumed run is told apart from a repeated one: a reused
    # task reports CACHED there and nowhere else.
    return (
        f"cd {V2ECOLI_DIR}"
        f"{session_restore}"
        f" && aws s3 cp {shlex.quote(runner_s3_uri)} /tmp/render_nf.py"
        # render_nf reuses run_pbg's resolver, and the simulator image has no
        # `viva_api` -- so the sibling it imports has to be staged too, into
        # the SAME directory. Staging only render_nf fails at import, after a
        # successful pull and a clean start.
        f" && aws s3 cp {shlex.quote(pbg_runner_s3_uri)} /tmp/run_pbg.py"
        f" && python /tmp/render_nf.py"
        f" --composite-id {shlex.quote(composite_id)}"
        f" --outdir {shlex.quote(outdir)}"
        f" --executor {shlex.quote(executor)}"
        f" --trace {shlex.quote(outdir)}/trace.csv"
        f"{overrides_flag}{nf_params_flag}{resources_flag}{launch_flag}{resume_flag}{work_dir_flag}"
        f"{nextflow_args_flag}"
        f"{stage_out}"
    )


def nf_head_job_name(experiment_id: str) -> str:
    """A DNS-1123 label: lowercase alphanumerics and '-', at most 63 chars.

    K8s rejects the underscores and uppercase that experiment ids carry, and
    it rejects them at create time -- so the sanitising happens here rather
    than surfacing as an ApiException on a dispatch that otherwise worked.
    """
    safe = re.sub(r"[^a-z0-9-]+", "-", experiment_id.lower()).strip("-")
    return f"nf-{safe}-{_rand_suffix()}"[:63].rstrip("-")


class NextflowStrategy:
    def __init__(
        self,
        batch: NextflowBatch,
        k8s: "K8sJobService | None",
        *,
        stage_runner: Callable[[str], Awaitable[str]],
    ) -> None:
        self._batch = batch
        self._k8s = k8s
        self._stage_runner = stage_runner

    def _awsbatch_nf_params(
        self, commit: str, experiment_id: str, task_env: dict[str, str] | None = None
    ) -> dict[str, object]:
        """The `awsbatch` profile's inputs, derived from settings -- never from the request.

        These name the deployment's queue, registry and work bucket, so they are
        server-side facts rather than something a caller supplies. Missing ones raise
        here, at dispatch, instead of surfacing as an AWS error minutes later inside a
        Batch container.

        ``container_image`` is the PLAIN science image, not the ``-submit`` head: only
        the process running ``nextflow run`` needs a JVM, and the task container needs
        only the AWS CLI to stage the S3 work dir (which v2ecoli's Dockerfile installs).

        ``container_env`` carries ``PYTHONPATH``, which is not optional: Nextflow moves
        the task's cwd off ``/app/v2ecoli``, and v2ecoli bare-imports ``scripts.*``
        throughout. viva-api#359 fixed this for the chain/Ray paths by way of
        ``PBG_RUNNER_ENV``, which a Nextflow-emitted process block has no idea exists.
        It rides in ``container_env`` rather than a dedicated profile directive because
        it is a fact about THIS image, not about AWS Batch -- process-bigraph#204 keeps
        the profile free of any one consumer's layout.
        """
        settings = _seams.get_settings()
        missing = [
            name
            for name, value in (
                ("batch_amd64_queue", settings.batch_amd64_queue),
                ("s3_work_bucket", settings.s3_work_bucket),
                ("ecr_account_id", settings.ecr_account_id),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                f"nextflow_dispatch executor='awsbatch' needs settings {', '.join(missing)}; "
                f"without them the profile renders with nulls and fails at submission."
            )
        return {
            "container_image": self._batch.image_uri(commit),
            "queue": settings.batch_amd64_queue,
            "aws_region": settings.batch_region,
            # GovCloud's S3 endpoint. Emitting it natively is what retires the `sed`
            # that injects the same line into vEcoli's config.template
            # (simulation_service_k8s.py).
            "s3_endpoint": f"https://s3.{settings.batch_region}.amazonaws.com",
            # task_env (sms-ecoli#166) rides in the same directive: the renderer emits
            # every entry as a `--env K=V` containerOption on the awsbatch profile's
            # process scope, so it reaches EVERY Batch task (parca, lineage,
            # analysis) -- not the K8s head, which needs none of it. The service's
            # own two keys are listed first so a request cannot shadow them (the
            # validator refuses those names anyway).
            "container_env": {"PYTHONPATH": V2ECOLI_DIR, "V2E_ROOT": V2ECOLI_DIR, **(task_env or {})},
            "work_dir": f"s3://{settings.s3_work_bucket}/{settings.s3_work_prefix}/{experiment_id}/work",
        }

    def _nf_head_job(self, job_name: str, experiment_id: str, commit: str, command: str) -> "V1Job":
        """The head Job, modelled on vEcoli's (``simulation_service_k8s.py``).

        ``backoff_limit=0`` deliberately: a half-finished Nextflow run is not
        safely restartable from scratch, and re-running the head would resubmit
        every task. Recovery is ``-resume`` on a NEW dispatch, which reuses the
        cached successful tasks -- that is the whole point of the session cache.
        """
        from kubernetes import client as k8s_client

        settings = _seams.get_settings()
        return k8s_client.V1Job(
            metadata=k8s_client.V1ObjectMeta(
                name=job_name,
                labels={
                    "app": "sms-api",
                    "job-type": "nextflow-head",
                    "experiment-id": re.sub(r"[^a-z0-9.-]+", "-", experiment_id.lower())[:63],
                },
            ),
            spec=k8s_client.V1JobSpec(
                backoff_limit=0,
                ttl_seconds_after_finished=86400,  # 24h, for log access after it ends
                template=k8s_client.V1PodTemplateSpec(
                    spec=k8s_client.V1PodSpec(
                        # The whole reason the head is here and not on Batch.
                        service_account_name="batch-submit",
                        restart_policy="Never",
                        # Nextflow's shutdown hook calls Batch TerminateJob once per
                        # in-flight task on SIGTERM. The default 30 s is not enough for
                        # a wide campaign, and the pod is SIGKILLed mid-way: measured on
                        # simulation 441, where 8 of 10 lineage tasks survived the
                        # cancel and ran for a further ~100 minutes, filling host disk
                        # until they broke the NEXT campaign (viva-api#472).
                        termination_grace_period_seconds=NF_HEAD_TERMINATION_GRACE_SECONDS,
                        containers=[
                            k8s_client.V1Container(
                                name="nextflow-head",
                                image=self._batch.submit_image_uri(commit),
                                command=["/bin/bash", "-c", command],
                                env=[
                                    k8s_client.V1EnvVar(name="AWS_DEFAULT_REGION", value=settings.batch_region),
                                    k8s_client.V1EnvVar(name="AWS_REGION", value=settings.batch_region),
                                    k8s_client.V1EnvVar(name="AWS_STS_REGIONAL_ENDPOINTS", value="regional"),
                                    k8s_client.V1EnvVar(name="NXF_ANSI_LOG", value="false"),
                                    # The head RESOLVES the composite, so it needs the
                                    # workspace's own core builder and import root -- the
                                    # generic core registers only process-bigraph's base
                                    # types, and a nested Composite then fails to realize
                                    # (`no link found at address: local:composite`).
                                    # These live in PBG_RUNNER_ENV for the chain/Ray
                                    # paths (#359), which a K8s Job never sees; §Phase 0
                                    # of the plan predicted exactly this for PYTHONPATH.
                                    k8s_client.V1EnvVar(name="PBG_CORE_BUILDER", value=V2ECOLI_CORE_BUILDER),
                                    k8s_client.V1EnvVar(name="PYTHONPATH", value=V2ECOLI_DIR),
                                    # The checkout root, for artefacts that ship in the
                                    # repo rather than the wheel (v2ecoli resolves
                                    # scripts/build_cache.py against it at RENDER time).
                                    k8s_client.V1EnvVar(name="V2E_ROOT", value=V2ECOLI_DIR),
                                ],
                                resources=k8s_client.V1ResourceRequirements(
                                    requests={"cpu": "500m", "memory": "1Gi"},
                                    limits={"cpu": "1", "memory": "2Gi"},
                                ),
                            ),
                        ],
                    ),
                ),
            ),
        )

    async def submit(
        self,
        ecoli_simulation: Simulation,
        database_service: DatabaseService,
        nf_dispatch: NextflowDispatch,
        *,
        correlation_id: str | None = None,
    ) -> JobId:
        """Compile a registered composite to a Nextflow workflow and run it.

        The third dispatch path (docs/plan-nextflow-dispatch.md). The HEAD runs
        as a **K8s Job**, not as a Batch container job, and that is a permission
        fact rather than a preference:

        * A Batch-hosted head runs under the job definition's ``jobRoleArn``,
          which on this stack is ``smsvpctest-ray-mnp-job`` -- S3 on the shared
          bucket and **no ``batch:*`` whatsoever**. It could pull its image,
          parse its config and start, then fail at the first task submission.
        * None of the four roles viva-api may ``iam:PassRole`` fixes that: two
          carry no ``batch:*``, and the two that do (the IRSA submit role, the
          compute role) trust the EKS OIDC provider and ``ec2.amazonaws.com``
          respectively, so neither can be an ECS/Batch task role at all.
        * A K8s Job with ``serviceAccountName: batch-submit`` inherits the IRSA
          identity that **is** allowed to submit -- the same one vEcoli's
          Nextflow head has always used (``simulation_service_k8s.py``).

        It also fits the workload: the head submits and waits, so it wants
        500m/1Gi for hours, not a 16-vCPU Batch instance held idle.
        """
        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {ecoli_simulation.simulator_id} not found")

        # Re-checked here, not only at the API boundary: this method is also
        # reachable directly, and it is the last point before a Job is created.
        validate_nextflow_dispatch(nf_dispatch)
        composite_id = nf_dispatch.get("composite_id")
        # The request's env, plus the run's PBG_* identity UNDER it (observability
        # plan D4a): every Batch task of this campaign gets the same trace id.
        task_env = with_events_env(
            resolve_task_env(ecoli_simulation.config, nf_dispatch),
            correlation_id=correlation_id,
            experiment_id=str(ecoli_simulation.experiment_id),
            sim_id=ecoli_simulation.database_id,
            backend="nextflow",
            settings=_seams.get_settings(),
        )

        commit = simulator.environment_key
        # The RUN's own id, read from the simulation record rather than from the
        # config. Since #450 force-assigns the config's `experiment_id` these agree,
        # but they agree by way of a coupling nothing here would notice breaking --
        # and this path keys a *cache* on it, so a silent re-collision would resume
        # one campaign's tasks into another's. The record is the authority (viva-api#439).
        run_id = str(ecoli_simulation.experiment_id)
        outdir = f"{V2ECOLI_DIR}/nf-render"

        # Which campaign's work dir and session cache this dispatch joins.
        #
        # Every dispatch gets its own by default: two campaigns sharing one prefix
        # share the cache that decides what gets recomputed, and because Nextflow
        # task hashes are content-derived, a `-resume` in one could legitimately
        # match and reuse a task from the other -- silently, reported as `Cached`.
        #
        # Which makes `-resume` explicit rather than implicit. Before #450 a config's
        # baked `experiment_id` collided every dispatch onto one prefix, so a resume
        # found the previous run's session by accident. Now it would find nothing,
        # and Nextflow does not treat that as an error -- it warns "Option `-resume`
        # is ignored" and silently re-runs the entire campaign at full cost. So a
        # resume must NAME the run it continues.
        resume = bool(nf_dispatch.get("resume", False))
        resume_from = nf_dispatch.get("resume_from")
        campaign_key = str(resume_from) if resume_from else run_id

        if self._k8s is None:
            raise RuntimeError(
                "nextflow_dispatch runs its head as a K8s Job (it needs the batch-submit "
                "ServiceAccount to submit Batch tasks), but k8s_job_namespace is not configured"
            )
        runner_s3_uri = await stage_render_nf(run_id)
        pbg_runner_s3_uri = await self._stage_runner(run_id)
        executor = str(nf_dispatch.get("executor", "local"))
        nf_params: dict[str, object] | None = None
        work_dir = nf_dispatch.get("work_dir")
        if executor == "awsbatch":
            nf_params = self._awsbatch_nf_params(commit, campaign_key, task_env=task_env)
            if task_env:
                logger.info("Nextflow dispatch %s: task_env passthrough %s", run_id, task_env)
            # Retry counts are the caller's to tune; the deployment's identity is not.
            for key in _NF_RETRY_KEYS:
                if nf_dispatch.get(key) is not None:
                    nf_params[key] = nf_dispatch[key]
            # `-work-dir` on the command line wins over the profile's `workDir`; both
            # are set so a config lifted out of the render dir and run by hand behaves
            # the same as the dispatch did.
            work_dir = work_dir or str(nf_params["work_dir"])  # a str already: built just above
            # Where `publishDir` copies task outputs. Without it a campaign that
            # exits 0 leaves its science in the work dir under a content hash --
            # measured at 633 MB across 43 objects, against 78 KB of render
            # artifacts in the results prefix (viva-api#439's verification).
            # The RUN's prefix, not the campaign's: a resumed run reuses another
            # run's cached TASKS, but its results are its own.
            nf_params["publish_dir"] = data_layout.RayLayout.results_uri(run_id).rstrip("/")
        command = render_nf_command(
            runner_s3_uri=runner_s3_uri,
            pbg_runner_s3_uri=pbg_runner_s3_uri,
            composite_id=str(composite_id),
            params=nf_generator_params(nf_dispatch.get("params"), run_id),
            executor=executor,
            launch=bool(nf_dispatch.get("launch", False)),
            outdir=outdir,
            nf_params=nf_params,
            # Defaults MERGED per label, not replaced wholesale: a caller who
            # overrides `lineage` must not silently lose parca's memory scaling.
            resources=_merge_nf_resources(nf_dispatch.get("resources")),
            work_dir=work_dir,
            resume=resume,
            stage_out_s3=data_layout.RayLayout.results_uri(run_id),
            session_s3=nf_session_s3_uri(campaign_key),
            nextflow_args=nf_dispatch.get("nextflow_args"),
        )
        # The Job names the RUN, never the campaign: each dispatch is its own pod,
        # and a name colliding with a live Job fails the create outright.
        job_name = nf_head_job_name(run_id)
        self._k8s.create_job(self._nf_head_job(job_name, run_id, commit, command))
        logger.info(
            "Created Nextflow head Job %s for run %s (campaign %s)",
            job_name,
            run_id,
            campaign_key,
        )
        # NOT JobId.ray: the value is a Job name, and NOT JobId.k8s either --
        # that tag also selects vEcoli's output layout on the download path.
        return JobId.k8s_nextflow(job_name)

    async def reap_cancelled_campaign(self, head_job_name: str) -> int | None:
        """Terminate Batch tasks that outlived a cancelled Nextflow head.

        Returns ``None`` when the head Job still exists -- it is inside its
        termination grace period and Nextflow's own shutdown hook is terminating
        tasks. Reaping THEN is worse than waiting: Nextflow treats each external
        termination as a task failure and resubmits it (viva-api#478's finding).
        The caller retries next tick. Otherwise returns how many were terminated.

        Tasks are identified by the campaign's S3 WORK DIR, which every task
        carries in its container command -- there is no per-campaign tag, and
        Batch job names are the process names, which repeat across campaigns.
        The campaign key is recovered from the head Job name
        (``_nf_head_job_name`` builds ``nf-<sanitised run id>-<rand>``); the
        EXACT-match rule in ``_command_belongs_to_campaign`` is what keeps a
        resumed run from reaping the campaign it joined.

        Scans EVERY configured task queue and paginates ``list_jobs`` (the API
        caps a page at 100; a Run-4-scale campaign is 336 tasks), the two
        defects of the inline reap this replaces.
        """
        if self._k8s is not None and self._k8s.get_job_status(head_job_name) is not None:
            return None
        stem = head_job_name[3:] if head_job_name.startswith("nf-") else head_job_name
        stem = stem.rsplit("-", 1)[0]  # drop _rand_suffix
        settings = _seams.get_settings()
        queues = [q for q in (settings.batch_amd64_queue, settings.batch_arm64_queue) if q]
        if not stem or not queues:
            return 0
        return await asyncio.to_thread(self._terminate_campaign_tasks, queues, stem)

    def _terminate_campaign_tasks(self, queues: list[str], stem: str) -> int:
        def _is_this_campaigns(job: "JobDetailTypeDef") -> bool:
            command = " ".join(job.get("container", {}).get("command", []) or [])
            return _command_belongs_to_campaign(command, stem)

        return self._batch.engine().terminate_matching(
            queues=queues, matches=_is_this_campaigns, reason=f"campaign {stem} cancelled via sms-api"
        )
