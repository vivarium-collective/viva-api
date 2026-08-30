"""Env-worker lifecycle — run a simulator's own image as a workbench env worker.

Step 2 of vivarium-workbench#942 / REFACTOR-PLAN §2A.8: a hosted workbench does
not build environments. It asks viva-api to run the **prebuilt image for a
`(repo, commit)`** as an env worker, and talks to it over the protocol in
``docs/env-worker-protocol.md``.

Why viva-api owns this and not the workbench: §2B.2 gives the workbench **no
cluster access at all** — viva-api holds every credential. Its service account
may create Jobs (not Pods, not Services), which is exactly enough.

**The workbench listens; the worker dials back.** The caller supplies where to
connect and the one-time token to prove; this module only puts them in the Job's
environment. That inversion is deliberate — with no Services RBAC there is no
stable DNS for a worker pod, and the alternative (reading ``status.podIP``)
would need pod-get plus a race against scheduling. It also means viva-api
discovers nothing: the workbench already knows its own address, so it says so.

The token is passed as an env var, never in ``command`` — a pod's command line
is world-readable via ``/proc/<pid>/cmdline``.
"""

from __future__ import annotations

import logging
import re
import secrets
from dataclasses import dataclass

from kubernetes import client as k8s_client

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.hpc.k8s_job_service import K8sJobService
from viva_api.config import get_settings

logger = logging.getLogger(__name__)

# A worker is interactive: it answers list/resolve/render-preview queries (spec
# §12). Simulations and heavy analyses are jobs elsewhere, so this pod stays
# small — sized like the workbench itself rather than like a compute node.
WORKER_CPU_REQUEST = "250m"
WORKER_CPU_LIMIT = "1"
WORKER_MEM_REQUEST = "512Mi"
WORKER_MEM_LIMIT = "2Gi"

# Backstop only. The workbench deletes its worker when the session ends; this
# catches the case where it never gets the chance (pod evicted, browser closed
# mid-call, api restarted). Long enough not to kill a live session, short enough
# that leaked workers do not accumulate on a node.
WORKER_TTL_SECONDS = 3600

# Where the initContainer stages the worker module, and where the worker writes.
MODULE_MOUNT = "/opt/env-worker"
SCRATCH_MOUNT = "/scratch"

# Only what a worker actually imports. The package is 211 MB, but 199 MB of that
# is the `loom` frontend bundle — copying it would bloat an emptyDir on every
# worker for nothing. `lib/` is what env_worker.py lazily imports at call time
# (study_spec, composite_lookup, readout_validation, ...), which is exactly the
# part a one-file mount would have missed: those imports fail *inside a call*,
# not at startup, so the worker would look healthy and then break.
_MODULE_PARTS = ("__init__.py", "env_worker.py", "lib")

# The dial-back target must be a bare host/IP and port. Rejected early and
# explicitly: this string ends up in a container's argv, so a caller-supplied
# value with shell metacharacters or a stray flag is not something to discover
# at runtime.
_HOST_RE = re.compile(r"^[A-Za-z0-9._-]{1,253}$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9]{16,128}$")


class EnvWorkerLaunchError(ValueError):
    """The launch request is not something we will turn into a Job."""


class EnvWorkerJobExists(RuntimeError):
    """A Job of this name already exists, or is still terminating."""


@dataclass(frozen=True)
class EnvWorkerHandle:
    """What the caller needs to poll and later delete this worker."""

    job_name: str
    image: str
    namespace: str


class EnvWorkerService:
    """Create, poll and delete env-worker Jobs. One Job == one worker."""

    def __init__(self, k8s: K8sJobService | None = None, *, namespace: str | None = None) -> None:
        settings = get_settings()
        self._namespace = namespace or settings.k8s_job_namespace
        self._k8s = k8s or K8sJobService(namespace=self._namespace)

    # -- image ---------------------------------------------------------------
    def image_for_commit(self, commit: str) -> str:
        """The prebuilt simulator image for a commit — the environment itself.

        This is the whole point of §2A.8: the environment is not rebuilt, it is
        *referenced*. If the image is missing the correct behavior is to fail
        here, naming the tag, rather than to fall back to something that would
        run the science under different dependencies.
        """
        settings = get_settings()
        if not settings.ecr_account_id:
            raise EnvWorkerLaunchError("ecr_account_id is unset; cannot resolve a worker image")
        registry = f"{settings.ecr_account_id}.dkr.ecr.{settings.batch_region}.amazonaws.com"
        return f"{registry}/{settings.ray_ecr_repository}:{commit}"

    # -- lifecycle -----------------------------------------------------------
    def start(
        self,
        *,
        commit: str,
        callback_host: str,
        callback_port: int,
        token: str,
        workspace: str | None = None,
        session_key: str | None = None,
    ) -> EnvWorkerHandle:
        """Create a Job that runs the image for ``commit`` as an env worker."""
        workspace = workspace or get_settings().env_worker_workspace_path
        commit = _validate_commit(commit)
        _validate_host(callback_host)
        _validate_port(callback_port)
        _validate_token(token)

        image = self.image_for_commit(commit)
        job_name = _job_name(commit, session_key)
        job = self._build_job(
            job_name=job_name,
            image=image,
            commit=commit,
            callback_host=callback_host,
            callback_port=callback_port,
            token=token,
            workspace=workspace,
            session_key=session_key,
        )
        try:
            self._k8s.create_job(job)
        except k8s_client.rest.ApiException as e:
            if e.status == 409:
                raise EnvWorkerJobExists(f"env-worker Job {job_name} already exists or is terminating") from e
            raise
        logger.info(
            "env-worker Job %s created (image %s, dial-back %s:%s)", job_name, image, callback_host, callback_port
        )
        return EnvWorkerHandle(job_name=job_name, image=image, namespace=self._namespace)

    def status(self, job_name: str) -> JobStatusInfo | None:
        """Job status, or ``None`` when the Job no longer exists."""
        return self._k8s.get_job_status(job_name)

    def logs(self, job_name: str) -> str | None:
        """Worker stdout/stderr. The protocol never rides these (spec §5), so
        they carry only diagnostics — which is exactly what is wanted when a
        worker fails to dial back."""
        return self._k8s.get_job_logs(job_name)

    def explain_exit(self, job_name: str) -> str | None:
        """Why the worker's pod stopped, in a few words — or ``None``.

        Used to turn "worker closed the connection" into something a caller can
        act on. Best-effort: the pod may be gone, and a missing answer must
        never turn a reported failure into a raised one.
        """
        try:
            return self._k8s.get_pod_termination(job_name)
        except Exception:
            logger.warning("could not read pod termination for env-worker Job %s", job_name)
            return None

    def stop(self, job_name: str) -> None:
        """Delete the Job (foreground propagation kills the pod). Idempotent."""
        try:
            self._k8s.delete_job(job_name)
        except k8s_client.rest.ApiException as e:
            if e.status != 404:
                raise
            logger.info("env-worker Job %s already gone", job_name)

    # -- job body ------------------------------------------------------------
    def _build_job(
        self,
        *,
        job_name: str,
        image: str,
        commit: str,
        callback_host: str,
        callback_port: int,
        token: str,
        workspace: str,
        session_key: str | None,
    ) -> k8s_client.V1Job:
        labels = {"app": "sms-api", "job-type": "env-worker", "commit": commit}
        if session_key:
            labels["session"] = _label_safe(session_key)

        env = [
            # Token via env, never argv: /proc/<pid>/cmdline is world-readable.
            k8s_client.V1EnvVar(name="VIVARIUM_ENV_WORKER_TOKEN", value=token),
            k8s_client.V1EnvVar(name="VIVARIUM_WORKBENCH_WORKSPACE", value=workspace),
            # The staged module, ahead of anything the image ships.
            k8s_client.V1EnvVar(name="PYTHONPATH", value=MODULE_MOUNT),
            # Keep scratch writes off the container layer.
            k8s_client.V1EnvVar(name="TMPDIR", value=SCRATCH_MOUNT),
            # UTF-8 mode, because the container sets no locale at all and so
            # Python's default text encoding here is ASCII. The workbench has
            # ~130 text reads/writes that pass no `encoding=`, and every one of
            # them raises UnicodeEncodeError the first time a study title
            # carries an em dash. Two of those sites were fixed by hand
            # (workbench 0.3.70, 0.3.71) before it was clear the fault was
            # environmental rather than local; this fixes the class.
            k8s_client.V1EnvVar(name="PYTHONUTF8", value="1"),
        ]
        return k8s_client.V1Job(
            metadata=k8s_client.V1ObjectMeta(name=job_name, labels=labels),
            spec=k8s_client.V1JobSpec(
                backoff_limit=0,  # a worker that failed to dial back should not
                # silently respawn against a dead listener
                ttl_seconds_after_finished=WORKER_TTL_SECONDS,
                template=k8s_client.V1PodTemplateSpec(
                    metadata=k8s_client.V1ObjectMeta(labels=labels),
                    spec=k8s_client.V1PodSpec(
                        service_account_name="batch-submit",
                        restart_policy="Never",
                        containers=[
                            k8s_client.V1Container(
                                name="env-worker",
                                image=image,
                                # Run under the IMAGE's interpreter: PATH puts the
                                # simulator's own venv first, so `python` here is
                                # the environment being asked about. That is the
                                # whole point — the worker must import workspace
                                # Python, and this image is where it lives.
                                command=["python", "-m", "vivarium_workbench.env_worker"],
                                args=["--connect-to", f"{callback_host}:{callback_port}", "--workspace", workspace],
                                env=env,
                                volume_mounts=[
                                    k8s_client.V1VolumeMount(
                                        name="worker-module", mount_path=MODULE_MOUNT, read_only=True
                                    ),
                                    k8s_client.V1VolumeMount(name="scratch", mount_path=SCRATCH_MOUNT),
                                ],
                                resources=k8s_client.V1ResourceRequirements(
                                    requests={"cpu": WORKER_CPU_REQUEST, "memory": WORKER_MEM_REQUEST},
                                    limits={"cpu": WORKER_CPU_LIMIT, "memory": WORKER_MEM_LIMIT},
                                ),
                            ),
                        ],
                        init_containers=[self._module_init_container()],
                        volumes=[
                            # Both ephemeral: the worker is stateless with respect
                            # to the scientific record. Specs travel in protocol
                            # messages (§2A.2's composite-code boundary rule), so
                            # nothing here needs the PVC — which is what lets a
                            # worker be scheduled on any node despite the PVC being
                            # ReadWriteOnce.
                            k8s_client.V1Volume(name="worker-module", empty_dir=k8s_client.V1EmptyDirVolumeSource()),
                            k8s_client.V1Volume(name="scratch", empty_dir=k8s_client.V1EmptyDirVolumeSource()),
                        ],
                    ),
                ),
            ),
        )

    def _module_init_container(self) -> k8s_client.V1Container:
        """Stage the worker module out of the workbench image into the shared volume.

        Delivered rather than installed: protocol §4 requires the workspace venv to
        carry no ``vivarium-workbench`` dependency, and the simulator image is built
        with ``--no-install-package vivarium-workbench``. Copying keeps that true
        while still giving the worker its own code.

        ``cp -R`` of a fixed subset, not the whole package — see ``_MODULE_PARTS``.
        """
        settings = get_settings()
        if not settings.env_worker_module_image:
            raise EnvWorkerLaunchError(
                "env_worker_module_image is unset; set it to the workbench image whose "
                "worker module this deployment should run"
            )
        srcs = " ".join(f"/app/vivarium-workbench/vivarium_workbench/{p}" for p in _MODULE_PARTS)
        dest = f"{MODULE_MOUNT}/vivarium_workbench"
        return k8s_client.V1Container(
            name="stage-worker-module",
            image=settings.env_worker_module_image,
            command=[
                "/bin/sh",
                "-c",
                # -R (not -a): ownership/timestamps are irrelevant here and
                # -a fails noisily across some overlay filesystems.
                f"set -e; mkdir -p {dest}; cp -R {srcs} {dest}/; test -f {dest}/env_worker.py",
            ],
            volume_mounts=[
                k8s_client.V1VolumeMount(name="worker-module", mount_path=MODULE_MOUNT),
            ],
            resources=k8s_client.V1ResourceRequirements(
                requests={"cpu": "100m", "memory": "128Mi"},
                limits={"cpu": "500m", "memory": "512Mi"},
            ),
        )


# -- validation --------------------------------------------------------------
def _validate_commit(commit: str) -> str:
    c = (commit or "").strip()
    if not re.fullmatch(r"[0-9a-fA-F]{7,40}", c):
        raise EnvWorkerLaunchError(f"commit must be a hex sha (7-40 chars), got {commit!r}")
    return c.lower()


def _validate_host(host: str) -> None:
    if not _HOST_RE.fullmatch(host or ""):
        raise EnvWorkerLaunchError(f"callback_host must be a bare host or IP, got {host!r}")


def _validate_port(port: int) -> None:
    if not isinstance(port, int) or not (1 <= port <= 65535):
        raise EnvWorkerLaunchError(f"callback_port out of range: {port!r}")


def _validate_token(token: str) -> None:
    # Alphanumeric by construction on the workbench side (token_hex). Enforced
    # here too: a token with a leading "-" would be read as a flag by any argv
    # consumer, and one with shell metacharacters has no business in a pod spec.
    if not _TOKEN_RE.fullmatch(token or ""):
        raise EnvWorkerLaunchError("token must be 16-128 alphanumeric characters")


def _label_safe(value: str) -> str:
    """K8s label values: alphanumeric, '-', '_', '.', max 63 chars."""
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "-", value)[:63]
    return cleaned.strip("-._") or "session"


def _job_name(commit: str, session_key: str | None) -> str:
    """A DNS-1123 name, unique per LAUNCH.

    Uniqueness comes from a random suffix, not from the session key: without one
    every launch for a commit reused ``env-worker-<commit>-shared``, so two
    sessions collided -- and so did a sequential relaunch, because a deleted Job
    lingers while foreground propagation finishes ("object is being deleted ...
    already exists"). Observed on dev. The session key stays as a *label* for
    correlation, which is what it is actually good for.
    """
    stem = _label_safe(session_key)[:8].lower() if session_key else "w"
    stem = re.sub(r"[^a-z0-9]", "0", stem) or "w"
    return f"env-worker-{commit[:7]}-{stem}-{secrets.token_hex(4)}"
