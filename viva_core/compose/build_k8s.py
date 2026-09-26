"""A composite's container built in a Kubernetes Job (UConn track, 2026-09-26): the ``k8s``
value of ``compose_build_backend``, the ``ContainerBuild`` implementation the SLURM compose
service is handed when the HPC's nodes cannot build (no subuid entry for the service user).

The shape is vcell-fluxcd's ``vcell-sif-prepull-job`` with one difference that decides everything:
our definitions have a ``%post`` (``pip install``), and running that needs mount namespaces, which
an unprivileged container's root does not have -- ``CAP_SYS_ADMIN`` was not enough, proot is not
used by Apptainer 1.3 for definition builds, and a root-mapped user namespace could not start. So
the build runs in a **privileged init container** that mounts nothing but an ``emptyDir``, and the
copy onto the shared filesystem is the **unprivileged main container's**, running as the service
user with the filesystem mounted: the writer IS that user, so root squash is respected rather than
worked around, and the privileged process never sees the filesystem the SLURM nodes read. Init
containers run to completion in order, so a failed build fails the pod, and the Job, with no
coordination. Measured on ``sms-api-rke-dev``, 2026-09-26 (Jim's shape).

The definition file is already on the shared filesystem -- the service uploads it over SSH before
asking for the build -- and the init container needs it too, so it gets the same mount READ-ONLY.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from textwrap import dedent

from kubernetes import client as k8s_client

from viva_core.backends.k8s_job_service import K8sJobService
from viva_core.models import JobBackend
from viva_core.settings import CoreSettings

logger = logging.getLogger(__name__)

WORK_DIR = "/work"
BUILD_LABEL = "compose-build"


def _build_script(definition: Path) -> str:
    return dedent(f"""\
        set -eu
        apptainer --version
        apptainer build {WORK_DIR}/image.sif "{definition}"
        ls -la {WORK_DIR}/image.sif
        """)


def _copy_script(container: Path) -> str:
    """The copy onto the shared filesystem, written beside the target and renamed so a reader never
    sees a partial image."""
    return dedent(f"""\
        set -eu
        id
        cp {WORK_DIR}/image.sif "{container}.part" && mv "{container}.part" "{container}"
        ls -la "{container}"
        echo "Finished building container."
        """)


class K8sContainerBuild:
    """``ContainerBuild`` over a Kubernetes Job. ``k8s`` is the namespace's Job client; the Job's
    volume, image and the copy's user come from the settings."""

    backend = JobBackend.K8S

    def __init__(self, k8s: K8sJobService, settings: CoreSettings) -> None:
        if not settings.compose_build_pvc_claim or not settings.compose_build_pvc_mount_path:
            raise ValueError(
                "compose_build_backend=k8s needs compose_build_pvc_claim and compose_build_pvc_mount_path: "
                "the shared filesystem the SLURM nodes read images from, mounted in the build Job"
            )
        self._k8s = k8s
        self._settings = settings

    def job(self, job_name: str, definition: Path, container: Path) -> k8s_client.V1Job:
        """The Job, as a value: what a test reads."""
        s = self._settings
        labels = {"app": BUILD_LABEL, "compose-build": job_name}
        shared = k8s_client.V1VolumeMount(name="shared", mount_path=s.compose_build_pvc_mount_path)
        if s.compose_build_pvc_sub_path:
            shared.sub_path = s.compose_build_pvc_sub_path
        shared_read_only = k8s_client.V1VolumeMount(
            name="shared", mount_path=s.compose_build_pvc_mount_path, sub_path=shared.sub_path, read_only=True
        )
        work = k8s_client.V1VolumeMount(name="work", mount_path=WORK_DIR)
        groups = [int(g) for g in s.compose_build_supplemental_groups.replace(" ", "").split(",") if g]
        return k8s_client.V1Job(
            metadata=k8s_client.V1ObjectMeta(name=job_name, labels=labels),
            spec=k8s_client.V1JobSpec(
                backoff_limit=0,  # a failed build is reported, not retried behind the caller's back
                ttl_seconds_after_finished=86400,
                active_deadline_seconds=s.compose_build_timeout_seconds,
                template=k8s_client.V1PodTemplateSpec(
                    metadata=k8s_client.V1ObjectMeta(labels=labels),
                    spec=k8s_client.V1PodSpec(
                        restart_policy="Never",
                        node_selector={"vlan": "internal"},
                        # the pod's identity is the SERVICE USER's; the init container alone escalates
                        security_context=k8s_client.V1PodSecurityContext(
                            fs_group=s.compose_build_run_as_gid or None, supplemental_groups=groups or None
                        ),
                        init_containers=[
                            k8s_client.V1Container(
                                name="build",
                                image=s.compose_build_image,
                                command=["/bin/bash", "-c", _build_script(definition)],
                                security_context=k8s_client.V1SecurityContext(privileged=True, run_as_user=0),
                                volume_mounts=[shared_read_only, work],
                            )
                        ],
                        containers=[
                            k8s_client.V1Container(
                                name="copy",
                                image=s.compose_build_image,
                                command=["/bin/bash", "-c", _copy_script(container)],
                                security_context=k8s_client.V1SecurityContext(
                                    privileged=False,
                                    run_as_user=s.compose_build_run_as_uid,
                                    run_as_group=s.compose_build_run_as_gid,
                                    run_as_non_root=bool(s.compose_build_run_as_uid),
                                    allow_privilege_escalation=False,
                                ),
                                volume_mounts=[shared, work],
                            )
                        ],
                        volumes=[
                            k8s_client.V1Volume(
                                name="shared",
                                persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                                    claim_name=s.compose_build_pvc_claim
                                ),
                            ),
                            k8s_client.V1Volume(name="work", empty_dir=k8s_client.V1EmptyDirVolumeSource()),
                        ],
                    ),
                ),
            ),
        )

    async def __call__(self, job_name: str, definition: Path, container: Path) -> str:
        """Create the Job; the handle is its name (what the monitor asks Kubernetes about)."""
        await asyncio.to_thread(self._k8s.create_job, self.job(job_name, definition, container))
        logger.info("Submitted container build %s as a Kubernetes Job (%s -> %s)", job_name, definition, container)
        return job_name
