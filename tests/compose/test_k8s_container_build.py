"""A composite's container built in a Kubernetes Job (U-track, 2026-09-26): the Job the SLURM
compose service asks for, the monitor's poll of it, and the dispatch that waits on the ROW rather
than on a SLURM id -- so a build with no SLURM id is waited on the same way.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from viva_core.backends.job_service import JobStatusInfo
from viva_core.compose.build_k8s import K8sContainerBuild
from viva_core.compose.job_monitor import ComposeJobMonitor
from viva_core.compose.models import ComposeHpcRun, ComposeJobStatus, ComposeJobType
from viva_core.compose.simulation_service_hpc import ComposeSimulationServiceHpc
from viva_core.models import JobBackend, JobId, JobStatus
from viva_core.settings import CoreSettings

DEFINITION = Path("/projects/SMS/viva_core/dev/compose/images/abc.def")
CONTAINER = Path("/projects/SMS/viva_core/dev/compose/images/abc.sif")


def _settings(**overrides: Any) -> CoreSettings:
    fields: dict[str, Any] = {
        "compose_build_backend": "k8s",
        "compose_build_pvc_claim": "vivarium-home-pvc",
        "compose_build_pvc_mount_path": "/projects/SMS",
        "compose_build_pvc_sub_path": "SMS",
        "compose_build_run_as_uid": 17163,
        "compose_build_run_as_gid": 10000,
        "compose_build_supplemental_groups": "10274,10281,10269",
    }
    fields.update(overrides)
    return CoreSettings(_env_file=None, **fields)  # type: ignore[call-arg]


def _must(value: Any) -> Any:
    """The kubernetes client types every field Optional; a test reads what it set."""
    assert value is not None
    return value


# --------------------------------------------------------------------------- the Job


def test_the_build_is_a_privileged_init_container_and_the_copy_an_unprivileged_one_as_the_service_user() -> None:
    """Jim's shape: the privileged process mounts nothing but the emptyDir (and the shared filesystem
    READ-ONLY, for the definition); the writer onto the shared filesystem IS the service user."""
    job = K8sContainerBuild(MagicMock(), _settings()).job("compose-build-abc", DEFINITION, CONTAINER)
    spec = _must(job.spec)
    pod = _must(_must(spec.template).spec)
    assert _must(job.metadata).name == "compose-build-abc" and spec.backoff_limit == 0
    assert spec.active_deadline_seconds == 1800 and spec.ttl_seconds_after_finished == 86400
    (build,) = _must(pod.init_containers)
    (copy,) = _must(pod.containers)
    assert build.image == copy.image == "ghcr.io/apptainer/apptainer:1.3.6"
    # the build: privileged (a definition's %post needs mount namespaces), root, the shared filesystem read-only
    assert _must(build.security_context).privileged is True and _must(build.security_context).run_as_user == 0
    build_mounts = {m.name: m for m in _must(build.volume_mounts)}
    assert build_mounts["shared"].read_only is True and build_mounts["shared"].sub_path == "SMS"
    assert build_mounts["work"].mount_path == "/work"
    assert f'apptainer build /work/image.sif "{DEFINITION}"' in _must(build.command)[-1]
    # the copy: unprivileged, the service user, the shared filesystem writable
    sc = _must(copy.security_context)
    assert sc.privileged is False and sc.run_as_user == 17163 and sc.run_as_group == 10000
    assert sc.run_as_non_root is True and sc.allow_privilege_escalation is False
    copy_mounts = {m.name: m for m in _must(copy.volume_mounts)}
    assert copy_mounts["shared"].mount_path == "/projects/SMS" and not copy_mounts["shared"].read_only
    script = _must(copy.command)[-1]
    assert "setpriv" not in script and "apptainer build" not in script
    assert f'cp /work/image.sif "{CONTAINER}.part" && mv "{CONTAINER}.part" "{CONTAINER}"' in script
    # the pod's identity is the service user's groups, so the emptyDir and the tree are writable
    pod_sc = _must(pod.security_context)
    assert pod_sc.fs_group == 10000 and pod_sc.supplemental_groups == [10274, 10281, 10269]
    volumes = {v.name: v for v in _must(pod.volumes)}
    assert _must(volumes["shared"].persistent_volume_claim).claim_name == "vivarium-home-pvc"


def test_root_copies_when_no_service_user_is_named() -> None:
    job = K8sContainerBuild(MagicMock(), _settings(compose_build_run_as_uid=0, compose_build_run_as_gid=0)).job(
        "j", DEFINITION, CONTAINER
    )
    (copy,) = _must(_must(_must(_must(job.spec).template).spec).containers)
    sc = _must(copy.security_context)
    assert sc.run_as_user == 0 and sc.run_as_non_root is False


def test_the_build_needs_the_shared_filesystem_named() -> None:
    with pytest.raises(ValueError, match="compose_build_pvc_claim"):
        K8sContainerBuild(MagicMock(), _settings(compose_build_pvc_claim=""))


@pytest.mark.asyncio
async def test_submitting_creates_the_job_and_hands_back_its_name() -> None:
    k8s = MagicMock()
    build = K8sContainerBuild(k8s, _settings())
    assert build.backend is JobBackend.K8S
    assert await build("compose-build-abc", DEFINITION, CONTAINER) == "compose-build-abc"
    (created,) = _must(k8s.create_job.call_args).args
    assert created.metadata.name == "compose-build-abc"


# --------------------------------------------------------------------------- the service


@pytest.mark.asyncio
async def test_the_slurm_service_uses_the_build_hook_and_tags_the_row_k8s(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The definition still goes onto the shared filesystem over SSH; the build itself is the hook's,
    and the row says k8s with the Job's name, no SLURM id."""
    import random

    from viva_core.compose.container_def import ContainerizationFileRepr

    monkeypatch.setattr(random, "choices", lambda *a, **k: list("Bf6f0"))  # the suffix UConn's API refused
    from viva_core.compose.models import ComposeSimulatorVersion
    from viva_core.settings import get_core_settings

    settings = get_core_settings()
    saved = settings.compose_image_base_path, settings.slurm_log_base_path
    settings.compose_image_base_path = str(tmp_path / "images")
    try:
        ssh = MagicMock()
        ssh.scp_upload = AsyncMock()
        session = MagicMock()
        session.__aenter__ = AsyncMock(return_value=ssh)
        session.__aexit__ = AsyncMock(return_value=None)
        sessions = MagicMock()
        sessions.session.return_value = session

        hook = AsyncMock(return_value="compose-build-abc")
        hook.backend = JobBackend.K8S
        inserted: dict[str, Any] = {}

        async def insert_hpcrun(**kwargs: Any) -> ComposeHpcRun:
            inserted.update(kwargs)
            return ComposeHpcRun(
                database_id=9,
                slurmjobid=kwargs["slurmjobid"],
                job_id_ext=kwargs["job_id_ext"],
                job_backend=kwargs["backend"].value,
                correlation_id=kwargs["correlation_id"],
                job_type=kwargs["job_type"],
                sim_id=None,
                simulator_id=kwargs["ref_id"],
            )

        db = MagicMock()
        db.get_hpc_db.return_value.insert_hpcrun = AsyncMock(side_effect=insert_hpcrun)
        service = ComposeSimulationServiceHpc(slurm_ssh=lambda: sessions, container_build=hook)
        simulator = ComposeSimulatorVersion(
            singularity_def=ContainerizationFileRepr(representation="Bootstrap: docker\nFrom: busybox\n"),
            singularity_def_hash="abc123def456",
            packages=None,
            database_id=4,
        )
        row = await service.build_container(simulator, random_str="r", db_service=db)
        ssh.scp_upload.assert_awaited_once()
        hook.assert_awaited_once()
        job_name, definition, container = _must(hook.await_args).args
        assert job_name == "singularity-build-abc12-bf6f0"  # a DNS-1123 label: no underscore, no uppercase
        assert definition == Path(settings.compose_image_base_path) / "abc123def456.def"
        assert container == Path(settings.compose_image_base_path) / "abc123def456.sif"
        assert inserted["backend"] is JobBackend.K8S and inserted["job_id_ext"] == "compose-build-abc"
        assert inserted["slurmjobid"] == -1 and inserted["job_type"] is ComposeJobType.BUILD_CONTAINER
        assert row.job_backend == "k8s" and row.job_id_ext == "compose-build-abc"
    finally:
        settings.compose_image_base_path, settings.slurm_log_base_path = saved


# --------------------------------------------------------------------------- the monitor


def _row(database_id: int, job_name: str, status: ComposeJobStatus = ComposeJobStatus.RUNNING) -> ComposeHpcRun:
    return ComposeHpcRun(
        database_id=database_id,
        slurmjobid=-1,
        job_id_ext=job_name,
        job_backend="k8s",
        correlation_id=f"c{database_id}",
        job_type=ComposeJobType.BUILD_CONTAINER,
        sim_id=None,
        simulator_id=1,
        status=status,
    )


def _info(name: str, status: JobStatus, **fields: Any) -> JobStatusInfo:
    return JobStatusInfo(job_id=JobId.k8s(name), status=status, **fields)


@pytest.mark.asyncio
async def test_a_completed_job_ends_the_row_and_wakes_the_waiter() -> None:
    k8s = MagicMock()
    k8s.get_job_status.return_value = _info(
        "b1", JobStatus.COMPLETED, start_time="2026-09-26T06:00:00", end_time="2026-09-26T06:03:00"
    )
    db = MagicMock()
    hpc_db = db.get_hpc_db.return_value
    done = _row(1, "b1", ComposeJobStatus.COMPLETED)
    hpc_db.update_hpcrun_result = AsyncMock()
    hpc_db.get_hpcrun = AsyncMock(return_value=done)
    monitor = ComposeJobMonitor(nats_client=None, database_service=db, k8s_jobs=k8s)
    queue: asyncio.Queue[ComposeHpcRun] = asyncio.Queue()
    monitor.internal_subscribe(queue, 1)
    await monitor._update_k8s_jobs([_row(1, "b1")])
    hpc_db.update_hpcrun_result.assert_awaited_once_with(
        1, ComposeJobStatus.COMPLETED, start_time="2026-09-26T06:00:00", end_time="2026-09-26T06:03:00"
    )
    assert queue.get_nowait().status is ComposeJobStatus.COMPLETED


@pytest.mark.asyncio
async def test_a_failed_job_carries_the_pods_last_word() -> None:
    k8s = MagicMock()
    k8s.get_job_status.return_value = _info(
        "b2", JobStatus.FAILED, error_message="Job has reached the specified backoff limit"
    )
    k8s.get_pod_termination.return_value = "OOMKilled (exit 137)"
    db = MagicMock()
    hpc_db = db.get_hpc_db.return_value
    hpc_db.mark_hpcrun_failed = AsyncMock()
    hpc_db.get_hpcrun = AsyncMock(return_value=_row(2, "b2", ComposeJobStatus.FAILED))
    monitor = ComposeJobMonitor(nats_client=None, database_service=db, k8s_jobs=k8s)
    await monitor._update_k8s_jobs([_row(2, "b2")])
    hpc_db.mark_hpcrun_failed.assert_awaited_once_with(2, "OOMKilled (exit 137)")


@pytest.mark.asyncio
async def test_a_job_still_running_or_unknown_leaves_the_row_alone() -> None:
    k8s = MagicMock()
    k8s.get_job_status.side_effect = [_info("b3", JobStatus.RUNNING), None]
    db = MagicMock()
    monitor = ComposeJobMonitor(nats_client=None, database_service=db, k8s_jobs=k8s)
    await monitor._update_k8s_jobs([_row(3, "b3"), _row(4, "b4")])
    db.get_hpc_db.return_value.update_hpcrun_result.assert_not_called()
    db.get_hpc_db.return_value.mark_hpcrun_failed.assert_not_called()


@pytest.mark.asyncio
async def test_the_split_sends_k8s_rows_to_the_k8s_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    db = MagicMock()
    rows = [_row(5, "b5")]
    db.get_hpc_db.return_value.list_running_hpcruns = AsyncMock(return_value=rows)
    monitor = ComposeJobMonitor(nats_client=None, database_service=db, k8s_jobs=MagicMock())
    seen: dict[str, list[int]] = {}

    async def k8s(runs: list[ComposeHpcRun]) -> None:
        seen["k8s"] = [r.database_id for r in runs]

    async def other(runs: list[ComposeHpcRun]) -> None:
        seen.setdefault("other", []).extend(r.database_id for r in runs)

    monkeypatch.setattr(monitor, "_update_k8s_jobs", k8s)
    monkeypatch.setattr(monitor, "_update_slurm_jobs", other)
    monkeypatch.setattr(monitor, "_update_backend_jobs", other)
    await monitor.update_running_jobs()
    assert seen == {"k8s": [5], "other": []}
