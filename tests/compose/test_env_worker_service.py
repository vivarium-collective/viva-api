"""Env-worker lifecycle (vivarium-workbench#942 step 2).

The K8s API is faked: what matters here is the *Job we would create* — its
image, its dial-back wiring, and the requests we refuse before Kubernetes ever
sees them. These values land in a pod spec, so the validation boundary is the
substance of this module, not decoration around it.
"""

from typing import Any
from unittest.mock import MagicMock

import pytest

from viva_api.compose.env_worker_service import (
    MODULE_MOUNT,
    SCRATCH_MOUNT,
    WORKER_TTL_SECONDS,
    EnvWorkerLaunchError,
    EnvWorkerService,
)

COMMIT = "234dc76"
TOKEN = "a" * 64  # token_hex(32) shape
HOST = "10.99.45.175"
PORT = 44321


@pytest.fixture
def service(monkeypatch: pytest.MonkeyPatch) -> tuple[EnvWorkerService, MagicMock]:
    """Service with a faked K8s API and the ECR settings a deployment supplies.

    The settings are patched rather than defaulted because the service
    deliberately REFUSES to invent an image when ``ecr_account_id`` is unset —
    guessing a registry would run the science under an environment nobody chose.
    """

    class _S:
        ecr_account_id = "476270107793"
        batch_region = "us-gov-west-1"
        ray_ecr_repository = "v2ecoli"
        k8s_job_namespace = "sms-api-stanford-test"
        env_worker_module_image = "ghcr.io/vivarium-collective/vivarium-workbench:0.3.57"
        env_worker_workspace_path = "/app/v2ecoli"

    monkeypatch.setattr("viva_api.compose.env_worker_service.get_settings", lambda: _S())
    k8s = MagicMock()
    svc = EnvWorkerService(k8s=k8s, namespace="sms-api-stanford-test")
    return svc, k8s


def test_missing_ecr_account_refuses_rather_than_guessing(monkeypatch: pytest.MonkeyPatch) -> None:
    class _S:
        ecr_account_id = ""
        batch_region = "us-gov-west-1"
        ray_ecr_repository = "v2ecoli"
        k8s_job_namespace = "ns"
        env_worker_module_image = "ghcr.io/vivarium-collective/vivarium-workbench:0.3.57"
        env_worker_workspace_path = "/app/v2ecoli"

    monkeypatch.setattr("viva_api.compose.env_worker_service.get_settings", lambda: _S())
    svc = EnvWorkerService(k8s=MagicMock(), namespace="ns")
    with pytest.raises(EnvWorkerLaunchError, match="ecr_account_id"):
        svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)


def _created_job(k8s: MagicMock) -> Any:
    assert k8s.create_job.call_count == 1
    return k8s.create_job.call_args[0][0]


# --- the Job we build -------------------------------------------------------


def test_worker_runs_the_prebuilt_image_for_the_commit(service: tuple[EnvWorkerService, MagicMock]) -> None:
    svc, k8s = service
    handle = svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    job = _created_job(k8s)
    image = job.spec.template.spec.containers[0].image
    assert image.endswith(f":{COMMIT}")
    assert "v2ecoli" in image  # the Ray/compose repo, not vecoli
    assert handle.image == image


def test_dial_back_target_reaches_the_container(service: tuple[EnvWorkerService, MagicMock]) -> None:
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    c = _created_job(k8s).spec.template.spec.containers[0]
    assert "--connect-to" in c.args
    assert f"{HOST}:{PORT}" in c.args


def test_token_travels_in_env_never_in_argv(service: tuple[EnvWorkerService, MagicMock]) -> None:
    """/proc/<pid>/cmdline is world-readable — the token must not be on it."""
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    c = _created_job(k8s).spec.template.spec.containers[0]
    assert TOKEN not in " ".join(c.args or [])
    env = {e.name: e.value for e in c.env}
    assert env["VIVARIUM_ENV_WORKER_TOKEN"] == TOKEN


def test_job_does_not_respawn_and_has_a_ttl_backstop(service: tuple[EnvWorkerService, MagicMock]) -> None:
    """A worker that failed to dial back must not retry against a dead listener."""
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    spec = _created_job(k8s).spec
    assert spec.backoff_limit == 0
    assert spec.ttl_seconds_after_finished == WORKER_TTL_SECONDS
    assert spec.template.spec.restart_policy == "Never"


def test_worker_is_sized_for_interactive_queries_not_compute(service: tuple[EnvWorkerService, MagicMock]) -> None:
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    res = _created_job(k8s).spec.template.spec.containers[0].resources
    assert res.limits["memory"] == "2Gi" and res.limits["cpu"] == "1"


def test_two_sessions_on_one_commit_get_distinct_jobs(service: tuple[EnvWorkerService, MagicMock]) -> None:
    svc, k8s = service
    a = svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN, session_key="aad8c485-d8c1-41a6")
    b = svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN, session_key="cd48141b-d5c2-4030")
    assert a.job_name != b.job_name
    for name in (a.job_name, b.job_name):
        assert name.islower() and " " not in name and len(name) <= 63


# --- what we refuse before Kubernetes sees it -------------------------------


@pytest.mark.parametrize("commit", ["", "not-hex", "zz1234", "abc", "../../etc/passwd", "234dc76; rm -rf /"])
def test_bad_commit_is_refused(service: tuple[EnvWorkerService, MagicMock], commit: str) -> None:
    svc, k8s = service
    with pytest.raises(EnvWorkerLaunchError, match="commit"):
        svc.start(commit=commit, callback_host=HOST, callback_port=PORT, token=TOKEN)
    k8s.create_job.assert_not_called()


@pytest.mark.parametrize("host", ["", "10.0.0.1 --evil", "host;whoami", "a" * 300, "$(hostname)"])
def test_bad_callback_host_is_refused(service: tuple[EnvWorkerService, MagicMock], host: str) -> None:
    svc, k8s = service
    with pytest.raises(EnvWorkerLaunchError, match="callback_host"):
        svc.start(commit=COMMIT, callback_host=host, callback_port=PORT, token=TOKEN)
    k8s.create_job.assert_not_called()


@pytest.mark.parametrize("port", [0, -1, 70000])
def test_bad_port_is_refused(service: tuple[EnvWorkerService, MagicMock], port: int) -> None:
    svc, k8s = service
    with pytest.raises(EnvWorkerLaunchError, match="callback_port"):
        svc.start(commit=COMMIT, callback_host=HOST, callback_port=port, token=TOKEN)
    k8s.create_job.assert_not_called()


@pytest.mark.parametrize("token", ["", "short", "-leading-dash-token-aaaaaaaaaaaaaaaa", "tok en with space", "tok;en"])
def test_bad_token_is_refused(service: tuple[EnvWorkerService, MagicMock], token: str) -> None:
    """A leading '-' would be read as a flag by any argv consumer — the same
    class of bug that made ~2% of worker spawns fail on the workbench side."""
    svc, k8s = service
    with pytest.raises(EnvWorkerLaunchError, match="token"):
        svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=token)
    k8s.create_job.assert_not_called()


# --- status / stop ----------------------------------------------------------


def test_status_returns_none_when_the_job_is_gone(service: tuple[EnvWorkerService, MagicMock]) -> None:
    svc, k8s = service
    k8s.get_job_status.return_value = None
    assert svc.status("env-worker-234dc76-shared") is None


def test_stop_is_idempotent_when_already_deleted(service: tuple[EnvWorkerService, MagicMock]) -> None:
    from kubernetes import client as k8s_client

    svc, k8s = service
    k8s.delete_job.side_effect = k8s_client.rest.ApiException(status=404)
    svc.stop("env-worker-234dc76-shared")  # must not raise


def test_stop_propagates_a_real_failure(service: tuple[EnvWorkerService, MagicMock]) -> None:
    from kubernetes import client as k8s_client

    svc, k8s = service
    k8s.delete_job.side_effect = k8s_client.rest.ApiException(status=403)
    with pytest.raises(k8s_client.rest.ApiException):
        svc.stop("env-worker-234dc76-shared")


# --- step 3: the pod spec ---------------------------------------------------


def test_worker_runs_under_the_images_own_interpreter(service: tuple[EnvWorkerService, MagicMock]) -> None:
    """`python` in the simulator image resolves to ITS venv — that is the
    environment being asked about, and why the worker runs here at all."""
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    c = _created_job(k8s).spec.template.spec.containers[0]
    assert c.command == ["python", "-m", "vivarium_workbench.env_worker"]


def test_worker_module_is_staged_from_the_workbench_image(service: tuple[EnvWorkerService, MagicMock]) -> None:
    """Delivered, not installed: protocol §4 keeps vivarium-workbench out of the
    workspace venv, and the simulator image is built --no-install-package."""
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    pod = _created_job(k8s).spec.template.spec
    init = pod.init_containers[0]
    assert "vivarium-workbench" in init.image
    script = init.command[-1]
    assert "env_worker.py" in script and "/lib" in script
    # The bulk of the package is the loom frontend bundle; a worker must not pay
    # ~199 MB of emptyDir for it.
    assert "loom" not in script


def test_module_and_scratch_are_ephemeral_and_the_pvc_is_untouched(service: tuple[EnvWorkerService, MagicMock]) -> None:
    """The worker is stateless w.r.t. the record (specs travel in messages), which
    is what frees it from the ReadWriteOnce PVC and its single-node binding."""
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    pod = _created_job(k8s).spec.template.spec
    kinds = {v.name: v for v in pod.volumes}
    assert set(kinds) == {"worker-module", "scratch"}
    for v in pod.volumes:
        assert v.empty_dir is not None
        assert getattr(v, "persistent_volume_claim", None) is None


def test_staged_module_is_on_pythonpath_and_mounted_read_only(service: tuple[EnvWorkerService, MagicMock]) -> None:
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    c = _created_job(k8s).spec.template.spec.containers[0]
    env = {e.name: e.value for e in c.env}
    assert env["PYTHONPATH"] == MODULE_MOUNT
    assert env["TMPDIR"] == SCRATCH_MOUNT
    mounts = {m.name: m for m in c.volume_mounts}
    assert mounts["worker-module"].read_only is True
    assert mounts["scratch"].read_only in (None, False)


def test_workspace_defaults_to_the_images_own_checkout(service: tuple[EnvWorkerService, MagicMock]) -> None:
    """Under §2A.8 the image's copy IS the environment — not the PVC."""
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    c = _created_job(k8s).spec.template.spec.containers[0]
    assert "/app/v2ecoli" in c.args


def test_missing_module_image_refuses_rather_than_guessing(monkeypatch: pytest.MonkeyPatch) -> None:
    class _S:
        ecr_account_id = "476270107793"
        batch_region = "us-gov-west-1"
        ray_ecr_repository = "v2ecoli"
        k8s_job_namespace = "ns"
        env_worker_module_image = ""
        env_worker_workspace_path = "/app/v2ecoli"

    monkeypatch.setattr("viva_api.compose.env_worker_service.get_settings", lambda: _S())
    svc = EnvWorkerService(k8s=MagicMock(), namespace="ns")
    with pytest.raises(EnvWorkerLaunchError, match="env_worker_module_image"):
        svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)


# --- job-name uniqueness + 409 (found on dev) -------------------------------


def test_each_launch_gets_a_unique_job_name(service: tuple[EnvWorkerService, MagicMock]) -> None:
    """A fixed name per commit collided between sessions AND on a sequential
    relaunch, because a deleted Job lingers while foreground propagation
    finishes: "object is being deleted ... already exists" (observed on dev)."""
    svc, k8s = service
    names = {svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN).job_name for _ in range(5)}
    assert len(names) == 5
    for n in names:
        assert n.startswith("env-worker-234dc76-") and len(n) <= 63
        assert n.islower() and " " not in n


def test_job_already_exists_is_a_conflict_not_a_crash(
    service: tuple[EnvWorkerService, MagicMock],
) -> None:
    from kubernetes import client as k8s_client

    from viva_api.compose.env_worker_service import EnvWorkerJobExists

    svc, k8s = service
    k8s.create_job.side_effect = k8s_client.rest.ApiException(status=409)
    with pytest.raises(EnvWorkerJobExists):
        svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)


def test_workspace_defaults_to_the_deployment_setting_when_caller_omits_it(
    service: tuple[EnvWorkerService, MagicMock],
) -> None:
    """The router used to default workspace to "/workspace", which is TRUTHY and
    so shadowed env_worker_workspace_path entirely — every worker ran against a
    path that does not exist in its pod and silently fell back to a global scan."""
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN, workspace=None)
    c = _created_job(k8s).spec.template.spec.containers[0]
    assert "/app/v2ecoli" in c.args


# --- the environment the worker actually runs in ----------------------------


def test_the_worker_runs_in_utf8_mode(service: tuple[EnvWorkerService, MagicMock]) -> None:
    """The container sets no locale, so Python's default text encoding is ASCII.

    The workbench has ~130 text reads/writes with no explicit ``encoding=``, and
    every one of them raises ``UnicodeEncodeError`` the first time a study title
    contains an em dash. Two were fixed at the call site (workbench 0.3.70 and
    0.3.71) before it was clear the fault was the environment, not the code.
    This is the class fix, and it belongs in the pod spec.
    """
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    c = _created_job(k8s).spec.template.spec.containers[0]
    env = {e.name: e.value for e in c.env}
    assert env["PYTHONUTF8"] == "1"


# --- why a worker stopped ---------------------------------------------------


def test_explain_exit_reports_the_reason_and_the_code(service: tuple[EnvWorkerService, MagicMock]) -> None:
    """An OOM kill and a deliberate delete are the same event at the socket.

    The caller of a task that died mid-call gets "worker closed the connection",
    which is true of both. The pod's terminated state is the only thing that
    tells them apart, so it has to reach the caller.
    """
    svc, k8s = service
    k8s.get_pod_termination.return_value = "OOMKilled (exit 137)"
    assert svc.explain_exit("env-worker-234dc76-shared") == "OOMKilled (exit 137)"
    k8s.get_pod_termination.assert_called_once_with("env-worker-234dc76-shared")


def test_explain_exit_is_silent_when_the_pod_is_gone(service: tuple[EnvWorkerService, MagicMock]) -> None:
    """Job TTL reaps pods, so "no answer" is routine, not exceptional."""
    svc, k8s = service
    k8s.get_pod_termination.return_value = None
    assert svc.explain_exit("env-worker-234dc76-shared") is None


def test_a_failing_diagnosis_never_masks_the_fault_it_describes(
    service: tuple[EnvWorkerService, MagicMock],
) -> None:
    """This runs only when something has ALREADY gone wrong. An unreachable K8s
    API must not convert a reported failure into a raised one — the caller would
    lose the real error and gain one about looking the real error up."""
    svc, k8s = service
    k8s.get_pod_termination.side_effect = RuntimeError("API server unreachable")
    assert svc.explain_exit("env-worker-234dc76-shared") is None
