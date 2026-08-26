"""Env-worker lifecycle (vivarium-workbench#942 step 2).

The K8s API is faked: what matters here is the *Job we would create* — its
image, its dial-back wiring, and the requests we refuse before Kubernetes ever
sees them. These values land in a pod spec, so the validation boundary is the
substance of this module, not decoration around it.
"""

from unittest.mock import MagicMock

import pytest

from viva_api.compose.env_worker_service import (
    WORKER_TTL_SECONDS,
    EnvWorkerLaunchError,
    EnvWorkerService,
)

COMMIT = "234dc76"
TOKEN = "a" * 64          # token_hex(32) shape
HOST = "10.99.45.175"
PORT = 44321


@pytest.fixture
def service(monkeypatch):
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

    monkeypatch.setattr("viva_api.compose.env_worker_service.get_settings", lambda: _S())
    k8s = MagicMock()
    svc = EnvWorkerService(k8s=k8s, namespace="sms-api-stanford-test")
    return svc, k8s


def test_missing_ecr_account_refuses_rather_than_guessing(monkeypatch):
    class _S:
        ecr_account_id = ""
        batch_region = "us-gov-west-1"
        ray_ecr_repository = "v2ecoli"
        k8s_job_namespace = "ns"

    monkeypatch.setattr("viva_api.compose.env_worker_service.get_settings", lambda: _S())
    svc = EnvWorkerService(k8s=MagicMock(), namespace="ns")
    with pytest.raises(EnvWorkerLaunchError, match="ecr_account_id"):
        svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)


def _created_job(k8s):
    assert k8s.create_job.call_count == 1
    return k8s.create_job.call_args[0][0]


# --- the Job we build -------------------------------------------------------

def test_worker_runs_the_prebuilt_image_for_the_commit(service):
    svc, k8s = service
    handle = svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    job = _created_job(k8s)
    image = job.spec.template.spec.containers[0].image
    assert image.endswith(f":{COMMIT}")
    assert "v2ecoli" in image                    # the Ray/compose repo, not vecoli
    assert handle.image == image


def test_dial_back_target_reaches_the_container(service):
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    c = _created_job(k8s).spec.template.spec.containers[0]
    assert "--connect-to" in c.args
    assert f"{HOST}:{PORT}" in c.args


def test_token_travels_in_env_never_in_argv(service):
    """/proc/<pid>/cmdline is world-readable — the token must not be on it."""
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    c = _created_job(k8s).spec.template.spec.containers[0]
    assert TOKEN not in " ".join(c.args or [])
    env = {e.name: e.value for e in c.env}
    assert env["VIVARIUM_ENV_WORKER_TOKEN"] == TOKEN


def test_job_does_not_respawn_and_has_a_ttl_backstop(service):
    """A worker that failed to dial back must not retry against a dead listener."""
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    spec = _created_job(k8s).spec
    assert spec.backoff_limit == 0
    assert spec.ttl_seconds_after_finished == WORKER_TTL_SECONDS
    assert spec.template.spec.restart_policy == "Never"


def test_worker_is_sized_for_interactive_queries_not_compute(service):
    svc, k8s = service
    svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN)
    res = _created_job(k8s).spec.template.spec.containers[0].resources
    assert res.limits["memory"] == "2Gi" and res.limits["cpu"] == "1"


def test_two_sessions_on_one_commit_get_distinct_jobs(service):
    svc, k8s = service
    a = svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN,
                  session_key="aad8c485-d8c1-41a6")
    b = svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=TOKEN,
                  session_key="cd48141b-d5c2-4030")
    assert a.job_name != b.job_name
    for name in (a.job_name, b.job_name):
        assert name.islower() and " " not in name and len(name) <= 63


# --- what we refuse before Kubernetes sees it -------------------------------

@pytest.mark.parametrize("commit", ["", "not-hex", "zz1234", "abc", "../../etc/passwd", "234dc76; rm -rf /"])
def test_bad_commit_is_refused(service, commit):
    svc, k8s = service
    with pytest.raises(EnvWorkerLaunchError, match="commit"):
        svc.start(commit=commit, callback_host=HOST, callback_port=PORT, token=TOKEN)
    k8s.create_job.assert_not_called()


@pytest.mark.parametrize("host", ["", "10.0.0.1 --evil", "host;whoami", "a" * 300, "$(hostname)"])
def test_bad_callback_host_is_refused(service, host):
    svc, k8s = service
    with pytest.raises(EnvWorkerLaunchError, match="callback_host"):
        svc.start(commit=COMMIT, callback_host=host, callback_port=PORT, token=TOKEN)
    k8s.create_job.assert_not_called()


@pytest.mark.parametrize("port", [0, -1, 70000])
def test_bad_port_is_refused(service, port):
    svc, k8s = service
    with pytest.raises(EnvWorkerLaunchError, match="callback_port"):
        svc.start(commit=COMMIT, callback_host=HOST, callback_port=port, token=TOKEN)
    k8s.create_job.assert_not_called()


@pytest.mark.parametrize("token", ["", "short", "-leading-dash-token-aaaaaaaaaaaaaaaa", "tok en with space", "tok;en"])
def test_bad_token_is_refused(service, token):
    """A leading '-' would be read as a flag by any argv consumer — the same
    class of bug that made ~2% of worker spawns fail on the workbench side."""
    svc, k8s = service
    with pytest.raises(EnvWorkerLaunchError, match="token"):
        svc.start(commit=COMMIT, callback_host=HOST, callback_port=PORT, token=token)
    k8s.create_job.assert_not_called()


# --- status / stop ----------------------------------------------------------

def test_status_returns_none_when_the_job_is_gone(service):
    svc, k8s = service
    k8s.get_job_status.return_value = None
    assert svc.status("env-worker-234dc76-shared") is None


def test_stop_is_idempotent_when_already_deleted(service):
    from kubernetes import client as k8s_client
    svc, k8s = service
    k8s.delete_job.side_effect = k8s_client.rest.ApiException(status=404)
    svc.stop("env-worker-234dc76-shared")      # must not raise


def test_stop_propagates_a_real_failure(service):
    from kubernetes import client as k8s_client
    svc, k8s = service
    k8s.delete_job.side_effect = k8s_client.rest.ApiException(status=403)
    with pytest.raises(k8s_client.rest.ApiException):
        svc.stop("env-worker-234dc76-shared")
