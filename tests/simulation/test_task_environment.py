"""A task may name a registered ENVIRONMENT instead of a simulator's image (``docs/plan-core.md`` P2.3d).

``environment="runtime"`` is the core runtime image: Python and the process-bigraph engine, nothing
of any application. An uploaded script that needs nothing of the science image runs there -- and
starts in seconds rather than the minutes a 5.74 GB pull takes.
"""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from tests.simulation.test_ray_backend import _container_settings, _fake_container_batch
from viva_api.common.site_environments import job_definition_key
from viva_api.simulation.dispatch.tasks import TaskRequestRefused
from viva_api.simulation.models import TaskRunRequest
from viva_api.simulation.simulation_service_ray import SimulationServiceRay
from viva_core.environments import EnvironmentNotResolvable

RUNTIME = "ghcr.io/vivarium-collective/viva-core-runtime:0.1.0"


def _settings_with_runtime() -> Any:
    return _container_settings(core_runtime_image=RUNTIME)


def _submit_uploaded(request: TaskRunRequest, settings: Any, service: SimulationServiceRay | None = None) -> Any:
    batch = _fake_container_batch(["c-1"])
    database = AsyncMock()
    with (
        patch("viva_api.simulation.dispatch._seams.get_settings", settings),
        patch("viva_api.common.storage.data_layout.get_settings", settings),
        patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=batch),
    ):
        asyncio.run(
            (service or SimulationServiceRay()).tasks.submit_uploaded_task(
                request, script_bytes=b"print(1)\n", filename="s.py", database_service=database
            )
        )
    return batch


# ------------------------------------------------------------------ the request


def test_a_request_names_a_known_environment_or_a_commit_never_both() -> None:
    assert TaskRunRequest(script="s.py", environment="runtime").environment == "runtime"
    with pytest.raises(ValidationError, match="unknown environment 'copasi'"):
        TaskRunRequest(script="s.py", environment="copasi")
    with pytest.raises(ValidationError, match="not both"):
        TaskRunRequest(script="s.py", environment="runtime", commit="abc1234")
    assert TaskRunRequest(script="s.py").environment is None  # unchanged for every existing caller


# ------------------------------------------------------------------ the service


def test_an_uploaded_task_in_the_runtime_environment_runs_in_the_runtime_image() -> None:
    service = SimulationServiceRay()
    with patch.object(service, "get_latest_commit_hash", new=AsyncMock(side_effect=AssertionError("not asked"))):
        batch = _submit_uploaded(TaskRunRequest(script="s.py", environment="runtime"), _settings_with_runtime, service)

    # the job definition is derived for THAT image, under a key that is not a commit
    registered = batch.register_job_definition.call_args.kwargs
    assert registered["containerProperties"]["image"] == RUNTIME
    assert registered["jobDefinitionName"] == "smscdk-ray-container-viva-core-runtime-0-1-0"
    (submitted,) = batch.submit_job.call_args_list
    assert submitted.kwargs["jobDefinition"].startswith("smscdk-ray-container-viva-core-runtime-0-1-0")
    env = {e["name"]: e["value"] for e in submitted.kwargs["containerOverrides"]["environment"]}
    assert env["CONTAINER_JOB_CMD"].startswith("python ") and env["CONTAINER_JOB_CMD"].endswith("/s.py")
    assert "tasks/scripts/" in env["CONTAINER_STAGE_S3"]  # the contract is the same; only the image differs


def test_without_an_environment_nothing_changes() -> None:
    batch = _submit_uploaded(TaskRunRequest(script="s.py", commit="abc1234"), _settings_with_runtime)
    registered = batch.register_job_definition.call_args.kwargs
    assert registered["containerProperties"]["image"].endswith("/v2ecoli:abc1234")
    assert registered["jobDefinitionName"] == "smscdk-ray-container-abc1234"


def test_a_site_with_no_runtime_image_refuses_rather_than_running_the_task_somewhere_else() -> None:
    with pytest.raises(EnvironmentNotResolvable, match="no runtime image"):
        _submit_uploaded(TaskRunRequest(script="s.py", environment="runtime"), _container_settings)


def test_a_repo_path_script_cannot_run_in_an_environment_that_has_no_repo() -> None:
    service, database = SimulationServiceRay(), AsyncMock()
    batch = _fake_container_batch(["c-1"])
    with (
        patch("viva_api.simulation.dispatch._seams.get_settings", _settings_with_runtime),
        patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=batch),
        pytest.raises(TaskRequestRefused, match="upload the script"),
    ):
        request = TaskRunRequest(script="scripts/build_cache.py", environment="runtime")
        asyncio.run(service.tasks.submit_task(request, database))
    batch.submit_job.assert_not_called()
    database.record_task.assert_not_called()


def test_the_job_definition_key_of_an_image_is_safe_and_says_which_image() -> None:
    assert job_definition_key(RUNTIME) == "viva-core-runtime-0-1-0"
    assert job_definition_key("123.dkr.ecr.us-gov-west-1.amazonaws.com/core/runtime:1.2.3_rc1") == "runtime-1-2-3_rc1"
    assert len(job_definition_key("r/" + "x" * 200)) <= 64


# ------------------------------------------------------------------ the routes


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    from viva_api.api.main import app
    from viva_api.api.routers import tasks as tasks_router

    service = SimulationServiceRay()
    monkeypatch.setattr(tasks_router, "_require_ray_service", lambda: service)
    monkeypatch.setattr(tasks_router, "_require_database_service", lambda: AsyncMock())
    return TestClient(app)


def test_the_routes_say_whose_fault_it_is(client: TestClient) -> None:
    upload = {"script": ("s.py", b"print(1)\n", "text/x-python")}
    with (
        patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
        patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=_fake_container_batch(["c-1"])),
    ):
        # the caller's: an environment nobody registered, or both ways of naming an image
        assert client.post("/api/v1/tasks/upload", files=upload, data={"environment": "copasi"}).status_code == 422
        both = client.post("/api/v1/tasks/upload", files=upload, data={"environment": "runtime", "commit": "abc1234"})
        assert both.status_code == 422
        # the caller's: a repo-path script in an environment with no repo
        refused = client.post("/api/v1/tasks", json={"script": "scripts/x.py", "environment": "runtime"})
        assert refused.status_code == 400 and "upload the script" in refused.json()["detail"]
        # nobody's: the request is fine, this deployment registers no runtime image
        unavailable = client.post("/api/v1/tasks/upload", files=upload, data={"environment": "runtime"})
        assert unavailable.status_code == 501 and "not available here" in unavailable.json()["detail"]
