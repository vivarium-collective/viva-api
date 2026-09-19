"""No unit test may reach AWS -- and one that tries fails, loudly, even if the code under
test swallows the error.

Why this exists (``docs/plan-core.md`` P2.0). Two ways a unit test can reach AWS, and the
suite had no defence against either:

* **Patch lifetime.** Code that spawns a background task can outlive the test's
  ``with patch(...)`` block; the task then runs against the real client. This is the one the
  guard actually caught on its first run (a real ``batch.SubmitJob`` from a unit test).
* **Patch position.** ``get_settings`` is patched by NAME inside the module under test, and a
  name patch reaches only that module: move a function to another file and it silently runs
  with the developer's real settings -- real region, real queues, real bucket. (The
  ``boto3.client`` patches are NOT positional -- ``patch("<module>.boto3.client")`` resolves
  to the shared ``boto3`` module and replaces ``client`` for everyone -- so they survive a
  move. But a test that patches only settings, or nothing, has no such cover.)

With a logged-in developer profile either one is a real, billable AWS call from ``pytest``.

So the guard sits at the one place every AWS API call passes through, whatever created the
client and wherever the code lives: ``BaseClient._make_api_call`` (boto3) and
``AioBaseClient._make_api_call`` (aioboto3). Creating a client is harmless and is left
alone; *calling* AWS is refused.

* A refused call raises :class:`RealAwsCallInUnitTest` at the call site.
* It is ALSO recorded, and the test fails at teardown if anything was recorded -- because
  much of this code deliberately catches broad exceptions around AWS calls (best-effort
  staging, status polling), and a swallowed refusal must not read as a pass.
* A client pointed at a local endpoint (a MinIO / LocalStack test container) is allowed.
* A test that genuinely needs AWS says so: ``@pytest.mark.real_aws`` -- or asks for one of
  the fixtures that ARE a real cloud service (``file_service_s3``, ``file_service_qumulo``),
  which is the same statement made by the test's signature. Those tests are already skipped
  unless their own credentials / environment switch is present.
"""

from collections.abc import Iterator
from typing import Any
from urllib.parse import urlparse

import pytest

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0", "host.docker.internal"}  # noqa: S104


#: botocore's single chokepoint for every API call, sync and async. PRIVATE -- which is why it
#: is named once, here: if botocore ever renames it, `test_the_chokepoint_still_exists` fails
#: instead of the guard silently guarding nothing.
API_CALL = "_make_api_call"

#: Requesting one of these IS the declaration "this test talks to a real object store".
REAL_CLOUD_FIXTURES = frozenset({"file_service_s3", "file_service_qumulo"})


class RealAwsCallInUnitTest(AssertionError):
    """A unit test reached for AWS. Patch the client where the code NOW lives, or -- if the
    test really is an integration test -- mark it ``@pytest.mark.real_aws``."""


def _is_local(client: Any) -> bool:
    endpoint = getattr(getattr(client, "meta", None), "endpoint_url", "") or ""
    return (urlparse(endpoint).hostname or "") in _LOCAL_HOSTS


def _describe(client: Any, operation_name: str) -> str:
    service = getattr(getattr(getattr(client, "meta", None), "service_model", None), "service_name", "?")
    return f"{service}.{operation_name}"


@pytest.fixture(autouse=True)
def no_real_aws(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Autouse. Yields the list of refused calls, for the guard's own tests."""
    refused: list[str] = []
    opted_out = request.node.get_closest_marker("real_aws") is not None or bool(
        REAL_CLOUD_FIXTURES & set(getattr(request, "fixturenames", ()))
    )
    if opted_out:
        yield refused
        return

    from botocore.client import BaseClient

    real_sync = getattr(BaseClient, API_CALL)

    def sync_guard(self: Any, operation_name: str, api_params: Any) -> Any:
        if _is_local(self):
            return real_sync(self, operation_name, api_params)
        refused.append(_describe(self, operation_name))
        raise RealAwsCallInUnitTest(f"unit test called AWS: {refused[-1]}")

    monkeypatch.setattr(BaseClient, API_CALL, sync_guard)

    try:
        from aiobotocore.client import AioBaseClient
    except ImportError:  # aioboto3 is a runtime dependency, but the guard must not require it
        AioBaseClient = None  # type: ignore[assignment,misc]

    if AioBaseClient is not None:
        real_async = getattr(AioBaseClient, API_CALL)

        async def async_guard(self: Any, operation_name: str, api_params: Any) -> Any:
            if _is_local(self):
                return await real_async(self, operation_name, api_params)
            refused.append(_describe(self, operation_name))
            raise RealAwsCallInUnitTest(f"unit test called AWS: {refused[-1]}")

        monkeypatch.setattr(AioBaseClient, API_CALL, async_guard)

    yield refused

    if refused and not getattr(request.node, "_expects_refused_aws_calls", False):
        pytest.fail(
            "this test reached for AWS (the call was refused, but the code under test may have "
            f"swallowed the error): {', '.join(refused)}. Patch the client where the code now lives, "
            "or mark the test @pytest.mark.real_aws.",
            pytrace=False,
        )
