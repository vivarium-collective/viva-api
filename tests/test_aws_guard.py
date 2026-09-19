"""The guard against real AWS calls in unit tests (``tests/fixtures/aws_guard.py``)."""

import contextlib
from typing import Any

import boto3
import pytest

from tests.fixtures.aws_guard import API_CALL, RealAwsCallInUnitTest

_FAKE_CREDS: dict[str, Any] = {
    "region_name": "us-east-1",
    "aws_access_key_id": "AKIAFAKEFAKEFAKEFAKE",
    "aws_secret_access_key": "fake",
}


def test_creating_a_client_is_allowed() -> None:
    assert boto3.client("batch", **_FAKE_CREDS) is not None


def test_calling_aws_is_refused_at_the_call_site(no_real_aws: list[str], request: pytest.FixtureRequest) -> None:
    request.node._expects_refused_aws_calls = True
    with pytest.raises(RealAwsCallInUnitTest, match="batch.DescribeJobs"):
        boto3.client("batch", **_FAKE_CREDS).describe_jobs(jobs=["x"])
    assert no_real_aws == ["batch.DescribeJobs"]


def test_a_swallowed_refusal_is_still_recorded(no_real_aws: list[str], request: pytest.FixtureRequest) -> None:
    """Best-effort code catches broad exceptions around AWS calls. The refusal must survive
    that -- it is what the teardown check turns into a failure."""
    request.node._expects_refused_aws_calls = True
    with contextlib.suppress(Exception):
        boto3.client("s3", **_FAKE_CREDS).put_object(Bucket="b", Key="k", Body=b"")
    assert no_real_aws == ["s3.PutObject"]


@pytest.mark.asyncio
async def test_the_async_client_is_guarded_too(no_real_aws: list[str], request: pytest.FixtureRequest) -> None:
    import aioboto3

    request.node._expects_refused_aws_calls = True
    async with aioboto3.Session(**_FAKE_CREDS).client("s3") as client:
        with pytest.raises(RealAwsCallInUnitTest, match="s3.ListObjectsV2"):
            await client.list_objects_v2(Bucket="b")
    assert no_real_aws == ["s3.ListObjectsV2"]


def test_a_local_endpoint_is_allowed_through(no_real_aws: list[str]) -> None:
    """A MinIO / LocalStack test container is not AWS. Nothing listens here, so the call
    fails to CONNECT -- which proves it was let through rather than refused."""
    from botocore.config import Config
    from botocore.exceptions import EndpointConnectionError

    client = boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:9",
        config=Config(retries={"max_attempts": 1}, connect_timeout=1, read_timeout=1),
        **_FAKE_CREDS,
    )
    with pytest.raises(EndpointConnectionError):
        client.list_buckets()
    assert no_real_aws == []


@pytest.mark.real_aws
def test_the_marker_opts_out(no_real_aws: list[str]) -> None:
    """With the marker the guard installs nothing: botocore's own method is in place."""
    from botocore.client import BaseClient

    assert getattr(BaseClient, API_CALL).__name__ == API_CALL
    assert no_real_aws == []


@pytest.mark.real_aws
def test_the_chokepoint_still_exists() -> None:
    """The guard patches a PRIVATE botocore method. If a release renames it, the guard would
    patch a new attribute onto the class and protect nothing -- so pin that it is real."""
    from aiobotocore.client import AioBaseClient
    from botocore.client import BaseClient

    assert callable(vars(BaseClient).get(API_CALL)), "botocore.BaseClient lost its API-call chokepoint"
    assert callable(vars(AioBaseClient).get(API_CALL)), "aiobotocore.AioBaseClient lost its API-call chokepoint"


def test_a_real_cloud_fixture_is_the_same_declaration_as_the_marker() -> None:
    from tests.fixtures.aws_guard import REAL_CLOUD_FIXTURES

    assert {"file_service_s3", "file_service_qumulo"} <= REAL_CLOUD_FIXTURES
