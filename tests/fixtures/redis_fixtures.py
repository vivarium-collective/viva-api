from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from testcontainers.redis import RedisContainer  # type: ignore[import-untyped]

from tests.docker_utils import SKIP_DOCKER_REASON, SKIP_DOCKER_TESTS
from viva_api.common.messaging.messaging_service_redis import MessagingServiceRedis


@pytest_asyncio.fixture(scope="session")
async def redis_container_host_and_port() -> AsyncGenerator[tuple[str, int]]:
    if SKIP_DOCKER_TESTS:
        pytest.skip(SKIP_DOCKER_REASON)
    with RedisContainer() as redis_container:
        # RedisContainer uses get_connection_url() method
        yield (redis_container.get_container_host_ip(), int(redis_container.get_exposed_port(6379)))


@pytest_asyncio.fixture(scope="function")
async def redis_subscriber_service(
    redis_container_host_and_port: tuple[str, int],
) -> AsyncGenerator[MessagingServiceRedis]:
    service = MessagingServiceRedis()
    await service.connect(host=redis_container_host_and_port[0], port=redis_container_host_and_port[1])
    yield service
    await service.disconnect()


@pytest_asyncio.fixture(scope="function")
async def redis_producer_service(
    redis_container_host_and_port: tuple[str, int],
) -> AsyncGenerator[MessagingServiceRedis]:
    service = MessagingServiceRedis()
    await service.connect(host=redis_container_host_and_port[0], port=redis_container_host_and_port[1])
    yield service
    await service.disconnect()
