"""``shutdown_standalone`` stops everything that polls or holds a socket -- first.

Before this, shutdown disposed the Postgres engine and only THEN closed the JobScheduler,
and never stopped the ComposeJobMonitor, the env-worker TaskRunner or the relay's sockets
at all: a 30 s poll loop and per-worker drainers outlived the engine they query. With one
process that is untidy; with a second process sharing the database (docs/plan-core.md,
the core/SMS split) it is two owners for the same rows.
"""

import asyncio
import time
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from viva_api import dependencies
from viva_core.compose.job_monitor import ComposeJobMonitor
from viva_core.container import ComposeServices, EnvWorkerServices


@pytest.fixture
def restore_globals() -> Iterator[None]:
    saved_scheduler = dependencies.get_job_scheduler()
    saved_compose = dependencies.get_compose_services()
    saved_engine = dependencies.get_postgres_engine()
    saved_env_worker = dependencies.get_env_worker_services()
    yield
    dependencies.set_job_scheduler(saved_scheduler)
    dependencies.set_compose_services(saved_compose)
    dependencies.set_postgres_engine(saved_engine)
    dependencies.set_env_worker_services(saved_env_worker)


@pytest.mark.asyncio
async def test_compose_monitor_stops_without_waiting_out_its_interval() -> None:
    database = MagicMock()
    database.get_hpc_db.return_value.list_running_hpcruns = AsyncMock(return_value=[])
    monitor = ComposeJobMonitor(nats_client=None, database_service=database, sim_registry={})

    await monitor.start_polling(interval_seconds=30)
    await asyncio.sleep(0.05)  # let the first tick run and park on the interval

    started = time.monotonic()
    await asyncio.wait_for(monitor.close(), timeout=5)
    assert time.monotonic() - started < 2, "stop_polling() waited out the 30 s interval"


@pytest.mark.asyncio
async def test_shutdown_stops_background_work_before_disposing_the_engine(restore_globals: None) -> None:
    order: list[str] = []

    def recorder(name: str) -> AsyncMock:
        async def _record(*_: Any, **__: Any) -> None:
            order.append(name)

        return AsyncMock(side_effect=_record)

    scheduler = MagicMock()
    scheduler.close = recorder("scheduler")
    monitor = MagicMock()
    monitor.close = recorder("compose_monitor")
    runner = MagicMock()
    runner.close = recorder("task_runner")
    engine = MagicMock()
    engine.dispose = recorder("engine")

    dependencies.set_job_scheduler(scheduler)
    dependencies.set_compose_services(ComposeServices(db=MagicMock(), sim=MagicMock(), monitor=monitor))
    dependencies.set_postgres_engine(engine)
    dependencies.set_env_worker_services(EnvWorkerServices(runner=runner))

    await dependencies.shutdown_standalone()

    assert order == ["scheduler", "compose_monitor", "task_runner", "engine"]
    assert dependencies.get_job_scheduler() is None
    assert dependencies.get_compose_job_monitor() is None
    assert dependencies.get_env_worker_services().runner is None


@pytest.mark.asyncio
async def test_one_subsystem_failing_to_stop_does_not_strand_the_others(restore_globals: None) -> None:
    scheduler = MagicMock()
    scheduler.close = AsyncMock(side_effect=RuntimeError("redis is already gone"))
    monitor = MagicMock()
    monitor.close = AsyncMock()
    runner = MagicMock()
    runner.close = AsyncMock()

    dependencies.set_job_scheduler(scheduler)
    dependencies.set_compose_services(ComposeServices(db=MagicMock(), sim=MagicMock(), monitor=monitor))
    dependencies.set_postgres_engine(None)
    dependencies.set_env_worker_services(EnvWorkerServices(runner=runner))

    await dependencies.shutdown_standalone()

    monitor.close.assert_awaited_once()
    runner.close.assert_awaited_once()
