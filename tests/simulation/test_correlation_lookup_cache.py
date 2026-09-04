"""viva-api#416: the correlation_id -> hpcrun_id lookup both message handlers
use must never cache a miss. Every dispatch path submits to the backend
BEFORE inserting the HpcRun row, so a worker event that arrives first sees
"no row" -- and with @alru_cache that None was the permanent answer, dropping
every later event for the run."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from viva_api.compose.job_monitor import ComposeJobMonitor
from viva_api.simulation.job_scheduler import JobScheduler


@pytest.mark.asyncio
async def test_job_scheduler_retries_a_miss_and_caches_the_hit() -> None:
    db = MagicMock()
    db.get_hpcrun_id_by_correlation_id = AsyncMock(side_effect=[None, 506, 999])
    scheduler = JobScheduler(messaging_service=MagicMock(), database_service=db)

    assert await scheduler.get_hpcrun_by_correlation_id("corr") is None  # row not inserted yet
    assert await scheduler.get_hpcrun_by_correlation_id("corr") == 506  # asked again: found
    assert await scheduler.get_hpcrun_by_correlation_id("corr") == 506  # hit is cached (999 never read)
    assert db.get_hpcrun_id_by_correlation_id.await_count == 2


@pytest.mark.asyncio
async def test_compose_job_monitor_retries_a_miss_and_caches_the_hit() -> None:
    hpc_db = MagicMock()
    hpc_db.get_hpcrun_id_by_correlation_id = AsyncMock(side_effect=[None, 7, 999])
    db = MagicMock()
    db.get_hpc_db.return_value = hpc_db
    monitor = ComposeJobMonitor(nats_client=None, database_service=db)

    assert await monitor.get_hpcrun_by_correlation_id("corr") is None
    assert await monitor.get_hpcrun_by_correlation_id("corr") == 7
    assert await monitor.get_hpcrun_by_correlation_id("corr") == 7
    assert hpc_db.get_hpcrun_id_by_correlation_id.await_count == 2
