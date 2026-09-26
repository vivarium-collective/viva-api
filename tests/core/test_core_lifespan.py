"""A standalone core's lifespan (U2e): what ``start_core`` builds from settings alone, and the app
booted with SLURM settings against the cluster in Docker and a database of its own.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import make_url

from tests.fixtures.slurm_fixtures_backend import SlurmBackend
from viva_core.api import CORE_PREFIX, create_core_app
from viva_core.compose.simulation_service_hpc import ComposeSimulationServiceHpc
from viva_core.container import current_container
from viva_core.infra.db import postgres_configured
from viva_core.lifespan import start_core
from viva_core.models import JobBackend
from viva_core.settings import CoreSettings, get_core_settings, set_core_settings_provider


@pytest.mark.asyncio
async def test_nothing_configured_starts_with_nothing_and_stops_cleanly() -> None:
    """The settings' defaults name no database, no submit host, no namespace: the container carries
    the absences, the routes answer by name, and stop() has nothing to stop."""
    settings = CoreSettings(_env_file=None)  # type: ignore[call-arg]  # pydantic-settings' own kwarg
    assert not postgres_configured(settings)
    running = await start_core(settings)
    try:
        assert running.container.compose is None and running.engine is None and running.monitor is None
        workers = running.container.env_worker
        assert workers is not None and (workers.service, workers.task_db, workers.runner) == (None, None, None)
    finally:
        await running.stop()


@pytest.mark.slurm
def test_the_app_boots_with_slurm_settings_and_its_own_database(slurm_backend: SlurmBackend, postgres_url: str) -> None:
    """``create_core_app()`` with no container: the lifespan opens the database, creates the compose
    schema, registers the SLURM compose service over the cluster's SSH, starts the monitor, and the
    routes that were 500-by-name answer; shutdown stops the monitor and disposes the engine."""
    url = make_url(postgres_url)
    base = get_core_settings()  # the fixture pointed its SLURM fields at the cluster
    settings = CoreSettings(
        _env_file=None,  # type: ignore[call-arg]
        postgres_user=url.username or "",
        postgres_password=url.password or "",
        postgres_host=url.host or "",
        postgres_port=url.port or 5432,
        postgres_database=url.database or "",
        db_create_all=True,
        slurm_submit_host=base.slurm_submit_host,
        slurm_submit_port=base.slurm_submit_port,
        slurm_submit_user=base.slurm_submit_user,
        slurm_submit_key_path=base.slurm_submit_key_path,
        slurm_submit_known_hosts=None,
        slurm_partition=base.slurm_partition,
        slurm_qos=base.slurm_qos,
        slurm_log_base_path=base.slurm_log_base_path,
        slurm_base_path=base.slurm_base_path,
        compose_image_base_path=base.compose_image_base_path,
        compose_sim_base_path=base.compose_sim_base_path,
    )
    # The provider that was registered (the application's, when viva_api is imported) is put back
    # by tests/core/conftest.py -- not cleared, which would leave later tests reading the environment.
    set_core_settings_provider(lambda: settings)
    with TestClient(create_core_app()) as client:
        health = client.get(f"{CORE_PREFIX}/health").json()
        assert health["services"] == {"environments": False, "compose": True, "workers": False, "datasets": False}, (
            health
        )
        held = current_container()
        assert held.compose is not None and isinstance(held.compose.sim, ComposeSimulationServiceHpc)
        assert held.compose.sim.backend is JobBackend.SLURM
        assert held.compose.monitor.is_polling
        # the compose schema exists in core's own database, and a read route answers from it
        assert client.get(f"{CORE_PREFIX}/compose/simulations/status/batch", params={"ids": [1]}).json() == []
        assert client.get(f"{CORE_PREFIX}/compose/simulation/1/status").status_code == 404
        # the task tier is there (the relay's task routes need it), the worker service is not (no namespace)
        workers = held.env_worker
        assert workers is not None and workers.task_db is not None and workers.runner is not None
        assert workers.service is None
        monitor = held.compose.monitor
    # after shutdown: the poller is stopped and the select-only container is back
    assert not monitor.is_polling
    assert current_container().compose is None
