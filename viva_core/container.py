"""What a running core holds: its settings and the services its routes reach.

One object, handed to the routes through a provider -- not module globals pushed into routers by
setters, which is what ``viva_api/dependencies.py`` did until P3e and what this replaces. Each
service group is ``None`` where a deployment does not provide it; a route then answers by name
(500 "not initialized", the status those routes have always used) rather than pretending a 404.

The provider is registered the way core's settings provider is (``viva_core.settings``): the
application that embeds core -- or ``create_core_app`` for a standalone core -- says how to get the
container of the moment. Routes call :func:`current_container` at request time, so an application
whose services exist only after its lifespan has run can still provide them.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from viva_core.environments import EnvironmentResolver
from viva_core.settings import CoreSettings

if TYPE_CHECKING:
    from viva_core.compose.database_service import ComposeDatabaseService, EnvWorkerTaskDatabaseService
    from viva_core.compose.job_monitor import ComposeJobMonitor
    from viva_core.compose.service import ComposeSimulationService
    from viva_core.env_worker.relay import TaskRunner
    from viva_core.env_worker.service import EnvWorkerService
    from viva_core.storage.file_service import FileService


@dataclass(frozen=True, slots=True)
class ComposeServices:
    """What the compose routes reach: the compose database, the default backend, the monitor (which
    also holds the per-backend registry), the file service results stream from, and the allow-list
    the check falls back to when the table is empty -- the APPLICATION's list (SMS: its science stack);
    core's default is nothing beyond what the table says."""

    db: "ComposeDatabaseService"
    sim: "ComposeSimulationService"
    monitor: "ComposeJobMonitor"
    files: "FileService | None" = None
    default_allow_list: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EnvWorkerServices:
    """What the env-worker routes reach. Every field is optional on its own: a deployment may run
    workers without the durable task tier (no compose database) or the tier without workers."""

    service: "EnvWorkerService | None" = None
    task_db: "EnvWorkerTaskDatabaseService | None" = None
    runner: "TaskRunner | None" = None


@dataclass(frozen=True, slots=True)
class CoreContainer:
    settings: CoreSettings
    environments: EnvironmentResolver | None = None
    compose: ComposeServices | None = None
    env_worker: EnvWorkerServices | None = None


_provider: Callable[[], CoreContainer] | None = None


def set_container_provider(provider: Callable[[], CoreContainer] | None) -> None:
    """Register how the container of the moment is obtained. ``create_core_app`` registers the one it
    holds; an application that embeds core registers its own (SMS: ``viva_api.core_wiring``)."""
    global _provider
    _provider = provider


def current_container() -> CoreContainer:
    """The container of the moment. Raises by name when no application has registered one."""
    if _provider is None:
        raise RuntimeError(
            "no core container is registered: an application embedding viva_core must call "
            "set_container_provider(...), and create_core_app() does so for a standalone core"
        )
    return _provider()
