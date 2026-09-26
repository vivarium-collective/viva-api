"""How THIS application provides core's container (``docs/plan-core.md`` P3).

Core's routes reach services through a ``CoreContainer``; an application that embeds core says what
goes in it. For SMS that is its own settings object -- which IS a ``CoreSettings`` -- and the site's
environment resolver (``common/site_environments``: one ECR repository, images tagged by key).

Built per request from the settings of the moment and the services ``dependencies.py`` built in
the lifespan (P3e): cheap, and it means a test that patches ``get_settings`` is what core's routes
see, like everything else here. The provider is registered at import, so core's routers can ask for
the container before the lifespan has run and get one with no services yet.
"""

from viva_api.analysis.models import DATASET_KINDS
from viva_api.common.capabilities import CAPABILITY_REGISTRY
from viva_api.common.handlers.datasets import UNSERVED_KINDS
from viva_api.common.site_environments import site_resolver
from viva_api.config import get_settings
from viva_api.dependencies import (
    get_compose_services,
    get_database_service,
    get_env_worker_services,
    get_file_service,
)
from viva_api.simulation.dataset_store import SmsDatasetStore
from viva_core.container import CoreContainer, DatasetServices, set_container_provider


def _dataset_services() -> DatasetServices | None:
    """Core's dataset reads over this application's ``dataset`` table (P4a-2) -- once the database
    exists (the lifespan), with this application's kind vocabulary and its storage bucket."""
    db = get_database_service()
    if db is None:
        return None
    settings = get_settings()
    return DatasetServices(
        store=SmsDatasetStore(db),
        kinds=DATASET_KINDS,
        files=get_file_service(),
        storage_bucket=settings.storage_s3_bucket or None,
        unserved_kinds=UNSERVED_KINDS,
    )


def core_container() -> CoreContainer:
    settings = get_settings()
    return CoreContainer(
        settings=settings,
        environments=site_resolver(settings),
        compose=get_compose_services(),
        env_worker=get_env_worker_services(),
        datasets=_dataset_services(),
        # This application's probes, so `/viva/v1/capabilities` and `/core/v1/capabilities` agree (P3g).
        capabilities=tuple(CAPABILITY_REGISTRY),
    )


set_container_provider(core_container)
