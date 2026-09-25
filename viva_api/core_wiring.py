"""How THIS application provides core's container (``docs/plan-core.md`` P3).

Core's routes reach services through a ``CoreContainer``; an application that embeds core says what
goes in it. For SMS that is its own settings object -- which IS a ``CoreSettings`` -- and the site's
environment resolver (``common/site_environments``: one ECR repository, images tagged by key).

Built per request from the settings of the moment and the services ``dependencies.py`` built in
the lifespan (P3e): cheap, and it means a test that patches ``get_settings`` is what core's routes
see, like everything else here. The provider is registered at import, so core's routers can ask for
the container before the lifespan has run and get one with no services yet.
"""

from viva_api.common.site_environments import site_resolver
from viva_api.config import get_settings
from viva_api.dependencies import get_compose_services, get_env_worker_services
from viva_core.container import CoreContainer, set_container_provider


def core_container() -> CoreContainer:
    settings = get_settings()
    return CoreContainer(
        settings=settings,
        environments=site_resolver(settings),
        compose=get_compose_services(),
        env_worker=get_env_worker_services(),
    )


set_container_provider(core_container)
