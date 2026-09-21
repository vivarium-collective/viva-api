"""How THIS application provides core's container (``docs/plan-core.md`` P3).

Core's routes reach services through a ``CoreContainer``; an application that embeds core says what
goes in it. For SMS that is its own settings object -- which IS a ``CoreSettings`` -- and the site's
environment resolver (``common/site_environments``: one ECR repository, images tagged by key).

Built per request from the settings of the moment: cheap, and it means a test that patches
``get_settings`` is what core's routes see, like everything else here. The rest of
``dependencies.py`` moves into containers as its services move into core.
"""

from viva_api.common.site_environments import site_resolver
from viva_api.config import get_settings
from viva_core.container import CoreContainer


def core_container() -> CoreContainer:
    settings = get_settings()
    return CoreContainer(settings=settings, environments=site_resolver(settings))
