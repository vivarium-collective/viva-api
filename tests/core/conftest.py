"""Core-test hygiene: every test here leaves the two process-wide providers as it found them.

``viva_core.settings`` and ``viva_core.container`` each hold ONE provider for the process -- how the
settings and the container of the moment are obtained. ``create_core_app()`` registers its own
container provider; ``set_core_settings_provider`` is how a test hands core a settings object; and
when the application's tests share the process, ``viva_api`` has registered ITS providers at import.
A core test that sets either and does not put it back leaves every later test reading a standalone
core's empty container (503 by name on its routes) or ``CoreSettings`` from the environment -- the
trap viva-api#807 hit once and each test file then guarded on its own. This fixture guards them all.
"""

from collections.abc import Iterator

import pytest

import viva_core.container as container_mod
import viva_core.settings as settings_mod


@pytest.fixture(autouse=True)
def _restore_the_core_providers() -> Iterator[None]:
    saved_settings = settings_mod._provider
    saved_container = container_mod._provider
    try:
        yield
    finally:
        settings_mod._provider = saved_settings
        container_mod._provider = saved_container
