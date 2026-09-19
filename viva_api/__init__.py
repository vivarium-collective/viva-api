"""viva-api: the application built on :mod:`viva_core`."""

from viva_core.settings import CoreSettings, set_core_settings_provider


def _application_settings() -> CoreSettings:
    # Imported lazily: importing the package must stay cheap, and viva_api.config loads the
    # dotenv files and builds the full Settings on first use.
    from viva_api.config import get_settings

    return get_settings()


# Anything imported from `viva_api.*` -- including the stubs left at a moved module's old path
# -- runs this first, so core never builds a second settings object from the environment in a
# process that has the application's. A standalone core never imports viva_api and keeps its
# own default.
set_core_settings_provider(_application_settings)
