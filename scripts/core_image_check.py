"""Does the core image boot and answer? Run INSIDE the built image, with no network and no
settings: `create_core_app()` from `CoreSettings` defaults, then core's own health and
capabilities routes through the ASGI test client. Proves the image carries what core imports
(and nothing it must not: `viva_api` is absent, and importing it here must fail).

    docker run --rm viva-core:check /app/.venv/bin/python -c "$(cat scripts/core_image_check.py)"
"""

import importlib.util
import sys

from fastapi.testclient import TestClient

from viva_core.api.app import CORE_PREFIX, create_core_app
from viva_core.version import __version__

if importlib.util.find_spec("viva_api") is not None:
    sys.exit("the core image carries viva_api; it must carry core alone")

with TestClient(create_core_app()) as client:
    health = client.get(f"{CORE_PREFIX}/health")
    if health.status_code != 200 or health.json().get("version") != __version__:
        sys.exit(f"health answered {health.status_code}: {health.text}")
    capabilities = client.get(f"{CORE_PREFIX}/capabilities")
    if "viva-v1-surface" not in capabilities.json().get("capabilities", []):
        sys.exit(f"capabilities answered {capabilities.status_code}: {capabilities.text}")
print(f"core {__version__} ok: {health.json()}")
