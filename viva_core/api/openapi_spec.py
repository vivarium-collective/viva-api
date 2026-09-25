"""Write core's own OpenAPI document (``docs/plan-core.md`` P3f): the routes ``create_core_app()`` serves,
and nothing of the application's. The SMS document stays the union; this one is what the generated core
client (D8) and a standalone core's callers are built against.

``make spec`` runs both generators.
"""

import json
from pathlib import Path

import yaml
from fastapi.openapi.utils import get_openapi

from viva_core.api.app import create_core_app
from viva_core.container import CoreContainer
from viva_core.settings import CoreSettings

SPEC_DIR = Path(__file__).resolve().parent / "spec"


def core_openapi() -> dict[str, object]:
    """The document, from an app built on bare ``CoreSettings`` -- what a standalone core serves."""
    app = create_core_app(CoreContainer(settings=CoreSettings()))
    document: dict[str, object] = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=app.openapi_version,
        description=app.description,
        routes=app.routes,
        servers=app.servers,
    )
    return document


def main() -> None:
    document = core_openapi()
    openapi_version = str(document.get("openapi", "3.1.0")).replace(".", "_")
    target = SPEC_DIR / f"openapi_{openapi_version}_generated.yaml"
    SPEC_DIR.mkdir(parents=True, exist_ok=True)
    target.write_text(yaml.dump(json.loads(json.dumps(document)), sort_keys=False))
    paths = document.get("paths")
    n = len(paths) if isinstance(paths, dict) else 0
    print(f"core OpenAPI document written: {target} ({n} paths)")


if __name__ == "__main__":
    main()
