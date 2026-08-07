import json
import os

import yaml
from fastapi.openapi.utils import get_openapi

from viva_api.api.main import app


def main() -> None:
    openapi_spec = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=app.openapi_version,
        description=app.description,
        routes=app.routes,
        servers=app.servers,
    )

    # Convert the JSON OpenAPI spec to YAML
    openapi_spec_yaml = yaml.dump(json.loads(json.dumps(openapi_spec)), sort_keys=False)

    current_directory = os.path.dirname(os.path.realpath(__file__))

    # Write the YAML OpenAPI spec to a file in subdirectory spec
    openapi_version = app.openapi_version.replace(".", "_")
    spec_fp = f"{current_directory}/spec/openapi_{openapi_version}_generated.yaml"
    if os.path.exists(spec_fp):
        print("Spec exists, overwriting")
        os.remove(spec_fp)

    with open(spec_fp, "w") as f:
        f.write(openapi_spec_yaml)

    print("New OpenAPI spec for viva_api.api generated!")


if __name__ == "__main__":
    main()
