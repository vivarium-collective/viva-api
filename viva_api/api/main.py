"""
- base sim (cached)
- antibiotic
- biomanufacturing
- batch variant endpoint
- design specific endpoints.
- downsampling ...
- biocyc id
- api to download the data
- marimo instead of Jupyter notebooks....(auth). ... also on gov cloud.
- endpoint to send sql like queries to parquet files back to client
"""

import importlib
import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path

import marimo
import uvicorn
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from starlette import templating
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse

from viva_api.common.gateway.models import ServerMode
from viva_api.config import get_settings
from viva_api.core_wiring import core_container
from viva_api.dependencies import (
    get_job_scheduler,
    init_standalone,
    shutdown_standalone,
)
from viva_api.version import __version__
from viva_core.api import build_core_router

logger = logging.getLogger(__name__)


APP_VERSION = __version__
APP_TITLE = "sms-api"
APP_ORIGINS = [
    "http://0.0.0.0:8000",
    "http://127.0.0.1:8000",
    "http://127.0.0.1:8888",
    "http://127.0.0.1:4200",
    "http://127.0.0.1:4201",
    "http://127.0.0.1:4202",
    "http://localhost:4200",
    "http://localhost:4201",
    "http://localhost:4202",
    "http://localhost:8888",
    "http://localhost:8000",
    "http://localhost:3001",
    "https://sms.cam.uchc.edu",
]
APP_ROUTERS = [
    "sms",
    "core",
    "tasks",
    "datasets",
]
ENV = get_settings()
assets_dir = Path(ENV.assets_dir)
ACTIVE_URL = ServerMode.detect(assets_dir / "dev" / "config" / ".dev_env")
UI_NAMES = [
    "configure",  # no dataservice needed; possible uses though!
    "explore",  # uses dataservice, with nfs
    "dashboard",  # Atlantis EUTE dashboard — full end-to-end workflow
    "composer",  # compose (process-bigraph) colony simulation builder
]


# -- app configuration: lifespan and middleware -- #


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    # configure and start standalone services (data, sim, db, etc)
    dev_mode = os.getenv("DEV_MODE", "0")
    start_standalone = partial(init_standalone)
    if bool(int(dev_mode)):
        logger.warning("Development Mode is currently engaged!!!", stacklevel=1)
        start_standalone.keywords["enable_ssl"] = True
    await start_standalone()

    # --- JobScheduler setup ---
    # The scheduler is SMS's (the simulation and analysis state machines). A deployment that
    # configures no simulation backend has none, and the app serves without it -- core's routes,
    # compose and env workers do not need it (P3f). Say so once, rather than refuse to start.
    job_scheduler = get_job_scheduler()
    if job_scheduler:
        await job_scheduler.subscribe()
        await job_scheduler.start_polling(interval_seconds=5)  # configurable interval
    else:
        logger.warning("JobScheduler is not initialized: no simulation backend configured; serving without it")

    try:
        yield
    finally:
        if job_scheduler:
            await job_scheduler.close()
    await shutdown_standalone()


app = FastAPI(title=APP_TITLE, version=APP_VERSION, lifespan=lifespan, redoc_url="/documentation", docs_url="/docs")
# CORS. Two things are deliberate here and both were wrong before (#336).
#
# `allow_origins=["*"]` together with `allow_credentials=True` is INVALID per the
# CORS spec -- a browser rejects `Access-Control-Allow-Origin: *` on any request
# made with credentials. So the old combination did not mean "permissive"; it
# meant "credentialed cross-origin requests fail", which is a different thing and
# not what the code read as.
#
# `allow_credentials=False` is the honest setting TODAY, because nothing sends a
# credential: this API has no cookies, no session and no Authorization header
# (79 routes, zero security schemes). The browsers viva-api serves -- /docs,
# /documentation, /home -- are same-origin, so CORS does not apply to them at
# all. The marimo GUI's calls run in the marimo KERNEL (httpx, server-side), and
# the CLI and TUI are not browsers, so none of the three clients is a CORS
# caller either.
#
# The origin list is narrowed from `["*"]` back to APP_ORIGINS, which is what it
# was always meant to be -- it has sat unused above since the `["*"]` was
# introduced. Narrowing costs nothing today (every real caller is same-origin or
# not a browser) and closes one small real hole: a developer with the SSM tunnel
# open on localhost has an internal API reachable from their browser, and `["*"]`
# lets any page they happen to visit read it.
#
# WHEN #337 LANDS and a bearer token becomes a thing, revisit this: a credentialed
# browser client would need `allow_credentials=True`, and that is only valid with
# an explicit origin list -- which is the one below. Add the origin, do not widen
# back to `["*"]`.
app.add_middleware(
    CORSMiddleware,
    allow_origins=APP_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# rely on core router for:
#   - images/simulators (build, status, list)
#   - parca (run, status, list)

# rely on api router for:
#   - simulations
#   - analyses
for api_name in APP_ROUTERS:
    try:
        api = importlib.import_module(f"viva_api.api.routers.{api_name}")
        app.include_router(
            router=api.config.router,
            prefix=api.config.prefix,
            dependencies=api.config.dependencies,
        )
    except ImportError:
        logger.exception(f"Could not register the following api: {api_name}")

# -- viva core (docs/plan-core.md P3): core's own router, under /viva/v1 -- #
# INCLUDED, not mounted: the paths are the ones a standalone core serves (`create_core_app`), they
# stay in this application's OpenAPI document (the union, until core has a client of its own), and
# the container is asked for per request -- so it may be built from services that exist only once
# the lifespan has run. Not to be confused with the `core` router above (`/core/v1/simulator/*`),
# which is SMS's and older than the split.
app.include_router(build_core_router(core_container))

# -- compose (process-bigraph) router -- #
try:
    from viva_api.api.routers.compose_sms import router as compose_sms_router
    from viva_core.api.routers.compose import router as compose_router

    # Core's compose router at the prefix SMS's callers use (unchanged for this refactor), plus
    # SMS's own curated-model route at the same prefix (P3d-4d-2).
    app.include_router(compose_router, prefix="/compose/v1")
    app.include_router(compose_sms_router, prefix="/compose/v1")
    logger.info("Compose router registered at /compose/v1")
except ImportError:
    logger.warning("Could not register compose router (compose deps may not be installed)")

# -- env-worker router (vivarium-workbench#942 / REFACTOR-PLAN §2A.8) -- #
# Runs a simulator's prebuilt image as a workbench env worker. Registered
# unconditionally; the endpoints answer 503 until dependencies.py wires the
# service, so a deployment without K8s job support fails clearly rather than 404.
try:
    from viva_core.api.routers.env_worker import router as env_worker_router

    app.include_router(env_worker_router, prefix="/env-worker/v1")
    logger.info("Env-worker router registered at /env-worker/v1")
except ImportError:
    logger.warning("Could not register env-worker router")

# -- the same two routers at core's own prefix: the dated interim spelling (plan-core P3g, D17) -- #
# `/viva/v1/compose` and `/viva/v1/env-worker` are what a standalone core serves today, so a caller
# switching on the `viva-v1-surface` capability can leave `/compose/v1` and `/env-worker/v1` before
# the resource families (`/viva/v1/composites`, `/viva/v1/workers`) exist; these mounts go at M5.
# Served but NOT in this application's OpenAPI document: the routes carry explicit operation ids,
# and a second copy would duplicate every one -- core's own document is where this spelling is
# described. `routes` in `atlantis smoke` compares documents, so it is unaffected; the `core` check
# sees them through the gateway.
try:
    from viva_core.api.capabilities import CAPABILITY_VIVA_V1_SURFACE, mark_served
    from viva_core.api.routers.compose import router as _core_compose_router
    from viva_core.api.routers.env_worker import router as _core_env_worker_router

    app.include_router(_core_compose_router, prefix="/viva/v1/compose", include_in_schema=False)
    app.include_router(_core_env_worker_router, prefix="/viva/v1/env-worker", include_in_schema=False)
    mark_served(CAPABILITY_VIVA_V1_SURFACE)
    logger.info("Core's compose and env-worker routers also registered at /viva/v1 (interim spelling)")
except ImportError:
    logger.warning("Could not register core's routers at /viva/v1")


# -- set ui templates and marimo notebook apps -- #

client_dir = Path(ENV.app_dir) or Path("app")
ui_dir = client_dir / "ui"
templates_dir = client_dir / "templates"
server = marimo.create_asgi_app()

app_filenames = [f"{modname}.py" for modname in UI_NAMES]
for filename in sorted(os.listdir(ui_dir)):
    if filename in app_filenames:
        app_name = filename.replace(".py", "").capitalize()
        app_path = ui_dir / filename
        server = server.with_app(path=f"/{app_name}", root=app_path.__str__())

templates = Jinja2Templates(directory=templates_dir)


# -- main-level endpoints -- #
@app.get("/")
async def redirect_old_path() -> RedirectResponse:
    return RedirectResponse(url="/home")


@app.get("/home", tags=["SMS API"])
async def home(request: Request) -> templating._TemplateResponse:
    app_info = [
        # ("Antibiotic", "Explore new possibilities"),
        # ("Biofactory", "Create new strains"),
        ("Configure", "Invent and configure new Ecoli experiments"),
        ("Explore", "Introspect and explore simulation data"),
        ("Dashboard", "Full end-to-end simulation workflow"),
        ("Composer", "Build and run v2ecoli colony simulations via process-bigraph"),
        # ("Single Cell", "interactive"),
    ]
    return templates.TemplateResponse(
        request, "home.html", {"request": request, "app_names": app_info, "marimo_path_prefix": "/ws"}
    )


@app.get("/health", tags=["SMS API"])
async def check_health() -> dict[str, str]:
    from viva_api.config import get_settings
    from viva_api.simulation.db_startup import get_schema_state

    settings = get_settings()
    schema = get_schema_state()
    return {
        "docs": f"{ACTIVE_URL}{app.docs_url}",
        "version": APP_VERSION,
        "deployment_namespace": settings.deployment_namespace,
        "compute_backend": settings.compute_backend,
        # The database's Alembic revision against this image's head, as read at startup. Absent
        # when no database is configured. `atlantis smoke` fails a deploy on db_at_head=false.
        **(schema.as_health_fields() if schema is not None else {}),
    }


@app.get("/version", tags=["SMS API"])
async def get_version() -> str:
    return APP_VERSION


# -- mount marimo apps to FastAPI root -- #

app.mount("/ws", server.build())


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, loop="auto")  # noqa: S104 binding to all interfaces
    logger.info("API Gateway Server started")
