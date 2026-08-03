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

from sms_api.common.gateway.models import ServerMode
from sms_api.config import get_settings
from sms_api.dependencies import (
    get_job_scheduler,
    init_standalone,
    shutdown_standalone,
)
from sms_api.version import __version__

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
    # "antibiotics",
    # "biofactory",
    "sms",
    "core",
    # "inference",
    # "variants",
]
ENV = get_settings()
assets_dir = Path(ENV.assets_dir)
ACTIVE_URL = ServerMode.detect(assets_dir / "dev" / "config" / ".dev_env")
UI_NAMES = [
    # "antibiotic",
    # "biofactory",
    "configure",  # no dataservice needed; possible uses though!
    "explore",  # uses dataservice, with nfs
    "dashboard",  # Atlantis EUTE dashboard — full end-to-end workflow
    "composer",  # compose (process-bigraph) colony simulation builder
    # "single_cell",  # uses /core router w/ generated client, no nfs
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
    job_scheduler = get_job_scheduler()
    if not job_scheduler:
        raise RuntimeError("JobScheduler is not initialized. Please check your configuration.")
    await job_scheduler.subscribe()
    await job_scheduler.start_polling(interval_seconds=5)  # configurable interval

    try:
        yield
    finally:
        await job_scheduler.close()
    await shutdown_standalone()


app = FastAPI(title=APP_TITLE, version=APP_VERSION, lifespan=lifespan, redoc_url="/documentation", docs_url="/docs")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],  # TODO: change origins back to allowed
)

# rely on core router for:
#   - images/simulators (build, status, list)
#   - parca (run, status, list)

# rely on api router for:
#   - simulations
#   - analyses
for api_name in APP_ROUTERS:
    try:
        api = importlib.import_module(f"sms_api.api.routers.{api_name}")
        app.include_router(
            router=api.config.router,
            prefix=api.config.prefix,
            dependencies=api.config.dependencies,
        )
    except ImportError:
        logger.exception(f"Could not register the following api: {api_name}")

# -- compose (process-bigraph) router -- #
try:
    from sms_api.api.routers.compose import router as compose_router

    app.include_router(compose_router, prefix="/compose/v1")
    logger.info("Compose router registered at /compose/v1")
except ImportError:
    logger.warning("Could not register compose router (compose deps may not be installed)")


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
    from sms_api.config import get_settings

    settings = get_settings()
    return {
        "docs": f"{ACTIVE_URL}{app.docs_url}",
        "version": APP_VERSION,
        "deployment_namespace": settings.deployment_namespace,
        "compute_backend": settings.compute_backend,
    }


@app.get("/version", tags=["SMS API"])
async def get_version() -> str:
    return APP_VERSION


# -- mount marimo apps to FastAPI root -- #

app.mount("/ws", server.build())


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, loop="auto")  # noqa: S104 binding to all interfaces
    logger.info("API Gateway Server started")
