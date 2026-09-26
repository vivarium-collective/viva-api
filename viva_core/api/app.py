"""``create_core_app()`` and the router it serves (``docs/plan-core.md`` P3, first step).

Core's routes live under ONE prefix, :data:`CORE_PREFIX`, and the paths are the same whether core
runs on its own (``create_core_app``) or an application includes the router beside its own routes
(``build_core_router``) -- so a client written against core works against either. An application
includes the ROUTER rather than mounting the app: a mounted sub-application's lifespan is never run,
and its routes would vanish from the application's OpenAPI document, which stays the union until
core has a client of its own (P8).

The routes reach services through a ``CoreContainer`` handed in as a PROVIDER, called per request:
an application builds its container when it starts, after this router is included.
"""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, HTTPException

from viva_core.api.capabilities import CAPABILITY_VIVA_V1_DATASETS, CAPABILITY_VIVA_V1_SURFACE, detect, mark_served
from viva_core.api.schemas import CoreCapabilities, CoreHealth, EnvironmentModel, ResolveEnvironmentRequest
from viva_core.container import CoreContainer, set_container_provider
from viva_core.environments import (
    Dependency,
    DerivedSpec,
    EnvironmentNotResolvable,
    EnvironmentResolverNotConfigured,
    EnvironmentSpec,
    ExplicitSpec,
)
from viva_core.lifespan import environments_from_settings, start_core
from viva_core.settings import CoreSettings, get_core_settings
from viva_core.version import __version__

CORE_PREFIX = "/viva/v1"


def _spec(request: ResolveEnvironmentRequest) -> EnvironmentSpec:
    if request.kind == "explicit":
        if not request.key:
            raise HTTPException(422, "an explicit environment needs a `key` (a commit, or its marked tag)")
        try:
            return ExplicitSpec(key=request.key, variant=request.variant)
        except ValueError as e:
            raise HTTPException(422, str(e)) from e
    return DerivedSpec(tuple(Dependency(d.name, d.source, d.version) for d in request.dependencies))


def build_core_router(container: Callable[[], CoreContainer], *, prefix: str = CORE_PREFIX) -> APIRouter:
    """``prefix`` is configurable for a deployment that must serve core somewhere else; every client and
    every document assumes the default."""
    router = APIRouter(prefix=prefix)

    @router.get("/health", response_model=CoreHealth, operation_id="core-health", tags=["Viva Core"])
    def health() -> CoreHealth:
        """Which core services this deployment provides. Core's own; the application has its own."""
        held = container()
        workers = held.env_worker
        return CoreHealth(
            version=__version__,
            services={
                "environments": held.environments is not None,
                "compose": held.compose is not None,
                "workers": workers is not None and workers.service is not None,
                "datasets": held.datasets is not None,
            },
        )

    @router.get(
        "/capabilities",
        response_model=CoreCapabilities,
        operation_id="core-capabilities",
        tags=["Viva Core"],
        summary="What this deployment can serve (for client feature detection)",
    )
    def capabilities() -> CoreCapabilities:
        """Stable names a client tests for membership -- never a version comparison. Core's own
        (the surfaces mounted in this process) plus the application's probes, if it embeds core."""
        held = container()
        # Core's own service probes go beside the application's; a surface that is mounted but has
        # no service behind it (datasets on a core with no store) is not advertised.
        probes = (*held.capabilities, (CAPABILITY_VIVA_V1_DATASETS, lambda: held.datasets is not None))
        return CoreCapabilities(version=__version__, capabilities=detect(probes))

    @router.post(
        "/environments/resolve",
        response_model=EnvironmentModel,
        operation_id="core-resolve-environment",
        tags=["Viva Core", "Environments"],
        summary="Which environment would a run with this need be given? (select; never builds)",
    )
    def resolve_environment(request: ResolveEnvironmentRequest) -> EnvironmentModel:
        resolver = container().environments
        if resolver is None:
            raise HTTPException(501, "this deployment provides no environment resolver")
        try:
            found = resolver.resolve(_spec(request))
        except EnvironmentNotResolvable as e:
            # Not something close: no environment answers this, and core's select half cannot build one.
            raise HTTPException(404, str(e)) from e
        except EnvironmentResolverNotConfigured as e:
            # The request is fine; the DEPLOYMENT is missing a setting, and the message names it.
            raise HTTPException(501, str(e)) from e
        return EnvironmentModel(image=found.image, spec_hash=found.spec_hash, image_digest=found.image_digest)

    # The datasets family (P4a-2): served wherever this router is -- a standalone core and the
    # application that includes it -- from the container's store, 503 by name where there is none.
    from viva_core.api.routers.datasets import router as datasets_router

    router.include_router(datasets_router, prefix="/datasets")
    return router


def container_from_settings(settings: CoreSettings) -> CoreContainer:
    """A standalone core's container BEFORE its lifespan has run: what the settings are enough to
    build synchronously (the environment resolver). The rest -- the database, compose, the env
    workers -- is ``viva_core.lifespan.start_core``'s, and arrives when the app starts (U2e)."""
    return CoreContainer(settings=settings, environments=environments_from_settings(settings))


def create_core_app(container: CoreContainer | None = None) -> FastAPI:
    """Core as an application of its own. With no container it builds one from ``CoreSettings`` --
    which is all a standalone core has, and all it may need (decision D7): the select-only
    container at once, and on startup everything ``start_core`` can build from the settings, with
    a shutdown that stops the pollers before the engine goes. A container handed in is served as
    is, with no lifespan -- the embedding application (or a test) owns its services' lives."""
    held: list[CoreContainer] = [container or container_from_settings(get_core_settings())]
    set_container_provider(lambda: held[0])

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if container is not None:
            yield
            return
        running = await start_core(get_core_settings())
        held[0] = running.container
        try:
            yield
        finally:
            await running.stop()
            held[0] = container_from_settings(get_core_settings())

    app = FastAPI(
        title="viva-core",
        version=__version__,
        docs_url=f"{CORE_PREFIX}/docs",
        openapi_url=f"{CORE_PREFIX}/openapi.json",
        lifespan=lifespan,
    )
    app.include_router(build_core_router(lambda: held[0]))
    # The compose and env-worker routers, at core's own prefix. Their services are the container's
    # (P3e); a standalone core whose container holds none answers by name on those routes, and
    # serves everything else.
    from viva_core.api.routers.compose import router as compose_router
    from viva_core.api.routers.env_worker import router as env_worker_router

    app.include_router(compose_router, prefix=f"{CORE_PREFIX}/compose")
    app.include_router(env_worker_router, prefix=f"{CORE_PREFIX}/env-worker")
    mark_served(CAPABILITY_VIVA_V1_SURFACE)
    return app
