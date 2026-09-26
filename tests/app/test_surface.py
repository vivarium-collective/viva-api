"""The clients address core's surfaces by CAPABILITY, never by version (plan-core §5.3, D14 / D17):
``viva-v1-surface`` advertised means ``/viva/v1/compose`` and ``/viva/v1/env-worker``; nothing
advertised means the application's spelling, which every deployment served before any of this.
"""

from __future__ import annotations

import httpx

from app.app_data_service import E2EDataService
from app.surface import CAPABILITY_ROUTES, FAMILIES, VIVA_V1_SURFACE, Surface, fetch_capabilities


class _Server:
    """A fake server that records every request; ``capabilities`` says what it advertises and on
    which route (``None`` = it has no capabilities route at all)."""

    def __init__(self, capabilities: list[str] | None, route: str = "/viva/v1/capabilities") -> None:
        self.calls: list[tuple[str, str]] = []
        self.capabilities = capabilities
        self.route = route

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append((request.method, request.url.path))
        if self.capabilities is not None and request.url.path == self.route:
            return httpx.Response(200, json={"version": "x", "capabilities": self.capabilities})
        if request.url.path.endswith("/status"):
            return httpx.Response(200, json={"status": "completed"})
        return httpx.Response(404, json={"detail": "no such route in this fake"})

    def client(self) -> httpx.Client:
        return httpx.Client(base_url="http://fake", transport=httpx.MockTransport(self.handler))


def _service(server: _Server) -> E2EDataService:
    service = E2EDataService(base_url="http://fake")
    service.client = server.client()
    return service


def test_a_server_advertising_the_surface_is_addressed_at_cores_spelling() -> None:
    server = _Server([VIVA_V1_SURFACE])
    service = _service(server)
    service.compose_get_simulation_status(7)
    assert ("GET", "/viva/v1/compose/simulation/7/status") in server.calls
    assert not any(path.startswith("/compose/v1") for _, path in server.calls)


def test_a_server_advertising_nothing_keeps_the_applications_spelling() -> None:
    for server in (_Server(None), _Server(["other"]), _Server(["other"], route="/core/v1/capabilities")):
        service = _service(server)
        service.compose_get_simulation_status(7)
        assert ("GET", "/compose/v1/simulation/7/status") in server.calls, server.calls


def test_the_applications_own_capabilities_route_is_honoured_when_cores_is_absent() -> None:
    """An application before P3g served only /core/v1/capabilities; a name there counts too."""
    server = _Server([VIVA_V1_SURFACE], route="/core/v1/capabilities")
    assert fetch_capabilities(server.client()) == {VIVA_V1_SURFACE}
    assert [p for _, p in server.calls] == list(CAPABILITY_ROUTES)


def test_capabilities_are_asked_once_per_client() -> None:
    server = _Server([VIVA_V1_SURFACE])
    service = _service(server)
    for sim in (1, 2, 3):
        service.compose_get_simulation_status(sim)
    assert [p for _, p in server.calls].count("/viva/v1/capabilities") == 1
    service.surface.forget()
    service.compose_get_simulation_status(4)
    assert [p for _, p in server.calls].count("/viva/v1/capabilities") == 2


def test_rewrite_touches_only_the_two_families() -> None:
    surface = Surface(_Server([VIVA_V1_SURFACE]).client)
    assert surface.rewrite("/env-worker/v1/relay/workers/j/call") == "/viva/v1/env-worker/relay/workers/j/call"
    assert surface.rewrite("/compose/v1") == "/viva/v1/compose"
    assert surface.rewrite("/api/v1/simulations") == "/api/v1/simulations"
    assert surface.rewrite("/core/v1/simulator/latest") == "/core/v1/simulator/latest"
    assert surface.rewrite("/compose/v1x") == "/compose/v1x"  # not the family, a different route
    legacy = Surface(_Server(None).client)
    for family, (application, _) in FAMILIES.items():
        assert legacy.path(family, "/x") == application + "/x"


def test_an_unreachable_server_is_taken_at_the_applications_spelling() -> None:
    def boom(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route to host")

    surface = Surface(lambda: httpx.Client(base_url="http://fake", transport=httpx.MockTransport(boom)))
    assert surface.capabilities() == frozenset()
    assert surface.path("compose", "/simulators") == "/compose/v1/simulators"
