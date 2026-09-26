"""Which spelling of core's surfaces a server serves -- decided by CAPABILITY, never by version
(``docs/plan-core.md`` §5.3; decisions D14, D17; ``viva_api/common/capabilities.py``).

Every SMS-shaped prefix is dated. During the dual period a server may serve compose at
``/compose/v1`` (the application's spelling, every deployment before 0.9.157), at
``/viva/v1/compose`` (core's, mounted in the application since P3g and the only spelling a
standalone core serves), or both. The clients keep writing the application's spelling -- it is the
logical name, readable in every call site -- and ONE chokepoint rewrites it to what the server
advertises: ``viva-v1-surface`` in its capabilities means core's spelling is served.

The capabilities are asked for once per client, from core's route first (``/viva/v1/capabilities``,
served by the application and by a standalone core alike) and then the application's; a server
that answers neither (an old deployment, a gateway that routes neither prefix) is taken at the
application's spelling, which is what it served before any of this existed.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import httpx

logger = logging.getLogger(__name__)

VIVA_V1_SURFACE = "viva-v1-surface"
CAPABILITY_ROUTES = ("/viva/v1/capabilities", "/core/v1/capabilities")

#: family -> (the application's spelling, core's spelling). The interim spelling of D17; the final
#: families (``/viva/v1/composites``, ``/viva/v1/workers``) arrive with their own capability names.
FAMILIES: dict[str, tuple[str, str]] = {
    "compose": ("/compose/v1", "/viva/v1/compose"),
    "env-worker": ("/env-worker/v1", "/viva/v1/env-worker"),
}


def fetch_capabilities(client: httpx.Client) -> frozenset[str]:
    """The names the server advertises, or an empty set when it advertises none. Never raises:
    a server that cannot say is served at the application's spelling."""
    for route in CAPABILITY_ROUTES:
        try:
            resp = client.get(route)
        except httpx.HTTPError as e:
            logger.debug("capabilities: %s unreachable (%s)", route, e)
            return frozenset()
        if resp.status_code != 200:
            continue
        try:
            names = resp.json().get("capabilities")
        except ValueError:
            continue
        if isinstance(names, list):
            return frozenset(str(n) for n in names)
    return frozenset()


class Surface:
    """One client's view of what the server serves. The client is read through a callable, so a
    caller that swaps its ``httpx.Client`` (a test, a re-pointed CLI) is followed."""

    def __init__(self, client: Callable[[], httpx.Client]) -> None:
        self._client = client
        self._capabilities: frozenset[str] | None = None

    def capabilities(self) -> frozenset[str]:
        if self._capabilities is None:
            self._capabilities = fetch_capabilities(self._client())
        return self._capabilities

    def forget(self) -> None:
        """Ask again on the next call (the CLI was re-pointed, the server redeployed)."""
        self._capabilities = None

    def serves_core_spelling(self) -> bool:
        return VIVA_V1_SURFACE in self.capabilities()

    def path(self, family: str, rest: str = "") -> str:
        """The served path of ``rest`` under a family: core's spelling when advertised, else the
        application's."""
        legacy, core = FAMILIES[family]
        return (core if self.serves_core_spelling() else legacy) + rest

    def rewrite(self, path: str) -> str:
        """A path written in the application's spelling, as the server serves it. A path under no
        family is returned unchanged."""
        for legacy, core in FAMILIES.values():
            if path == legacy or path.startswith(legacy + "/"):
                return (core if self.serves_core_spelling() else legacy) + path[len(legacy) :]
        return path
