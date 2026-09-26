"""What this running core can serve, as names a client tests for membership (``docs/plan-core.md`` P3g).

The contract is the application's (``viva_api.common.capabilities``), restated here because core is
where it now lives: a capability is advertised only when THIS deployment can genuinely serve it --
code present, configured, wired. Clients branch on membership and never on a version; names are
stable, additive, and an unfamiliar one is ignored. Core's own names below; an application appends
its own through ``CoreContainer.capabilities``.

Two kinds of probe. A **served-surface** capability says a set of routes is mounted -- a fact of the
application that included the routers, not of any service -- so it is *marked* by whoever mounts
them (``create_core_app`` for a standalone core; the application's composition root when it embeds
core). A **service** capability is a callable asked at request time, like the application's probes.
"""

import logging
from collections.abc import Callable, Iterable

logger = logging.getLogger(__name__)

#: The core routers answer under ``/viva/v1`` -- the interim spelling (``/viva/v1/compose``,
#: ``/viva/v1/env-worker``; decision D17) today, the resource families as they land. A caller that sees
#: this switches its pure-prefix operations off ``/compose/v1`` and ``/env-worker/v1``.
CAPABILITY_VIVA_V1_SURFACE = "viva-v1-surface"

#: ``/viva/v1/datasets`` answers from a dataset store (plan P4a-2): a service capability, probed at
#: request time -- the routes are always mounted, but a deployment with no store answers 503 on them.
CAPABILITY_VIVA_V1_DATASETS = "viva-v1-datasets"

#: (name, probe). A probe answers "can this deployment serve it right now?"; one that raises is
#: reported as unsupported, never as an error -- capability reporting must not take the route down.
Capability = tuple[str, Callable[[], bool]]

_served: set[str] = set()


def mark_served(name: str) -> None:
    """Record that the routes behind ``name`` are mounted in this process. Called once by whoever
    mounts them; idempotent."""
    _served.add(name)


def served() -> frozenset[str]:
    return frozenset(_served)


def detect(registry: Iterable[Capability]) -> list[str]:
    """Every marked surface plus every registry probe that answers True, sorted for stable output."""
    supported = set(_served)
    for name, probe in registry:
        try:
            if probe():
                supported.add(name)
        except Exception:
            logger.warning("capability probe %r failed; reporting it as unsupported", name, exc_info=True)
    return sorted(supported)
