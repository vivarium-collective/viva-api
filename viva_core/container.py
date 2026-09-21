"""What a running core holds: its settings and the services its routes reach.

One object, handed to the routes through a provider -- not module globals pushed into routers by
setters, which is what ``viva_api/dependencies.py`` does today and what ``docs/plan-core.md`` P3
replaces. It starts with the one service core has; each service that moves into core becomes a
field here, ``None`` where a deployment does not provide it (a route then answers 501, by name).
"""

from dataclasses import dataclass

from viva_core.environments import EnvironmentResolver
from viva_core.settings import CoreSettings


@dataclass(frozen=True, slots=True)
class CoreContainer:
    settings: CoreSettings
    environments: EnvironmentResolver | None = None
