"""Moved to :mod:`viva_core.compose.handlers` (core split, ``docs/plan-core.md`` P3d-4d-1).

This name stays importable, and at runtime it IS the new module -- the same object -- so
patching an attribute through this path patches the one real attribute, and ``isinstance``
checks agree across both names. New code should import from the new path.

Two things did NOT move, because they are SMS's: ``hooks_source`` (its runner hooks) is
``viva_api.simulation.compose_hooks``; ``run_compose_v2ecoli`` (its curated model run) is
``viva_api.simulation.compose_curated``.
"""

import sys
from typing import TYPE_CHECKING

from viva_core.compose import handlers as _moved

if TYPE_CHECKING:
    from viva_core.compose.handlers import *  # noqa: F403

sys.modules[__name__] = _moved
