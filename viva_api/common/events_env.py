"""Moved to :mod:`viva_core.events.events_env` (core split, ``docs/plan-core.md`` P1).

This name stays importable, and at runtime it IS the new module -- the same object -- so
patching an attribute through this path patches the one real attribute, and ``isinstance``
checks agree across both names. New code should import from the new path.
"""

import sys
from typing import TYPE_CHECKING

from viva_core.events import events_env as _moved

if TYPE_CHECKING:
    from viva_core.events.events_env import *  # noqa: F403

sys.modules[__name__] = _moved
