"""Moved to :mod:`viva_core.api.routers.compose` (core split, ``docs/plan-core.md`` P3d-4d-2).

This name stays importable, and at runtime it IS the new module -- the same object -- so
patching an attribute through this path patches the one real attribute, and ``isinstance``
checks agree across both names. New code should import from the new path.

The one route that was SMS's, ``/curated/ecoli``, is :mod:`viva_api.api.routers.compose_sms`.
"""

import sys
from typing import TYPE_CHECKING

from viva_core.api.routers import compose as _moved

if TYPE_CHECKING:
    from viva_core.api.routers.compose import *  # noqa: F403

sys.modules[__name__] = _moved
