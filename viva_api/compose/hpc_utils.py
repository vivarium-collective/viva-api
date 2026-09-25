"""Moved to :mod:`viva_core.compose.hpc_paths` (core split, ``docs/plan-core.md`` §4b, U2b-2).

The remote paths the SLURM compose service writes to, derived from core's settings. This name
stays importable, and at runtime it IS the new module -- the same object. New code should import
from the new path.
"""

import sys
from typing import TYPE_CHECKING

from viva_core.compose import hpc_paths as _moved

if TYPE_CHECKING:
    from viva_core.compose.hpc_paths import *  # noqa: F403

sys.modules[__name__] = _moved
