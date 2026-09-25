"""Moved to :mod:`viva_core.contrib.sysbio.biomodel_documents` (core split, ``docs/plan-core.md`` P3d-4d-2a).

This name stays importable, and at runtime it IS the new module -- the same object -- so
patching an attribute through this path patches the one real attribute, and ``isinstance``
checks agree across both names. New code should import from the new path.
"""

import sys
from typing import TYPE_CHECKING

from viva_core.contrib.sysbio import biomodel_documents as _moved

if TYPE_CHECKING:
    from viva_core.contrib.sysbio.biomodel_documents import *  # noqa: F403

sys.modules[__name__] = _moved
