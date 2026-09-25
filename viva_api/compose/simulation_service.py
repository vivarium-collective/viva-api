"""Moved to :mod:`viva_core.compose.simulation_service_hpc` (core split, ``docs/plan-core.md`` §4b, U2b-2).

The SLURM compose service is core's, now that a SLURM site is an end goal (UConn) and a SLURM
cluster can be run in Docker for its tests. This name stays importable, and at runtime it IS the
new module -- the same object -- so patching an attribute through this path patches the one real
attribute. New code should import from the new path.

What did NOT move, because it is SMS's: the ``v2ecoli`` run mode, which left first as the
``ContainerRun`` hook ``viva_api.simulation.compose_run_command.v2ecoli_run_command`` (U2b-1).
"""

import sys
from typing import TYPE_CHECKING

from viva_core.compose import simulation_service_hpc as _moved

if TYPE_CHECKING:
    from viva_core.compose.simulation_service_hpc import *  # noqa: F403

sys.modules[__name__] = _moved
