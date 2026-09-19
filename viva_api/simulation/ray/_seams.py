"""The two names every Ray-service module reaches the outside world through.

    from viva_api.simulation.ray import _seams
    _seams.get_settings().batch_region
    _seams.boto3.client("batch", ...)

Why a seam, and why here. The tests isolate this code by patching NAMES -- ``get_settings``
205 times and ``boto3`` 93 times -- and a patch replaces a name in ONE module. While all the
code sat in ``simulation_service_ray.py`` that was the right module. The moment a function
moves to another file it looks the name up THERE, the patch no longer reaches it, and it
runs with the developer's real settings and a real AWS client while its test still passes.

So the names live here, every Ray-service module reads them THROUGH this module at call time
(``_seams.boto3``, never ``from ... import boto3``), and the tests patch them here. One
place to patch, and it keeps working wherever the code goes.

Do not import ``boto3`` or ``get_settings`` directly in a Ray-service module:
``tests/simulation/test_ray_seams.py`` fails if one does.

This contains the smell; it does not cure it. P2.2 replaces module-global lookups with
injected dependencies, and the tests then pass fakes instead of patching.
"""

import boto3

from viva_api.config import get_settings

__all__ = ["boto3", "get_settings"]
