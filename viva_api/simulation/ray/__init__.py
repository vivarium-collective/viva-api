"""The Ray / AWS Batch simulation service, being carved out of ``simulation_service_ray.py``.

That module grew into one class of 4,305 lines holding eleven concerns and four dispatch
mechanisms (``docs/plan-core.md`` P2). Its pieces land here, one concern per PR:

* :mod:`._seams` -- the two names (settings, boto3) every module here reads the outside
  world through; the prerequisite that makes moving anything safe.
* :mod:`.image_paths` -- absolute paths inside the simulator image. A leaf: no imports.
* :mod:`.config_interpretation` -- pure functions from a simulation config to dispatch
  parameters.
* :mod:`.batch_layer` -- ``RayBatchLayer``, the service's half of the Batch seam (settings,
  queue choice, this application's env entries) over ``viva_core.backends.batch``. The base
  class every mixin below inherits.
* :mod:`.tasks` -- ``RayTasksMixin``: run a script in the image, follow it, read its logs.
* :mod:`.build` -- ``RayBuildMixin``: build a simulator image (a DooD build on Batch, run as
  a LOCAL task).

``SimulationServiceRay`` inherits the mixins, so callers and tests see one class, as before.
"""
