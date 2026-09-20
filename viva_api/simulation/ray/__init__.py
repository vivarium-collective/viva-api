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
* :mod:`.parca` -- ``RayParcaMixin``: ParCa and the caches a simulation stages from. The dispatch
  mixins inherit it.

Two concerns are composed SERVICES rather than mixins, because each needs one collaborator
and is headed for ``viva_core``, where a class that inherits an SMS class cannot go:

* :mod:`.build` -- ``RayImageBuilder(local_task_service)``: build a simulator image.
* :mod:`.tasks` -- ``RayTaskService(dispatch)``: run a script in the image, follow it, read its
  logs. ``TaskDispatch`` is the Protocol of what it asks of whatever runs container jobs.

``SimulationServiceRay`` inherits the mixins and composes the services.
"""
