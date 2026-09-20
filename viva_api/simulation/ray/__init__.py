"""The Ray / AWS Batch simulation service, being carved out of ``simulation_service_ray.py``.

That module grew into one class of 5,019 lines holding eleven concerns and five dispatch
mechanisms (``docs/plan-core.md`` P2). Its pieces land here, one concern per PR, each in the
shape its ROLE calls for: a specification is a module of pure functions, a common capability
is a composed service, a dispatch mechanism will be a strategy object. Nothing is a mixin.

* :mod:`._seams` -- the two names (settings, boto3) every module here reads the outside
  world through; the prerequisite that makes moving anything safe.
* :mod:`.image_paths` -- absolute paths inside the simulator image. A leaf: no imports.
* :mod:`.config_interpretation` -- pure functions from a simulation config to dispatch
  parameters.
* :mod:`.analysis_spec` -- pure functions: which analysis modules a simulation asks for, and
  the memory class that analysis needs.
* :mod:`.parca_spec` -- pure functions: where ParCa caches live in S3 and the commands that
  build them. Every dispatch mechanism needs these two things from ParCa and nothing else.
* :mod:`.batch_layer` -- ``RayBatchLayer``, the service's half of the Batch seam (settings,
  queue choice, this application's env entries) over ``viva_core.backends.batch``. Today the
  base class of ``SimulationServiceRay``; to be composed as ``service.batch``.

Three concerns are composed SERVICES: each is a common capability that works one way
whatever the dispatch mechanism, and each is handed the one collaborator it needs:

* :mod:`.build` -- ``RayImageBuilder(local_task_service)``: build a simulator image.
* :mod:`.tasks` -- ``RayTaskService(dispatch)``: run a script in the image, follow it, read its
  logs. ``TaskDispatch`` is the Protocol of what it asks of whatever runs container jobs.
* :mod:`.parca` -- ``RayParcaService(dispatch)``: the three ParCa cache jobs (a commit's cache,
  a new-gene cache, a variant cache). Submitting ParCa as part of a RUN is not here: it is a
  container job in two mechanisms and an MNP job in two others, so it stays with each.

``SimulationServiceRay`` inherits the Batch layer and composes the services.
"""
