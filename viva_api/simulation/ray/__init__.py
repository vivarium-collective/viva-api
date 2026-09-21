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
  queue choice, this application's env entries) over ``viva_core.backends.batch``. Composed:
  one instance lives on the service as ``service.batch``. Consumers take the narrowest of its
  two Protocols, ``ContainerSubmitter`` and ``MnpSubmitter``, not the class.

Three concerns are composed SERVICES: each is a common capability that works one way
whatever the dispatch mechanism, and each is handed the one collaborator it needs:

* :mod:`.build` -- ``RayImageBuilder(local_task_service)``: build a simulator image.
* :mod:`.tasks` -- ``RayTaskService(batch, latest_commit=, results_uri=)``: run a script in the
  image, follow it, read its logs. ``TaskBatch`` (a ``ContainerSubmitter`` that can also say what
  became of a job) is the whole of what it asks of the Batch layer.
* :mod:`.parca` -- ``RayParcaService(batch)``: the three ParCa cache jobs (a commit's cache,
  a new-gene cache, a variant cache). Submitting ParCa as part of a RUN is not here: it is a
  container job in two mechanisms and an MNP job in two others, so it stays with each.

Dispatch mechanisms are STRATEGY objects, each handed the submitter it needs and nothing else:

* :mod:`.mbp_tracked` -- ``MbpTrackedStrategy(batch)``: one container job running the image's own
  ``run_mbp_tracked.py``, behind a ParCa job unless a staged variant cache is named.
* :mod:`.nextflow` -- ``NextflowStrategy(batch, k8s, stage_runner=)``: a Nextflow head as a K8s Job that
  submits its own Batch tasks; and the reap of a cancelled campaign's tasks.
* :mod:`.ensemble` -- ``EnsembleStrategy(batch, stage_runner=)``: ParCa as a one-node MNP job, then the
  simulation ensemble as an N-node MNP job. It was the router's fall-through tail, not a method.
* :mod:`.multi_node` -- ``MultiNodeCompositeStrategy(batch, stage_runner=)``: one composite on Ray actors
  across the nodes of one MNP job, and the analysis that follows it. The one place Ray is literally
  what runs.
* :mod:`.runner_env` -- leaf constants: the environment every ``run_pbg.py`` invocation runs under.
* :mod:`.run_records` -- ``record_run_with_companions``: a function the mechanisms share, to write
  down the jobs they submit ahead of the one they return.

``SimulationServiceRay`` inherits nothing from this package: it composes all of it.
"""
