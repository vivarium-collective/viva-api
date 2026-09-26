"""The dataset registry: the files runs actually wrote, as rows (``docs/plan-core.md`` P4a-2).

A dataset row is never pre-created. It is born from a run's trace -- one ``artifact.written``
event per consumable file set, registered by :mod:`viva_core.datasets.registry` -- or from a
reconciliation walk of the storage the run wrote to. Core owns the registration rules; what it
does not know it asks the application through three Protocols:

* :class:`~viva_core.datasets.registry.OwnerResolver` -- who produced an artifact (the
  application's tables: a simulation, an analysis, a parameter set);
* :class:`~viva_core.datasets.models.DatasetStore` -- where rows live;
* an ``ArtifactClassifier`` and a ``WalkSource`` for the walk (the next slice).

Vocabulary: a dataset has a ``uri``, a ``kind``, an ``origin`` (``event`` or ``walk``), an
owner ``(owner_kind, owner_id)`` and an open ``attributes`` map carrying its coordinate.
"""
