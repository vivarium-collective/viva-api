Composite Dispatch (``atlantis composite run``)
==================================================

CLI user guide and reproducible tutorial for the process-bigraph-native
multi-node composite dispatch mechanism. For the full design and internals,
see :doc:`/architecture/pbg-native-composite-dispatch`.

.. contents:: On this page
   :local:
   :depth: 2

What ``atlantis composite run`` does
---------------------------------------

Submits a real process-bigraph composite (by default,
``v2ecoli.composites.lineage_ray_batch`` — N independent whole-cell lineages,
each addressed via process-bigraph's own ``ray:`` protocol) as one AWS Batch
multi-node-parallel job. This is a different dispatch mechanism than
``atlantis simulation run`` (chain-dispatch — N × G independent, chained
single-node jobs) and different again from ``atlantis compose run`` (OMEX/
PBG/SBML file upload). See :doc:`/architecture/pbg-native-composite-dispatch`
for how the three relate.

Prerequisites
--------------

- A built simulator (``atlantis simulator latest --repo-url ... --branch
  ...``) whose commit is what gets dispatched.
- A tunnel to the target environment, for GovCloud/test-VPC deployments (see
  :doc:`running-a-campaign` Step 1b).

Quickstart
-----------

.. code-block:: bash

   atlantis composite run my-first-run 109 --seeds 2 --generations 1 --num-nodes 2

This dispatches ``v2ecoli.composites.lineage_ray_batch`` (the default
``--composite-id``) with 2 independent seed-lineages, 1 generation each,
across a real 2-node AWS Batch MNP job.

Command reference
-------------------

.. code-block:: text

   atlantis composite run EXPERIMENT_ID SIMULATOR_ID [OPTIONS]

**Positional arguments**

``experiment_id``
    Unique experiment identifier.

``simulator_id``
    Database ID of the simulator to use.

**Dispatch-level options** (size the real infrastructure, not the composite)

``--composite-id`` (default: ``v2ecoli.composites.lineage_ray_batch``)
    Any id resolvable by ``process_bigraph.composite_spec.get()`` — this
    command is not hardcoded to one composite.

``--num-nodes`` (default: ``2``)
    Real AWS Batch MNP nodes to request.

``--steps`` (default: ``36000``)
    Total simulated seconds requested for the whole composite run.

**Named convenience options** (``lineage_ray_batch``-shaped; each is a
shortcut for a key inside ``--params``, see below)

``--seeds``, ``--generations``, ``--base-seed``, ``--cache-dir``,
``--out-dir``, ``--emitter``, ``--n-workers``, ``--max-duration-per-gen``,
``--time-step``, ``--media``
    Every one of these maps directly onto ``lineage_ray_batch``'s own real
    ``@composite_generator`` parameters. Omitted flags are omitted from the
    request entirely (the composite's own defaults apply server-side), not
    sent as ``null``.

**Generic escape hatch** (works for *any* ``--composite-id``, not just
``lineage_ray_batch``)

``--params``
    A raw JSON object, merged into the dispatch's ``params`` on top of
    whatever the named options above produced — an explicit key in
    ``--params`` always wins on a collision. This is the fully generic path:
    a future composite with a different parameter shape needs nothing from
    this command but this one flag.

    .. code-block:: bash

       atlantis composite run my-run 109 \\
         --composite-id v2ecoli.composites.some_future_composite \\
         --params '{"n_seeds": 100, "n_generations": 10, "some_new_field": "value"}'

**Common options**

``--description``, ``--tag`` (repeatable), ``--poll``, ``--base-url``
    Same conventions as ``atlantis simulation run``.

Tutorial: a full, reproducible run
--------------------------------------

This walks through exactly the sequence used to verify this mechanism end to
end (ParCa → simulation → automatic post-completion analysis), reproducible
against any environment you have access to.

**Step 1 — build a simulator**

.. code-block:: bash

   atlantis simulator latest --repo-url https://github.com/CovertLabEcoli/sms-ecoli --branch main

Note the returned ``database_id`` — this is your ``SIMULATOR_ID`` below.

**Step 2 — dispatch, leaving ``out_dir`` unset**

.. important::

   Leave ``--out-dir`` unset for this tutorial. The automatic post-completion
   analysis job always reads from the deployment-standard results location —
   if you set a custom ``out_dir``, the real data lands somewhere the
   analysis never looks (see
   :doc:`/architecture/pbg-native-composite-dispatch`'s "Automatic
   post-completion analysis" section). This is exactly the scenario
   ``scripts/dispatch/verify-pbg-dispatch.sh`` in the ``ecosystem`` workspace
   exists to pin down.

.. code-block:: bash

   atlantis composite run pbg-native-tutorial <SIMULATOR_ID> \\
     --seeds 2 --generations 4 --num-nodes 2

This is a real, if modest, dispatch — each generation runs until either a
real cell division or the ``--max-duration-per-gen`` cap (3600s by default),
so a 4-generation run can take on the order of hours, not minutes.

**Step 3 — track progress**

.. code-block:: bash

   atlantis simulation status <DATABASE_ID>

**Step 4 — inspect the real output once complete**

.. code-block:: bash

   atlantis simulation outputs <DATABASE_ID> --dest ./debug

The automatic analysis job's own output (a small HTML/JSON summary) lands
under ``analyses/`` in the same experiment's S3 prefix — confirm it found
real history rather than reporting "No in-memory emitter history was
captured for this run" (the signature of the ``out_dir`` mismatch above).

Choosing between dispatch mechanisms
----------------------------------------

.. list-table::
   :widths: 20 40 40
   :header-rows: 1

   * - Mechanism
     - Command
     - Use when
   * - pbg-native composite
     - ``atlantis composite run``
     - You want the dispatch shape to be visible in the composite's own
       document structure (no external orchestrator), or you're targeting a
       composite other than ``ecoli_baseline``.
   * - chain-dispatch
     - ``atlantis simulation run``
     - You need the mechanism proven at the largest real production scale
       this ecosystem has run.
   * - compose (file upload)
     - ``atlantis compose run``
     - You have a standalone OMEX/PBG/SBML file to run, independent of a
       registered ``composite_id``.

See also
---------

- :doc:`/architecture/pbg-native-composite-dispatch` — full design reference.
- :doc:`running-a-campaign` — the chain-dispatch equivalent guide.
- :doc:`cli-reference` — the full ``atlantis`` command reference.
