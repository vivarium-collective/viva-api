Process-Bigraph-Native Composite Dispatch
==========================================

This is the design reference for the **pbg-native multi-node composite dispatch**
mechanism (``multi_node_dispatch``, backlog items 101/109) — the mechanism behind
``atlantis composite run``. It is written to be self-contained: no prior context
beyond general process-bigraph familiarity is assumed.

.. contents:: On this page
   :local:
   :depth: 2

What this is
-------------

A single process-bigraph composite document that wires N independent
components directly into its own state tree, each addressed via
process-bigraph's own ``ray:`` remote-address protocol. One dispatch, one AWS
Batch multi-node-parallel (MNP) job, N real ``ray:``-distributed actors. No
external orchestration script decides "run node 5 now" — process-bigraph's
own scheduler runs every ``ray:``-addressed node exactly the way it would run
a local one, and the ``ray:`` protocol transparently proxies each call to the
actor holding that node's real state.

The flagship, currently-registered composite for this mechanism is
``v2ecoli.composites.lineage_ray_batch`` — N real ``ray:LineageProcess``
nodes, one whole-cell lineage per seed. The mechanism itself is generic:
``composite_id`` is any id resolvable by
``process_bigraph.composite_spec.get()``, not hardcoded to this one composite.

Why this exists
-----------------

The alternative mechanism this ecosystem also ships, **chain-dispatch**, is
real and proven at large production scale, but it is not process-bigraph
native: it is an external AWS-Batch-job-dependency orchestrator that happens
to run process-bigraph composites as its payload — one standalone Batch job
per (seed, generation) pair, chained by an application-level scheduler.

process-bigraph already has a native protocol for distributing work across
real nodes (``ray:``) — the question this mechanism answers is whether
dispatch needs a bespoke external orchestrator at all, or whether the
``ray:`` protocol can carry that responsibility natively. Investigated
directly against primary source (``process_bigraph/protocols/ray.py``,
``process_bigraph/emitter.py``): the ``ray:`` protocol already unifies
distributed state correctly and natively. ``RayShadowProcess.update()``
returns via a plain synchronous ``ray.get()``, exactly like a local process —
the outer composite's state tree comes back unified with zero special-case
code anywhere in the composite layer.

Architecture
-------------

**One composite, N top-level nodes, one dispatch.**

.. code-block:: text

   composite document
   ├── lineages: {}                      (unused namespace key)
   ├── lineage_0000: ray:LineageProcess  (seed 0's own full multi-generation lineage)
   ├── lineage_0001: ray:LineageProcess  (seed 1's own full multi-generation lineage)
   ├── ...
   └── lineage_NNNN: ray:LineageProcess  (seed N's own full multi-generation lineage)

Each ``lineage_XXXX`` node is a real, independent state-tree entry — not a
Python loop variable, not an item in an internal list some Step iterates
over. process-bigraph's own scheduler discovers and runs each one exactly as
it would any other process; the ``ray:`` prefix on ``address`` is what tells
process-bigraph's core to proxy this node's ``update()`` calls to a remote
Ray actor instead of calling a local Python object directly.

The shared entrypoint
-----------------------

Both mechanisms in this ecosystem — pbg-native dispatch AND chain-dispatch —
run through the exact same generic script: ``viva_api/compose/run_pbg.py``.
It is entirely composite-agnostic:

.. code-block:: text

   python run_pbg.py --composite-id <id> --overrides '<json>' -n <steps>

``<id>`` is any id resolvable by ``process_bigraph.composite_spec.get()`` —
the single registry every ``@composite_generator`` decorator registers into.
The script resolves the composite spec by id, applies any ``core_extensions``
the spec declares (for ``lineage_ray_batch`` this registers the ``ray:``
protocol and its process classes on the core), calls
``spec.to_document(overrides=overrides, core=core)`` to build the real
document, registers process-bigraph's remote-address protocols on the core,
and constructs and runs the ``Composite``.

Pool sizing
------------

``process_bigraph.protocols.ray.RayProtocolRuntime`` sizes its actor pool for
a ``(class_name, config)`` key on first creation only. ``Composite.__init__``
resolves every ``ray:`` address as it builds the state tree — if nothing
sizes the pool first, it gets created with the protocol's own bare default
(``os.cpu_count()`` on whichever single node happens to be the Ray driver,
which on this ecosystem's AWS Batch MNP topology is the head node
specifically — a number with zero relationship to real cluster capacity
elsewhere).

The composite's own ``prewarm_lineage_pool(core, n_workers)`` runs before any
``ray:`` address is resolved. ``n_workers=None`` (the composite's own
default) falls through to the ``RAY_SHARDS_DEFAULT`` environment variable,
which viva-api's dispatch code computes correctly from real per-node vCPUs ×
real node count for every multi-node dispatch. Override ``n_workers``
explicitly only to deliberately cap concurrency below the cluster's real
capacity.

Output streams and ``out_dir``
--------------------------------

``LineageProcess`` writes two separate output streams per generation:

- **Parquet** — the real, analysis-critical stream. Written by a Step wired
  inside the inner biological composite each lineage builds.
- **XArray/zarr** — a coarser, dashboard-oriented snapshot, updated once per
  tick within a generation.

Both already natively support direct S3 output. Neither needed new code for
this mechanism — both already read the composite's own ``out_dir`` config
value. **The entire "how does data get out" question resolves to one
dispatch-time parameter**: pass ``out_dir="s3://..."`` at dispatch time and
both streams write there directly — no result-aggregation layer, no gather
step, nothing new crossing the ``ray:`` actor boundary. Nothing crosses that
boundary that wasn't already crossing it safely before — a real, measured RAM
leak in an earlier, unrelated design (one Emitter observing a whole
composite's deep-copied state) was seriously evaluated and found not to
apply here for exactly this reason.

Real dispatch mechanics
-------------------------

Submission (``SimulationServiceRay._submit_multi_node_composite``) takes a
caller-supplied ``composite_id`` plus a ``num_nodes`` count plus arbitrary
``params`` — threaded straight through to ``--overrides`` with zero
composite-specific code. It submits ParCa first (1 node, gated the same way
every dispatch shape is), then the real composite job (N nodes), the second
gated on the first.

The head-node command is:

.. code-block:: text

   cd /app/v2ecoli && aws s3 cp <runner-s3-uri> /tmp/run_pbg.py \
     && PBG_RESULTS_DIR=... PBG_CORE_BUILDER=v2ecoli.core:build_core PYTHONPATH=/app/v2ecoli \
        RAY_SHARDS_DEFAULT=<real per-node vCPUs x num_nodes> \
        python /tmp/run_pbg.py --composite-id v2ecoli.composites.lineage_ray_batch \
        --overrides '{"n_seeds": ..., "n_generations": ..., "out_dir": "s3://...", ...}' -n <steps>

No new Ray multi-node "pre-connect" code is needed anywhere in this chain:
the entrypoint already exports ``RAY_ADDRESS`` on the head node before this
command runs, and Ray's own bare ``ray.init(ignore_reinit_error=True)``
already respects ``RAY_ADDRESS`` from the environment.

Infrastructure: the same MNP job definition/queue the colony composite
already used (backlog item 88) — no new CDK job definition, no new compute
environment.

Automatic post-completion analysis
-------------------------------------

Every multi-node composite dispatch (not just this one, and not limited to
any particular ``composite_id``) automatically gets a real post-completion
analysis job once the MNP job reaches ``COMPLETED``
(``JobScheduler._advance_multi_node_job`` →
``SimulationServiceRay.submit_multi_node_analysis``, live since backlog item
88). This is real, generic, and unconditional.

**Important interaction, confirmed live**: this analysis job always reads
from the deployment-standard results location
(``_results_s3_uri(experiment_id)``) — it has no awareness of a caller-
supplied custom ``out_dir``. If a dispatch uses a custom ``out_dir``, its
real data lands somewhere the auto-analysis never looks, and the analysis
will report ``"No in-memory emitter history was captured for this run"`` even
though the dispatch itself succeeded and produced real output. **Leave
``out_dir`` unset (the deployment-standard default) whenever you want the
automatic analysis to find real data.**

Known, real, currently-open gaps
------------------------------------

- **Result aggregation across N nodes at large scale** is not yet built as a
  first-class composite-level mechanism for every registered composite — a
  gather-Step design was evaluated and found unsafe against the real AWS
  Batch MNP entrypoint's own sync semantics (no shared filesystem across
  nodes; the final, authoritative per-node sync to S3 only starts once a
  worker receives SIGTERM). A corrected design (per-node self-sync mirroring
  chain-dispatch's own already-proven S3 handoff mechanism) is the
  recommended direction, not yet built.
- ``lineage_ray_batch``'s own registered ``@composite_generator`` parameters
  do **not** currently include ``variants``/``injected_processes``/
  ``config_overrides`` — even though the underlying document-builder function
  accepts all three. Passing any of them today raises a real
  ``KeyError: unknown override(s): [...]`` at dispatch time
  (``process_bigraph.composite_spec.CompositeSpec._merged_params``). This is
  a real, currently-open gap, not a documentation omission.
- This mechanism has been proven at 2-node and 16-node/100-seed scale; it has
  not yet been run at the multi-hundred/thousand-seed scale chain-dispatch
  has proven.

See also
---------

- :doc:`/guides/composite-dispatch` — the CLI user guide and reproducible
  tutorial for this mechanism.
- :doc:`/guides/running-a-campaign` — the chain-dispatch equivalent, for
  comparison.
