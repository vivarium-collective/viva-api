## Path A — local preview (the loom Explorer, Model tab cards — what Eran's PRs today were about - env_worker)

This is what renders a composite's wiring for you to look at. It never touches AWS Batch and never runs composite.run() — it only builds the document.

Workbench UI (loom pop-out / Model tab card)
→ GET /api/composite-resolve  (or /api/composite-state)
→ composite_state_views.composite_state_via_subprocess()
→ EnvWorkerPool.call(ws_root, "resolve_composite_state", {ref, overrides})
— an RPC over a socket to a SEPARATE env-worker process, not the
workbench's own Python process
→ env_worker.py::_resolve_composite_state()   ← THE REAL ENTRYPOINT
- imports the workspace's own package (so its @composite_generators register)
     - discover_generators() populates process_bigraph's own _REGISTRY
     - entry = _REGISTRY.get(ref)
     - build_generator(entry, overrides)  → calls the actual decorated
   function (baseline(), lineage_ray_batch(), etc.)
→ document serialized back over the socket, rendered in the loom

Who calls it: the env worker, running on the workspace's own interpreter (not the workbench's) — deliberate, since a workspace like v2ecoli pins a Python the workbench process itself
can't run. This is the same mechanism I called directly by hand yesterday to reproduce Eran's antibiotic-cocktail document — the env worker is just doing programmatically what I did
manually.

## Path B — a real dispatch (my item 110 button, or "Run current spec" - NOT the pbg-native design: instead it's known as the "chain-dispatch")

This is what actually spends money and runs a simulation. Composite resolution happens in a completely different place — inside the dispatched AWS Batch container itself, not in the
workbench, not in viva-api's own API server.

Workbench UI → POST /api/remote-run-submit
→ SmsApiClient.run_simulation(extra_params=...)
→ real HTTP POST to viva-api's POST /api/v1/simulations
→ viva-api routes on payload shape (chain-dispatch vs. multi_node_dispatch)
→ viva-api builds the REAL container command and submits a real AWS Batch job:
cd /app/v2ecoli && aws s3 cp <runner-s3-uri> /tmp/run_pbg.py
&& python /tmp/run_pbg.py --composite-id <id> --overrides '<json>' -n <steps>
→ AWS Batch schedules + runs the container (forms a Ray cluster first, for MNP)
→ INSIDE that container: run_pbg.py's _resolve_document()   ← THE REAL ENTRYPOINT
- composite_spec.get(composite_id)   (same registry as Path A)
     - apply_core_extensions(spec, core)  (e.g. registers the ray: protocol)
     - spec.to_document(overrides=overrides, core=core)
   → calls the SAME decorated function Path A calls
→ Composite(document, core=core)
→ composite.run(steps)        ← the simulation actually executes here
→ output streams to S3 (out_dir); on completion, JobScheduler auto-triggers analysis

Who calls it: run_pbg.py's own _resolve_document(), executing inside the remote container, at simulation run-time — not the workbench, not viva-api's API server process. Both of those are
pure orchestration: they decide which composite_id + params to request and where to run it, but neither one ever imports process_bigraph.composite_generator itself.

The one thing both paths share

Same registry (process_bigraph.composite_spec/composite_generator._REGISTRY), same decorated builder function, same apply_core_extensions/to_document call shape. The only real difference
is where that call happens and what happens after: Path A stops at a JSON document for display; Path B goes on to actually construct and run the Composite. That's also exactly why my own
manual reproduction yesterday (calling baseline() directly) was a faithful stand-in for both — same underlying call, just made from my own shell instead of an env worker or a Batch
container.

## Path C - pbg-native dispatch:

Path C (pbg-native multi-node dispatch): atlantis composite run (or pbg-dispatch.sh) 
→ POST /api/v1/simulations with extra_params.multi_node_dispatch={composite_id, num_nodes, params, steps} 
→ SimulationServiceRay's real fork point (simulation_service_ray.py:1448, checked before chain-dispatch's own composite is None and n_generations > 1 fork specifically to stop a
  multi-node request from silently misrouting) → _submit_multi_node_composite() (line 1685) submits two real AWS Batch jobs via _submit_mnp — ParCa (1 node) first, then the composite job (N
  nodes) gated depends_on=[parca_job_id] — → the composite job's container command, built by _multi_node_composite_command() (line 1643): stages run_pbg.py fresh from S3, sets
  RAY_SHARDS_DEFAULT (real per-node vCPUs × num_nodes), runs python /tmp/run_pbg.py --composite-id v2ecoli.composites.lineage_ray_batch --overrides '{...}' -n <steps> →
  sms-cdk/scripts/ray-batch-entrypoint.sh forms the real multi-node Ray cluster across all N nodes and exports RAY_ADDRESS before that command ever runs → INSIDE the container,
  run_pbg.py::_resolve_document() is the entrypoint, same as Path B — composite_spec.get(composite_id) → apply_core_extensions() fires lineage_ray_batch's own registered
  core_extensions=[register_ray_lineage], registering LineageProcess for the ray: protocol on core before the document is built → spec.to_document() calls lineage_ray_batch(core=core,
  **overrides) directly, which runs prewarm_lineage_pool(core, n_workers) before build_lineage_ray_batch_document() — sizing the actor pool before any ray: address in the resulting document
  can resolve — then returns the document → Composite(document, core=core) creates each ray:-addressed LineageProcess actor → composite.run(steps) — the real simulation executes here, each
  actor running its own internal composite independently (_build_generation() calling the real baseline() function every generation, same function chain-dispatch uses), output streaming
  natively to S3 from inside the actor. JobScheduler._advance_multi_node_job auto-triggers post-completion analysis on completion — the same generic mechanism colony's own MNP jobs use, not
  lineage_ray_batch-specific.