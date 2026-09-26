- **2026-09-26** — **P4d step 1: the viva-api side of the live `artifact.written` feed — `sim_id` pinned in simulation baggage; the registry reads `lineage_seed` as `seed`.**
  **Found, not changed: `sim_id` was already there.** The design draft's gap G2 said simulation
  dispatches never put `sim_id` into `PBG_TRACE_BAGGAGE`. They do: all seven `with_events_env` calls
  that dispatch a simulation — `nextflow.submit`, `mbp_tracked.submit`, `multi_node.submit`,
  `ensemble.submit` (ParCa and sim), `chain.submit_chain_dispatch_job`, the scheduler's chain
  lineages, and the in-run gather `chain._submit_analysis_job` — pass `sim_id=<Simulation.database_id>`,
  a non-optional `int`, so it is always known at env-build time. What was missing was a guard: the
  #641 scan checks that `with_events_env` is called, not what it is told. A new AST test pins a
  non-`None` `sim_id=` on every such call in `viva_api/simulation/dispatch/` and `job_scheduler.py`
  (mutation-checked: deleting Nextflow's keyword fails it). Standalone analyses keep their own
  identity (`analysis_id`; the legacy K8s re-analysis names only `experiment_id`) and are untouched.
  On ingest, core keeps `sim_id` in the event's baggage map (`BAGGAGE_IDENTITY_KEYS`) and the
  application stores it under `tags._baggage`; `SmsOwnerResolver` still resolves a simulation run's
  files from the run row, so `sim_id` is attribution the row can be checked against, not yet a
  resolver input.
  **Changed: `lineage_seed` in attributes.** v2ecoli's history partitions spell the seed
  `lineage_seed`, as the baggage does. `viva_core/datasets/registry.py::_coordinate` now reads an
  axis's own key, then its alias (`seed` ← `lineage_seed`), then the promoted baggage — presence, not
  truthiness, so an explicit `null` still suppresses the fallback exactly as before. The producer's
  key stays in `attributes` beside `seed`; the baggage path is unchanged. This is #655's proposal for
  Q-P2, still open with Eran; if the helper renames instead, nothing here breaks (`seed` is read first).
  **Proof:** `tests/core/test_datasets_registry.py` (+2: a `lineage_seed` event registers with
  `seed` set; `seed` beats `lineage_seed`, which beats baggage), `tests/simulation/test_dispatch_events_identity.py`
  (+18, one per simulation dispatch module). `docs/OBSERVABILITY.md` §2 and §4 say what the dispatcher injects and the alias.
  Next (P4d step 2) is v2ecoli's `artifact()` and its call sites, which wait on Eran's review.
